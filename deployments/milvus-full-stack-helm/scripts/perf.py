#!/usr/bin/env python3
"""Bounded Milvus write benchmark with correctness checks and diagnostic evidence."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import random
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def utc(timestamp=None):
    return datetime.fromtimestamp(time.time() if timestamp is None else timestamp, timezone.utc).isoformat()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def http(base, path, payload=None, timeout=5, headers=None):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    req = urllib.request.Request(base + path, headers={"Content-Type": "application/json", **(headers or {})},
                                 data=None if payload is None else json.dumps(payload).encode())
    try:
        with opener.open(req, timeout=timeout) as response:
            result = json.load(response)
    except urllib.error.HTTPError as error:
        try:
            detail = error.read(1000).decode(errors="replace")
        finally:
            error.close()
        raise RuntimeError(f"HTTP {error.code}: {detail}") from error
    if isinstance(result, dict) and "code" in result and result["code"] != 0:
        raise RuntimeError(f"Milvus code {result['code']}: {result.get('message', '')}")
    if isinstance(result, dict) and result.get("status") == "error":
        raise RuntimeError("Prometheus: " + result.get("error", "query failed"))
    return result


def distribution(values):
    values = sorted(values)
    if not values:
        return None
    return {"samples": len(values), "min": values[0], "mean": sum(values) / len(values),
            **{name: values[max(0, math.ceil(q * len(values)) - 1)]
               for name, q in (("p50", .50), ("p95", .95), ("p99", .99))}, "max": values[-1]}


def metric_queries(namespace, collection):
    # JSON string quoting keeps selectors valid without interpolating PromQL syntax.
    scope = 'namespace=' + json.dumps(namespace) + ',cluster="local",woodpecker_id="milvus"'
    exact = scope + ',collection_name=' + json.dumps(collection)
    def metric(name, extra=""):
        return name + "{" + scope + extra + "}"
    def p99(name, labels="pod", extra=""):
        return f"histogram_quantile(0.99, sum by (le,{labels}) (rate({metric(name, extra)}[1m])))"
    def resource(name, rate=False):
        selector = 'namespace=' + json.dumps(namespace) + ',cluster="local",pod=~"milvus-.*"'
        def value(extra):
            expr = name + "{" + selector + extra + "}"
            return "rate(" + expr + "[1m])" if rate else expr
        # Some local cAdvisor setups export only Pod cgroups, without container
        # labels. Prefer summed containers; fall back without double counting.
        return ('sum by (pod) (' + value(',container!="",container!="POD"') + ') or on (pod) max by (pod) (' + value(',container=""') + ')')
    return {
        "milvus_rows": "sum(milvus_proxy_insert_vectors_count{" + exact + "})",
        "client_appends": metric("woodpecker_client_append_requests_total"),
        "server_appends": metric("woodpecker_server_logstore_operations_total", ',operation=~"add_entry|add_entry_batch",status="success"'),
        "milvus_mutation_p99_ms": p99("milvus_proxy_mutation_latency_bucket", extra=',collection_name=' + json.dumps(collection)),
        "client_append_p99_ms": p99("woodpecker_client_append_latency_bucket"),
        "client_pending_appends": "sum by (pod) (" + metric("woodpecker_client_segment_handle_pending_append_ops") + ")",
        "server_flush_p99_ms": p99("woodpecker_server_file_flush_latency_bucket", "node_id"),
        "server_rpc_p99_seconds": p99("grpc_server_handling_seconds_bucket", "node_id", ',grpc_method=~"AddEntry|AddEntries"'),
        "server_disk_usage": metric("woodpecker_server_logstore_disk_usage_ratio"),
        "server_backpressure": metric("woodpecker_server_logstore_write_backpressure_state"),
        "object_store_p99_ms": p99("woodpecker_server_object_storage_operation_latency_bucket", "node_id,operation"),
        "object_store_errors_per_second": "sum by (node_id) (rate(" + metric("woodpecker_server_object_storage_operations_total", ',status="error"') + "[1m]))",
        "pod_cpu_cores": resource("container_cpu_usage_seconds_total", rate=True),
        "pod_memory_bytes": resource("container_memory_working_set_bytes"),
        "alerts": 'ALERTS{namespace=' + json.dumps(namespace) + ',alert_set="woodpecker-full-stack"}',
    }


class Benchmark:
    def __init__(self, args):
        self.args = args
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
        self.collection = "perf_" + self.run_id.replace("-", "_")
        self.out = args.output / self.run_id
        self.out.mkdir(parents=True)
        self.lock = threading.Lock()
        self.abort = threading.Event()
        self.observer_stop = threading.Event()
        self.sequence = 0
        self.collection_created = False
        self.records = []
        self.queries = metric_queries(args.namespace, self.collection)
        self.report = {"schema_version": 1, "run_id": self.run_id, "collection": self.collection,
                       "started_at": utc(), "config": vars(args).copy(), "failures": [],
                       "diagnostic_errors": [], "metrics": {}, "phases": {}}
        self.report["config"]["output"] = str(args.output)
        self.report["scope"] = "Milvus REST via configured endpoint; WP/resource metrics aggregate this deployment"
        self.trace = (self.out / "requests.jsonl").open("w")
        self.signals = (self.out / "signals.jsonl").open("w")

    def call(self, path, payload, trace_id=None, sampled=True):
        headers = {"traceparent": "00-" + trace_id + "-" + uuid.uuid4().hex[:16] + "-" + ("01" if sampled else "00")} if trace_id else None
        return http(self.args.milvus, "/v2/vectordb/" + path, payload,
                    self.args.request_timeout, headers=headers).get("data")

    def diagnostic_error(self, stage, error):
        with self.lock:
            self.report["diagnostic_errors"].append({"stage": stage, "at": utc(), "error": str(error)})

    def snapshot(self):
        result = {"at": utc(), "metrics": {}}
        for name, query in self.queries.items():
            try:
                reply = http(self.args.prometheus, "/api/v1/query?" + urllib.parse.urlencode({"query": query}))
                result["metrics"][name] = reply["data"]["result"]
            except Exception as error:
                self.diagnostic_error("metric:" + name, error)
        try:
            result["alerts"] = [a for a in http(self.args.alertmanager, "/api/v2/alerts")
                                if a["labels"].get("namespace") == self.args.namespace]
        except Exception as error:
            self.diagnostic_error("alertmanager", error)
        with self.lock:
            self.signals.write(json.dumps(result) + "\n")
            self.signals.flush()
        return result

    def observe(self):
        while not self.observer_stop.wait(self.args.sample_interval):
            self.snapshot()
            with self.lock:
                rows = sum(r["rows"] for r in self.records if r["ok"] and r["phase"] == "measure")
                errors = sum(not r["ok"] for r in self.records)
            print(json.dumps({"stage": "progress", "at": utc(), "acknowledged_measurement_rows": rows,
                              "failed_requests": errors}), flush=True)

    def phase(self, name, duration):
        started_at = utc()
        started = time.monotonic()
        deadline = started + duration
        scheduled = 0
        def worker(worker_id):
            nonlocal scheduled
            rng = random.Random(42 + worker_id)
            while not self.abort.is_set():
                with self.lock:
                    slot = started + scheduled / self.args.rps
                    if slot >= deadline or scheduled >= self.args.max_requests:
                        return
                    scheduled += 1
                    seq = self.sequence
                    self.sequence += 1
                if self.abort.wait(max(0, slot - time.monotonic())) or time.monotonic() >= deadline:
                    return
                generated = time.monotonic()
                first_id = seq * self.args.batch_size
                rows = [{"id": first_id + i, "vector": [rng.random() for _ in range(self.args.dimension)]}
                        for i in range(self.args.batch_size)]
                submitted = time.monotonic()
                record = {"request_id": self.run_id + ":" + str(seq), "phase": name, "worker": worker_id,
                          "at": utc(), "first_id": first_id, "rows": len(rows), "ok": False,
                          "generation_ms": (submitted - generated) * 1000,
                          "schedule_lag_ms": max(0, generated - slot) * 1000}
                trace_id = uuid.uuid4().hex
                sampled = int(trace_id[:16], 16) / (1 << 64) < self.args.trace_sample_fraction
                jaeger = os.getenv("JAEGER_URL", "http://127.0.0.1:" + os.getenv("JAEGER_PORT", "16686"))
                record.update(trace_id=trace_id, trace_sampled=sampled,
                              trace_url=jaeger + "/trace/" + trace_id if sampled else None)
                try:
                    result = self.call("entities/insert", {"collectionName": self.collection, "data": rows}, trace_id, sampled)
                    if result.get("insertCount") != len(rows):
                        raise RuntimeError("insertCount does not match submitted batch")
                    record["ok"] = True
                except Exception as error:
                    # Never silently retry writes: a timeout can have committed.
                    record["error"] = str(error)
                record["latency_ms"] = (time.monotonic() - submitted) * 1000
                record["finished_at"] = utc()
                with self.lock:
                    self.records.append(record)
                    self.trace.write(json.dumps(record) + "\n")
                    self.trace.flush()
                    if record["ok"] and not hasattr(self, "sample_vector"):
                        self.sample_vector = rows[0]["vector"]
                        self.sample_id = first_id
                    if sum(not r["ok"] for r in self.records) >= self.args.max_errors:
                        self.abort.set()
        with ThreadPoolExecutor(max_workers=self.args.concurrency) as pool:
            futures = [pool.submit(worker, i) for i in range(self.args.concurrency)]
            try:
                for future in futures:
                    future.result()
                if scheduled < self.args.max_requests:
                    self.abort.wait(max(0, deadline - time.monotonic()))
            except BaseException:
                self.abort.set()
                raise
        elapsed = time.monotonic() - started
        records = [r for r in self.records if r["phase"] == name]
        good = [r for r in records if r["ok"]]
        result = {"started_at": started_at, "finished_at": utc(), "elapsed_seconds": elapsed,
                  "requests": len(records), "successful_requests": len(good),
                  "failed_requests": len(records) - len(good), "acknowledged_rows": sum(r["rows"] for r in good),
                  "acknowledged_rows_per_second": sum(r["rows"] for r in good) / max(elapsed, .001),
                  "successful_requests_per_second": len(good) / max(elapsed, .001),
                  "success_latency_ms": distribution([r["latency_ms"] for r in good]),
                  "failed_latency_ms": distribution([r["latency_ms"] for r in records if not r["ok"]]),
                  "schedule_lag_ms": distribution([r["schedule_lag_ms"] for r in records]),
                  "generation_ms": distribution([r["generation_ms"] for r in records]),
                  "request_limit_reached": scheduled >= self.args.max_requests}
        self.report["phases"][name] = result
        print(json.dumps({"stage": name, **result}), flush=True)

    def verify(self):
        good = [r for r in self.records if r["ok"]]
        expected = sum(r["rows"] for r in good)
        if not good:
            raise RuntimeError("No acknowledged writes to verify")
        started = time.monotonic()
        result = self.call("entities/query", {"collectionName": self.collection, "filter": "id >= 0",
                           "outputFields": ["count(*)"], "consistencyLevel": "Strong"})
        count = int(result[0]["count(*)"])
        # On failed requests, acknowledged rows are a lower bound, since timeouts
        # may commit. Any failed request still fails the benchmark independently.
        if count < expected or (all(r["ok"] for r in self.records) and count != expected):
            raise RuntimeError(f"Strong read count {count}, acknowledged rows {expected}")
        sampled = good[::max(1, math.ceil(len(good) / 64))] + good[-1:]
        ids = sorted({r["first_id"] + offset for r in sampled for offset in (0, r["rows"] - 1)})
        result = self.call("entities/query", {"collectionName": self.collection, "filter": "id in " + json.dumps(ids),
                           "outputFields": ["id"], "limit": len(ids), "consistencyLevel": "Strong"})
        if {int(row["id"]) for row in result} != set(ids):
            raise RuntimeError("Some acknowledged sample IDs are not visible")
        result = self.call("entities/search", {"collectionName": self.collection, "data": [self.sample_vector],
                           "filter": "id == " + str(self.sample_id), "limit": 1,
                           "outputFields": ["id"], "consistencyLevel": "Strong"})
        if not result or int(result[0]["id"]) != self.sample_id:
            raise RuntimeError("Acknowledged sample is not searchable")
        self.report["correctness"] = {"acknowledged_rows": expected, "strong_read_count": count,
                                      "sample_ids_checked": len(ids), "search_passed": True,
                                      "verification_seconds": time.monotonic() - started,
                                      "scope": "Live visibility and sampled search; no crash/recovery durability test"}

    def kubectl(self, args, filename=None):
        command = ["kubectl", "--context", self.args.context, "-n", self.args.namespace] + args
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise RuntimeError(result.stderr[:2000])
        if filename:
            (self.out / filename).write_text(result.stdout)
        return result.stdout

    def collect_logs(self):
        try:
            pods = json.loads(self.kubectl(["get", "pods", "-o", "json"]))["items"]
            inventory = [{"name": p["metadata"]["name"], "uid": p["metadata"]["uid"],
                          "labels": p["metadata"].get("labels", {}), "status": p.get("status", {}),
                          "containers": [{k: c[k] for k in ("name", "image", "resources") if k in c}
                                         for c in p["spec"]["containers"]]} for p in pods]
            save(self.out / "pods.json", inventory)  # No Pod env, Secrets or Helm values.
            self.kubectl(["get", "events", "--sort-by=.lastTimestamp"], "events.txt")
            selected = [p for p in inventory if p["labels"].get("app.kubernetes.io/instance") == "milvus"
                        or p["labels"].get("app") in ("full-stack-etcd", "full-stack-minio")]
            if not selected:
                raise RuntimeError("No Milvus/Woodpecker/dependency Pods found for log collection")
            for pod in selected:
                for container in pod["containers"]:
                    for previous in (False, True):
                        if previous and not any(c.get("restartCount", 0) > 0 and c["name"] == container["name"]
                                                for c in pod["status"].get("containerStatuses", [])):
                            continue
                        filename = pod["name"] + "-" + container["name"] + ("-previous" if previous else "") + ".log"
                        args = ["logs", pod["name"], "-c", container["name"], "--timestamps",
                                "--since-time=" + self.report["started_at"], "--tail=2000", "--limit-bytes=2097152"]
                        if previous:
                            args.append("--previous")
                        try:
                            self.kubectl(args, filename)
                        except Exception as error:
                            self.diagnostic_error("logs:" + filename, error)
            self.report["log_pods"] = [p["name"] for p in selected]
        except Exception as error:
            self.diagnostic_error("kubernetes", error)

    def collect_ranges(self, start, end):
        for name, query in self.queries.items():
            try:
                params = urllib.parse.urlencode({"query": query, "start": start, "end": end, "step": 15})
                reply = http(self.args.prometheus, "/api/v1/query_range?" + params)
                save(self.out / ("metric-" + name + ".json"), {"query": query, **reply})
                finite = [float(v) for row in reply["data"]["result"] for _, v in row["values"]
                          if math.isfinite(float(v))]
                self.report["metrics"][name] = {"query": query, "finite_samples": len(finite),
                                                "max": max(finite) if finite else None}
            except Exception as error:
                self.diagnostic_error("range:" + name, error)

    def triage(self):
        findings = []
        def add(area, evidence, next_step):
            findings.append({"area": area, "evidence": evidence, "next_step": next_step})
        failed = [r for r in self.records if not r["ok"]]
        if failed:
            add("write requests", [{k: r[k] for k in ("request_id", "at", "error")} for r in failed[:5]],
                "Match request times and the collection ID in proxy, streaming node and Woodpecker logs; inspect downstream signals before assigning a cause.")
        for metric, threshold, area, next_step in (
            ("server_backpressure", 1, "disk admission", "Inspect R5, node disk usage, configured watermarks and compaction/object-store progress."),
            ("client_pending_appends", 5000, "client queue", "Inspect C3, append latency and downstream replica/flush pressure."),
            ("server_flush_p99_ms", 100, "local flush", "Compare per-node flush latency, CPU and disk pressure; inspect F4/F5 hold periods."),
            ("object_store_errors_per_second", 0, "object storage", "Inspect F7/F8, MinIO logs, credentials, connectivity and compaction progress.")):
            value = self.report["metrics"].get(metric, {}).get("max")
            if value is not None and value > threshold:
                add(area, {"signal": metric, "observed_max": value}, next_step)
        if self.report["coverage_gaps"] or self.report["diagnostic_errors"]:
            add("observability coverage", {"missing_finite_signals": self.report["coverage_gaps"],
                                           "collection_errors": len(self.report["diagnostic_errors"])},
                "Check targets, scrape labels, exported metric families and access to Prometheus/AM/Kubernetes. Missing data does not establish health.")
        self.report["triage"] = findings
        self.report["triage_scope"] = "Heuristic investigation leads; not proven root causes or proof that alert hold periods were met"
        measured = self.report["phases"].get("measure", {})
        latency = measured.get("success_latency_ms") or {}
        lines = ["# Write performance run " + self.run_id, "", "Result: **" + ("PASS" if self.report["passed"] else "FAIL") + "**", "",
                 "Collection: `" + self.collection + "` (retained). See `report.json` for collection metadata and all stage times.", "",
                 "| Measurement | Value |", "| --- | --- |",
                 f"| Acknowledged rows | {measured.get('acknowledged_rows', 0)} |",
                 f"| Rows/s | {measured.get('acknowledged_rows_per_second', 0):.2f} |",
                 f"| Failed requests | {measured.get('failed_requests', 0)} |"]
        for key in ("p50", "p95", "p99"):
            lines.append(f"| Insert {key} (ms) | {latency.get(key, 'unavailable')} |")
        lines += ["", "## Failures", ""] + (["- " + f for f in self.report["failures"]] or ["None."])
        lines += ["", "## Investigation leads", ""]
        lines += ["- **" + f["area"] + "**: " + f["next_step"] for f in findings] or ["No heuristic triggered; inspect coverage before drawing conclusions."]
        lines += ["", "## Dashboards for this time window", ""]
        lines += ["- [" + name + "](" + url + ")" for name, url in self.report["dashboards"].items()]
        slow = sorted((r for r in self.records if r.get("trace_url")), key=lambda r: r["latency_ms"], reverse=True)[:5]
        if slow:
            lines += ["", "## Slow request traces", "",
                      "Trace links require retained sampled spans. An Insert trace may stop at Proxy in the supplied image; see TRACING.md.", ""]
            lines += [f"- [{r['request_id']}]({r['trace_url']}): {r['latency_ms']:.1f} ms" for r in slow]
        lines += ["", "Raw evidence: `requests.jsonl`, `signals.jsonl`, `metric-*.json`, `pods.json`, `events.txt`, and bounded `*.log` files.", "",
                  "Client latency includes REST serialization/transport and any port-forward overhead. WP/resource metrics cover the deployment, not an end-to-end trace of this collection. Review logs before sharing."]
        (self.out / "summary.md").write_text("\n".join(lines) + "\n")

    def run(self):
        started = time.time()
        observer = None
        baseline = {}
        try:
            print(json.dumps({"stage": "started", "run_id": self.run_id, "output": str(self.out),
                              "collection": self.collection}), flush=True)
            save(self.out / "report.json", self.report)
            baseline = self.snapshot()
            observer = threading.Thread(target=self.observe, daemon=True)
            observer.start()
            self.call("collections/create", {"collectionName": self.collection, "dimension": self.args.dimension,
                      "autoID": False, "metricType": "COSINE", "consistencyLevel": "Strong", "params": {"shardsNum": 1}})
            self.collection_created = True
            self.report["collection_info"] = self.call("collections/describe", {"collectionName": self.collection})
            if self.args.warmup:
                self.phase("warmup", self.args.warmup)
            if not self.abort.is_set():
                self.phase("measure", self.args.duration)
            self.verify()
            measured = self.report["phases"].get("measure", {})
            if not measured.get("successful_requests") or any(not r["ok"] for r in self.records):
                self.report["failures"].append("Write failures or no successful measurement requests; see requests.jsonl")
            if self.args.min_rows_per_second and measured.get("acknowledged_rows_per_second", 0) < self.args.min_rows_per_second:
                self.report["failures"].append("Measured throughput is below the configured minimum")
            if self.args.max_p99_ms and (measured.get("success_latency_ms") or {}).get("p99", math.inf) > self.args.max_p99_ms:
                self.report["failures"].append("Successful insert p99 exceeds the configured maximum")
        except (Exception, KeyboardInterrupt) as error:
            self.abort.set()
            self.report["failures"].append(str(error) or "Interrupted")
        finally:
            # Keep observation running briefly so the final writes reach a scrape.
            if observer:
                self.observer_stop.wait(self.args.cooldown)
            self.observer_stop.set()
            if observer:
                observer.join()
            final = self.snapshot()
            self.collect_ranges(started - 60, time.time())
            for name in ("milvus_rows", "client_appends", "server_appends"):
                def series(snapshot):
                    return {json.dumps(row["metric"], sort_keys=True): float(row["value"][1])
                            for row in snapshot.get("metrics", {}).get(name, []) if math.isfinite(float(row["value"][1]))}
                before, after = series(baseline), series(final)
                delta = sum(value - before.get(key, 0) if value >= before.get(key, 0) else value
                            for key, value in after.items())
                self.report["metrics"].setdefault(name, {})["counter_delta"] = delta
                if delta <= 0:
                    self.report["failures"].append("No observed positive write-counter delta: " + name)
            self.collect_logs()
            # ALERTS is intentionally absent when no rule is pending/firing.
            self.report["alert_states_observed"] = bool(self.report["metrics"].get("alerts", {}).get("finite_samples"))
            self.report["coverage_gaps"] = [name for name, row in self.report["metrics"].items()
                                            if name != "alerts" and not row.get("finite_samples")]
            if self.report["diagnostic_errors"]:
                self.report["failures"].append("Incomplete diagnostics; see diagnostic_errors")
            self.report["finished_at"] = utc()
            self.report["collection_retained"] = self.collection_created
            self.report["passed"] = not self.report["failures"]
            grafana = os.getenv("GRAFANA_URL", "http://127.0.0.1:" + os.getenv("GRAFANA_PORT", "3000"))
            self.report["dashboards"] = {uid: grafana + "/d/" + uid + "?" + urllib.parse.urlencode({
                "from": int((started - 60) * 1000), "to": int(time.time() * 1000), "var-namespace": self.args.namespace,
                "var-collection": self.collection, "var-woodpecker_id": "milvus"})
                for uid in ("milvus-e2e-write", "milvus-e2e-consume", "milvus-e2e-flush", "milvus-e2e-query",
                            "milvus-full-stack", "woodpecker-client-k8s", "woodpecker-server-k8s")}
            self.triage()
            save(self.out / "report.json", self.report)
            self.trace.close()
            self.signals.close()
            print(json.dumps({"stage": "passed" if self.report["passed"] else "failed",
                              "report": str(self.out / "report.json"), "failures": self.report["failures"]}), flush=True)
        return 0 if self.report["passed"] else 1


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", default=os.getenv("CONTEXT", os.getenv("PROFILE", "wp-ops-v0146")))
    parser.add_argument("--namespace", default=os.getenv("NAMESPACE", "milvus-stack"))
    parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parents[1] / ".local/perf")
    for flag, default in (("milvus", "19530"), ("prometheus", "9099"), ("alertmanager", "9093")):
        parser.add_argument("--" + flag, default=os.getenv(flag.upper() + "_URL", "http://127.0.0.1:" + os.getenv(flag.upper() + "_PORT", default)))
    for name, default, kind in (("duration", 60, float), ("warmup", 5, float), ("cooldown", 35, float),
                                ("concurrency", 2, int), ("batch-size", 100, int), ("dimension", 128, int),
                                ("rps", 5, float), ("request-timeout", 30, float), ("max-requests", 10000, int),
                                ("max-errors", 5, int), ("sample-interval", 10, float),
                                ("min-rows-per-second", 0, float), ("max-p99-ms", 0, float),
                                ("trace-sample-fraction", 1, float)):
        parser.add_argument("--" + name, type=kind, default=os.getenv("PERF_" + name.upper().replace("-", "_"), default))
    args = parser.parse_args(argv)
    limits = {"duration": (1, 3600), "warmup": (0, 300), "cooldown": (0, 120), "concurrency": (1, 32),
              "batch_size": (1, 10000), "dimension": (2, 4096), "rps": (.1, 1000), "request_timeout": (1, 90),
              "max_requests": (1, 10000), "max_errors": (1, 10000), "sample_interval": (1, 60),
              "min_rows_per_second": (0, 1e9), "max_p99_ms": (0, 1e9), "trace_sample_fraction": (0, 1)}
    for name, (low, high) in limits.items():
        if not low <= getattr(args, name) <= high:
            parser.error(f"{name} must be in [{low}, {high}]")
    for name in ("milvus", "prometheus", "alertmanager"):
        url = urllib.parse.urlsplit(getattr(args, name))
        if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password:
            parser.error(name + " URL must use HTTP(S) without embedded credentials")
    return args


if __name__ == "__main__":
    raise SystemExit(Benchmark(arguments()).run())
