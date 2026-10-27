#!/usr/bin/env python3
"""Opted-in deployed CLI acceptance: baseline -> admission slowdown -> recovery.

Uses existing Kubernetes, Helm stack services, the existing bounded perf worker,
and wpdiag's source/stage rules. This harness alone mutates its own test data and
four temporary Milvus settings. The diagnostic commands remain read-only.
"""
import argparse
import copy
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import queue
import re
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("stack_perf", ROOT / "deployments/milvus-full-stack-helm/scripts/perf.py")
PERF = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PERF)

LABEL = "diagnostics.woodpecker.io/acceptance"
PROFILE = "cli-admission-v1"
# Explicit reproduction controls, not diagnostic thresholds or production advice.
FAULT = {
    "streaming.walRateLimit.nodeMemory.slowdownThreshold": "0.0001",
    "streaming.walRateLimit.nodeMemory.adaptiveRateLimit.slowdown.hwm": "512kb",
    "streaming.walRateLimit.nodeMemory.adaptiveRateLimit.slowdown.lwm": "256kb",
    "streaming.walRateLimit.defaultBurst": "1mb",
}
QUOTA_RULE = "milvus.append.step.milvus.append.quota"
SLOW = "suspected_quota_admission_wait"


def utc():
    return datetime.now(timezone.utc).isoformat()


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def http(base, path, payload=None, timeout=10):
    url = base + path
    request = urllib.request.Request(url, data=None if payload is None else json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json", "Accept": "application/json"})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout) as response:
            raw = response.read((2 << 20) + 1)
            if len(raw) > 2 << 20:
                raise ValueError("HTTP response exceeds its byte budget")
            data = json.loads(raw)
    except urllib.error.HTTPError as error:
        error.close()
        raise RuntimeError("HTTP request failed with status " + str(error.code)) from None
    except (OSError, ValueError) as error:
        raise RuntimeError("HTTP request failed at " + path.split("?", 1)[0] + " (" + type(error).__name__ + "); response/connection values withheld") from None
    if isinstance(data, dict) and data.get("code", 0) != 0:
        raise RuntimeError("Milvus returned code " + str(data["code"]))
    return data


def command(argv, timeout=30):
    p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    if p.returncode:
        raise RuntimeError("Command failed: " + " ".join(argv[:2]) + "; exit=" + str(p.returncode) + "; private output withheld")
    return p.stdout


@contextmanager
def cleanup_signals():
    """Finish bounded restoration on a second Ctrl+C; SIGKILL needs the journal."""
    previous = {}
    if threading.current_thread() is threading.main_thread():
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous[sig] = signal.signal(sig, signal.SIG_IGN)
    try:
        yield
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


class Forward:
    def __init__(self, kube, target, port):
        self.argv = kube + ["port-forward", "--address", "127.0.0.1", target, ":" + str(port)]
        self.process = None
        self.lines = queue.Queue(maxsize=64)

    def __enter__(self):
        self.process = subprocess.Popen(self.argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        def read():
            for line in self.process.stdout:
                try:
                    self.lines.put_nowait(line)
                except queue.Full:
                    pass
        threading.Thread(target=read, daemon=True).start()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                line = self.lines.get(timeout=.1)
            except queue.Empty:
                if self.process.poll() is not None:
                    break
                continue
            match = re.search(r"Forwarding from 127\.0\.0\.1:(\d+) ->", line)
            if match:
                return "http://127.0.0.1:" + match[1]
        self.__exit__(None, None, None)
        raise RuntimeError("Unable to establish a bounded localhost port-forward")

    def __exit__(self, *_):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)


class ConfigFault:
    """Journal before mutation; restore etcd overrides or their original absence."""
    def __init__(self, endpoint, output):
        self.endpoint, self.output = endpoint, output
        self.before = None
        self.changed = False
        self.restored = False

    def read(self):
        data = http(self.endpoint, "/management/config/get?" + urllib.parse.urlencode({"keys": ",".join(FAULT)}))
        rows = data.get("configs", [])
        if len(rows) != len(FAULT) or {row.get("key") for row in rows} != set(FAULT):
            raise RuntimeError("Management API did not return all registered fault-control keys")
        for row in rows:
            if row.get("error") or row.get("source") not in ("FileSource", "EtcdSource") or "value" not in row or row["value"] == "[REDACTED]":
                raise RuntimeError("This build cannot supply restorable configuration provenance")
        return {row["key"]: row for row in rows}

    def apply(self):
        current = self.read()
        if self.before is not None and current != self.before:
            raise RuntimeError("Fault controls changed since preflight; refusing injection")
        self.before = current
        # This file also supports explicit recovery after SIGKILL/process loss.
        save(self.output / "restoration.json", {"schema_version": "wpdiag.acceptance.restore/v1", "profile": PROFILE,
             "before": self.before, "fault": FAULT, "restored": False})
        self.changed = True  # Even an HTTP timeout can have committed the write.
        http(self.endpoint, "/management/config/alter", {"configs": [{"key": key, "value": value} for key, value in FAULT.items()]})
        self.wait(lambda rows: all(rows[k]["value"] == v and rows[k]["source"] == "EtcdSource" for k, v in FAULT.items()))
        save(self.output / "fault-applied.json", self.read())

    def wait(self, predicate):
        deadline = time.monotonic() + 30
        while True:
            rows = self.read()
            if predicate(rows):
                return rows
            if time.monotonic() >= deadline:
                raise RuntimeError("Configuration readback did not converge before its deadline")
            time.sleep(.25)

    def restore(self):
        if not self.changed:
            return
        configs = []
        for key, row in self.before.items():
            item = {"key": key}
            if row["source"] == "EtcdSource":
                item["value"] = row["value"]
            configs.append(item)
        http(self.endpoint, "/management/config/alter", {"configs": configs})
        rows = self.wait(lambda after: all(after[key]["value"] == row["value"] and after[key]["source"] == row["source"]
                                           for key, row in self.before.items()))
        self.restored = True
        save(self.output / "restoration.json", {"schema_version": "wpdiag.acceptance.restore/v1", "profile": PROFILE,
             "before": self.before, "fault": FAULT, "restored": True, "after": rows})


class Workload(PERF.Benchmark):
    def call(self, path, payload, trace_id=None, sampled=True):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("Acceptance wall-clock budget exhausted")
        return http(self.args.milvus, "/v2/vectordb/" + path, payload, min(self.args.request_timeout, remaining)).get("data")


def findings(report):
    return [f for a in report.get("assessments", []) for f in a.get("findings", [])]


def signature(report):
    return sorted((f["rule_id"], f["rule_version"], f["outcome"], f["severity"], f["evidence_strength"])
                  for f in findings(report))


def completed_population(report, rule):
    """Missing/idle/inconclusive metrics cannot establish working traffic."""
    positive = {"observed_slow_proxy_completions", "within_configured_latency_for_observed_completions"}
    rows = [f for f in findings(report) if f["rule_id"] == rule and f["outcome"] in positive
            and f.get("supporting_observations")]
    if not rows:
        raise RuntimeError("No positively observed completed traffic for " + rule)
    return [f["id"] for f in rows]


def lineage(report):
    for result in report.get("results", []):
        observations = {o["id"] for o in result.get("observations", [])}
        source = {f["id"] for f in result.get("findings", [])}
        for f in result.get("findings", []) + findings(report):
            if not set(f.get("supporting_observations", []) + f.get("contradicting_observations", [])) <= observations:
                raise RuntimeError("Dangling original observation reference")
            if not set(f.get("derived_from_findings", [])) <= source:
                raise RuntimeError("Dangling stage/source finding reference")


class Acceptance:
    def __init__(self, args):
        self.args = args
        self.kube = ["kubectl", "--context", args.context, "-n", args.namespace]
        self.stack = ExitStack()
        self.workload = None
        self.fault = None
        self.started = time.monotonic()
        self.lock_id = None
        self.report = {"schema_version": "wpdiag.acceptance/v1", "profile": PROFILE, "started_at": utc(),
                       "passed": False, "failures": [], "phases": {}, "coverage_gaps": [
                           "CLI admission scenario only; WebUI and full-program validation remain #458",
                           "WP service context does not establish Milvus client-metadata authority",
                           "REST latency includes serialization, transport and port-forward overhead; no durability/crash claim"],
                       "selection": {"context": args.context, "namespace": args.namespace, "release": args.release},
                       "limits": {key: getattr(args, key) for key in ("duration", "prime_duration", "settle", "scrape_wait", "rps", "concurrency", "batch_size", "dimension", "max_requests", "request_timeout", "total_budget", "min_slowdown_ratio", "max_recovery_ratio")}}

    def checkpoint(self):
        self.report["finished_at"] = utc()
        save(self.workload.out / "acceptance.json", self.report)

    def inventory(self):
        namespace = json.loads(command(self.kube + ["get", "namespace", self.args.namespace, "-o", "json"]))
        if namespace["metadata"].get("labels", {}).get(LABEL) != PROFILE:
            raise RuntimeError("Execution requires the dedicated namespace label " + LABEL + "=" + PROFILE)
        pods = json.loads(command(self.kube + ["get", "pods", "-l", "app.kubernetes.io/instance=" + self.args.release, "-o", "json"]))["items"]
        def ready(p):
            return p["metadata"].get("deletionTimestamp") is None and any(c["type"] == "Ready" and c["status"] == "True" for c in p.get("status", {}).get("conditions", []))
        if not pods or not all(ready(p) for p in pods):
            raise RuntimeError("All selected Milvus/WP Pods must already be Ready")
        coordinators = [p for p in pods if p["metadata"].get("labels", {}).get("component") in ("mixcoord", "mixcoordinator")]
        servers = [p for p in pods if p["metadata"].get("labels", {}).get("component") == "woodpecker"]
        if len(coordinators) != 1 or not servers:
            raise RuntimeError("Profile requires one mix coordinator and ready service-mode Woodpecker Pods")
        self.report["builds"] = [{"pod": p["metadata"]["name"], "uid": p["metadata"]["uid"], "component": p["metadata"].get("labels", {}).get("component"),
                                   "containers": [{"name": c["name"], "image": c["image"], "image_id": c.get("imageID", ""), "restarts": c.get("restartCount", 0)}
                                                  for c in p["status"].get("containerStatuses", [])]} for p in pods]
        return coordinators[0]["metadata"]["name"], servers

    def setup(self):
        # Allocate the receipt before any reachability/capability check, so
        # preflight failures also leave a machine-readable artifact.
        bench_args = PERF.arguments(["--context", self.args.context, "--namespace", self.args.namespace, "--output", str(self.args.output),
            "--milvus", "http://127.0.0.1:1", "--skip-metrics", "--skip-alerts", "--warmup", "0", "--cooldown", "0", "--trace-sample-fraction", "0",
            *[value for key in ("duration", "rps", "concurrency", "batch_size", "dimension", "max_requests", "request_timeout")
              for value in ("--" + key.replace("_", "-"), str(getattr(self.args, key)))]] )
        self.workload = Workload(bench_args)
        self.workload.deadline = self.started + self.args.total_budget
        coordinator, servers = self.inventory()
        self.lock_id = self.workload.run_id
        # A fixed create-only lock prevents two opted-in runs sharing a namespace.
        print(json.dumps({"stage": "lock", "command": self.kube + ["create", "configmap", "wpdiag-cli-acceptance-lock"]}), flush=True)
        try:
            lock = json.loads(command(self.kube + ["create", "configmap", "wpdiag-cli-acceptance-lock", "--from-literal=run-id=" + self.lock_id, "-o", "json"]))
            self.report["lock_uid"] = lock["metadata"]["uid"]
        except Exception:
            self.lock_id = None
            raise RuntimeError("Namespace acceptance lock is unavailable; inspect any previous run/restoration receipt before removing a stale lock") from None
        self.milvus = self.stack.enter_context(Forward(self.kube, "svc/" + self.args.release, 19530))
        self.management = self.stack.enter_context(Forward(self.kube, "pod/" + coordinator, 9091))
        self.prometheus = self.stack.enter_context(Forward(self.kube, "svc/" + self.args.prometheus_service, 9090))
        self.workload.args.milvus = self.milvus
        self.workload.collection = "diag_accept_" + self.workload.run_id.replace("-", "_")
        self.report.update(run_id=self.workload.run_id, collection=self.workload.collection)
        print(json.dumps({"stage": "started", "run_id": self.workload.run_id, "output": str(self.workload.out)}), flush=True)
        self.fault = ConfigFault(self.management, self.workload.out)
        self.fault.before = self.fault.read()  # Capability check before creating data.
        self.report["initial_config"] = self.fault.before
        self.report["wpdiag_sha256"] = hashlib.sha256(Path(self.args.wpdiag).read_bytes()).hexdigest()
        self.report["harness_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                          for p in (Path(__file__), ROOT / "deployments/milvus-full-stack-helm/scripts/perf.py")}
        save(self.workload.out / "capabilities.json", json.loads(command([self.args.wpdiag, "capabilities", "-o", "json"])))
        config = {"schema_version": "wpdiag.config/v1", "sources": {"prometheus": {"connections": {"acceptance": {"settings": {
            "endpoint": self.prometheus, "deployment": self.args.namespace + "/" + self.args.release,
            "limits": {"max_series": 2500},
            "match_labels": {"namespace": self.args.namespace, "cluster": "local", "woodpecker_id": self.args.release}}}}}}}
        if self.args.wp:
            admin = {}
            for p in servers:
                endpoint = self.stack.enter_context(Forward(self.kube, "pod/" + p["metadata"]["name"], 9091))
                members = http(endpoint, "/admin/memberlist").get("members", [])
                # Published Helm server identity is NODE_NAME (Pod name).
                node_id = p["metadata"]["name"]
                if not any(m.get("id") == node_id for m in members):
                    raise RuntimeError("Cannot authoritatively associate this WP Pod with its service node ID")
                admin[node_id] = endpoint
            context = "acceptance/" + self.workload.run_id
            cli_path = self.workload.out / "cli.private.json"
            save(cli_path, {"contexts": {context: {"endpoint": next(iter(admin.values())), "node_admin_urls": admin}}})
            config["sources"]["wpcli"] = {"connections": {"acceptance": {"settings": {"executable": self.args.wp, "context": context, "cli_config": str(cli_path)}}}}
            self.report["wp_sha256"] = hashlib.sha256(Path(self.args.wp).read_bytes()).hexdigest()
        else:
            self.report["coverage_gaps"].append("Native current-context probe not selected; no WP runtime claim")
        self.config = self.workload.out / "wpdiag.private.json"
        save(self.config, config)
        self.checkpoint()

    def diagnose(self, phase, start, end, native=False):
        stem = self.workload.out / (phase + ("-current-context" if native else ""))
        common = ["--config", str(self.config), "--scope", "e2e", "--mode", "historical", "--from", start, "--to", end, "--budget", "30s", "-o", "json"]
        def invoke(argv, path):
            remaining = self.args.total_budget - (time.monotonic() - self.started)
            if remaining <= 0:
                raise RuntimeError("Acceptance wall-clock budget exhausted")
            p = subprocess.run(argv, capture_output=True, text=True, timeout=min(40, remaining))
            if len(p.stdout.encode()) > 16 << 20:
                raise RuntimeError("Diagnostic report exceeds artifact byte bound")
            try:
                report = json.loads(p.stdout)
            except ValueError:
                raise RuntimeError("Diagnostic did not return JSON; private output withheld") from None
            path.write_text(p.stdout)
            if p.returncode != 0 or report.get("status") != "completed":
                raise RuntimeError("Diagnostic execution incomplete: " + str(report.get("status")))
            return report
        single = invoke([self.args.wpdiag, "milvus", "append", "--connection", "acceptance"] + common, stem.with_suffix(".append.json"))
        lineage(single)
        argv = [self.args.wpdiag, "run", "--flow", "write", "--stages", "append", "--sources", "prometheus" + (",wpcli" if native else ""), "--connection", "acceptance"] + common
        if native:
            argv.append("--current-native")
        integrated = invoke(argv, stem.with_suffix(".integrated.json"))
        child = next(r["report"] for r in integrated["runs"] if r["module_id"] == "milvus.append")
        lineage(child)
        if signature(single) != signature(child):
            raise RuntimeError("Standalone and composed stage diagnoses disagree for the same retained interval")
        originals = {f["id"] for r in child["results"] for f in r["findings"]} | {f["id"] for f in findings(child)}
        if not all(v["id"] in originals for v in integrated["summary"]["findings"] if v["module_id"] == "milvus.append"):
            raise RuntimeError("Integrated view changed original finding identity")
        if native:
            native_run = next(r for r in integrated["runs"] if r["module_id"] == "wpcli")
            if native_run["role"] != "current_context" or native_run["report"]["selection"]["window"]["mode"] != "snapshot":
                raise RuntimeError("Current native context was mislabeled as historical evidence")
        query = invoke([self.args.wpdiag, "milvus", "query", "--connection", "acceptance"] + common, stem.with_suffix(".query.json"))
        lineage(query)
        append_completed = completed_population(single, "milvus.append.step.milvus.append")
        query_completed = completed_population(query, "milvus.query.proxy_completion")
        quota = [f for f in findings(single) if f["rule_id"] == QUOTA_RULE]
        if not quota:
            raise RuntimeError("Append profile returned no admission-step finding")
        return {"append_status": single["status"], "query_status": query["status"], "quota_outcomes": sorted({f["outcome"] for f in quota}),
                "quota_finding_ids": [f["id"] for f in quota], "quota_support": [ref for f in quota for ref in f["supporting_observations"]],
                "append_completed": append_completed, "query_completed": query_completed,
                "independent_composed_equal": True, "current_native_context": native}

    def phase(self, name):
        self.wait(self.args.settle)
        # Lazy histograms only exist after the first relevant event. Exercise
        # this condition before selecting a fully scraped measurement window.
        self.workload.phase(name + "_prime", self.args.prime_duration)
        if any(not r["ok"] for r in self.workload.records):
            raise RuntimeError("Priming workload failed")
        start = utc()
        # Boundary scrapes: preserve the workload interval separately from the
        # query interval; both include no other scenario's traffic.
        self.wait(self.args.scrape_wait)
        stop = threading.Event()
        reads = []
        def reader():
            while not stop.is_set():
                if hasattr(self.workload, "sample_id"):
                    began = time.monotonic()
                    row = {"at": utc(), "ok": False}
                    try:
                        data = self.workload.call("entities/query", {"collectionName": self.workload.collection, "filter": "id == " + str(self.workload.sample_id),
                            "outputFields": ["id"], "consistencyLevel": "Strong", "limit": 1})
                        row["ok"] = bool(data) and int(data[0]["id"]) == self.workload.sample_id
                    except Exception as error:
                        row["error"] = str(error)
                    row["latency_ms"] = (time.monotonic() - began) * 1000
                    reads.append(row)
                stop.wait(.5)
        thread = threading.Thread(target=reader, daemon=True)
        thread.start()
        try:
            self.workload.phase(name, self.args.duration)
        finally:
            stop.set()
            thread.join(timeout=self.args.request_timeout + 1)
        if thread.is_alive():
            raise RuntimeError("Read probe exceeded its bounded shutdown")
        self.workload.verify()
        self.wait(self.args.scrape_wait)
        end = utc()
        row = dict(self.workload.report["phases"][name])
        row.update(window={"from": start, "to": end}, read_latency_ms=PERF.distribution([r["latency_ms"] for r in reads if r["ok"]]),
                   read_requests=len(reads), failed_reads=sum(not r["ok"] for r in reads), correctness=dict(self.workload.report["correctness"]))
        save(self.workload.out / (name + ".reads.json"), reads)
        self.report["phases"][name] = row
        save(self.workload.out / "workload.json", self.workload.report)
        self.checkpoint()
        if row["failed_requests"] or not row["successful_requests"] or row["failed_reads"] or not row["read_requests"]:
            raise RuntimeError("Workload request or live correctness checks failed")
        row["diagnosis"] = self.diagnose(name, start, end)
        self.checkpoint()
        return row

    def negative_controls(self, start, end):
        """Exercise real source/stage commands against absent or unreachable data."""
        original = json.loads(self.config.read_text())
        checks = {}
        for kind in ("absent_series", "unreachable_backend"):
            config = copy.deepcopy(original)
            settings = config["sources"]["prometheus"]["connections"]["acceptance"]["settings"]
            if kind == "absent_series":
                settings["match_labels"]["namespace"] = self.args.namespace + "-no-such-fixture"
            else:
                settings["endpoint"] = "http://127.0.0.1:1"
            path = self.workload.out / (kind + ".private.json")
            save(path, config)
            remaining = self.args.total_budget - (time.monotonic() - self.started)
            if remaining <= 0:
                raise RuntimeError("Acceptance wall-clock budget exhausted")
            p = subprocess.run([self.args.wpdiag, "milvus", "append", "--config", str(path), "--connection", "acceptance",
                "--scope", "e2e", "--mode", "historical", "--from", start, "--to", end, "--budget", "15s", "-o", "json"],
                capture_output=True, text=True, timeout=min(20, remaining))
            if len(p.stdout.encode()) > 16 << 20:
                raise RuntimeError("Negative-control artifact exceeds its byte budget")
            try:
                report = json.loads(p.stdout)
            except ValueError:
                raise RuntimeError("Negative-control diagnostic returned no usable JSON") from None
            save(self.workload.out / (kind + ".append.json"), report)
            try:
                completed_population(report, "milvus.append.step.milvus.append")
            except RuntimeError:
                pass
            else:
                raise RuntimeError("Missing source evidence unexpectedly passed completion gate")
            if kind == "absent_series" and (p.returncode or report.get("status") != "completed"):
                raise RuntimeError("Absent-series control failed to complete its source queries")
            if kind == "unreachable_backend" and (not p.returncode or report.get("status") == "completed"):
                raise RuntimeError("Unavailable backend was incorrectly reported as complete")
            checks[kind] = {"status": report["status"], "exit_code": p.returncode, "traffic_gate_rejected": True}
        self.report["negative_controls"] = checks
        self.checkpoint()

    def wait(self, seconds):
        if time.monotonic() - self.started + seconds > self.args.total_budget:
            raise RuntimeError("Acceptance wall-clock budget exhausted")
        time.sleep(seconds)

    def run(self):
        try:
            self.setup()
            existing = self.workload.call("collections/has", {"collectionName": self.workload.collection})
            if existing.get("has"):
                raise RuntimeError("Unique owned collection already exists; refusing to reuse or delete it")
            self.workload.collection_created = True  # Create can commit before a timeout.
            self.workload.call("collections/create", {"collectionName": self.workload.collection, "dimension": self.args.dimension,
                "autoID": False, "metricType": "COSINE", "consistencyLevel": "Strong", "params": {"shardsNum": 1}})
            baseline = self.phase("baseline")
            if SLOW in baseline["diagnosis"]["quota_outcomes"]:
                raise RuntimeError("Baseline already reports admission waiting; no controlled contrast can be established")
            self.negative_controls(baseline["window"]["from"], baseline["window"]["to"])
            self.fault.apply()
            injected = self.phase("admission_slowdown")
            if SLOW not in injected["diagnosis"]["quota_outcomes"]:
                raise RuntimeError("Fault did not produce the source-owned suspected admission-wait finding")
            self.fault.restore()
            recovered = self.phase("recovery")
            if SLOW in recovered["diagnosis"]["quota_outcomes"]:
                raise RuntimeError("Admission-wait suspicion persists after recovery")
            base = baseline["success_latency_ms"]["p95"]
            slow = injected["success_latency_ms"]["p95"]
            if slow < base * self.args.min_slowdown_ratio:
                raise RuntimeError("Fault did not measurably increase user-side p95 latency")
            if recovered["success_latency_ms"]["p95"] > slow * self.args.max_recovery_ratio:
                raise RuntimeError("Recovered user-side p95 did not improve enough")
            if self.args.wp:
                window = injected["window"]
                self.report["historical_after_recovery"] = self.diagnose("admission_slowdown", window["from"], window["to"], native=True)
                if SLOW not in self.report["historical_after_recovery"]["quota_outcomes"]:
                    raise RuntimeError("Recovered current context cleared the historical admission suspicion")
            initial_builds = self.report["builds"]
            self.inventory()
            final_builds = self.report["builds"]
            self.report.update(builds=initial_builds, final_builds=final_builds)
            def identities(builds):
                return sorted((p["pod"], p["uid"], c["name"], c["image_id"], c["restarts"])
                              for p in builds for c in p["containers"])
            if identities(initial_builds) != identities(final_builds):
                raise RuntimeError("Selected Pods restarted or changed during this non-restart scenario")
            self.report["passed"] = True
        except (Exception, KeyboardInterrupt) as error:
            self.report["failures"].append(str(error) or "Interrupted")
        finally:
            with cleanup_signals():
                try:
                    self.finish()
                finally:
                    self.stack.close()
        return 0 if self.report["passed"] else 1


    def finish(self):
        if self.fault and self.fault.changed and not self.fault.restored:
            try:
                self.fault.restore()
            except Exception as error:
                self.report["passed"] = False
                self.report["failures"].append("RESTORATION FAILED: " + str(error))
        if self.workload:
            self.report["configuration_restored"] = not self.fault or not self.fault.changed or self.fault.restored
            self.report["collection_retained"] = self.workload.collection_created
            if self.workload.collection_created and not self.args.keep_collection:
                try:
                    # Cleanup has its own bounded allowance after an expired
                    # workload budget. Never delete an existing/unowned name.
                    exists = http(self.milvus, "/v2/vectordb/collections/has", {"collectionName": self.workload.collection}, 10).get("data", {})
                    if exists.get("has"):
                        http(self.milvus, "/v2/vectordb/collections/drop", {"collectionName": self.workload.collection}, 10)
                    self.report["collection_retained"] = False
                except Exception as error:
                    self.report["passed"] = False
                    self.report["failures"].append("Owned collection cleanup failed: " + str(error))
            self.workload.trace.close()
            self.workload.signals.close()
            if self.lock_id and self.report["configuration_restored"]:
                try:
                    current = json.loads(command(self.kube + ["get", "configmap", "wpdiag-cli-acceptance-lock", "-o", "json"]))
                    if current["metadata"]["uid"] != self.report["lock_uid"] or current.get("data", {}).get("run-id") != self.lock_id:
                        raise RuntimeError("Acceptance lock ownership changed")
                    print(json.dumps({"stage": "unlock", "command": self.kube + ["delete", "configmap", "wpdiag-cli-acceptance-lock"]}), flush=True)
                    command(self.kube + ["delete", "configmap", "wpdiag-cli-acceptance-lock", "--wait=false"])
                except Exception as error:
                    self.report["passed"] = False
                    self.report["failures"].append("Owned acceptance lock cleanup failed: " + str(error))
            self.checkpoint()
            print(json.dumps({"passed": self.report["passed"], "report": str(self.workload.out / "acceptance.json"), "failures": self.report["failures"]}), flush=True)
        else:
            print(json.dumps({"passed": False, "failures": self.report["failures"]}), flush=True)


def arguments(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--execute", action="store_true", help="Opt in to owned workload and the explicit temporary fault; default is plan only")
    p.add_argument("--context", required=True)
    p.add_argument("--namespace", required=True)
    p.add_argument("--release", default="milvus")
    p.add_argument("--prometheus-service", default="monitoring-kube-prometheus-prometheus")
    p.add_argument("--wpdiag", default=str(ROOT / "bin/wpdiag"))
    p.add_argument("--wp", default="", help="Optional current native probe using a compatible wp binary")
    p.add_argument("--output", type=Path, default=ROOT / "tests/integration/diagnostics/.local")
    p.add_argument("--keep-collection", action="store_true")
    for key, default, kind in (("duration", 45, float), ("prime-duration", 20, float), ("settle", 30, float), ("scrape-wait", 16, float), ("rps", 8, float),
        ("concurrency", 4, int), ("batch-size", 256, int), ("dimension", 128, int), ("max-requests", 1000, int), ("request-timeout", 15, float),
        ("total-budget", 720, float), ("min-slowdown-ratio", 1.5, float), ("max-recovery-ratio", .8, float)):
        p.add_argument("--" + key, type=kind, default=default)
    a = p.parse_args(argv)
    for name in ("context", "namespace", "release", "prometheus_service"):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}", getattr(a, name)):
            p.error(name + " must be an explicit bounded identifier")
    for name, lo, hi in (("duration", 10, 120), ("prime_duration", 5, 30), ("settle", 0, 120), ("scrape_wait", 1, 30), ("rps", 1, 50),
        ("concurrency", 1, 8), ("batch_size", 1, 1024), ("dimension", 2, 512), ("max_requests", 20, 2000), ("request_timeout", 1, 30),
        ("total_budget", 60, 1800), ("min_slowdown_ratio", 1.1, 20), ("max_recovery_ratio", .1, .99)):
        if not math.isfinite(getattr(a, name)) or not lo <= getattr(a, name) <= hi:
            p.error(name + " exceeds the acceptance bounds")
    if a.batch_size * a.dimension * 4 > 512 << 10:
        p.error("Logical vector payload per request must fit 512KiB")
    requests = min(a.max_requests, math.ceil(a.rps * a.duration)) + min(a.max_requests, math.ceil(a.rps * a.prime_duration))
    logical_bytes = 3 * requests * a.batch_size * a.dimension * 4
    if logical_bytes > 256 << 20:
        p.error("Three-phase logical vector payload must fit 256MiB")
    a.wpdiag = str(Path(a.wpdiag).resolve())
    if a.wp:
        a.wp = str(Path(a.wp).resolve())
    return a


def main(argv=None):
    a = arguments(argv)
    if not a.execute:
        print(json.dumps({"schema_version": "wpdiag.acceptance.plan/v1", "profile": PROFILE,
            "target": {"context": a.context, "namespace": a.namespace, "release": a.release}, "required_namespace_label": LABEL + "=" + PROFILE,
            "sequence": ["Verify ready service-mode stack and configuration provenance", "Create one uniquely owned collection",
                "Baseline writes and strong reads", "Apply explicit temporary rate-guard controls", "Confirm client slowdown and source-owned quota suspicion",
                "Restore exact config values/override absence", "Check recovery and retained historical diagnosis", "Drop only the owned collection unless --keep-collection"],
            "temporary_controls": FAULT, "coordination": "create-only namespace ConfigMap lock; preserve it if restoration fails", "cluster_lifecycle": "external; no create/start/stop/delete", "execute": False}, indent=2))
        return 0
    return Acceptance(a).run()


if __name__ == "__main__":
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
