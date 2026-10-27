#!/usr/bin/env python3
"""Opted-in loaded query/search consistency acceptance (#486); plan by default.

Uses only merged #482 helpers. No shared configuration, Pod state, diagnostic
threshold or cluster lifecycle changes. The request-level fence is Customized.
"""
import argparse
import copy
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import signal
import subprocess
import time
import urllib.parse
import uuid

import cli_acceptance as shared

ROOT = shared.ROOT
PROFILE = "cli-query-consistency-v1"
LABEL = "diagnostics.woodpecker.io/query-acceptance"
LOCK = "wpdiag-cli-acceptance-lock"
NAME = r"query_accept_[0-9]{8}T[0-9]{6}Z_[0-9a-f]{8}"
VISIBLE = "suspected_visibility_wait"
OTHER_CAUSES = {"suspected_scheduling_pressure", "suspected_scheduling_delay", "suspected_execution_work", "observed_slow_segment_loading"}


def signature(report):
    return sorted((f["rule_id"], f["rule_version"], f["outcome"], f["severity"], f["evidence_strength"],
                   f["flow"], f["stage"], json.dumps(f["target"], sort_keys=True), f["summary"], tuple(sorted(f["unknown"]))) for f in shared.findings(report))


def populations(report, rule, outcomes):
    facts = {o["id"] + ":" + f["population_id"]: f for r in report.get("results", []) for o in r.get("observations", [])
             if o.get("status") == "observed" and o.get("kind") == "prometheus_matrix" for f in o["data"].get("facts", [])}
    by_operation = {}
    for f in shared.findings(report):
        if f["rule_id"] == rule and f["outcome"] in outcomes:
            for source_id in f.get("derived_from_findings", []):
                fact = facts.get(source_id)
                if fact and fact.get("labels", {}).get("query_type") in ("query", "search"):
                    by_operation.setdefault(fact["labels"]["query_type"], []).append({"finding_id": f["id"], "fact": fact})
    if set(by_operation) != {"query", "search"}:
        raise RuntimeError("No qualified query AND search populations for " + rule)
    return by_operation


def metric_gate(report, phase):
    if phase == "idle":
        return populations(report, "milvus.query.proxy_completion", {"idle_or_no_completions"})
    completion = populations(report, "milvus.query.proxy_completion", {"observed_slow_proxy_completions", "within_configured_latency_for_observed_completions"})
    visibility = populations(report, "milvus.query.tsafe", {VISIBLE} if phase == "future" else {"within_configured_latency_for_observed_completions"})
    if any(f["outcome"] in OTHER_CAUSES for f in shared.findings(report)):
        raise RuntimeError("Independent scheduling/execution/loading suspicion prevents a controlled visibility-only contrast")
    return {"proxy": completion, "visibility": visibility}


def workload_gate(report, batches, future_ms):
    records = report.get("records", [])
    if not report.get("passed") or len(records) != batches * 5 or not all(r.get("ok") for r in records):
        raise RuntimeError("Marker/read populations, transport/status or exact-ID correctness failed")
    inserts = [r for r in records if r["operation"] == "insert"]
    if len(inserts) != batches or len({r["expected_id"] for r in inserts}) != batches:
        raise RuntimeError("Marker insert IDs missing or duplicated")
    ack = {r["expected_id"]: int(r["ack_timestamp"]) for r in inserts}
    output = {}
    for operation in ("query", "search"):
        for control in (False, True):
            selected = [r for r in records if r["operation"] == operation and r["strong_control"] is control]
            if len(selected) != batches or {r["expected_id"] for r in selected} != set(ack):
                raise RuntimeError("Paired read IDs differ from acknowledged marker IDs")
            for r in selected:
                ts = ack[r["expected_id"]]
                offset = 0 if control else future_ms
                expected = ts + (offset << 18) if offset else 0
                if not ts or int(r["ack_timestamp"]) != ts or int(r["guarantee_timestamp"]) != expected or r["consistency"] != ("Customized" if offset else "Strong"):
                    raise RuntimeError("Fence was not derived exactly from the server acknowledgement")
            output[operation + ("_strong_control" if control else "_target")] = shared.PERF.distribution([r["latency_ms"] for r in selected])
        if future_ms:
            held, control = output[operation + "_target"]["p95"], output[operation + "_strong_control"]["p95"]
            if held < future_ms * .5 or control >= held * .5:
                raise RuntimeError("Successful waits did not contrast with prompt Strong controls")
    return output


def drop_owned(endpoint, name):
    if not re.fullmatch(NAME, name):
        raise RuntimeError("Not an owned query acceptance collection name")
    payload = {"collectionName": name}
    base = "/v2/vectordb/collections/"
    exists = shared.http(endpoint, base + "has", payload, 10).get("data", {}).get("has")
    if not isinstance(exists, bool):
        raise RuntimeError("Owned collection presence unknown")
    if exists:
        shared.http(endpoint, base + "drop", payload, 15)
        if shared.http(endpoint, base + "has", payload, 10).get("data", {}).get("has") is not False:
            raise RuntimeError("Owned collection remains after drop")


class Acceptance:
    def __init__(self, args):
        self.args = args
        self.kube = ["kubectl", "--context", args.context, "-n", args.namespace]
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
        self.out = args.output / self.run_id
        self.out.mkdir(parents=True, mode=0o700)
        self.deadline = time.monotonic() + args.budget
        self.stack = ExitStack()
        self.lock_uid, self.worker = None, None
        self.owned, self.rows = False, 0
        self.name = "query_accept_" + self.run_id.replace("-", "_")
        self.report = {"schema_version": "wpdiag.query-acceptance/v1", "profile": PROFILE, "run_id": self.run_id,
            "started_at": shared.utc(), "passed": False, "failures": [], "phases": {}, "collection": self.name,
            "selection": {"context": args.context, "namespace": args.namespace, "release": args.release},
            "limits": {k: getattr(args, k) for k in ("batches", "parallel", "duration", "future_ms", "scrape_wait", "budget")},
            "coverage_gaps": ["Customized future guarantee request control, not a Strong consistency production incident",
                "Recovery removes the requested future fence; no shared service/configuration was repaired",
                "Metric populations are not per-request timers or additive quantiles",
                "No authoritative collection/channel to WP metadata/log binding; runtime overrides not collected",
                "No trace/Loki/native/WebUI/disk/fsync/crash-durability claim"]}
        self.checkpoint()

    def save(self, filename, data):
        raw = json.dumps(data, indent=2, allow_nan=False).encode()
        used = sum(p.stat().st_size for p in self.out.iterdir() if p.is_file() and p.name != filename)
        if len(raw) > 16 << 20 or filename != "acceptance.json" and used + len(raw) > 96 << 20:
            raise RuntimeError("Acceptance JSON/raw artifact budget exhausted")
        shared.save(self.out / filename, data)

    def checkpoint(self):
        self.report.update(finished_at=shared.utc(), collection_owned=self.owned, acknowledged_rows=self.rows if self.owned else 0)
        self.save("acceptance.json", self.report)

    def remaining(self, cap=30):
        value = min(cap, self.deadline - time.monotonic())
        if value <= 0:
            raise RuntimeError("Query acceptance execution budget exhausted")
        return value

    def wait(self, seconds):
        if self.remaining(seconds + 1) < seconds:
            raise RuntimeError("Wait exceeds remaining execution budget")
        time.sleep(seconds)

    def inventory(self):
        ns = json.loads(shared.command(self.kube + ["get", "namespace", self.args.namespace, "-o", "json"]))
        if ns["metadata"].get("labels", {}).get(LABEL) != PROFILE:
            raise RuntimeError("Disposable namespace opt-in required: " + LABEL + "=" + PROFILE)
        pods = json.loads(shared.command(self.kube + ["get", "pods", "-l", "app.kubernetes.io/instance=" + self.args.release, "-o", "json"]))["items"]
        if not pods or not any(p["metadata"].get("labels", {}).get("component") == "woodpecker" for p in pods):
            raise RuntimeError("Service-mode Milvus/WP release required")
        rows, maps = [], set()
        for p in pods:
            if p["metadata"].get("deletionTimestamp") or not any(c["type"] == "Ready" and c["status"] == "True" for c in p.get("status", {}).get("conditions", [])):
                raise RuntimeError("All selected Pods must be Ready")
            rows.append({"pod": p["metadata"]["name"], "uid": p["metadata"]["uid"], "component": p["metadata"].get("labels", {}).get("component"),
                "containers": [{k: c.get(k) for k in ("name", "image", "imageID", "restartCount")} for c in p["status"].get("containerStatuses", [])]})
            maps.update(v["configMap"]["name"] for v in p["spec"].get("volumes", []) if "configMap" in v)
        fingerprints = []
        for name in sorted(maps):
            cm = json.loads(shared.command(self.kube + ["get", "configmap", name, "-o", "json"]))
            fingerprints.append({"name": name, "uid": cm["metadata"]["uid"], "resource_version": cm["metadata"]["resourceVersion"],
                "data_sha256": hashlib.sha256(json.dumps(cm.get("data", {}), sort_keys=True).encode()).hexdigest()})
        return {"pods": sorted(rows, key=lambda p: p["pod"]), "mounted_configmaps": fingerprints}

    def setup(self):
        self.report["builds"] = self.inventory()
        lock = json.loads(shared.command(self.kube + ["create", "configmap", LOCK, "--from-literal=run-id=" + self.run_id, "--from-literal=profile=" + PROFILE, "-o", "json"]))
        self.lock_uid = lock["metadata"]["uid"]
        self.report["lock_uid"] = self.lock_uid
        self.checkpoint()
        self.milvus = self.stack.enter_context(shared.Forward(self.kube, "svc/" + self.args.release, 19530))
        self.prometheus = self.stack.enter_context(shared.Forward(self.kube, "svc/" + self.args.prometheus_service, 9090))
        config = {"schema_version": "wpdiag.config/v1", "sources": {"prometheus": {"connections": {"acceptance": {"settings": {
            "endpoint": self.prometheus, "deployment": self.args.namespace + "/" + self.args.release, "limits": {"max_series": 2500},
            "match_labels": {"namespace": self.args.namespace, "cluster": "local", "woodpecker_id": self.args.release}}}}}}}
        self.config = self.out / "wpdiag.private.json"
        self.save(self.config.name, config)
        self.report["binary_sha256"] = {"wpdiag": hashlib.sha256(Path(self.args.wpdiag).read_bytes()).hexdigest(), "queryclient": hashlib.sha256(Path(self.args.queryclient).read_bytes()).hexdigest()}
        self.report["harness_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (Path(__file__), Path(shared.__file__))}
        if self.call("collections/has", {"collectionName": self.name})["has"]:
            raise RuntimeError("Unique collection already exists; refusing to reuse/delete it")
        self.owned = True
        self.checkpoint()
        self.call("collections/create", {"collectionName": self.name, "dimension": 16, "autoID": False, "metricType": "COSINE", "consistencyLevel": "Strong", "params": {"shardsNum": 1}})
        seed = [{"id": i, "vector": [((i * 17 + k * 31) % 251 + 1) / 251 for k in range(16)]} for i in range(64)]
        ack = self.call("entities/insert", {"collectionName": self.name, "data": seed})
        if ack.get("insertCount") != 64 or sorted(ack.get("insertIds", [])) != list(range(64)):
            raise RuntimeError("Seed acknowledgement IDs/count invalid")
        self.rows = 64
        self.report["collection_id"] = str(self.call("collections/describe", {"collectionName": self.name}).get("collectionID"))
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self.call("collections/get_load_state", {"collectionName": self.name}).get("loadState") == "LoadStateLoaded":
                break
            self.wait(.5)
        else:
            raise RuntimeError("Collection never became Loaded")
        if not self.client("prime", "verify").get("passed"):
            raise RuntimeError("Seed exact-ID/search verification failed")
        self.checkpoint()

    def call(self, path, payload):
        return shared.http(self.milvus, "/v2/vectordb/" + path, payload, self.remaining(20)).get("data")

    def loaded(self):
        state = self.call("collections/get_load_state", {"collectionName": self.name})
        if state.get("loadState") != "LoadStateLoaded" or state.get("loadProgress") != 100:
            raise RuntimeError("Already-loaded case encountered a loading/unloaded collection")
        return {"at": shared.utc(), **state}

    def client(self, label, mode="phase"):
        output = self.out / (label + ".workload.json")
        phase = label if label in ("baseline", "future", "recovery") else "baseline"
        argv = [self.args.queryclient, "--execute", "--parent-pipe", "--endpoint", urllib.parse.urlsplit(self.milvus).netloc,
            "--collection", self.name, "--output", str(output), "--mode", mode, "--phase", phase,
            "--start-id", str(self.rows), "--rows", str(self.rows), "--batches", str(self.args.batches), "--parallel", str(self.args.parallel),
            "--duration", str(self.args.duration) + "s", "--future-ms", str(self.args.future_ms)]
        states = [self.loaded()]
        with (self.out / (label + ".worker.log")).open("w") as log:
            self.worker = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=log, stderr=subprocess.STDOUT)
            try:
                last = 0
                while self.worker.poll() is None:
                    self.remaining()
                    time.sleep(.2)
                    if time.monotonic() - last >= 2:
                        states.append(self.loaded())
                        last = time.monotonic()
            finally:
                self.stop_worker()
        states.append(self.loaded())
        self.save(label + ".loaded.json", states)
        return json.loads(output.read_text())

    def stop_worker(self):
        if self.worker:
            if self.worker.stdin:
                self.worker.stdin.close()
            if self.worker.poll() is None:
                self.worker.terminate()
                try:
                    self.worker.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    self.worker.kill()
                    self.worker.wait(timeout=3)

    def diagnose(self, label, window, config=None, allow_failed=False):
        common = ["--config", str(config or self.config), "--connection", "acceptance", "--scope", "e2e", "--mode", "historical",
            "--from", window["from"], "--to", window["to"], "--budget", "30s", "-o", "json"]
        children = []
        for kind, argv in (("query", [self.args.wpdiag, "milvus", "query"]), ("integrated", [self.args.wpdiag, "run", "--flow", "read", "--stages", "query", "--sources", "prometheus"])):
            p = subprocess.run(argv + common, capture_output=True, text=True, timeout=self.remaining(40))
            if len(p.stdout.encode()) > 16 << 20:
                raise RuntimeError("Diagnostic output exceeds artifact budget")
            try:
                report = json.loads(p.stdout)
            except ValueError:
                raise RuntimeError("Diagnostic returned no usable JSON; private output withheld") from None
            self.save(label + "." + kind + ".json", report)
            if allow_failed:
                return p.returncode, report
            if p.returncode or report.get("status") != "completed":
                raise RuntimeError("Diagnostic did not complete: " + str(report.get("status")))
            child = report if kind == "query" else next(r["report"] for r in report["runs"] if r["module_id"] == "milvus.query")
            if kind == "integrated":
                originals = {f["id"] for f in shared.findings(child)} | {f["id"] for r in child["results"] for f in r["findings"]}
                if not all(f["id"] in originals for f in report["summary"]["findings"] if f["module_id"] == "milvus.query"):
                    raise RuntimeError("Integrated summary changed original finding identity")
            shared.lineage(child)
            children.append(child)
        if signature(children[0]) != signature(children[1]):
            raise RuntimeError("Standalone/composed conclusions, targets or uncertainty differ")
        return children[0]

    def phase(self, label):
        print(json.dumps({"stage": label, "run_id": self.run_id}), flush=True)
        self.wait(self.args.scrape_wait)
        row = {"window": {"from": shared.utc()}, "status": "running"}
        self.report["phases"][label] = row
        self.checkpoint()
        self.wait(self.args.scrape_wait)
        workload = None
        if label == "idle":
            self.wait(self.args.duration)
            self.loaded()
        else:
            workload = self.client(label)
        self.wait(self.args.scrape_wait)
        row["window"]["to"] = shared.utc()
        report = self.diagnose(label, row["window"])
        row.update(outcomes=sorted({f["outcome"] for f in shared.findings(report)}), independent_composed_equal=True)
        if workload:
            row["client_latency_ms"] = workload_gate(workload, self.args.batches, self.args.future_ms if label == "future" else 0)
            self.rows += self.args.batches
        metric_gate(report, label)
        if label != "idle":
            if not self.client(label + "_verify", "verify").get("passed"):
                raise RuntimeError("Every acknowledged ID and vector search did not verify")
            row["exact_ids"] = self.rows
        row["status"] = "passed"
        self.checkpoint()

    def negative_controls(self, window):
        original = json.loads(self.config.read_text())
        checks = {}
        for kind in ("absent_series", "unreachable_backend"):
            config = copy.deepcopy(original)
            settings = config["sources"]["prometheus"]["connections"]["acceptance"]["settings"]
            if kind == "absent_series":
                settings["match_labels"]["namespace"] = self.args.namespace + "-absent"
            else:
                settings["endpoint"] = "http://127.0.0.1:1"
            path = self.out / (kind + ".private.json")
            self.save(path.name, config)
            code, report = self.diagnose(kind, window, path, allow_failed=True)
            try:
                metric_gate(report, "baseline")
            except RuntimeError:
                pass
            else:
                raise RuntimeError("Missing source passed working-traffic gate")
            if kind == "absent_series" and (code or report.get("status") != "completed") or kind == "unreachable_backend" and (not code or report.get("status") == "completed"):
                raise RuntimeError("Negative control did not distinguish absence from failed collection")
            checks[kind] = {"exit_code": code, "status": report["status"], "working_gate_rejected": True}
        self.report["negative_controls"] = checks

    def finish(self):
        with shared.cleanup_signals():
            self.stop_worker()
            clean = not self.owned
            if self.owned:
                try:
                    drop_owned(self.milvus, self.name)
                    clean = True
                    self.report["collection_dropped"] = True
                except Exception as error:
                    self.report["passed"] = False
                    self.report["failures"].append("Owned cleanup failed: " + str(error))
            if self.lock_uid and clean:
                try:
                    lock = json.loads(shared.command(self.kube + ["get", "configmap", LOCK, "-o", "json"]))
                    if lock["metadata"]["uid"] != self.lock_uid or lock.get("data", {}).get("run-id") != self.run_id:
                        raise RuntimeError("Lock ownership changed")
                    shared.command(self.kube + ["delete", "configmap", LOCK, "--wait=false"])
                    self.report["lock_released"] = True
                except Exception as error:
                    self.report["passed"] = False
                    self.report["failures"].append("Lock cleanup failed: " + str(error))
            self.checkpoint()

    def run(self):
        print(json.dumps({"stage": "started", "run_id": self.run_id, "output": str(self.out)}), flush=True)
        try:
            self.setup()
            for label in ("baseline", "idle", "future", "recovery"):
                self.phase(label)
            historical = self.diagnose("historical_after_recovery", self.report["phases"]["future"]["window"])
            metric_gate(historical, "future")
            self.report["historical_after_recovery"] = {"visibility_suspicion_retained": True, "independent_composed_equal": True}
            self.negative_controls(self.report["phases"]["baseline"]["window"])
            self.report["final_builds"] = self.inventory()
            if self.report["builds"] != self.report["final_builds"]:
                raise RuntimeError("Pods or mounted configuration changed during this non-restart case")
            self.report["passed"] = True
        except (Exception, KeyboardInterrupt) as error:
            self.report["failures"].append(str(error) or "Interrupted")
        finally:
            try:
                self.finish()
            finally:
                self.stack.close()
            print(json.dumps({"passed": self.report["passed"], "report": str(self.out / "acceptance.json"), "failures": self.report["failures"]}), flush=True)
        return 0 if self.report["passed"] else 1


def arguments(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--execute", action="store_true")
    p.add_argument("--context", required=True)
    p.add_argument("--namespace", required=True)
    p.add_argument("--release", default="milvus")
    p.add_argument("--prometheus-service", default="monitoring-kube-prometheus-prometheus")
    p.add_argument("--wpdiag", default=str(ROOT / "bin/wpdiag"))
    p.add_argument("--queryclient", default=str(ROOT / "bin/query-requests"))
    p.add_argument("--output", type=Path, default=Path(__file__).parent / ".local/query")
    for key, value, kind in (("batches", 24, int), ("parallel", 2, int), ("duration", 30, int), ("future-ms", 2000, int), ("scrape-wait", 16, float), ("budget", 720, float)):
        p.add_argument("--" + key, default=value, type=kind)
    a = p.parse_args(argv)
    for key in ("context", "namespace", "release", "prometheus_service"):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}", getattr(a, key)):
            p.error(key + " must be a bounded explicit identifier")
    for key, lo, hi in (("batches", 20, 48), ("parallel", 1, 4), ("duration", 10, 60), ("future_ms", 1000, 5000), ("scrape_wait", 1, 30), ("budget", 120, 1800)):
        if not math.isfinite(getattr(a, key)) or not lo <= getattr(a, key) <= hi:
            p.error(key + " exceeds query fixture bounds")
    a.wpdiag, a.queryclient = str(Path(a.wpdiag).resolve()), str(Path(a.queryclient).resolve())
    return a


def main(argv=None):
    a = arguments(argv)
    if not a.execute:
        print(json.dumps({"profile": PROFILE, "execute": False, "context": a.context, "namespace": a.namespace,
            "required_namespace_label": LABEL + "=" + PROFILE, "cluster_lifecycle": "external", "configuration_changes": [],
            "request_control": {"consistency": "Customized", "server_ack_plus_ms": a.future_ms},
            "sequence": ["Ready stack/shared lock", "Create/seed/load one owned collection", "Baseline and idle", "Future fence/Strong controls",
                         "Normal-request recovery", "Historical/negative controls", "Exact owned-data cleanup"]}, indent=2))
        return 0
    return Acceptance(a).run()


if __name__ == "__main__":
    def interrupted(_sig, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
