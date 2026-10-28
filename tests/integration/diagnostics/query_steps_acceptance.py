#!/usr/bin/env python3
"""#490 real Loaded FLAT/L2 scheduling and execution contrasts; offline by default.

Only this opted-in harness creates uniquely owned data and bounded requests.
Existing query source/stage rules own all diagnosis and thresholds. No process
freezing, shared settings, CPU resizing or cluster lifecycle action is used.
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
import query_acceptance as original

ROOT = shared.ROOT
LABEL = "diagnostics.woodpecker.io/query-steps-acceptance"
PROFILE = "cli-query-steps-v1"
LOCK = original.LOCK
NAME = original.NAME


def require(condition, detail):
    if not condition:
        raise RuntimeError(detail)


def qualified(report, rule, outcomes, query_type="search"):
    """Resolve actual existing owner findings, not a second latency evaluator."""
    facts = {o["id"] + ":" + f["population_id"]: f for r in report.get("results", []) for o in r.get("observations", [])
             if o.get("status") == "observed" and o.get("kind") == "prometheus_matrix" for f in o["data"].get("facts", [])}
    source = {f["id"]: f for r in report.get("results", []) for f in r.get("findings", [])}
    selected = []
    for finding in shared.findings(report):
        if finding["rule_id"] != rule or finding["outcome"] not in outcomes:
            continue
        refs = finding.get("derived_from_findings", [])
        require(refs and all(k in source for k in refs), "Qualified stage finding lost original source lineage")
        fact = facts.get(refs[0])
        if fact and (query_type is None or fact.get("labels", {}).get("query_type") == query_type):
            selected.append({"finding": finding, "fact": fact, "associated_facts": [facts.get(k) for k in refs[1:]]})
    require(selected, "Missing default-qualified " + rule + " outcome " + ",".join(sorted(outcomes)))
    return selected


def metric_gate(report, phase):
    require(report.get("status") == "completed", "Incomplete required query source collection")
    normal = {"within_configured_latency_for_observed_completions"}
    if phase == "idle":
        rows = qualified(report, "milvus.query.proxy_completion", {"idle_or_no_completions"})
        require(all(r["fact"].get("observed_completions") == 0 for r in rows), "Idle needs observed zero deltas, not absent series")
        return {"proxy": rows}
    proxy = qualified(report, "milvus.query.proxy_completion", normal if phase in ("baseline", "recovery") else normal | {"observed_slow_proxy_completions"})
    tsafe = qualified(report, "milvus.query.tsafe", normal)
    if phase == "scheduling":
        queue = qualified(report, "milvus.query.queue", {"suspected_scheduling_pressure"})
        for row in queue:
            associated = row["associated_facts"]
            require(len(associated) == 1 and associated[0] is not None, "Scheduling pressure missing actual ready-queue association")
            ready = associated[0]
            require(row["fact"].get("exporter_identity") and row["fact"]["exporter_identity"] == ready.get("exporter_identity"), "Wrong scheduling exporter association")
            gauge = ready.get("gauge", {})
            require(gauge.get("minimum", 0) > 0 and gauge.get("last", 0) > 0, "No positive retained backlog throughout pressure interval")
        return {"proxy": proxy, "tsafe": tsafe, "queue": queue}
    core = qualified(report, "milvus.query.core", {"suspected_execution_work"} if phase == "execution" else normal)
    queue = qualified(report, "milvus.query.queue", normal)
    output = {"proxy": proxy, "tsafe": tsafe, "queue": queue, "core": core}
    if phase == "execution":
        output["segment"] = qualified(report, "milvus.query.segment", {"observed_slow_segment_work"})
        ready = qualified(report, "milvus.query.ready", {"no_ready_backlog_observed"}, None)
        for item in core:
            identity = item["fact"].get("exporter_identity")
            require(identity, "Missing core exporter identity")
            # Visibility is an independently exported SN/delegator observation;
            # a sealed QueryNode core need not export its own TSafe timer.
            for name, controls in (("queue", queue), ("ready", ready)):
                matching = [r for r in controls if r["fact"].get("exporter_identity") == identity]
                require(matching, "Execution contrast cannot borrow " + name + " from another exporter")
                if name == "ready":
                    require(all(r["fact"].get("gauge", {}).get("peak") == 0 for r in matching), "Execution-only contrast has observed ready pressure")
        output["ready"] = ready
    return output


def workload_gate(workload, minimum=1):
    records = workload.get("records", [])
    require(workload.get("passed") and all(r.get("ok") for r in records), "Actual workload transport/status/exact-result verification failed")
    require(len(records) >= minimum, "Successful workload completions " + str(len(records)) + " below required " + str(minimum))
    if workload["mode"] == "search":
        require(all(r["operation"] == "search" and r.get("verified_nq") == workload["nq"] for r in records), "Incomplete verified NQ population")
        require(workload["metadata"]["offered_requests"] == len(records), "Offered and completed request accounting differ")
    return shared.PERF.distribution([r["latency_ms"] for r in records])


class Acceptance(original.Acceptance):
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
        self.report = {"schema_version": "wpdiag.query-steps-acceptance/v1", "profile": PROFILE, "run_id": self.run_id,
            "started_at": shared.utc(), "passed": False, "failures": [], "phases": {}, "collection": self.name,
            "selection": {"context": args.context, "namespace": args.namespace, "release": args.release, "case": args.case},
            "limits": {k: getattr(args, k) for k in ("rows", "execution_nq", "scheduling_nq", "parallel", "duration", "scrape_wait", "budget")},
            "coverage_gaps": ["Controlled FLAT/L2 workload on one owned collection; not every query plan or an exclusive CPU/disk/network cause",
                "Closed-loop bounded offered requests; achieved work is measured, not an unbounded promised offered rate",
                "Retained ready gauge samples are discrete; no continuous or per-request backlog claim",
                "Independent timer populations may overlap; no addition/subtraction of quantiles",
                "No authoritative collection/channel to WP-client namespace/log binding",
                "No trace/Loki/native/WebUI/full SDK quorum or crash-durability claim"]}
        self.checkpoint()

    def namespace_check(self):
        ns = json.loads(shared.command(self.kube + ["get", "namespace", self.args.namespace, "-o", "json"]))
        require(ns["metadata"].get("labels", {}).get(LABEL) == PROFILE, "Disposable namespace opt-in required: " + LABEL + "=" + PROFILE)
        uid = ns["metadata"]["uid"]
        require(self.report.get("namespace_uid", uid) == uid, "Original namespace UID changed")
        self.report["namespace_uid"] = uid

    def inventory(self):
        self.namespace_check()
        pods = json.loads(shared.command(self.kube + ["get", "pods", "-l", "app.kubernetes.io/instance=" + self.args.release, "-o", "json"]))["items"]
        require(pods and any(p["metadata"].get("labels", {}).get("component") == "woodpecker" for p in pods), "Require a real service-mode Milvus/WP release")
        rows, maps = [], set()
        for p in pods:
            require(not p["metadata"].get("deletionTimestamp") and any(c["type"] == "Ready" and c["status"] == "True" for c in p.get("status", {}).get("conditions", [])), "Every selected core Pod must already be Ready")
            rows.append({"pod": p["metadata"]["name"], "uid": p["metadata"]["uid"], "component": p["metadata"].get("labels", {}).get("component"),
                "containers": [{k: c.get(k) for k in ("name", "image", "imageID", "restartCount")} for c in p["status"].get("containerStatuses", [])],
                "resources": [{"name": c["name"], "resources": c.get("resources", {})} for c in p["spec"]["containers"]]})
            maps.update(v["configMap"]["name"] for v in p["spec"].get("volumes", []) if "configMap" in v)
        fingerprints = []
        for name in sorted(maps):
            cm = json.loads(shared.command(self.kube + ["get", "configmap", name, "-o", "json"]))
            fingerprints.append({"name": name, "uid": cm["metadata"]["uid"], "resource_version": cm["metadata"]["resourceVersion"],
                "data_sha256": hashlib.sha256(json.dumps(cm.get("data", {}), sort_keys=True).encode()).hexdigest()})
        return {"pods": sorted(rows, key=lambda p: p["pod"]), "mounted_configmaps": fingerprints}

    def lock_check(self):
        self.namespace_check()
        lock = json.loads(shared.command(self.kube + ["get", "configmap", LOCK, "-o", "json"]))
        require(lock["metadata"]["uid"] == self.lock_uid and lock.get("data") == {"run-id": self.run_id, "profile": PROFILE}, "Original exclusive lock ownership changed")

    def setup(self):
        self.report["builds"] = self.inventory()
        # Journal creation intent before a possibly ambiguous API submission.
        self.report["lock_create_intent"] = True
        self.checkpoint()
        lock = json.loads(shared.command(self.kube + ["create", "configmap", LOCK, "--from-literal=run-id=" + self.run_id, "--from-literal=profile=" + PROFILE, "-o", "json"]))
        self.lock_uid = lock["metadata"]["uid"]
        self.report["lock_uid"] = self.lock_uid
        self.checkpoint()
        self.milvus = self.stack.enter_context(shared.Forward(self.kube, "svc/" + self.args.release, 19530))
        self.prometheus = self.stack.enter_context(shared.Forward(self.kube, "svc/" + self.args.prometheus_service, 9090))
        self.config = self.out / "wpdiag.private.json"
        self.save(self.config.name, {"schema_version": "wpdiag.config/v1", "sources": {"prometheus": {"connections": {"acceptance": {"settings": {
            "endpoint": self.prometheus, "deployment": self.args.namespace + "/" + self.args.release,
            "limits": {"max_series": 2500}, "match_labels": {"namespace": self.args.namespace, "cluster": "local", "woodpecker_id": self.args.release}}}}}}})
        self.report["binary_sha256"] = {"wpdiag": hashlib.sha256(Path(self.args.wpdiag).read_bytes()).hexdigest(), "queryclient": hashlib.sha256(Path(self.args.queryclient).read_bytes()).hexdigest()}
        self.report["harness_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (Path(__file__), Path(shared.__file__), Path(original.__file__))}
        require(not self.call("collections/has", {"collectionName": self.name})["has"], "Unique collection already exists; refusing reuse")
        self.owned = True
        self.checkpoint()
        metadata = self.work("create", "create")
        self.report["collection_id"] = metadata["metadata"]["collection_id"]
        self.checkpoint()
        seeded = self.work("seed", "seed")
        require(seeded.get("passed") and seeded.get("acknowledged_rows") == self.args.rows, "Seed acknowledgements incomplete")
        self.rows = self.args.rows
        self.save("initial-segments.json", seeded["metadata"])
        self.checkpoint()
        self.loaded()
        workload_gate(self.work("seed-verify", "verify"))
        workload_gate(self.work("prime", "search", nq=1, duration=1, requests=4))
        print(json.dumps({"stage": "loaded", "run_id": self.run_id, "rows": self.rows}), flush=True)

    def start_work(self, label, mode="search", nq=1, parallel=1, duration=30, requests=96):
        output = self.out / (label + ".workload.json")
        argv = [self.args.queryclient, "--execute", "--parent-pipe", "--endpoint", urllib.parse.urlsplit(self.milvus).netloc,
            "--collection", self.name, "--output", str(output), "--mode", mode, "--rows", str(self.args.rows),
            "--nq", str(nq), "--concurrency", str(parallel), "--duration", str(duration) + "s", "--requests", str(requests)]
        self.worker_log = (self.out / (label + ".worker.private.log")).open("w")
        self.worker = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=self.worker_log, stderr=subprocess.STDOUT)
        return output

    def finish_work(self, output):
        states = []
        last = 0
        try:
            while self.worker.poll() is None:
                self.remaining()
                time.sleep(.2)
                if time.monotonic() - last >= 5 and self.rows:
                    states.append(self.loaded())
                    last = time.monotonic()
            code = self.worker.returncode
        finally:
            self.stop_worker()
            self.worker_log.close()
        require(output.exists(), "Workload ended without a bounded receipt")
        data = json.loads(output.read_text())
        self.save(output.stem + ".loaded.json", states)
        require(code == 0 and data.get("passed"), "Real query workload failed; inspect its retained receipt")
        return data

    def work(self, label, mode, **kwargs):
        return self.finish_work(self.start_work(label, mode, **kwargs))

    def diagnose(self, label, window, config=None, allow_failed=False, instance=None):
        common = ["--config", str(config or self.config), "--connection", "acceptance", "--scope", "e2e", "--mode", "historical",
                  "--from", window["from"], "--to", window["to"], "--budget", "30s", "-o", "json"]
        if instance:
            common += ["--instance", instance]
        children = []
        for kind, argv in (("query", [self.args.wpdiag, "milvus", "query"]), ("integrated", [self.args.wpdiag, "run", "--flow", "read", "--stages", "query", "--sources", "prometheus"])):
            p = subprocess.run(argv + common, capture_output=True, text=True, timeout=self.remaining(40))
            require(len(p.stdout.encode()) <= 16 << 20, "Diagnostic response exceeds artifact budget")
            try:
                report = json.loads(p.stdout)
            except ValueError:
                raise RuntimeError("Diagnostic returned no JSON; private output withheld") from None
            self.save(label + "." + kind + ".json", report)
            if allow_failed:
                return p.returncode, report
            require(p.returncode == 0 and report.get("status") == "completed", "Required diagnosis did not complete")
            child = report if kind == "query" else next(r["report"] for r in report["runs"] if r["module_id"] == "milvus.query")
            shared.lineage(child)
            if kind == "integrated":
                originals = {f["id"] for f in shared.findings(child)} | {f["id"] for r in child["results"] for f in r["findings"]}
                require(all(f["id"] in originals for f in report["summary"]["findings"] if f["module_id"] == "milvus.query"), "Composed view changed original finding IDs")
            children.append(child)
        require(original.signature(children[0]) == original.signature(children[1]), "Independent/composed query semantics, targets or unknowns differ")
        return children[0]

    def ready(self):
        query = 'milvus_querynode_read_task_ready_len{cluster="local",namespace="' + self.args.namespace + '",woodpecker_id="' + self.args.release + '"}'
        raw = shared.http(self.prometheus, "/api/v1/query?" + urllib.parse.urlencode({"query": query}), timeout=self.remaining(10))
        require(raw.get("status") == "success", "Actual ready-queue source unavailable")
        return [{"labels": r["metric"], "sample": r["value"]} for r in raw.get("data", {}).get("result", [])]

    def phase(self, label):
        print(json.dumps({"stage": label, "run_id": self.run_id}), flush=True)
        self.wait(self.args.scrape_wait)
        row = {"window": {"from": shared.utc()}, "status": "running"}
        self.report["phases"][label] = row
        self.checkpoint()
        self.wait(self.args.scrape_wait)
        if label == "idle":
            self.wait(30)
            self.loaded()
        elif label == "scheduling":
            output = self.start_work(label, nq=self.args.scheduling_nq, parallel=self.args.parallel,
                                     duration=self.args.duration, requests=1024)
            deadline = time.monotonic() + self.remaining(35)
            ready_samples = []
            while time.monotonic() < deadline:
                require(self.worker.poll() is None, "Scheduling workload finished before ready backlog was observed")
                ready = self.ready()
                ready_samples.append({"at": shared.utc(), "observations": ready})
                if any(float(r["sample"][1]) > 0 for r in ready):
                    break
                self.wait(1)
            else:
                raise RuntimeError("No real ready backlog; pressure reproduction failed")
            row["window"]["from"] = shared.utc()
            self.wait(self.args.measurement)
            require(self.worker.poll() is None, "Scheduling workload stopped during the pressure interval")
            ready_samples.append({"at": shared.utc(), "observations": self.ready()})
            self.save(label + ".ready-probes.json", ready_samples)
            row["window"]["to"] = shared.utc()
            workload = self.finish_work(output)
            row["client_latency_ms"] = workload_gate(workload, 20)
            row["offered_requests"] = workload["metadata"]["offered_requests"]
        else:
            nq = self.args.execution_nq if label == "execution" else 1
            workload = self.work(label, "search", nq=nq, duration=self.args.duration if label == "execution" else 30,
                                 requests=256 if label == "execution" else 96)
            row["client_latency_ms"] = workload_gate(workload, 20)
            row["offered_requests"] = workload["metadata"]["offered_requests"]
        if label != "scheduling":
            self.wait(self.args.scrape_wait)
            row["window"]["to"] = shared.utc()
        report = self.diagnose(label, row["window"])
        proof = metric_gate(report, label)
        row.update(outcomes=sorted({f["outcome"] for f in shared.findings(report)}), evidence=proof, independent_composed_equal=True, status="passed")
        self.checkpoint()

    def negative_controls(self, window):
        original_config = json.loads(self.config.read_text())
        checks = {}
        for kind in ("absent_series", "unreachable_backend", "wrong_instance"):
            config = copy.deepcopy(original_config)
            settings = config["sources"]["prometheus"]["connections"]["acceptance"]["settings"]
            if kind == "absent_series":
                settings["match_labels"]["namespace"] = self.args.namespace + "-absent"
            elif kind == "unreachable_backend":
                settings["endpoint"] = "http://127.0.0.1:1"
            path = self.out / (kind + ".private.json")
            self.save(path.name, config)
            code, report = self.diagnose(kind, window, path, True, "192.0.2.1:9091" if kind == "wrong_instance" else None)
            try:
                metric_gate(report, "baseline")
            except RuntimeError:
                pass
            else:
                raise RuntimeError("Absent/wrong/failed source passed working gate")
            require((kind == "unreachable_backend" and code != 0 and report["status"] != "completed") or
                    (kind != "unreachable_backend" and code == 0 and report["status"] == "completed"), "Source absence versus transport failure lost")
            checks[kind] = {"exit": code, "status": report["status"], "working_gate_rejected": True}
        self.report["negative_controls"] = checks

    def drop_owned(self):
        self.lock_check()
        require(re.fullmatch(NAME, self.name), "Invalid owned collection name")
        if self.call("collections/has", {"collectionName": self.name})["has"]:
            description = self.call("collections/describe", {"collectionName": self.name})
            require(description.get("description") == "wpdiag query steps acceptance " + self.name, "Collection owner description changed; refuse deletion")
            if self.report.get("collection_id"):
                require(str(description.get("collectionID")) == self.report["collection_id"], "Original collection ID changed")
            original.drop_owned(self.milvus, self.name)
        self.report["collection_dropped"] = True

    def finish(self):
        with shared.cleanup_signals():
            self.stop_worker()
            if hasattr(self, "worker_log"):
                self.worker_log.close()
            # Resolve a create timeout only through the original run/profile.
            if not self.lock_uid and self.report.get("lock_create_intent"):
                try:
                    raw = shared.command(self.kube + ["get", "configmap", LOCK, "--ignore-not-found", "-o", "json"])
                    if raw.strip():
                        lock = json.loads(raw)
                        require(lock.get("data") == {"run-id": self.run_id, "profile": PROFILE}, "Ambiguous lock create belongs to another run")
                        self.lock_uid = lock["metadata"]["uid"]
                        self.report["lock_uid"] = self.lock_uid
                except Exception as error:
                    self.report["passed"] = False
                    self.report["failures"].append("Lock resolution failed: " + str(error))
                    self.checkpoint()
                    return
            if not self.lock_uid:
                self.checkpoint()
                return
            try:
                self.lock_check()
                if self.owned:
                    self.drop_owned()
                self.lock_check()
                options = self.out / "delete-lock.json"
                self.save(options.name, {"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": self.lock_uid}})
                shared.command(self.kube + ["delete", "--raw", "/api/v1/namespaces/" + self.args.namespace + "/configmaps/" + LOCK, "-f", str(options)])
                self.report["lock_released"] = True
            except Exception as error:
                self.report["passed"] = False
                self.report["failures"].append("Original-owner cleanup failed: " + str(error))
            self.checkpoint()

    def run(self):
        print(json.dumps({"stage": "started", "run_id": self.run_id, "output": str(self.out)}), flush=True)
        try:
            self.setup()
            for label in ("baseline", "idle", *(["execution", "scheduling"] if self.args.case == "all" else [self.args.case]), "recovery"):
                self.phase(label)
            workload_gate(self.work("final-verify", "verify"))
            for label in ("execution", "scheduling"):
                if label in self.report["phases"]:
                    metric_gate(self.diagnose(label + "_historical_after_recovery", self.report["phases"][label]["window"]), label)
                    self.report["phases"][label]["historical_suspicion_retained"] = True
            self.negative_controls(self.report["phases"]["baseline"]["window"])
            self.report["final_builds"] = self.inventory()
            require(self.report["builds"] == self.report["final_builds"], "Core Pod/image/restart/resource/config identity changed")
            self.report["passed"] = True
        except (Exception, KeyboardInterrupt) as error:
            self.report["failures"].append(str(error) or "Interrupted")
            # A failed reproducer still needs its actual source evidence; it is
            # never turned into a passing case by this best-effort collection.
            if hasattr(self, "config"):
                for label, phase in self.report["phases"].items():
                    if phase.get("status") == "running":
                        window = dict(phase["window"])
                        window.setdefault("to", shared.utc())
                        try:
                            self.diagnose("failed_" + label, window)
                            phase["failure_window"] = window
                        except Exception:
                            self.report["coverage_gaps"].append("Failed-case source evidence unavailable; original workload failure retained")
        finally:
            # Restoration is independent of the exhausted workload deadline.
            self.deadline = max(self.deadline, time.monotonic() + 90)
            try:
                self.finish()
            finally:
                self.stack.close()
            print(json.dumps({"passed": self.report["passed"], "report": str(self.out / "acceptance.json"), "failures": self.report["failures"]}), flush=True)
        return 0 if self.report["passed"] else 1


def arguments(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--context", required=True)
    p.add_argument("--namespace", required=True)
    p.add_argument("--release", default="milvus")
    p.add_argument("--prometheus-service", default="monitoring-kube-prometheus-prometheus")
    p.add_argument("--case", choices=("all", "execution", "scheduling"), default="all")
    p.add_argument("--queryclient", default=str(ROOT / "bin/query-steps"))
    p.add_argument("--wpdiag", default=str(ROOT / "bin/wpdiag"))
    p.add_argument("--output", type=Path, default=Path("/tmp/wp-query-steps"))
    p.add_argument("--execute", action="store_true")
    for key, value, kind in (("rows", 65536, int), ("execution-nq", 512, int), ("scheduling-nq", 256, int),
                             ("parallel", 8, int), ("duration", 120, int), ("measurement", 75, int), ("scrape-wait", 16, float), ("budget", 1200, float)):
        p.add_argument("--" + key, default=value, type=kind)
    a = p.parse_args(argv)
    for key in ("context", "namespace", "release", "prometheus_service"):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}", getattr(a, key)):
            p.error("Bounded explicit identifier required")
    for key, lo, hi in (("rows", 1024, 65536), ("execution_nq", 1, 16384), ("scheduling_nq", 1, 16384), ("parallel", 2, 16),
                         ("duration", 60, 120), ("measurement", 30, 90), ("scrape_wait", 1, 30), ("budget", 300, 1800)):
        if not math.isfinite(getattr(a, key)) or not lo <= getattr(a, key) <= hi:
            p.error(key + " exceeds bounded query steps profile")
    if a.measurement + 30 > a.duration:
        p.error("Measurement needs at least 30s offering headroom")
    a.queryclient, a.wpdiag = str(Path(a.queryclient).resolve()), str(Path(a.wpdiag).resolve())
    a.output = a.output.resolve()
    return a


def main(argv=None):
    a = arguments(argv)
    if not a.execute:
        print(json.dumps({"execute": False, "profile": PROFILE, "selection": {"context": a.context, "namespace": a.namespace, "case": a.case},
            "required_namespace_label": LABEL + "=" + PROFILE, "shared_lock": LOCK, "cluster_lifecycle": "external",
            "configuration_changes": [], "dataset": {"max_rows": a.rows, "dimension": 256, "index": "FLAT", "metric": "L2", "max_vector_bytes": a.rows * 256 * 4},
            "sequence": ["Create/seed/flush/index/load unique owned data", "Baseline/idle", "Serial execution-heavy search",
                         "Bounded concurrent search and actual ready backlog", "Recovery/exact vectors", "Historical/negative controls", "Original-owner cleanup"]}, indent=2))
        return 0
    return Acceptance(a).run()


if __name__ == "__main__":
    def interrupted(*_):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
