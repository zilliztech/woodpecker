#!/usr/bin/env python3
"""Opted-in real scanner-startup pause / initial visibility acceptance (#484).

No cluster lifecycle, diagnostic rule changes, fabricated metrics or Pod signals.
Only the harness owns workload/config mutations; diagnosis remains read-only.
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
import threading
import time
import uuid

import cli_acceptance as shared

ROOT = shared.ROOT
PROFILE = "cli-consume-v1"
LABEL = "diagnostics.woodpecker.io/consume-acceptance"
KEY = "streaming.walScanner.pauseConsumption"
CONTROLS = {KEY: "true"}
LOCK = "wpdiag-cli-acceptance-lock"  # Shared with admission acceptance.
PRESSURE = {"suspected_scanner_downstream_backpressure", "suspected_consumer_processing_pressure"}


def stage_findings(report):
    return shared.findings(report)


def signatures(report):
    return sorted((f["rule_id"], f["rule_version"], f["outcome"], f["severity"], f["evidence_strength"],
                   f["flow"], f["stage"], json.dumps(f["target"], sort_keys=True), f["summary"], tuple(sorted(f["unknown"])))
                  for f in stage_findings(report))


def facts(report, signal):
    """Return source facts, not values inferred from text or suggested links."""
    return [fact for result in report.get("results", []) for observation in result.get("observations", [])
            if observation.get("status") == "observed" and observation.get("kind") == "prometheus_matrix"
            for data in [observation["data"]] if data.get("signal") == signal for fact in data.get("facts", [])]


def working_gate(report, collection_id):
    selected = [f for f in facts(report, "milvus.consume.packs")
                if f.get("labels", {}).get("collection_id") == str(collection_id)
                and f.get("counter", {}).get("increase", 0) > 0]
    if not selected:
        raise RuntimeError("No positively observed MsgPack consumption for the authoritative owned collection ID")
    return selected


def pause_gate(report):
    paused = [f for f in facts(report, "milvus.consume.paused") if f.get("gauge", {}).get("minimum") == 1 and f.get("gauge", {}).get("peak") == 1]
    if not paused:
        raise RuntimeError("No continuously sampled scanner pause; actual fault was not observable in this window")
    # Startup pause precedes delivery. Zero pending work must not become a
    # downstream-processing or WP storage root cause merely because pause=1.
    if any(f["outcome"] in PRESSURE for f in stage_findings(report)):
        raise RuntimeError("Startup-only control unexpectedly reports processing pressure; inspect independent evidence")
    return paused


def resumed_gate(report):
    populations = facts(report, "milvus.consume.paused")
    if not populations or not all(f.get("gauge", {}).get("last_two_zero") is True for f in populations):
        raise RuntimeError("No retained two-scrape unpaused control for every observed scanner channel")
    return populations


def drop_owned_collection(endpoint, name):
    if not re.fullmatch(r"consume_accept_[0-9]{8}T[0-9]{6}Z_[0-9a-f]{8}_(baseline|paused)", name):
        raise RuntimeError("Not a consume acceptance collection name")
    path = "/v2/vectordb/collections/"
    payload = {"collectionName": name}
    exists = shared.http(endpoint, path + "has", payload, 10).get("data", {}).get("has")
    if not isinstance(exists, bool):
        raise RuntimeError("Cannot establish owned collection presence")
    if exists:
        shared.http(endpoint, path + "drop", payload, 15)
        if shared.http(endpoint, path + "has", payload, 10).get("data", {}).get("has") is not False:
            raise RuntimeError("Owned collection remains after drop")


class ConsumeFault(shared.ConfigFault):
    def __init__(self, endpoint, output):
        super().__init__(endpoint, output, CONTROLS, PROFILE)

    def prepare(self):
        self.before = self.read()
        if self.before[KEY]["value"] != "false":
            raise RuntimeError("Baseline scanner pause must explicitly be false")
        # Baseline/idle can run for minutes before injection. A process-loss
        # recovery journal must already exist before creating any owned data.
        shared.save(self.output / "restoration.json", {"schema_version": "wpdiag.acceptance.restore/v1", "profile": PROFILE,
                    "before": self.before, "fault": CONTROLS, "restored": True, "injection_started": False})
        return self.before

    def restore(self):
        if self.changed:
            current = self.read()
            for key, row in current.items():
                if row != self.before[key] and not (row["source"] == "EtcdSource" and row["value"] == CONTROLS[key]):
                    raise RuntimeError("Pause control changed outside this run; refusing to overwrite it")
        super().restore()


class Acceptance:
    def __init__(self, args):
        self.args = args
        self.kube = ["kubectl", "--context", args.context, "-n", args.namespace]
        self.started = time.monotonic()
        self.deadline = self.started + args.total_budget
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
        self.out = args.output / self.run_id
        self.out.mkdir(parents=True, mode=0o700)
        self.stack = ExitStack()
        self.fault = None
        self.lock_uid = None
        self.owned = {}
        self.records = []
        self.report = {"schema_version": "wpdiag.consume-acceptance/v1", "profile": PROFILE, "run_id": self.run_id,
            "started_at": shared.utc(), "passed": False, "failures": [], "phases": {},
            "selection": {"context": args.context, "namespace": args.namespace, "release": args.release},
            "limits": {k: getattr(args, k) for k in ("duration", "scrape_wait", "settle", "rps", "rows", "dimension", "request_timeout", "total_budget")},
            "coverage_gaps": ["New scanner startup / initial collection visibility only; not an already-loaded reader stall",
                "Pause gauge is shared by scanners on a channel; not a per-reader registry",
                "No authoritative Milvus channel to WP client metadata namespace/log association",
                "No trace, Loki, native reader or WebUI acceptance; no disk/fsync/crash durability claim"]}
        self.checkpoint()

    def save(self, filename, value):
        raw = json.dumps(value, indent=2, allow_nan=False) + "\n"
        used = sum(p.stat().st_size for p in self.out.iterdir() if p.is_file() and p.name != filename)
        if len(raw.encode()) > 16 << 20 or filename != "acceptance.json" and used + len(raw.encode()) > 96 << 20:
            raise RuntimeError("Acceptance artifact budget exceeded")
        shared.save(self.out / filename, value)

    def checkpoint(self):
        self.report["finished_at"] = shared.utc()
        self.report["owned_collections"] = {name: {k: v for k, v in data.items() if k != "ids"} for name, data in self.owned.items()}
        self.save("acceptance.json", self.report)

    def remaining(self, cap=30):
        value = min(cap, self.deadline - time.monotonic())
        if value <= 0:
            raise RuntimeError("Consume acceptance execution budget exhausted")
        return value

    def wait(self, seconds):
        if seconds > self.remaining(seconds + 1):
            raise RuntimeError("Wait exceeds the remaining execution budget")
        time.sleep(seconds)

    def inventory(self, ready=True):
        ns = json.loads(shared.command(self.kube + ["get", "namespace", self.args.namespace, "-o", "json"]))
        if ns["metadata"].get("labels", {}).get(LABEL) != PROFILE:
            raise RuntimeError("Dedicated disposable namespace opt-in required: " + LABEL + "=" + PROFILE)
        pods = json.loads(shared.command(self.kube + ["get", "pods", "-l", "app.kubernetes.io/instance=" + self.args.release, "-o", "json"]))["items"]
        rows = []
        for pod in pods:
            is_ready = not pod["metadata"].get("deletionTimestamp") and any(c["type"] == "Ready" and c["status"] == "True" for c in pod.get("status", {}).get("conditions", []))
            rows.append({"pod": pod["metadata"]["name"], "uid": pod["metadata"]["uid"], "ready": bool(is_ready),
                "component": pod["metadata"].get("labels", {}).get("component"),
                "containers": [{k: c.get(k) for k in ("name", "image", "imageID", "restartCount", "lastState")} for c in pod["status"].get("containerStatuses", [])]})
        if not rows or ready and not all(p["ready"] for p in rows):
            raise RuntimeError("All selected Milvus/WP Pods must already be Ready")
        if len([p for p in rows if p["component"] in ("mixcoord", "mixcoordinator")]) != 1 or not any(p["component"] == "woodpecker" for p in rows):
            raise RuntimeError("Profile requires one mix coordinator and service-mode WP")
        return sorted(rows, key=lambda p: p["pod"])

    def setup(self):
        self.report["builds"] = self.inventory()
        coordinator = next(p for p in self.report["builds"] if p["component"] in ("mixcoord", "mixcoordinator"))
        self.report["coordinator"] = {k: coordinator[k] for k in ("pod", "uid")}
        lock = json.loads(shared.command(self.kube + ["create", "configmap", LOCK, "--from-literal=run-id=" + self.run_id, "--from-literal=profile=" + PROFILE, "-o", "json"]))
        self.lock_uid = lock["metadata"]["uid"]
        self.report["lock_uid"] = self.lock_uid
        # Persist lock ownership even if a subsequent forward/preflight fails.
        self.checkpoint()
        self.milvus = self.stack.enter_context(shared.Forward(self.kube, "svc/" + self.args.release, 19530))
        self.management = self.stack.enter_context(shared.Forward(self.kube, "pod/" + coordinator["pod"], 9091))
        self.prometheus = self.stack.enter_context(shared.Forward(self.kube, "svc/" + self.args.prometheus_service, 9090))
        self.fault = ConsumeFault(self.management, self.out)
        self.fault.prepare()
        self.report["initial_config"] = self.fault.before
        config = {"schema_version": "wpdiag.config/v1", "sources": {"prometheus": {"connections": {"acceptance": {"settings": {
            "endpoint": self.prometheus, "deployment": self.args.namespace + "/" + self.args.release,
            "limits": {"max_series": 2500}, "match_labels": {"namespace": self.args.namespace, "cluster": "local", "woodpecker_id": self.args.release}}}}}}}
        self.config = self.out / "wpdiag.private.json"
        self.save(self.config.name, config)
        self.report["binary_sha256"] = hashlib.sha256(Path(self.args.wpdiag).read_bytes()).hexdigest()
        self.report["harness_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (Path(__file__), Path(shared.__file__))}
        self.checkpoint()

    def call(self, operation, payload, timeout=None):
        return shared.http(self.milvus, "/v2/vectordb/" + operation, payload,
                           self.remaining(self.args.request_timeout if timeout is None else timeout)).get("data")

    def create(self, suffix):
        name = "consume_accept_" + self.run_id.replace("-", "_") + "_" + suffix
        if self.call("collections/has", {"collectionName": name})["has"]:
            raise RuntimeError("Owned name already exists; refusing to reuse/delete it")
        self.owned[name] = {"ids": [], "acknowledged_rows": 0, "created_at": shared.utc(), "dropped": False}
        self.checkpoint()  # Creation can commit before a timeout.
        self.call("collections/create", {"collectionName": name, "dimension": self.args.dimension,
            "autoID": False, "metricType": "COSINE", "consistencyLevel": "Strong", "params": {"shardsNum": 1}}, 20)
        info = self.call("collections/describe", {"collectionName": name})
        cid = info.get("collectionID")
        if isinstance(cid, bool) or not re.fullmatch(r"[1-9][0-9]{0,19}", str(cid)):
            raise RuntimeError("REST describe lacks an authoritative collection ID")
        self.owned[name]["collection_id"] = str(cid)
        return name

    def vector(self, identity):
        return [((identity * 17 + k * 31) % 251 + 1) / 251 for k in range(self.args.dimension)]

    def insert(self, name, phase):
        ids = self.owned[name]["ids"]
        start = len(ids)
        chunk = list(range(start, start + self.args.rows))
        began = time.monotonic()
        record = {"phase": phase, "operation": "insert", "collection": name, "at": shared.utc(), "ok": False}
        try:
            data = self.call("entities/insert", {"collectionName": name, "data": [{"id": i, "vector": self.vector(i)} for i in chunk]})
            if data.get("insertCount") != len(chunk) or sorted(data.get("insertIds", [])) != chunk:
                raise RuntimeError("Acknowledged insert IDs/count differ from submitted IDs")
            ids.extend(chunk)
            self.owned[name]["acknowledged_rows"] = len(ids)
            record.update(ok=True, first_id=chunk[0], last_id=chunk[-1], rows=len(chunk))
        finally:
            record["latency_ms"] = (time.monotonic() - began) * 1000
            self.records.append(record)

    def probe(self, name, phase):
        identity = len(self.owned[name]["ids"]) - 1
        if identity < 0:
            raise RuntimeError("Visibility probe requires an actually acknowledged row")
        began = time.monotonic()
        record = {"phase": phase, "operation": "strong_query", "collection": name, "expected_id": identity, "at": shared.utc(), "ok": False}
        try:
            data = self.call("entities/query", {"collectionName": name, "filter": "id == " + str(identity),
                "outputFields": ["id"], "limit": 1, "consistencyLevel": "Strong"})
            record["ok"] = len(data) == 1 and data[0].get("id") == identity
            record["result"] = "visible" if record["ok"] else "incorrect_response"
        except RuntimeError as error:
            record["result"] = "bounded_timeout" if isinstance(error, shared.HTTPRequestError) and error.kind == "timeout" else "request_failed"
            record["error"] = str(error)
        record["latency_ms"] = (time.monotonic() - began) * 1000
        self.records.append(record)
        return record

    def verify(self, name):
        expected = self.owned[name]["ids"]
        data = self.call("entities/query", {"collectionName": name, "filter": "id >= 0", "outputFields": ["id"],
            "limit": len(expected), "consistencyLevel": "Strong"}, 20)
        if sorted(row.get("id") for row in data) != expected:
            raise RuntimeError("Strong query IDs differ from every acknowledged owned ID")
        data = self.call("entities/search", {"collectionName": name, "data": [self.vector(0)], "filter": "id == 0",
            "limit": 1, "outputFields": ["id"], "consistencyLevel": "Strong"}, 20)
        if len(data) != 1 or data[0].get("id") != 0:
            raise RuntimeError("Owned acknowledged vector search failed")
        return {"exact_ids": len(expected), "strong_query": True, "search": True}

    def diagnose(self, name, start, end, config=None):
        common = ["--config", str(config or self.config), "--connection", "acceptance", "--scope", "e2e",
                  "--mode", "historical", "--from", start, "--to", end, "--budget", "30s", "-o", "json"]
        reports = []
        for kind, argv in (("consume", [self.args.wpdiag, "milvus", "consume"]),
                           ("integrated", [self.args.wpdiag, "run", "--flow", "read", "--stages", "consume", "--sources", "prometheus"])):
            p = subprocess.run(argv + common, capture_output=True, text=True, timeout=self.remaining(40))
            if len(p.stdout.encode()) > 16 << 20:
                raise RuntimeError("Diagnostic output exceeds its byte budget")
            try:
                report = json.loads(p.stdout)
            except ValueError:
                raise RuntimeError("Diagnostic returned no usable JSON; private output withheld") from None
            self.save(name + "." + kind + ".json", report)
            if p.returncode or report.get("status") != "completed":
                raise RuntimeError("Diagnostic did not complete: " + str(report.get("status")))
            if kind == "consume":
                child = report
            else:
                child = next(r["report"] for r in report["runs"] if r["module_id"] == "milvus.consume")
                originals = {f["id"] for f in stage_findings(child)} | {f["id"] for r in child["results"] for f in r["findings"]}
                if not all(f["id"] in originals for f in report["summary"]["findings"] if f["module_id"] == "milvus.consume"):
                    raise RuntimeError("Integrated summary lost original finding identity")
            shared.lineage(child)
            reports.append(child)
        if signatures(reports[0]) != signatures(reports[1]):
            raise RuntimeError("Standalone/composed consume conclusions, targets or uncertainty differ")
        return reports[0]

    def phase(self, label, name, write=True, control=None):
        print(json.dumps({"stage": label, "run_id": self.run_id}), flush=True)
        self.wait(self.args.scrape_wait)
        start = shared.utc()
        self.report["phases"][label] = {"window": {"from": start}, "collection": name, "status": "running"}
        self.checkpoint()
        self.wait(self.args.scrape_wait)
        offset = len(self.records)
        end_at = time.monotonic() + self.args.duration
        stop = threading.Event()
        failures = []
        def writer():
            sequence = 0
            began = time.monotonic()
            while not stop.is_set() and time.monotonic() < end_at:
                try:
                    self.insert(name, label)
                except Exception as error:
                    failures.append(str(error))
                    return
                sequence += 1
                stop.wait(max(0, began + sequence / self.args.rps - time.monotonic()))
        thread = threading.Thread(target=writer, daemon=True) if write else None
        if thread:
            thread.start()
        try:
            while time.monotonic() < end_at:
                self.probe(name, label)
                if control:
                    self.probe(control, label + "_working_control")
                self.wait(.5)
        finally:
            stop.set()
            if thread:
                thread.join(timeout=self.args.request_timeout + 1)
                if thread.is_alive():
                    raise RuntimeError("Bounded workload writer did not stop")
        self.wait(self.args.scrape_wait)
        end = shared.utc()
        records = self.records[offset:]
        self.save(label + ".requests.json", records)
        queries = [r for r in records if r["operation"] == "strong_query" and r["collection"] == name]
        writes = [r for r in records if r["operation"] == "insert"]
        row = {"window": {"from": start, "to": end}, "collection": name, "writes": len(writes),
            "acknowledged_rows": sum(r.get("rows", 0) for r in writes), "query_results": {key: sum(r.get("result") == key for r in queries)
                for key in ("visible", "bounded_timeout", "incorrect_response", "request_failed")},
            "query_attempt_duration_ms": shared.PERF.distribution([r["latency_ms"] for r in queries]),
            "visible_query_latency_ms": shared.PERF.distribution([r["latency_ms"] for r in queries if r["ok"]]),
            "timeout_attempt_duration_ms": shared.PERF.distribution([r["latency_ms"] for r in queries if r.get("result") == "bounded_timeout"]), "writer_errors": failures}
        self.report["phases"][label] = row
        self.checkpoint()
        # Preserve the actual source view even when a workload/contrast gate
        # fails. A failed request is evidence to inspect, not a reason to omit it.
        report = self.diagnose(label, start, end)
        row["outcomes"] = sorted({f["outcome"] for f in stage_findings(report)})
        row["independent_composed_equal"] = True
        self.checkpoint()
        if failures or not queries or write and not writes or not write and writes:
            raise RuntimeError("Workload population or write correctness gate failed")
        if control:
            controls = [r for r in records if r["operation"] == "strong_query" and r["collection"] == control]
            row["working_control_queries"] = len(controls)
            if not controls or not all(r["ok"] for r in controls) or not all(r.get("result") == "bounded_timeout" for r in queries):
                raise RuntimeError("Paused startup did not contrast with successful existing-collection queries")
            row["load_state"] = self.call("collections/get_load_state", {"collectionName": name})
            if row["load_state"].get("loadState") != "LoadStateLoading":
                raise RuntimeError("Control did not reproduce the documented initial-loading boundary")
        else:
            row["sampled_unpaused_populations"] = len(resumed_gate(report))
            if not all(r["ok"] for r in queries):
                raise RuntimeError("Working/idle/recovery Strong query failed")
            row["correctness"] = self.verify(name)
        if control:
            row["sampled_paused_populations"] = len(pause_gate(report))
        else:
            if write:
                row["observed_consuming_populations"] = len(working_gate(report, self.owned[name]["collection_id"]))
            if any(f["outcome"] in PRESSURE for f in stage_findings(report)):
                raise RuntimeError("Baseline/idle/recovery has independently suspicious pressure; controlled contrast unavailable")
        row["status"] = "passed"
        self.checkpoint()
        return report

    def negative_controls(self, window, collection_id):
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
            argv = [self.args.wpdiag, "milvus", "consume", "--config", str(path), "--connection", "acceptance",
                "--scope", "e2e", "--mode", "historical", "--from", window["from"], "--to", window["to"], "--budget", "15s", "-o", "json"]
            p = subprocess.run(argv, capture_output=True, text=True, timeout=self.remaining(20))
            if len(p.stdout.encode()) > 16 << 20:
                raise RuntimeError("Negative-control diagnostic byte budget exceeded")
            try:
                report = json.loads(p.stdout)
            except ValueError:
                raise RuntimeError("Negative control returned no usable JSON") from None
            self.save(kind + ".consume.json", report)
            try:
                working_gate(report, collection_id)
            except RuntimeError:
                pass
            else:
                raise RuntimeError("Missing evidence passed working-consumption gate")
            if kind == "absent_series" and (p.returncode or report.get("status") != "completed"):
                raise RuntimeError("Absent-series control did not complete its actual source queries")
            if kind == "unreachable_backend" and (not p.returncode or report.get("status") == "completed"):
                raise RuntimeError("Unavailable backend was treated as completed evidence")
            checks[kind] = {"status": report["status"], "exit_code": p.returncode, "working_gate_rejected": True}
        self.report["negative_controls"] = checks
        self.checkpoint()

    def finish(self):
        with shared.cleanup_signals():
            if self.fault and self.fault.changed and not self.fault.restored:
                try:
                    self.fault.restore()
                except Exception as error:
                    self.report["passed"] = False
                    self.report["failures"].append("RESTORATION FAILED: " + str(error))
            restored = not self.fault or not self.fault.changed or self.fault.restored
            self.report["configuration_restored"] = restored
            if restored and self.owned:
                for name, data in self.owned.items():
                    try:
                        # Cleanup is independent of the expired execution budget.
                        drop_owned_collection(self.milvus, name)
                        data["dropped"] = True
                    except Exception as error:
                        self.report["passed"] = False
                        self.report["failures"].append("Owned collection cleanup failed: " + str(error))
            if self.lock_uid and restored and all(v["dropped"] for v in self.owned.values()):
                try:
                    lock = json.loads(shared.command(self.kube + ["get", "configmap", LOCK, "-o", "json"]))
                    if lock["metadata"]["uid"] != self.lock_uid or lock.get("data", {}).get("run-id") != self.run_id:
                        raise RuntimeError("Namespace lock ownership changed")
                    shared.command(self.kube + ["delete", "configmap", LOCK, "--wait=false"])
                except Exception as error:
                    self.report["passed"] = False
                    self.report["failures"].append("Lock cleanup failed: " + str(error))
            self.checkpoint()

    def run(self):
        print(json.dumps({"stage": "started", "run_id": self.run_id, "output": str(self.out)}), flush=True)
        try:
            self.setup()
            baseline = self.create("baseline")
            self.insert(baseline, "prime")
            self.verify(baseline)
            self.phase("baseline", baseline)
            self.negative_controls(self.report["phases"]["baseline"]["window"], self.owned[baseline]["collection_id"])
            self.phase("idle", baseline, write=False)
            self.fault.apply()
            self.wait(self.args.settle)
            paused = self.create("paused")
            self.insert(paused, "paused_prime")
            self.phase("startup_paused", paused, control=baseline)
            self.fault.restore()
            self.wait(self.args.settle)
            self.verify(paused)
            self.phase("recovery", paused)
            window = self.report["phases"]["startup_paused"]["window"]
            historical = self.diagnose("historical_after_recovery", window["from"], window["to"])
            self.report["historical_after_recovery"] = {"sampled_paused_populations": len(pause_gate(historical)),
                "independent_composed_equal": True, "current_recovery_did_not_clear_historical_pause": True}
            self.report["final_builds"] = self.inventory(ready=False)
            if self.report["builds"] != self.report["final_builds"]:
                raise RuntimeError("Selected Pods restarted, changed or lost readiness during this non-restart scenario")
            self.report["passed"] = True
        except (Exception, KeyboardInterrupt) as error:
            self.report["failures"].append(str(error) or "Interrupted")
        finally:
            try:
                try:
                    self.save("all-requests.json", self.records)
                except Exception as error:
                    self.report["passed"] = False
                    self.report["failures"].append("Request artifact could not be retained: " + str(error))
                finally:
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
    p.add_argument("--output", type=Path, default=Path(__file__).parent / ".local/consume")
    for key, value, kind in (("duration", 30, float), ("scrape-wait", 16, float), ("settle", 8, float),
                             ("rps", 4, float), ("rows", 16, int), ("dimension", 16, int), ("request-timeout", 4, float), ("total-budget", 720, float)):
        p.add_argument("--" + key, default=value, type=kind)
    a = p.parse_args(argv)
    for name in ("context", "namespace", "release", "prometheus_service"):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}", getattr(a, name)):
            p.error(name + " must be an explicit bounded identifier")
    for name, lo, hi in (("duration", 20, 90), ("scrape_wait", 1, 30), ("settle", 1, 30), ("rps", 1, 8),
                         ("rows", 1, 64), ("dimension", 8, 128), ("request_timeout", 2, 8), ("total_budget", 120, 1800)):
        if not math.isfinite(getattr(a, name)) or not lo <= getattr(a, name) <= hi:
            p.error(name + " exceeds the consume acceptance bounds")
    if 2 * (math.ceil(a.duration * a.rps) + 2) * a.rows > 16000:
        p.error("Per-collection exact-ID verification must fit 16000 rows")
    a.wpdiag = str(Path(a.wpdiag).resolve())
    return a


def main(argv=None):
    a = arguments(argv)
    if not a.execute:
        print(json.dumps({"profile": PROFILE, "execute": False, "context": a.context, "namespace": a.namespace,
            "required_namespace_label": LABEL + "=" + PROFILE, "temporary_controls": CONTROLS,
            "sequence": ["Ready/identity/provenance and shared exclusive lock", "Working writes/Strong queries", "Intentional idle",
                "Pause new scanners; create owned collection; acknowledged writes versus blocked initial visibility",
                "Restore exact override/value; verify every acknowledged ID and search", "Recovery and retained historical diagnosis", "Drop only owned collections"],
            "cluster_lifecycle": "external", "diagnostic_threshold_changes": False}, indent=2))
        return 0
    return Acceptance(a).run()


if __name__ == "__main__":
    def interrupted(_sig, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
