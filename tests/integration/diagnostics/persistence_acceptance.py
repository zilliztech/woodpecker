#!/usr/bin/env python3
"""Opted-in real object-storage CPU slowdown / persistence completion acceptance (#488).

Plan by default. Only this disposable-target harness resizes CPU on the explicitly named
PVC-backed MinIO StatefulSet. Diagnosis remains read-only; policy is unchanged.
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
import consume_acceptance as consume
import query_acceptance as query
from persistence_fault import StorageFault, PROFILE, resources_applied

ROOT = shared.ROOT
LABEL = "diagnostics.woodpecker.io/persistence-acceptance"
LOCK = "wpdiag-cli-acceptance-lock"
NAME = r"persist_accept_[0-9]{8}T[0-9]{6}Z_[0-9a-f]{8}"
EXECUTE = "milvus.persistence.execute"
NORMAL = "within_configured_latency_for_observed_completions"
SLOW = "observed_slow_sync_execution"


def qualified(report, signal, outcomes):
    facts = {o["id"] + ":" + f["population_id"]: f for r in report.get("results", []) for o in r.get("observations", [])
             if o.get("status") == "observed" and o.get("kind") == "prometheus_matrix" and o["data"].get("signal") == signal for f in o["data"].get("facts", [])}
    found = [facts[original] for f in shared.findings(report) if f["rule_id"] == "milvus.persistence.step." + signal and f["outcome"] in outcomes
             for original in f.get("derived_from_findings", []) if original in facts]
    if not found:
        raise RuntimeError("No qualified persistence population for " + signal + ": " + ",".join(sorted(outcomes)))
    return found


def gate(report, phase):
    return qualified(report, EXECUTE, {"idle_or_no_completions"} if phase == "idle" else {SLOW} if phase == "slow_storage" else {NORMAL})


def workload_gate(report, batches, start, rows_per_batch=16):
    rows = report.get("records", [])
    if not report.get("passed") or report.get("mode") != "flush" or len(rows) != batches * 3 + 2 or not all(r.get("ok") for r in rows):
        raise RuntimeError("Insert/query/search/flush RPC populations or exact correctness failed")
    expected = {start + i * rows_per_batch for i in range(batches)}
    by_op = {op: [r for r in rows if r["operation"] == op] for op in ("insert", "query", "search", "flush")}
    if any(len(by_op[op]) != batches or {r["expected_id"] for r in by_op[op]} != expected for op in ("insert", "query", "search")):
        raise RuntimeError("Flush/read IDs differ from acknowledged markers")
    flush_markers = {start + (batches // 2 - 1) * rows_per_batch, start + (batches - 1) * rows_per_batch}
    if len(by_op["flush"]) != 2 or {r["expected_id"] for r in by_op["flush"]} != flush_markers:
        raise RuntimeError("Missing the two rate-bounded actual flush boundaries")
    for r in by_op["insert"]:
        if r.get("rows") != rows_per_batch or r.get("ids") != [str(i) for i in range(r["expected_id"], r["expected_id"] + rows_per_batch)]:
            raise RuntimeError("Actual acknowledged batch IDs/count differ from submitted IDs")
        index = (r["expected_id"] - start) // rows_per_batch
        if r.get("partition") != "persist_%02d" % (index % (batches // 2)):
            raise RuntimeError("Insert partition differs from the owned workload plan")
    ack = {r["expected_id"]: int(r["ack_timestamp"]) for r in by_op["insert"]}
    segments = set()
    for row in by_op["flush"]:
        if not ack[row["expected_id"]] or int(row["ack_timestamp"]) != ack[row["expected_id"]] or int(row["flush_timestamp"]) < ack[row["expected_id"]] or not row.get("segment_ids"):
            raise RuntimeError("Flush boundary was not derived from actual server acknowledgements")
        for identity in row["segment_ids"]:
            if not isinstance(identity, str) or not re.fullmatch(r"[1-9][0-9]{0,18}", identity):
                raise RuntimeError("Invalid original segment identity")
            segments.add(identity)
    for op in ("query", "search"):
        if any(r["consistency"] != "Strong" or int(r["guarantee_timestamp"]) != 0 or int(r["ack_timestamp"]) != ack[r["expected_id"]] for r in by_op[op]):
            raise RuntimeError("Persistence workload changed Strong read consistency")
    if len(segments) < 20:
        raise RuntimeError("Too few real sealed segments for the controlled completion population")
    return {"acknowledged_rows": batches * rows_per_batch, "sealed_segments": sorted(segments),
        "latency_ms": {op: shared.PERF.distribution([r["latency_ms"] for r in records]) for op, records in by_op.items()}}


def drop_owned(endpoint, name):
    if not re.fullmatch(NAME, name):
        raise RuntimeError("Refusing unrelated collection cleanup")
    payload = {"collectionName": name}
    path = "/v2/vectordb/collections/"
    exists = shared.http(endpoint, path + "has", payload, 10).get("data", {}).get("has")
    if type(exists) is not bool:
        raise RuntimeError("Owned collection presence unknown")
    if exists:
        shared.http(endpoint, path + "drop", payload, 20)
        if shared.http(endpoint, path + "has", payload, 10).get("data", {}).get("has") is not False:
            raise RuntimeError("Owned collection still present")


def storage_binding(defaults, override, services):
    """Validate only the allowlisted address/port; never export other settings."""
    first, second = defaults.get("minio", {}), override.get("minio", {})
    if not isinstance(first, dict) or not isinstance(second, dict):
        raise RuntimeError("Unsupported mounted storage configuration")
    host, port = second.get("address", first.get("address")), second.get("port", first.get("port"))
    if host not in services or str(port) != "9000":
        raise RuntimeError("Mounted deployment does not bind the selected MinIO service on port 9000")


class Acceptance:
    def __init__(self, args):
        self.args = args
        self.kube = ["kubectl", "--context", args.context, "-n", args.namespace]
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
        self.out = args.output / self.run_id
        self.out.mkdir(parents=True, mode=0o700)
        self.deadline = time.monotonic() + args.budget
        self.stack = ExitStack()
        self.fault, self.worker, self.lock_uid = None, None, None
        self.owned, self.rows = False, 0
        self.name = "persist_accept_" + self.run_id.replace("-", "_")
        self.report = {"schema_version": "wpdiag.persistence-acceptance/v1", "profile": PROFILE, "run_id": self.run_id,
            "started_at": shared.utc(), "passed": False, "failures": [], "phases": {}, "collection": self.name,
            "selection": {k: getattr(args, k) for k in ("context", "namespace", "release", "storage_statefulset", "storage_service")},
            "limits": {k: getattr(args, k) for k in ("batches", "duration", "slow_hold", "scrape_wait", "budget")},
            "coverage_gaps": ["Induced in-place object-store CPU quota, not a naturally occurring production incident",
                "Completed sync execution includes upload and callbacks; not isolated physical disk/fsync timing",
                "A sampled pending task includes queued AND executing work; idle checkpoint age is not a stall",
                "No authoritative Milvus-to-WP client metadata mapping, trace/Loki/native/UI or crash durability claim",
                "Only representative sync completion contrast; growing/queue/frontier hypotheses retain separate fixture coverage"]}
        self.checkpoint()

    save = query.Acceptance.save
    wait = query.Acceptance.wait
    stop_worker = query.Acceptance.stop_worker

    def remaining(self, cap=30):
        value = min(cap, self.deadline - time.monotonic())
        if value <= 0:
            raise RuntimeError("Persistence acceptance execution budget exhausted")
        return value

    def checkpoint(self):
        self.report.update(finished_at=shared.utc(), collection_owned=self.owned, acknowledged_rows=self.rows, lock_uid=self.lock_uid)
        self.save("acceptance.json", self.report)

    def inventory(self):
        namespace = json.loads(shared.command(self.kube + ["get", "namespace", self.args.namespace, "-o", "json"]))
        if namespace["metadata"].get("labels", {}).get(LABEL) != PROFILE:
            raise RuntimeError("Explicit dedicated namespace opt-in required: " + LABEL + "=" + PROFILE)
        pods = json.loads(shared.command(self.kube + ["get", "pods", "-l", "app.kubernetes.io/instance=" + self.args.release, "-o", "json"]))["items"]
        rows, maps = [], set()
        for p in pods:
            if p["metadata"].get("deletionTimestamp") or not any(c["type"] == "Ready" and c["status"] == "True" for c in p.get("status", {}).get("conditions", [])):
                raise RuntimeError("All selected Milvus/WP Pods must be Ready")
            rows.append({"pod": p["metadata"]["name"], "uid": p["metadata"]["uid"], "component": p["metadata"].get("labels", {}).get("component"),
                "containers": [{k: c.get(k) for k in ("name", "image", "imageID", "restartCount")} for c in p["status"].get("containerStatuses", [])]})
            maps.update(v["configMap"]["name"] for v in p["spec"].get("volumes", []) if "configMap" in v)
        if not any(p["component"] == "woodpecker" for p in rows) or len([p for p in rows if p["component"] == "streamingnode"]) != 1 or len([p for p in rows if p["component"] == "mixcoord"]) != 1:
            raise RuntimeError("Profile requires service WP, one StreamingNode and one mix coordinator")
        fingerprints = []
        for name in sorted(maps):
            cm = json.loads(shared.command(self.kube + ["get", "configmap", name, "-o", "json"]))
            fingerprints.append({"name": name, "uid": cm["metadata"]["uid"], "data_sha256": hashlib.sha256(json.dumps(cm.get("data", {}), sort_keys=True).encode()).hexdigest()})
        return {"pods": sorted(rows, key=lambda r: r["pod"]), "mounted_configmaps": fingerprints}

    def setup(self):
        self.report["builds"] = self.inventory()
        lock = json.loads(shared.command(self.kube + ["create", "configmap", LOCK, "--from-literal=run-id=" + self.run_id, "--from-literal=profile=" + PROFILE, "-o", "json"]))
        self.lock_uid = lock["metadata"]["uid"]
        self.checkpoint()
        self.fault = StorageFault(self.kube, self.args.storage_statefulset, self.out, self.remaining)
        self.fault.prepare()
        self.report["storage_before"] = self.fault.before
        self.checkpoint()
        self.milvus = self.stack.enter_context(shared.Forward(self.kube, "svc/" + self.args.release, 19530))
        self.prometheus = self.stack.enter_context(shared.Forward(self.kube, "svc/" + self.args.prometheus_service, 9090))
        sn = next(p for p in self.report["builds"]["pods"] if p["component"] == "streamingnode")
        coordinator = next(p for p in self.report["builds"]["pods"] if p["component"] == "mixcoord")
        management = self.stack.enter_context(shared.Forward(self.kube, "pod/" + coordinator["pod"], 9091))
        bindings = shared.http(management, "/management/config/get?keys=minio.address,minio.port").get("configs", [])
        config_values = {v["key"]: v.get("value") for v in bindings if not v.get("error")}
        permitted = {self.args.storage_service, self.args.storage_service + "." + self.args.namespace,
                     self.args.storage_service + "." + self.args.namespace + ".svc", self.args.storage_service + "." + self.args.namespace + ".svc.cluster.local"}
        import yaml  # Same pinned preparation dependency as the Helm example.
        maps = self.report["builds"]["mounted_configmaps"]
        if len(maps) != 1:
            raise RuntimeError("Profile requires one shared mounted Milvus configuration map")
        mounted = json.loads(shared.command(self.kube + ["get", "configmap", maps[0]["name"], "-o", "json"]))["data"]
        if set(mounted) != {"default.yaml", "user.yaml"}:
            raise RuntimeError("Unsupported mounted configuration file precedence")
        storage_binding(yaml.safe_load(mounted["default.yaml"]) or {}, yaml.safe_load(mounted["user.yaml"]) or {}, permitted)
        masked = {key for key, value in config_values.items() if value in ("*****", "[REDACTED]")}
        if set(config_values) != {"minio.address", "minio.port"}:
            raise RuntimeError("Management API did not return the selected binding fields")
        if "minio.address" not in masked and config_values["minio.address"] not in permitted or "minio.port" not in masked and str(config_values["minio.port"]) != "9000":
            raise RuntimeError("Effective storage configuration contradicts mounted binding")
        service = json.loads(shared.command(self.kube + ["get", "svc", self.args.storage_service, "-o", "json"]))
        selector = service["spec"].get("selector", {})
        if not selector or not any(p.get("port") == 9000 for p in service["spec"].get("ports", [])):
            raise RuntimeError("Object-store service lacks an explicit selector/port")
        backing = json.loads(shared.command(self.kube + ["get", "pods", "-l", ",".join(k + "=" + v for k, v in sorted(selector.items())), "-o", "json"]))["items"]
        if len(backing) != 1 or backing[0]["metadata"]["uid"] != self.fault.before["pods"][0]["uid"]:
            raise RuntimeError("Object-store service selects unrelated Pods")
        self.report["storage_binding"] = {"service": self.args.storage_service, "uid": service["metadata"]["uid"], "streamingnode": sn["uid"], "coordinator": coordinator["uid"],
            "config_provenance": "mounted default.yaml then user.yaml plus service/controller ownership", "runtime_binding_masked": bool(masked),
            "yaml_version": yaml.__version__, "port": 9000}
        if masked:
            self.report["coverage_gaps"].append("Management API masks object-store binding; mounted file configuration verified, effective overrides not independently readable")
        config = {"schema_version": "wpdiag.config/v1", "sources": {"prometheus": {"connections": {"acceptance": {"settings": {
            "endpoint": self.prometheus, "deployment": self.args.namespace + "/" + self.args.release, "limits": {"max_series": 2500},
            "match_labels": {"namespace": self.args.namespace, "cluster": "local", "woodpecker_id": self.args.release}}}}}}}
        self.config = self.out / "wpdiag.private.json"
        self.save(self.config.name, config)
        self.report["binary_sha256"] = {"wpdiag": hashlib.sha256(Path(self.args.wpdiag).read_bytes()).hexdigest(), "client": hashlib.sha256(Path(self.args.queryclient).read_bytes()).hexdigest()}
        self.report["harness_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (Path(__file__), Path(shared.__file__), Path(query.__file__), Path(__file__).with_name("persistence_fault.py"))}
        if self.call("collections/has", {"collectionName": self.name})["has"]:
            raise RuntimeError("Owned name already exists; refusing to reuse/delete it")
        self.owned = True
        self.checkpoint()
        self.call("collections/create", {"collectionName": self.name, "dimension": 16, "autoID": False, "metricType": "COSINE", "consistencyLevel": "Strong", "params": {"shardsNum": 2}})
        for i in range(self.args.batches // 2):
            self.call("partitions/create", {"collectionName": self.name, "partitionName": "persist_%02d" % i})
        self.call("collections/load", {"collectionName": self.name})
        seed = [{"id": i, "vector": [((i * 17 + k * 31) % 251 + 1) / 251 for k in range(16)]} for i in range(64)]
        ack = self.call("entities/insert", {"collectionName": self.name, "data": seed})
        if ack.get("insertCount") != 64 or sorted(ack.get("insertIds", [])) != list(range(64)):
            raise RuntimeError("Seed acknowledgement IDs/count invalid")
        self.rows = 64
        self.complete_flush()
        self.verify("prime")

    def call(self, path, payload, timeout=20):
        return shared.http(self.milvus, "/v2/vectordb/" + path, payload, self.remaining(timeout)).get("data")

    def assert_lock(self):
        lock = json.loads(shared.command(self.kube + ["get", "configmap", LOCK, "-o", "json"]))
        if lock["metadata"]["uid"] != self.lock_uid or lock.get("data", {}).get("run-id") != self.run_id or lock.get("data", {}).get("profile") != PROFILE:
            raise RuntimeError("Original lock ownership changed; refusing further mutation")

    def complete_flush(self):
        began = time.monotonic()
        self.call("collections/flush", {"collectionName": self.name}, 120)
        return {"at": shared.utc(), "duration_ms": (time.monotonic() - began) * 1000, "completed": True}

    def client(self, label, mode):
        output = self.out / (label + ".workload.json")
        argv = [self.args.queryclient, "--execute", "--parent-pipe", "--endpoint", urllib.parse.urlsplit(self.milvus).netloc,
            "--collection", self.name, "--output", str(output), "--mode", mode, "--phase", "baseline", "--start-id", str(self.rows),
            "--rows", str(self.rows), "--batches", str(self.args.batches), "--rows-per-batch", "16", "--parallel", "1", "--duration", str(self.args.duration) + "s"]
        with (self.out / (label + ".worker.log")).open("w") as log:
            self.worker = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=log, stderr=subprocess.STDOUT)
            try:
                while self.worker.poll() is None:
                    self.remaining()
                    time.sleep(.2)
            finally:
                self.stop_worker()
        result = json.loads(output.read_text())
        if not result.get("passed"):
            raise RuntimeError("Official-protocol workload failed; retain its original receipt")
        return result

    def verify(self, label):
        self.client(label + "_verify", "verify")
        return {"exact_ids": self.rows, "strong_query": True, "vector_search": True}

    def diagnose(self, label, window, config=None, allow_failed=False, instance=None):
        common = ["--config", str(config or self.config), "--connection", "acceptance", "--scope", "e2e", "--mode", "historical",
            "--from", window["from"], "--to", window["to"], "--budget", "30s", "-o", "json"]
        if instance:
            common += ["--instance", instance]
        reports = []
        kinds = [("persistence", [self.args.wpdiag, "milvus", "persistence"])]
        if not allow_failed:
            kinds.append(("integrated", [self.args.wpdiag, "run", "--flow", "write", "--stages", "persistence", "--sources", "prometheus"]))
        for kind, argv in kinds:
            p = subprocess.run(argv + common, capture_output=True, text=True, timeout=self.remaining(40))
            if len(p.stdout.encode()) > 16 << 20:
                raise RuntimeError("Diagnostic output exceeds byte budget")
            result = json.loads(p.stdout)
            self.save(label + "." + kind + ".json", result)
            if allow_failed:
                return p.returncode, result
            if p.returncode or result.get("status") != "completed":
                raise RuntimeError("Persistence diagnostic did not complete")
            child = result if kind == "persistence" else next(r["report"] for r in result["runs"] if r["module_id"] == "milvus.persistence")
            shared.lineage(child)
            if kind == "integrated":
                originals = {f["id"] for f in shared.findings(child)} | {f["id"] for r in child["results"] for f in r["findings"]}
                if not all(f["id"] in originals for f in result["summary"]["findings"] if f["module_id"] == "milvus.persistence"):
                    raise RuntimeError("Integrated summary lost original identity")
            reports.append(child)
        if query.signature(reports[0]) != query.signature(reports[1]):
            raise RuntimeError("Standalone/composed persistence findings differ")
        return reports[0]

    def phase(self, label):
        print(json.dumps({"stage": label, "run_id": self.run_id}), flush=True)
        self.wait(self.args.scrape_wait)
        row = {"window": {"from": shared.utc()}, "status": "running"}
        self.report["phases"][label] = row
        self.checkpoint()
        self.wait(self.args.scrape_wait)
        if label == "idle":
            self.wait(self.args.duration)
        else:
            if label == "slow_storage":
                self.assert_lock()
                row["storage_throttled"] = self.fault.apply()
                row["storage_fault_applied_at"] = shared.utc()
                self.checkpoint()
            workload = self.client(label, "flush")
            # Keep actual acknowledgements even if a later diagnostic gate fails.
            row["workload"] = workload_gate(workload, self.args.batches, self.rows)
            self.rows += self.args.batches * 16
            self.checkpoint()
            if label == "slow_storage":
                self.wait(self.args.slow_hold)
                row["storage_slow_after_workload"] = self.fault.read()
                self.fault.check(row["storage_slow_after_workload"])
                if not resources_applied(row["storage_slow_after_workload"], self.fault.desired()):
                    raise RuntimeError("Object-store CPU quota was not applied through workload")
                self.assert_lock()
                row["storage_restored"] = self.fault.restore()
                row["storage_restored_at"] = shared.utc()
                self.checkpoint()
                if row["storage_restored"]["pods"][0]["containers"] != self.fault.before["pods"][0]["containers"]:
                    raise RuntimeError("Storage container restarted during CPU-only contrast; resources restored but case failed")
            self.wait(11)  # Keep the default per-collection flush quota intact.
            row["completed_flush"] = self.complete_flush()
            row["correctness"] = self.verify(label)
        self.wait(self.args.scrape_wait)
        row["window"]["to"] = shared.utc()
        report = self.diagnose(label, row["window"])
        row.update(outcomes=sorted({f["outcome"] for f in shared.findings(report)}), independent_composed_equal=True)
        qualified_facts = gate(report, label)
        row["qualified_execute_populations"] = qualified_facts
        if label == "slow_storage":
            pending = consume.facts(report, "milvus.persistence.pending")
            if not any(f.get("gauge", {}).get("peak", 0) > 0 for f in pending):
                raise RuntimeError("No retained pending sync work in actual slow-storage interval")
            row["pending_populations"] = pending
        row["status"] = "passed"
        self.checkpoint()

    def negatives(self, window):
        original = json.loads(self.config.read_text())
        checks = {}
        for kind in ("absent_series", "wrong_instance", "unreachable_backend"):
            config = copy.deepcopy(original)
            settings = config["sources"]["prometheus"]["connections"]["acceptance"]["settings"]
            if kind == "unreachable_backend":
                settings["endpoint"] = "http://127.0.0.1:1"
            elif kind == "absent_series":
                settings["match_labels"]["namespace"] = self.args.namespace + "-absent"
            path = self.out / (kind + ".private.json")
            self.save(path.name, config)
            code, report = self.diagnose(kind, window, path, True, "absent-exporter:9091" if kind == "wrong_instance" else None)
            try:
                gate(report, "baseline")
            except RuntimeError:
                pass
            else:
                raise RuntimeError("Missing/wrong source passed working gate")
            if kind == "unreachable_backend" and (not code or report.get("status") == "completed") or kind != "unreachable_backend" and (code or report.get("status") != "completed"):
                raise RuntimeError("Missing versus failed source was not distinguished")
            checks[kind] = {"exit_code": code, "status": report["status"], "working_gate_rejected": True}
        self.report["negative_controls"] = checks

    def finish(self):
        with shared.cleanup_signals():
            self.stop_worker()
            if self.lock_uid:
                try:
                    self.assert_lock()
                except Exception as error:
                    self.report["passed"] = False
                    self.report["failures"].append(str(error))
                    self.checkpoint()
                    return
            restored = self.fault is None or self.fault.before is None
            if self.fault and self.fault.before:
                try:
                    # Recovery has its own bounded allowance after workload expiry.
                    self.fault.remaining = lambda cap=30: cap
                    self.report["storage_final"] = self.fault.restore()
                    restored = True
                except Exception as error:
                    self.report["passed"] = False
                    self.report["failures"].append("Storage restoration failed: " + str(error))
            clean = not self.owned
            if restored and self.owned:
                try:
                    self.assert_lock()
                    drop_owned(self.milvus, self.name)
                    clean = True
                    self.report["collection_dropped"] = True
                except Exception as error:
                    self.report["passed"] = False
                    self.report["failures"].append("Owned cleanup failed: " + str(error))
            if self.lock_uid and restored and clean:
                try:
                    self.assert_lock()
                    shared.command(self.kube + ["delete", "configmap", LOCK, "--wait=false"])
                    self.report["lock_released"] = True
                except Exception as error:
                    self.report["passed"] = False
                    self.report["failures"].append("Lock release failed: " + str(error))
            self.checkpoint()

    def run(self):
        print(json.dumps({"stage": "started", "run_id": self.run_id, "output": str(self.out)}), flush=True)
        try:
            self.setup()
            for phase in ("baseline", "idle", "slow_storage", "recovery"):
                self.phase(phase)
            historical = self.diagnose("historical_after_recovery", self.report["phases"]["slow_storage"]["window"])
            gate(historical, "slow_storage")
            self.report["historical_after_recovery"] = {"slow_execution_retained": True, "independent_composed_equal": True}
            self.negatives(self.report["phases"]["baseline"]["window"])
            self.report["final_builds"] = self.inventory()
            if self.report["builds"] != self.report["final_builds"]:
                raise RuntimeError("Milvus/WP Pods or mounted configuration changed during this case")
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
    p.add_argument("--storage-statefulset", default="full-stack-minio")
    p.add_argument("--storage-service", default="full-stack-minio")
    p.add_argument("--prometheus-service", default="monitoring-kube-prometheus-prometheus")
    p.add_argument("--wpdiag", default=str(ROOT / "bin/wpdiag"))
    p.add_argument("--queryclient", default=str(ROOT / "bin/query-requests"))
    p.add_argument("--output", type=Path, default=Path(__file__).parent / ".local/persistence")
    for key, value, kind in (("batches", 24, int), ("duration", 30, int), ("slow-hold", 20, float), ("scrape-wait", 16, float), ("budget", 1200, float)):
        p.add_argument("--" + key, default=value, type=kind)
    a = p.parse_args(argv)
    for key in ("context", "namespace", "release", "storage_statefulset", "storage_service", "prometheus_service"):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}", getattr(a, key)):
            p.error("Explicit bounded identifier required for " + key)
    for key, lo, hi in (("batches", 20, 48), ("duration", 24, 60), ("slow_hold", 15, 60), ("scrape_wait", 1, 30), ("budget", 300, 1800)):
        if not math.isfinite(getattr(a, key)) or not lo <= getattr(a, key) <= hi:
            p.error(key + " exceeds persistence fixture bounds")
    if a.batches % 2:
        p.error("batches must be even for two rate-bounded flushes")
    a.wpdiag, a.queryclient = str(Path(a.wpdiag).resolve()), str(Path(a.queryclient).resolve())
    return a


def main(argv=None):
    a = arguments(argv)
    if not a.execute:
        print(json.dumps({"profile": PROFILE, "execute": False, "context": a.context, "namespace": a.namespace,
            "required_namespace_label": LABEL + "=" + PROFILE, "cluster_lifecycle": "external", "milvus_configuration_changes": [],
            "fault": {"statefulset": a.storage_statefulset, "pod_cpu": "25m request/limit -> exact original resources", "replicas": "unchanged", "persistent_claims": "unchanged"},
            "sequence": ["Ready/bound target/shared lock", "Journal storage before owned data", "Prime/baseline and idle", "In-place CPU quota/real seal requests",
                "Restore exact pre-existing CPU request/limit", "Completed flush/exact query/search", "Normal recovery/historical/negative controls", "Owned cleanup"]}, indent=2))
        return 0
    return Acceptance(a).run()


if __name__ == "__main__":
    def interrupted(_sig, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
