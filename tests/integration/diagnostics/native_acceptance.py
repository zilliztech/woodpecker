#!/usr/bin/env python3
"""#489 real WP service operations, native diagnosis and focused adapter acceptance.

Plan-only by default. Execution creates a new emptyDir-backed fixture Pod inside
an explicitly opted-in namespace. It never changes an existing service, context,
Helm release, cluster lifecycle, tenant namespace or diagnostic rule/policy.
"""
import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import threading
import time
import uuid

import cli_acceptance as shared
from native_fixture import Fixture, LABEL, PROFILE, OWNER, LOCK, pod_identity

ROOT = Path(__file__).resolve().parents[3]
RULE = {"write": "write_stall", "read": "read_stall"}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def native_lineage(native, imported):
    """Compare one actual native subprocess to its own import, not another run."""
    result = imported["results"][0]
    require(result["source"] == "wpcli", "Wrong imported source")
    require(native["collection_status"] == result["status"], "Changed native collection status")
    originals = {f["id"]: f for f in native["findings"]}
    require(set(originals) == {f["id"] for f in result["findings"]}, "Changed native finding IDs")
    observations = {o["id"]: o for o in native["observations"]}
    require(set(observations) == {o["id"] for o in result["observations"]}, "Changed native observation IDs")
    for obs in result["observations"]:
        original = observations[obs["id"]]
        for key in ("source_id", "started_at", "finished_at", "kind", "status"):
            require(obs[key] == original[key], "Changed native observation provenance")
    for f in result["findings"]:
        original = originals[f["id"]]
        for key in ("rule_id", "rule_version", "flow", "stage", "outcome", "severity", "evidence_strength"):
            require(f[key] == original[key], "Changed native finding semantics")
        for dest, source in (("supporting_observations", "supporting_observations"),
                             ("contradicting_observations", "contradicting_observations"), ("unknown", "unknown")):
            require(set(f.get(dest, [])) == set(original.get(source, [])), "Changed native evidence references/gaps")
        target, scope = f["target"], original["target"]
        require(target["node_id"] == scope["node_id"], "Changed node identity")
        for target_key, scope_key in (("operation_id", "operation_id"), ("log_id", "log_id"), ("segment_id", "segment_id")):
            require(str(target.get(target_key, "")) == str(scope.get(scope_key, "")), "Changed native operation identity")
    return True


def stall_gate(report, flow, expected_log):
    """Require the existing rule's suspicion with its original operation evidence."""
    require(report.get("collection_status") == "completed", "Required native collection incomplete")
    selected = [f for f in report["findings"] if f["rule_id"] == RULE[flow] and f["outcome"] == "suspected_stall"
                and f["target"].get("log_id") == expected_log]
    require(len(selected) == 20, "Expected twenty real pending operations with qualified existing-rule suspicions")
    observations = {o["id"]: o for o in report["observations"]}
    identities = []
    for f in selected:
        scope = f["target"]
        require(f["severity"] == "warning" and f["evidence_strength"] == "suspected", "Unexpected suspicion strength")
        refs = [observations[k] for k in f["supporting_observations"]]
        samples = [o for o in refs if o["kind"] == "ops"]
        keys = []
        for sample in samples:
            ops = [o for o in sample.get("ops", []) if o["op_id"] == scope["operation_id"]]
            require(len(ops) == 1, "Original operation missing from a sampling round")
            keys.append((ops[0]["op_id"], ops[0]["started_at"], ops[0].get("log_id"), ops[0].get("segment_id")))
        require(len(keys) >= 2 and len(set(keys)) == 1, "Operation/start identity did not persist")
        require(any(o["kind"] == "metrics" and o.get("counters") for o in refs), "Completion coverage absent")
        identities.append(keys[0])
    require(len({key[0] for key in identities}) == 20, "Duplicate operation counted twice")
    return identities


class Acceptance:
    def __init__(self, args):
        self.args = args
        self.owner = "native-" + datetime.now(timezone.utc).strftime("%Y%m%dt%H%M%Sz") + "-" + uuid.uuid4().hex[:8]
        self.folder = args.output / self.owner
        self.started = time.monotonic()
        self.stack = ExitStack()
        self.workers = []
        self.worker_files = []
        self.fixture_receipt = {"schema_version": "wpdiag.native.fixture/v1", "profile": PROFILE,
            "run_id": self.owner, "selection": {"context": args.context, "namespace": args.namespace}, "resources": {}}
        self.fixture = Fixture(args.context, args.namespace, self.folder, self.fixture_receipt)
        self.report = {"schema_version": "wpdiag.native.acceptance/v1", "profile": PROFILE, "run_id": self.owner,
            "started_at": shared.utc(), "passed": False, "failures": [], "phases": {},
            "limits": {"total_budget_seconds": args.total_budget, "diagnostic_budget_seconds": 30,
                       "gate_lease_seconds": 70, "rpc_worker_seconds": 75, "pending_requests_per_case": 20,
                       "max_artifact_bytes": 96 << 20, "max_diagnostic_bytes": 16 << 20},
            "selection": self.fixture_receipt["selection"], "coverage_gaps": [
                "Representative MinIO-backed gRPC WP service, not staged WAL disk fsync or multi-replica failure coverage",
                "Direct bounded WP logstore RPC client; no SDK reader lease/quorum/Milvus tenant association",
                "No client etcd/prefix supplied: reader_progress and replica visibility stay explicitly unassessed",
                "Snapshot/current native evidence cannot reconstruct a historical incident",
                "Metrics/ops samples are non-atomic; API coverage, registry bounds and unknowns remain in raw reports",
                "EOF controls exercise actual caught-up RPCs; a normally blocking SDK tail is covered by existing semantic fixtures"]}

    def remaining(self, limit):
        value = min(limit, self.args.total_budget - (time.monotonic() - self.started))
        require(value > 0, "Acceptance total budget exhausted")
        return value

    def checkpoint(self):
        self.report["finished_at"] = shared.utc()
        shared.save(self.folder / "acceptance.json", self.report)

    def artifact_budget(self, incoming=0):
        size = sum(p.stat().st_size for p in self.folder.rglob("*") if p.is_file())
        require(size + incoming <= 96 << 20, "Native acceptance artifact budget exceeded")

    def wait(self, seconds):
        time.sleep(self.remaining(seconds))

    def invoke(self, argv, name, expected=(0,), timeout=35):
        p = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.env)
        try:
            out, err = p.communicate(timeout=self.remaining(timeout))
        except BaseException:
            if p.poll() is None:
                p.send_signal(signal.SIGINT)
                try:
                    p.communicate(timeout=3)
                except subprocess.TimeoutExpired:
                    p.kill()
                    p.communicate(timeout=3)
            raise
        require(len(out) <= 16 << 20 and len(err) <= 64 << 10, "Diagnostic output budget exceeded")
        self.artifact_budget(len(out))
        (self.folder / (name + ".json")).write_bytes(out)
        shared.save(self.folder / (name + ".exit.json"), {"exit": p.returncode})
        require(p.returncode in expected, "Command " + name + " failed; inspect its retained JSON/exit")
        return json.loads(out) if out.strip() else None

    def setup(self):
        self.folder.mkdir(parents=True, mode=0o700)
        self.checkpoint()
        self.fixture.checkpoint()
        self.env = {k: v for k, v in os.environ.items() if k not in ("WOODPECKER_ENDPOINT", "WOODPECKER_CLI_CONFIG")}
        self.report["binary_sha256"] = {key: hashlib.sha256(Path(getattr(self.args, key)).read_bytes()).hexdigest()
                                        for key in ("wp", "wpdiag", "worker")}
        self.report["source_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (Path(__file__), Path(__file__).with_name("native_fixture.py"), Path(__file__).with_name("native_capture.py"))}
        self.fixture.setup({name: getattr(self.args, name) for name in ("gate_image", "wp_image", "minio_image", "prometheus_image")})
        self.admin = self.stack.enter_context(shared.Forward(self.fixture.kube, "pod/" + self.owner, 9091))
        self.rpc = self.stack.enter_context(shared.Forward(self.fixture.kube, "pod/" + self.owner, 18080)).removeprefix("http://")
        self.control = self.stack.enter_context(shared.Forward(self.fixture.kube, "pod/" + self.owner, 19001))
        self.prom = self.stack.enter_context(shared.Forward(self.fixture.kube, "pod/" + self.owner, 19090))
        deadline = time.monotonic() + self.remaining(30)
        members = []
        while time.monotonic() < deadline:
            members = shared.http(self.admin, "/admin/memberlist").get("members", [])
            if members:
                break
            self.wait(.25)
        shared.save(self.folder / "members.json", [{key: row.get(key) for key in ("id", "service_addr", "cluster_name")} for row in members])
        require(len(members) == 1 and members[0]["id"] == self.owner, "Fixture node identity is not authoritative")
        self.cli = self.folder / "cli.private.json"
        shared.save(self.cli, {"contexts": {self.owner: {"endpoint": self.admin, "node_admin_urls": {self.owner: self.admin}}}})
        self.env["WOODPECKER_CLI_CONFIG"] = str(self.cli)
        self.captures = self.folder / "native-captures"
        self.captures.mkdir(mode=0o700)
        self.env.update(WP_NATIVE_ACCEPTANCE_BINARY=self.args.wp, WP_NATIVE_ACCEPTANCE_CAPTURE=str(self.captures))
        wrapper = self.folder / "capture-wp"
        wrapper.write_text("#!" + sys.executable + "\nimport runpy\nrunpy.run_path(" + repr(str(Path(__file__).with_name("native_capture.py"))) + ",run_name='__main__')\n")
        wrapper.chmod(0o700)
        self.config = self.folder / "wpdiag.private.json"
        shared.save(self.config, {"schema_version": "wpdiag.config/v1", "sources": {
            "wpcli": {"connections": {"acceptance": {"settings": {"executable": str(wrapper), "context": self.owner,
                "cli_config": str(self.cli), "native_limits": {"timeout_ms": 2000}}}}},
            "prometheus": {"connections": {"acceptance": {"settings": {"endpoint": self.prom,
                "deployment": self.owner, "match_labels": {"namespace": self.args.namespace,
                "cluster": "native-acceptance", "woodpecker_id": self.owner}}}}}}})
        self.report["pod_before"] = self.fixture_receipt["pod_before"]
        self.checkpoint()

    def native(self, name, scenario, mode="live", expected=(0,), node=None):
        argv = [self.args.wp, "--context", self.owner, "diagnose", node or self.owner,
                "--scenario", scenario, "--mode", mode, "--window", "6s", "--interval", "2s", "--budget", "25s",
                "--timeout", "2s", "--max-nodes", "1", "--max-writers", "64", "-o", "json"]
        return self.invoke(argv, name, expected)

    def imported(self, name, flow, integrated=False, mode="live", window=None, expected=(0,), node=None):
        previous = set(self.captures.glob("*.native.json"))
        if integrated:
            argv = [self.args.wpdiag, "run", "--sources", "wpcli" if mode != "historical" else "prometheus,wpcli"]
        else:
            argv = [self.args.wpdiag, "wpcli", "diagnose"]
        argv += ["--config", str(self.config), "--connection", "acceptance", "--scope", "wp", "--flow", flow,
                 "--mode", mode, "--budget", "30s", "-o", "json"]
        if mode == "live":
            argv += ["--window", "6s", "--interval", "2s"]
        if mode == "historical":
            argv += ["--from", window[0], "--to", window[1], "--current-native", "--target", "wpcli.node_id=" + self.owner]
        elif integrated:
            argv += ["--target", "wpcli.node_id=" + (node or self.owner)]
        else:
            argv += ["--node-id", node or self.owner]
        report = self.invoke(argv, name, expected)
        files = set(self.captures.glob("*.native.json")) - previous
        require(len(files) == 1, "Expected exactly one genuine native subprocess capture")
        original_raw = next(iter(files)).read_bytes()
        (self.folder / (name + ".original-native.json")).write_bytes(original_raw)
        original = json.loads(original_raw) if original_raw.strip() else None
        if original is not None:
            child = next(r["report"] for r in report["runs"] if r["module_id"] == "wpcli") if integrated else report
            native_lineage(original, child)
            if integrated:
                ids = {f["id"] for r in child["results"] for f in r["findings"]}
                summary_ids = {f["id"] for f in report["summary"]["findings"] if f["module_id"] == "wpcli"}
                require(summary_ids <= ids, "Integrated summary replaced native finding IDs")
        return report, original

    def start_work(self, name, operation, log=1001, segment=1, entries=4, parallel=1):
        path = self.folder / (name + ".workload.ndjson")
        output = path.open("xb")
        argv = [self.args.worker, "work", "--owner", self.owner, "--endpoint", self.rpc, "--operation", operation,
                "--log", str(log), "--segment", str(segment), "--entries", str(entries), "--parallel", str(parallel), "--timeout", "75s"]
        p = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=output, stderr=subprocess.DEVNULL, env=self.env)
        self.workers.append(p)
        self.worker_files.append(output)
        return p, path

    def finish_work(self, job):
        p, path = job
        require(p.wait(timeout=self.remaining(80)) == 0, "Real WP workload failed: " + path.name)
        p.stdin.close()
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        require(rows and all(r["ok"] for r in rows), "Missing/failed real operation receipt")
        return rows

    def work(self, name, operation, **kwargs):
        return self.finish_work(self.start_work(name, operation, **kwargs))

    def gate(self, blocked):
        # Persist intent before any possibly committed transport mutation.
        self.report["gate_restore_required"] = blocked
        self.checkpoint()
        data = shared.http(self.control, "/", {"owner": self.owner, "blocked": blocked, "hold_seconds": 70})
        require(data["owner"] == self.owner and data["blocked"] == blocked, "Gate state did not converge")
        shared.save(self.folder / ("gate-" + str(time.time_ns()) + ".json"), data)
        self.report["gate_restore_required"] = blocked
        self.checkpoint()
        return data

    def pending(self, log, op):
        end = time.monotonic() + self.remaining(15)
        while time.monotonic() < end:
            ops = shared.http(self.admin, "/admin/runtime/ops?limit=100")
            matching = [r for r in ops if r.get("op_type") == op and r.get("log_id") == log]
            state = shared.http(self.control, "/")
            if len(matching) == 20 and state["waiting"]:
                shared.save(self.folder / ("pending-" + str(log) + ".json"), {"ops": matching, "gate": state})
                return
            self.wait(.2)
        raise RuntimeError("Twenty real operations did not enter the gate; refuse a silent/idle stall reproduction")

    def fault(self, flow):
        log = 1001 if flow == "write" else 1002
        self.wait(4)
        start = shared.utc()
        self.gate(True)
        job = self.start_work(flow + "-pending", "append" if flow == "write" else "read", log=log,
                              segment=22 if flow == "write" else 2, entries=1 if flow == "write" else 4, parallel=20)
        self.pending(log, "logstore.add_entry" if flow == "write" else "logstore.get_batch_entries")
        snapshot = self.native(flow + "-snapshot", "stuck-" + flow, "snapshot")
        require(all(f["outcome"] == "inconclusive" for f in snapshot["findings"]), "Snapshot invented a sustained stall")
        direct = self.native(flow + "-native", "stuck-" + flow)
        identities = stall_gate(direct, flow, log)
        single, single_native = self.imported(flow + "-adapter", flow)
        stall_gate(single_native, flow, log)
        composed, composed_native = self.imported(flow + "-integrated-live", flow, integrated=True)
        stall_gate(composed_native, flow, log)
        # Expanded read interprets expected work conservatively without tenant meta.
        if flow == "read":
            expanded = self.native("read-pending-expanded", "read")
            require(not any(f["outcome"] == "suspected_catchup_no_progress" for f in expanded["findings"]),
                    "Missing reader expectation invented a catch-up stall")
        state = self.gate(False)
        require(state["automatic_releases"] == 0, "Gate expired before explicit restoration; contrast invalid")
        rows = self.finish_work(job)
        drained = shared.http(self.control, "/")
        require(not drained["blocked"] and drained["waiting"] == 0, "Original object transport did not drain after restoration")
        self.wait(4)
        end = shared.utc()
        recovered = self.native(flow + "-recovery", "stuck-" + flow)
        require(not any(f["outcome"] == "suspected_stall" for f in recovered["findings"]), "Pending suspicion survived completion")
        # Keep immutable incident receipts; fresh recovery is a different run.
        require(stall_gate(direct, flow, log) == identities, "Recovery rewrote retained incident evidence")
        self.report["phases"][flow] = {"historical_window": [start, end], "pending_operations": len(identities),
            "completed_operations": len(rows), "held_requests": state["held_requests"], "native_adapter_lineage_equal": True,
            "restored": True, "gate_drained": True, "incident_retained": True}
        self.checkpoint()
        return start, end

    def history(self, flow, window):
        report, native = self.imported(flow + "-historical-current", flow, integrated=True, mode="historical", window=window)
        current = next(r for r in report["runs"] if r["module_id"] == "wpcli")
        historical = next(r for r in report["runs"] if r["module_id"] == "prometheus")
        require(current["role"] == "current_context" and native["observation_mode"] == "snapshot", "Current native context was backdated")
        require(historical["report"]["selection"]["window"]["mode"] == "historical", "Retained source lost its incident window")
        findings = [f for r in historical["report"]["results"] for f in r["findings"]]
        require(any(f["outcome"] == "suspected_high_latency" for f in findings),
                "Default-qualified historical completion latency missing; do not lower policy")
        require(not any(f["outcome"] == "suspected_stall" for f in native["findings"]), "Historical context resurrected a current stall")
        self.report["phases"][flow]["historical_slow_completions_retained"] = True
        self.checkpoint()

    def run(self):
        code = 1
        try:
            self.setup()
            print(json.dumps({"stage": "fixture-ready", "run_id": self.owner, "output": str(self.folder)}), flush=True)
            lazy = self.native("before-work-lazy", "stuck-write", "snapshot")
            require(not any(o.get("counters") for o in lazy["observations"] if o["kind"] == "metrics"), "Lazy-series control already has traffic")
            self.work("seed-write", "append")
            self.work("seed-read", "append", log=1002)
            self.work("seed-cold-read", "append", log=1002, segment=2)
            self.work("baseline-writes", "append", segment=2, parallel=20)
            self.work("baseline-reads", "read", log=1002, parallel=20)
            self.work("prime-eof", "eof", log=1002)
            self.native("baseline-write-expanded", "write")
            idle = self.native("idle-read", "stuck-read")
            require(not any(f["outcome"] == "suspected_stall" for f in idle["findings"]), "Idle became a stall")
            stop = threading.Event()
            errors = []
            def tail():
                i = 0
                while not stop.is_set():
                    try:
                        self.work("tail-" + str(i), "eof", log=1002)
                    except Exception as error:
                        errors.append(type(error).__name__)
                        return
                    i += 1
                    stop.wait(.5)
            thread = threading.Thread(target=tail, daemon=True)
            thread.start()
            try:
                tail_report = self.native("tail-read-expanded", "read")
            finally:
                stop.set()
                thread.join(timeout=80)
            require(not thread.is_alive() and not errors, "Tail worker did not finish successfully")
            require(any(f["rule_id"] == "read_activity" and f["outcome"] == "eof_observed" for f in tail_report["findings"]), "Real tail EOF was not recognized as progress")
            wrong, _ = self.imported("wrong-node", "write", node="native-unrelated-node", expected=(1,))
            require(wrong["status"] != "completed", "Wrong node became a successful diagnosis")
            print(json.dumps({"stage": "write-fault"}), flush=True)
            write_window = self.fault("write")
            print(json.dumps({"stage": "read-fault"}), flush=True)
            read_window = self.fault("read")
            # Every acknowledged segment, including baseline and blocked writes.
            verified = 0
            for segment in range(1, 42):
                count = 4 if segment <= 21 else 1
                self.work("verify-write-" + str(segment), "read", segment=segment, entries=count)
                verified += count
            for segment in (1, 2):
                self.work("verify-read-" + str(segment), "read", log=1002, segment=segment)
                verified += 4
            self.report["verified_acknowledged_entries"] = verified
            self.native("recovery-write-expanded", "write")
            self.history("write", write_window)
            self.history("read", read_window)
            after = pod_identity(self.fixture.command(["get", "pod", self.owner, "-o", "json"]))
            require(after == self.report["pod_before"], "Fixture process/image/spec identity changed during contrast")
            self.report["pod_after"] = after
            self.report["passed"] = True
            code = 0
        except BaseException as error:
            self.report["passed"] = False
            self.report["failures"].append(type(error).__name__ + ": " + str(error))
            if self.fixture_receipt.get("pod_before"):
                try:
                    pod = self.fixture.command(["get", "pod", self.owner, "-o", "json"])
                    self.report["failure_pod"] = pod_identity(pod)
                    self.report["failure_container_states"] = [{"name": c["name"], "state": c.get("state"),
                        "last_state": c.get("lastState")} for c in pod.get("status", {}).get("containerStatuses", [])]
                    logs = subprocess.run(self.fixture.kube + ["logs", self.owner, "-c", "woodpecker", "--tail=60"],
                                          capture_output=True, timeout=10)
                    (self.folder / "fixture-wp.private.log").write_bytes(logs.stdout[-(32 << 10):])
                except Exception:
                    self.report["coverage_gaps"].append("Failure-time Pod/log context could not be retrieved")
        finally:
            with shared.cleanup_signals():
                # Deleting the original owned ephemeral Pod is also restoration
                # if its control port is unavailable. Always attempt it, even if
                # the explicit early release failed; never leave an avoidable
                # fault merely because its own dependency is unreachable.
                if hasattr(self, "control") and self.report.get("gate_restore_required"):
                    try:
                        self.gate(False)
                    except BaseException:
                        self.report["coverage_gaps"].append("Gate control unavailable during cleanup; restore through original owned Pod removal")
                try:
                    for p in self.workers:
                        if p.poll() is None:
                            p.stdin.close()
                            try:
                                p.wait(timeout=3)
                            except subprocess.TimeoutExpired:
                                p.kill()
                                p.wait(timeout=3)
                    self.fixture.cleanup()
                    self.report["owned_fixture_cleaned"] = bool(self.fixture_receipt.get("cleaned"))
                    if self.report["owned_fixture_cleaned"]:
                        self.report["gate_restore_required"] = False
                except BaseException as error:
                    self.report["passed"] = False
                    self.report["failures"].append("cleanup: " + type(error).__name__ + ": " + str(error))
                    code = 1
                for f in self.worker_files:
                    f.close()
                self.stack.close()
                if self.folder.exists():
                    self.checkpoint()
            print(json.dumps({"stage": "finished", "passed": self.report["passed"], "output": str(self.folder), "failures": self.report["failures"]}), flush=True)
        return code


def arguments(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--context", required=True)
    p.add_argument("--namespace", required=True)
    p.add_argument("--gate-image", required=True, help="Explicit prebuilt native-fixture image available on target nodes")
    p.add_argument("--wp-image", default="milvusdb/woodpecker:v0.1.46")
    p.add_argument("--minio-image", default="milvusdb/minio:RELEASE.2024-12-18T13-15-44Z")
    p.add_argument("--prometheus-image", default="quay.io/prometheus/prometheus:v3.0.1")
    p.add_argument("--wp", default=str(ROOT / "bin/wp"))
    p.add_argument("--wpdiag", default=str(ROOT / "bin/wpdiag"))
    p.add_argument("--worker", default=str(ROOT / "bin/wp-native-fixture"))
    p.add_argument("--output", type=Path, default=Path("/tmp/wp-native-acceptance"))
    p.add_argument("--total-budget", type=int, default=900)
    p.add_argument("--execute", action="store_true")
    a = p.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,200}", a.context) or not re.fullmatch(r"[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?", a.namespace):
        p.error("Explicit bounded context/namespace required")
    if not 300 <= a.total_budget <= 1800:
        p.error("Total budget must be 300..1800s")
    for name in ("gate_image", "wp_image", "minio_image", "prometheus_image"):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/@-]{0,255}", getattr(a, name)):
            p.error("Invalid image reference")
    a.output = a.output.resolve()
    for key in ("wp", "wpdiag", "worker"):
        setattr(a, key, str(Path(getattr(a, key)).resolve()))
    return a


def main():
    a = arguments()
    if not a.execute:
        print(json.dumps({"mode": "plan", "profile": PROFILE, "target": {"context": a.context, "namespace": a.namespace},
            "required_namespace_label": LABEL + "=" + PROFILE, "lock": LOCK,
            "creates": ["owned ConfigMap", "owned Pod: WP + MinIO + gate + Prometheus, emptyDir only"],
            "fault": "70s maximum reversible owned-object HTTP gate; unchanged native rules/policy",
            "steps": ["lazy series", "working append/read", "idle", "caught-up EOF", "write pending", "read pending",
                      "restore", "all acknowledged payloads", "historical/current separation", "UID-guarded cleanup"],
            "cluster_lifecycle": "external", "metadata": "not selected; no tenant discovery", "network_or_mutation": False}, indent=2))
        return 0
    def cancel(*_):
        raise KeyboardInterrupt("Acceptance interrupted; restoring owned fixture")
    signal.signal(signal.SIGTERM, cancel)
    return Acceptance(a).run()


if __name__ == "__main__":
    sys.exit(main())
