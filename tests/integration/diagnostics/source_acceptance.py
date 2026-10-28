#!/usr/bin/env python3
"""Real WP -> Alloy/Loki + OTLP/Jaeger acceptance (#491); offline plan by default."""
import argparse
import copy
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import time
import uuid
import urllib.parse

import cli_acceptance as shared
import source_fixture as fixture


def save(path, value):
    raw = (json.dumps(value, indent=2, allow_nan=False) + "\n").encode()
    if len(raw) > 16 << 20:
        raise RuntimeError("Artifact exceeds 16 MiB")
    used = sum(p.stat().st_size for p in path.parent.iterdir() if p.is_file() and p != path)
    if path.name != "acceptance.json" and used + len(raw) > 96 << 20:
        raise RuntimeError("Run artifacts exceed 96 MiB")
    shared.save(path, value)


def findings(report):
    return [f for result in report.get("results", []) for f in result.get("findings", [])]


def signature(report):
    return sorted((f["rule_id"], f["rule_version"], f["outcome"], f["severity"], f["evidence_strength"],
                   f["flow"], f["stage"], json.dumps(f["target"], sort_keys=True), f["summary"], tuple(sorted(f["unknown"])))
                  for f in findings(report))


def events(report):
    return sorted([e for r in report.get("results", []) for o in r.get("observations", [])
            if o.get("kind") == "loki_log_page" and o.get("status") == "observed" for e in o.get("data", {}).get("events", [])], key=lambda e: e["event_id"])


def trace_spans(report):
    return sorted([s for r in report.get("results", []) for o in r.get("observations", [])
            if o.get("kind") == "jaeger_trace" and o.get("status") == "observed" for s in o.get("data", {}).get("spans", []) if s.get("selected")],
            key=lambda s: (s["trace_id"], s["span_id"]))


def equivalent(independent, child):
    shared.lineage(independent)
    shared.lineage(child)
    def execution(report):
        return report.get("status"), sorted((r.get("source", ""), r.get("profile", ""), r.get("status", ""),
            tuple(sorted(g["code"] for g in r.get("coverage_gaps", [])))) for r in report.get("results", []))
    if execution(independent) != execution(child) or signature(independent) != signature(child) or events(independent) != events(child) or trace_spans(independent) != trace_spans(child):
        raise RuntimeError("Independent/composed source findings, uncertainty or event lineage differ")


def positive_gate(report, trace_id):
    children = {r["module_id"]: r["report"] for r in report["runs"]}
    selected = [e for e in events(children["loki"]) if e["pattern"] == "wp.append_failure" and e.get("trace_id") == trace_id]
    if not selected or not any(s["trace_id"] == trace_id for s in trace_spans(children["trace"])):
        raise RuntimeError("Real producer event or retained supported span identity absent")
    joins = [a for a in report["summary"]["associations"] if a["identity"] == trace_id]
    if len(joins) != 1:
        raise RuntimeError("Genuine trace/log event-time association gate incomplete")
    event_ids = {e["event_id"] for e in selected}
    if not any(o.get("event_id") in event_ids for o in joins[0]["observations"]):
        raise RuntimeError("Association lacks original producer event lineage")
    referenced_spans = {o.get("span_id") for o in joins[0]["observations"] if o["module_id"] == "trace"}
    if not any(s["span_id"] in referenced_spans and s["trace_id"] == e["trace_id"] and
               s["start_us"] * 1000 <= int(e["timestamp_ns"]) <= (s["start_us"] + s["duration_us"]) * 1000
               for s in trace_spans(children["trace"]) for e in selected):
        raise RuntimeError("Original event time does not overlap a referenced selected span")
    if any(o["module_id"] == "prometheus" for o in joins[0]["observations"]):
        raise RuntimeError("Aggregate metric wrongly associated to an individual request")
    return {"trace_id": trace_id, "event_ids": sorted(event_ids), "association": joins[0]}


class Run:
    def __init__(self, args):
        self.a = args
        self.owner = "source-" + datetime.now(timezone.utc).strftime("%Y%m%dt%H%M%Sz") + "-" + uuid.uuid4().hex[:8]
        self.out = args.output / self.owner
        self.out.mkdir(parents=True, mode=0o700)
        self.receipt = {"schema_version": "wpdiag.source-acceptance/v1", "run_id": self.owner, "profile": fixture.PROFILE,
                        "context": args.context, "namespace": args.namespace, "started_at": shared.utc(), "passed": False,
                        "checks": [], "failures": [], "resources": {}, "coverage_gaps": [
                            "Representative real WP MinIO backend, not staged disk durability or Milvus E2E release proof",
                            "Missing instrumentation/parents, sampling/export and collection completeness remain unknown",
                            "No real Milvus producer fault or query-path trace acceptance is claimed by this WP subset"]}
        self.fixture = fixture.Fixture(args.context, args.namespace, self.out, self.receipt)
        self.deadline = time.monotonic() + args.budget
        self.fixture.deadline = self.deadline

    def remaining(self, reserve=0):
        remaining = self.deadline - time.monotonic()
        if remaining <= reserve:
            raise RuntimeError("Whole acceptance work budget exhausted")
        return remaining

    def pause(self, seconds):
        self.remaining(seconds)
        time.sleep(seconds)

    def checkpoint(self):
        save(self.out / "acceptance.json", self.receipt)

    def diagnose(self, name, modules, flow, trace_id=None, scope="wp", config=None, failed=False):
        config = config or self.config
        common = ["--config", str(config), "--connection", "acceptance", "--scope", scope, "--flow", flow,
                  "--mode", "historical", "--from", self.start, "--to", self.end, "--budget", "20s", "-o", "json"]
        if trace_id:
            common += ["--trace-id", trace_id]
        standalone = {}
        for module in modules:
            selected = [v for v in common]
            if module != "trace" and trace_id:
                selected = selected[:-2]
            argv = [self.a.wpdiag, module, "diagnose"] + selected
            p, report = self.invoke(argv)
            save(self.out / (name + "." + module + ".json"), report)
            if not failed and (p.returncode or report["status"] != "completed"):
                raise RuntimeError("Standalone source execution did not complete: " + module)
            standalone[module] = report
        p, result = self.invoke([self.a.wpdiag, "run", "--modules", ",".join(modules)] + common)
        save(self.out / (name + ".integrated.json"), result)
        if not failed and (p.returncode or result["status"] != "completed"):
            raise RuntimeError("Integrated source execution did not complete")
        for row in result["runs"]:
            equivalent(standalone[row["module_id"]], row["report"])
        self.receipt["checks"].append({"case": name, "modules": modules, "scope": scope, "flow": flow,
                                       "status": result["status"], "associations": len(result["summary"]["associations"])})
        self.checkpoint()
        print(json.dumps({"stage": name, "status": result["status"], "associations": len(result["summary"]["associations"])}), flush=True)
        return result

    def invoke(self, argv):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("Whole acceptance budget exhausted")
        p = subprocess.run(argv, capture_output=True, text=True, timeout=min(25, remaining))
        if len(p.stdout.encode()) > 16 << 20:
            raise RuntimeError("Diagnostic output exceeds 16 MiB")
        try:
            return p, json.loads(p.stdout)
        except ValueError:
            raise RuntimeError("Diagnostic returned no JSON; private output withheld") from None

    def run(self):
        self.checkpoint()
        print(json.dumps({"stage": "setup", "run_id": self.owner, "output": str(self.out)}), flush=True)
        try:
            self.receipt["binaries"] = {"wpdiag": hashlib.sha256(Path(self.a.wpdiag).read_bytes()).hexdigest()}
            self.receipt["request_client_source_sha256"] = hashlib.sha256((shared.ROOT / "tests/integration/diagnostics/sourceclient/main.go").read_bytes()).hexdigest()
            self.fixture.setup()
            self.receipt["existing_before"] = self.fixture.existing_inventory()
            with ExitStack() as stack:
                endpoints = {}
                for name, port in (("trace", 16686), ("loki", 3100), ("prometheus", 19090)):
                    self.remaining(30)  # Shared forward has a bounded 30s startup.
                    endpoints[name] = stack.enter_context(shared.Forward(self.fixture.kube, "pod/" + self.owner, port))
                config = {"schema_version": "wpdiag.config/v1", "sources": {}}
                for source in ("trace", "loki", "prometheus"):
                    settings = {"endpoint": endpoints[source], "deployment": self.owner}
                    if source == "trace":
                        settings["services"] = {self.owner: "wp_server", self.owner + "-client": "wp_client"}
                        settings["search"] = {"service": self.owner + "-client", "operation": "woodpecker.proto.logstore.LogStore/AddEntry"}
                    else:
                        settings["match_labels"] = {"namespace": self.a.namespace, "cluster": "source-acceptance"}
                        if source == "prometheus":
                            settings["match_labels"]["woodpecker_id"] = self.owner
                    config["sources"][source] = {"connections": {"acceptance": {"settings": settings}}}
                self.config = self.out / "wpdiag.json"
                save(self.config, config)
                self.start = shared.utc()
                self.pause(6)  # Initial actual scrapes, not fabricated counter baselines.
                p = subprocess.run(self.fixture.kube + ["exec", self.owner, "-c", "client", "--", "/source-client",
                                    "--endpoint", "127.0.0.1:18080", "--otlp", "127.0.0.1:4318",
                                    "--owner", self.owner, "--execute"], capture_output=True, text=True, timeout=min(100, self.remaining()))
                records = [json.loads(line) for line in p.stdout.splitlines()]
                save(self.out / "requests.json", {"records": records, "exit_code": p.returncode,
                                                        "fixture_client_stderr": p.stderr[:16384]})
                if p.returncode or len(records) != 14:
                    raise RuntimeError("Real RPC workload failed; partial request receipts retained")
                by_case = {r["case"]: r for r in records}
                self.receipt["requests"] = records
                # Bounded delivery/export wait. Absence after this grace remains failure,
                # never rewritten as no error or silently joined by timestamp.
                self.pause(12)
                self.end = shared.utc()
                # Preserve failed export/format evidence before owned ephemeral
                # storage is removed. Only the fixture's private local folder.
                save(self.out / "retained-services.json", shared.http(endpoints["trace"], "/api/services", timeout=min(10, self.remaining())))
                for service in (self.owner, self.owner + "-client"):
                    query = urllib.parse.urlencode({"service": service, "limit": 32,
                        "start": int(datetime.fromisoformat(self.start).timestamp() * 1000000),
                        "end": int(datetime.fromisoformat(self.end).timestamp() * 1000000)})
                    save(self.out / (service + ".retained.json"), shared.http(endpoints["trace"], "/api/traces?" + query, timeout=min(10, self.remaining())))
                raw = shared.command(self.fixture.kube + ["logs", self.owner, "-c", "woodpecker", "--tail=100"], timeout=min(30, self.remaining()))
                save(self.out / "producer-lines.json", {"lines": raw.splitlines()})
                for name, expression in (("up", "up"), ("append-completions", 'woodpecker_server_logstore_operation_latency_count{operation="add_entry",status="success"}'),
                                         ("read-completions", 'woodpecker_server_logstore_operation_latency_count{operation="get_batch_entries",status="success"}')):
                    save(self.out / (name + ".current-metrics.json"), shared.http(endpoints["prometheus"], "/api/v1/query?" + urllib.parse.urlencode({"query": expression}), timeout=min(10, self.remaining())))
                result = self.diagnose("wp-write", ["trace", "loki", "prometheus"], "write")
                associated, unresolved = [], []
                for row in records:
                    if not row["case"].startswith("rejected_empty_append_"):
                        continue
                    try:
                        associated.append(positive_gate(result, row["trace_id"]))
                    except RuntimeError:
                        unresolved.append(row["trace_id"])
                self.receipt["error_association_coverage"] = {"emitted": 10, "associated": len(associated), "unresolved_trace_ids": unresolved}
                if not associated:
                    raise RuntimeError("No actual rejected RPC had strict trace/log event-time overlap; gate remains incomplete")
                self.receipt["genuine_association"] = associated[0]
                error_id = associated[0]["trace_id"]
                self.diagnose("e2e-write-partial", ["trace", "loki"], "write", scope="e2e")
                wrong = self.diagnose("unrelated-trace-same-window", ["trace", "loki"], "write", by_case["baseline"]["trace_id"])
                if wrong["summary"]["associations"]:
                    raise RuntimeError("Unrelated request was associated solely by incident window")
                read = self.diagnose("wp-read", ["trace", "loki", "prometheus"], "read", by_case["read"]["trace_id"])
                if not trace_spans(next(r["report"] for r in read["runs"] if r["module_id"] == "trace")):
                    raise RuntimeError("Actual read trace selection absent")
                self.diagnose("e2e-read-wp-only", ["trace", "loki"], "read", by_case["read"]["trace_id"], "e2e")
                absent_config = copy.deepcopy(config)
                absent_config["sources"]["trace"]["connections"]["acceptance"]["settings"]["search"]["operation"] = "Write"
                absent = self.out / "not-emitted-operation.json"
                save(absent, absent_config)
                missing = self.diagnose("not-retained-search", ["trace"], "write", config=absent)
                if not any(f["rule_id"] == "trace.no_retained_evidence" and f["severity"] == "unknown"
                           for r in missing["runs"] for f in findings(r["report"])):
                    raise RuntimeError("No-retained-evidence did not remain unknown")
                exact_missing = self.diagnose("not-retained-exact-id", ["trace"], "write", uuid.uuid4().hex, failed=True)
                if exact_missing["status"] not in ("failed", "partial"):
                    raise RuntimeError("Exact missing lookup HTTP error was hidden")
                # No Milvus producer runs in this owned fixture. This is an explicit
                # incompatible-selection control, not a positive Milvus acceptance.
                self.diagnose("milvus-write-no-selected-spans", ["trace", "loki"], "write", error_id, "milvus")
                self.diagnose("milvus-read-no-selected-spans", ["trace", "loki"], "read", by_case["read"]["trace_id"], "milvus")
                self.unsupported_read_stage()
                previous_start, previous_end = self.start, self.end
                # A new idle observation AFTER the export/ingestion grace and
                # successful data recovery. CRI timestamps may lag RPC receipt;
                # never pretend the RPC-start instant splits application logs.
                self.start = shared.utc()
                self.pause(2)
                self.end = shared.utc()
                recovery = self.diagnose("post-recovery-idle-no-match", ["loki"], "write")
                if events(recovery["runs"][0]["report"]) or not all(f["outcome"] == "inconclusive" for f in findings(recovery["runs"][0]["report"])):
                    raise RuntimeError("Recovery no-match was misinterpreted or still includes the rejected request")
                self.start, self.end = previous_start, previous_end
                limited_config = copy.deepcopy(config)
                limited_config["sources"]["loki"]["connections"]["acceptance"]["settings"]["limits"] = {
                    "max_queries": 1, "page_size": 1, "max_lines": 1}
                limited_path = self.out / "one-line-budget.json"
                save(limited_path, limited_config)
                limited = self.diagnose("loki-line-budget", ["trace", "loki"], "write", error_id, config=limited_path, failed=True)
                if not any(r["report"]["status"] == "budget_exceeded" for r in limited["runs"] if r["module_id"] == "loki"):
                    raise RuntimeError("Real returned-line budget was not enforced")
                self.cancel_live()
                config["sources"]["loki"]["connections"]["acceptance"]["settings"]["endpoint"] = "http://127.0.0.1:1"
                bad = self.out / "unreachable.json"
                save(bad, config)
                requested = self.diagnose("requested-unreachable", ["trace", "loki"], "write", error_id, config=bad, failed=True)
                if requested["status"] != "partial":
                    raise RuntimeError("Explicit failed source was hidden")
                self.diagnose("unselected-unreachable", ["trace"], "write", error_id, config=bad)
                self.inspect_sources(endpoints)
                after = self.fixture.command(["get", "pod", self.owner, "-o", "json"])
                self.receipt["pod_after"] = fixture.pod_identity(after)
                if self.receipt["pod_after"] != self.receipt["pod_before"]:
                    raise RuntimeError("Selected image/settings/process changed during capture")
                self.receipt["existing_after"] = self.fixture.existing_inventory()
                if self.receipt["existing_after"] != self.receipt["existing_before"]:
                    raise RuntimeError("Existing workload/configuration identity changed during capture")
                self.receipt["passed"] = True
        except (Exception, KeyboardInterrupt) as error:
            self.receipt["failures"].append(type(error).__name__ + ": " + str(error))
            self.receipt["passed"] = False
        finally:
            with shared.cleanup_signals():
                try:
                    self.fixture.cleanup()
                except Exception as error:
                    self.receipt["failures"].append("cleanup: " + str(error))
                    self.receipt["passed"] = False
            self.receipt["finished_at"] = shared.utc()
            self.checkpoint()
        print(json.dumps({"passed": self.receipt["passed"], "failures": self.receipt["failures"], "output": str(self.out)}), flush=True)
        return 0 if self.receipt["passed"] else 1

    def inspect_sources(self, endpoints):
        """Optional consumer acceptance while owned sources and lock are alive."""

    def cancel_live(self):
        self.remaining(8)
        process = subprocess.Popen([self.a.wpdiag, "trace", "diagnose", "--config", str(self.config), "--connection", "acceptance",
            "--scope", "wp", "--flow", "write", "--mode", "live", "--window", "30s", "--budget", "40s", "-o", "json"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            time.sleep(.5)
            process.send_signal(signal.SIGINT)
            stdout, _ = process.communicate(timeout=5)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=3)
        report = json.loads(stdout)
        save(self.out / "cancelled-live.json", report)
        if process.returncode != 7 or report["status"] != "cancelled" or any(
                o.get("query", {}).get("executed") for r in report.get("results", []) for o in r.get("observations", [])):
            raise RuntimeError("Live cancellation did not stop before collection")
        self.receipt["checks"].append({"case": "cancelled-live", "status": "cancelled", "exit_code": process.returncode})

    def unsupported_read_stage(self):
        p, report = self.invoke([self.a.wpdiag, "trace", "diagnose", "--config", str(self.config), "--connection", "acceptance",
            "--scope", "e2e", "--flow", "read", "--stage", "consume", "--mode", "historical", "--from", self.start,
            "--to", self.end, "-o", "json"])
        save(self.out / "unsupported-read-stage.json", {"exit_code": p.returncode, "report": report})
        if p.returncode != 3 or report["status"] != "unsupported" or any(
                (o.get("query") or {}).get("executed") for r in report.get("results", []) for o in r.get("observations", [])):
            raise RuntimeError("Trace read profile was incorrectly extended to WAL consumption")
        self.receipt["checks"].append({"case": "unsupported-read-stage", "status": "unsupported", "exit_code": p.returncode})


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--context", required=True)
    p.add_argument("--namespace", required=True)
    p.add_argument("--output", type=Path, default=Path("/tmp/wp-source-491"))
    p.add_argument("--wpdiag", default=str(shared.ROOT / "bin/wpdiag"))
    p.add_argument("--budget", type=int, default=480)
    p.add_argument("--execute", action="store_true")
    a = p.parse_args()
    if not 240 <= a.budget <= 900:
        p.error("budget must be 240..900 seconds")
    if not a.execute:
        print(json.dumps({"execute": False, "context": a.context, "namespace": a.namespace, "profile": fixture.PROFILE,
                          "requires_namespace_label": fixture.LABEL + "=" + fixture.PROFILE,
                          "images": fixture.IMAGES, "resources": ["exclusive lock", "owned ConfigMap/Pod/ServiceAccount/Role/RoleBinding"],
                          "workload": "2 real synced entries, 10 rejected empty append attempts, exact read and EOF",
                          "cluster_lifecycle": "external; no current-context changes"}, indent=2))
        return 0
    def interrupted(*_):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, interrupted)
    return Run(a).run()


if __name__ == "__main__":
    raise SystemExit(main())
