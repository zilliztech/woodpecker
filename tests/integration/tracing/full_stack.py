#!/usr/bin/env python3
"""Opted-in real Milvus/WP trace acceptance; no cluster lifecycle or fault injection.

Uses a real instrumented request client, actual Jaeger spans and the independent
diagnostic CLI. Async batch links are causal evidence, never serial timing.
"""
import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import queue
import re
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[3]
LABEL = "diagnostics.woodpecker.io/trace-acceptance"
WAL = {"wal.append", "wal.appendimpl"}
WP = {"LogWriter", "AppendOp", "BatchAppendOp"}
SERVER_APPEND = {"AddEntry", "AddEntries", "AddEntryBatch", "AppendAsync",
                 "woodpecker.proto.logstore.LogStore/AddEntry", "woodpecker.proto.logstore.LogStore/AddEntries"}


def utc():
    return datetime.now(timezone.utc).isoformat()


def save(path, value):
    encoded = json.dumps(value, indent=2, allow_nan=False) + "\n"
    if len(encoded.encode()) > 16 << 20:
        raise RuntimeError("JSON artifact exceeds 16 MiB")
    # Reserve space for the final failed receipt even if raw evidence fills up.
    used = sum(p.stat().st_size for p in path.parent.iterdir() if p.is_file() and p != path)
    if path.name != "acceptance.json" and used + len(encoded.encode()) > 96 << 20:
        raise RuntimeError("Raw artifact budget exhausted (96 MiB)")
    temp = path.with_suffix(".tmp")
    temp.write_text(encoded)
    temp.replace(path)


def command(argv, timeout=30):
    p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    if p.returncode:
        raise RuntimeError("Command failed; private process output withheld")
    return p.stdout


def get(base, path):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(base + path, timeout=5) as response:
            raw = response.read((8 << 20) + 1)
        if len(raw) > 8 << 20:
            raise RuntimeError("Jaeger response exceeds 8 MiB")
        value = json.loads(raw)
    except urllib.error.HTTPError as error:
        error.close()
        if error.code == 404:
            return []
        raise RuntimeError("Jaeger HTTP status " + str(error.code)) from None
    except (OSError, ValueError):
        raise RuntimeError("Jaeger query failed; response/connection values withheld") from None
    if value.get("errors"):
        raise RuntimeError("Jaeger returned backend errors; body withheld")
    return value.get("data", [])


class Forward:
    def __init__(self, kube, target, port):
        self.argv = kube + ["port-forward", "--address", "127.0.0.1", target, ":" + str(port)]
        self.process = None

    def __enter__(self):
        self.process = subprocess.Popen(self.argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        lines = queue.Queue(maxsize=64)
        def read():
            for line in self.process.stdout:
                try:
                    lines.put_nowait(line)
                except queue.Full:
                    pass
        threading.Thread(target=read, daemon=True).start()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and self.process.poll() is None:
            try:
                line = lines.get(timeout=.1)
            except queue.Empty:
                continue
            match = re.search(r"Forwarding from 127\.0\.0\.1:(\d+) ->", line)
            if match:
                return "http://127.0.0.1:" + match[1]
        self.__exit__(None, None, None)
        raise RuntimeError("Local port-forward did not become ready")

    def __exit__(self, *_):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)


def scope(span, process=None):
    # Jaeger 1.53 commonly stores the instrumentation scope on its process row.
    values = {tag.get("value") for tag in span.get("tags", []) + (process or {}).get("tags", [])
              if tag.get("key") in ("otel.scope.name", "otel.library.name")}
    return next(iter(values)) if len(values) == 1 else ""


def graph(raw):
    """Retained local CHILD_OF ancestry only; never turn links into parents."""
    if not 1 <= len(raw.get("spans", [])) <= 5000:
        raise RuntimeError("Trace span budget or shape invalid")
    spans, parents = {}, {}
    for span in raw["spans"]:
        sid = span["spanID"]
        if sid in spans and spans[sid] != span:
            raise RuntimeError("Conflicting duplicate span")
        if span["traceID"] != raw["traceID"] or span["processID"] not in raw["processes"]:
            raise RuntimeError("Trace identity/process mismatch")
        spans[sid] = span
        refs = {r["spanID"] for r in span.get("references", []) if r["refType"] == "CHILD_OF" and r["traceID"] == raw["traceID"]}
        if len(refs) > 1:
            raise RuntimeError("Ambiguous local parents")
        if refs:
            parents[sid] = next(iter(refs))
    def chain(sid):
        out, seen = [], set()
        while sid in spans:
            if sid in seen:
                raise RuntimeError("Cyclic parent graph")
            seen.add(sid)
            out.append(spans[sid])
            sid = parents.get(sid)
        return list(reversed(out))
    for sid in spans:
        chain(sid)
    return spans, chain


def role(raw, span, services):
    return services.get(raw["processes"][span["processID"]]["serviceName"], "other")


def prefix(raw, chain, services):
    """Proxy -> actual SN WAL -> embedded WP client, in observed ancestry."""
    stage, witnesses = 0, []
    for span in chain:
        component = role(raw, span, services)
        if stage == 0 and component == "proxy" and span["operationName"] in ("Proxy-Insert", "Proxy-Insert-Execute", "/v2/vectordb/entities/insert"):
            stage = 1
            witnesses.append(span["spanID"])
        elif stage == 1 and component == "streamingnode" and scope(span, raw["processes"][span["processID"]]) == "milvus.streaming.wal" and span["operationName"] in WAL:
            stage = 2
            witnesses.append(span["spanID"])
        elif stage == 2 and component == "streamingnode" and scope(span, raw["processes"][span["processID"]]) in WP and span["operationName"] in ("Write", "WriteAsync", "Execute"):
            stage = 3
            witnesses.append(span["spanID"])
    return witnesses if stage == 3 else []


def child_proof(raw, services):
    spans, chain = graph(raw)
    for sid, span in spans.items():
        if role(raw, span, services) != "wp_server" or span["operationName"] not in SERVER_APPEND:
            continue
        witness = prefix(raw, chain(sid), services)
        if witness:
            return {"kind": "child_of", "trace_id": raw["traceID"], "span_path": witness + [sid]}
    return None


def linked_proof(original, batch, services):
    origins, origin_chain = graph(original)
    spans, chain = graph(batch)
    for batch_id, candidate in spans.items():
        if role(batch, candidate, services) != "streamingnode" or scope(candidate, batch["processes"][candidate["processID"]]) != "BatchAppendOp" or candidate["operationName"] != "Execute":
            continue
        for ref in candidate.get("references", []):
            if ref["refType"] != "FOLLOWS_FROM" or ref["traceID"] != original["traceID"] or ref["spanID"] not in origins:
                continue
            upstream = prefix(original, origin_chain(ref["spanID"]), services)
            if not upstream:
                continue
            for sid, span in spans.items():
                if role(batch, span, services) == "wp_server" and span["operationName"] in SERVER_APPEND and batch_id in {s["spanID"] for s in chain(sid)}:
                    return {"kind": "async_link", "input_trace_id": original["traceID"], "input_span_id": ref["spanID"],
                            "batch_trace_id": batch["traceID"], "batch_span_id": batch_id, "server_span_id": sid,
                            "interpretation": "Exact retained causal link; not a serial critical path or additive timing"}
    return None


def inventory(kube, release):
    namespace = json.loads(command(kube + ["get", "namespace", kube[4], "-o", "json"]))
    if namespace["metadata"].get("labels", {}).get(LABEL) != "459":
        raise RuntimeError("Explicit opt-in namespace label required: " + LABEL + "=459")
    pods = json.loads(command(kube + ["get", "pods", "-l", "app.kubernetes.io/instance=" + release, "-o", "json"]))["items"]
    if not pods:
        raise RuntimeError("No selected Milvus/WP Pods")
    rows = []
    for pod in pods:
        ready = any(c["type"] == "Ready" and c["status"] == "True" for c in pod.get("status", {}).get("conditions", []))
        if not ready or pod["metadata"].get("deletionTimestamp"):
            raise RuntimeError("All selected Pods must be Ready")
        rows.append({"pod": pod["metadata"]["name"], "uid": pod["metadata"]["uid"],
            "containers": [{k: c.get(k) for k in ("name", "image", "imageID", "restartCount")} for c in pod["status"]["containerStatuses"]]})
    return sorted(rows, key=lambda p: p["pod"])


def diagnose(a, config, trace_id, flow, out, start, end):
    common = ["--config", str(config), "--connection", "fixture", "--scope", "e2e", "--flow", flow,
              "--mode", "historical", "--from", start, "--to", end, "--trace-id", trace_id, "--max-targets", "16", "--budget", "20s", "-o", "json"]
    reports = []
    for kind, argv in (("source", [a.wpdiag, "trace", "diagnose"]), ("integrated", [a.wpdiag, "run", "--modules", "trace"])):
        remaining = a.deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("Acceptance execution budget exhausted")
        p = subprocess.run(argv + common, capture_output=True, text=True, timeout=min(25, remaining))
        if len(p.stdout.encode()) > 16 << 20:
            raise RuntimeError("Diagnostic output budget exceeded")
        try:
            report = json.loads(p.stdout)
        except ValueError:
            raise RuntimeError("Diagnostic returned no usable JSON; private output withheld") from None
        save(out / (trace_id + "." + kind + ".json"), report)
        if p.returncode or report.get("status") != "completed":
            raise RuntimeError("Diagnostic execution did not complete")
        reports.append(report if kind == "source" else report["runs"][0]["report"])
    if signatures(reports[0]) != signatures(reports[1]):
        raise RuntimeError("Independent and composed trace diagnoses disagree")
    return [f["outcome"] for r in reports[0]["results"] for f in r["findings"] if f["rule_id"] == "trace.relationship_coverage"]


def signatures(report):
    # Run/observation UUIDs and collection timestamps legitimately differ.
    # Conclusions, applicability, selected targets and uncertainty must agree.
    return sorted((f["rule_id"], f["rule_version"], f["outcome"], f["severity"], f["evidence_strength"],
                   f["flow"], f["stage"], json.dumps(f["target"], sort_keys=True), f["summary"], tuple(sorted(f["unknown"])))
                  for r in report["results"] for f in r["findings"])


def run(a):
    a.deadline = time.monotonic() + a.budget
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    out = a.output / run_id
    out.mkdir(parents=True, mode=0o700)
    report = {"schema_version": "wpdiag.full-stack-trace-acceptance/v1", "run_id": run_id, "started_at": utc(),
              "selection": {"context": a.context, "namespace": a.namespace, "release": a.release, "expect": a.expect},
              "passed": False, "failures": [], "checks": [], "limits": {"requests": a.requests, "concurrency": a.concurrency, "rows": a.rows, "dimension": a.dimension, "budget_seconds": a.budget, "raw_artifact_bytes": 96 << 20},
              "coverage_gaps": ["Sampling/export/retention completeness unverified", "No disk Write/Sync or crash-durability claim", "Trace causality does not establish client metadata authority"]}
    kube = ["kubectl", "--context", a.context, "-n", a.namespace]
    try:
        report["builds"] = inventory(kube, a.release)
        report["binary_sha256"] = {"wpdiag": hashlib.sha256(Path(a.wpdiag).read_bytes()).hexdigest(),
                                   "request_client": hashlib.sha256(Path(a.request_binary).read_bytes()).hexdigest()}
        services = {"proxy": "proxy", "streamingnode": "streamingnode", "querynode": "querynode", "wp-trace-acceptance-client": "other",
                    **{a.release + "-woodpecker-" + str(i): "wp_server" for i in range(3)}}
        if a.services:
            services = json.loads(a.services.read_text())
        with ExitStack() as stack:
            milvus = stack.enter_context(Forward(kube, "svc/" + a.release, 19530))
            query = stack.enter_context(Forward(kube, "svc/" + a.jaeger_service, 16686))
            ingest = stack.enter_context(Forward(kube, "svc/" + a.jaeger_service, 4318))
            config = out / "connections.private.json"
            save(config, {"schema_version": "wpdiag.config/v1", "sources": {"trace": {"connections": {"fixture": {"settings": {
                "endpoint": query, "deployment": a.namespace + "/" + a.release, "services": services}}}}}})
            receipt = out / "workload.json"
            argv = [a.request_binary, "--execute", "--milvus", milvus, "--otlp", urllib.parse.urlsplit(ingest).netloc,
                    "--output", str(receipt), "--requests", str(a.requests), "--concurrency", str(a.concurrency), "--rows", str(a.rows), "--dimension", str(a.dimension)]
            print(json.dumps({"stage": "workload", "run_id": run_id, "output": str(out)}), flush=True)
            with (out / "worker.log").open("w") as log:
                process = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT)
                try:
                    code = process.wait(timeout=max(.1, min(150, a.deadline - time.monotonic())))
                finally:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=25)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=3)
            workload = json.loads(receipt.read_text())
            report["workload"] = workload
            workload_failed = code or not workload.get("passed") or not workload.get("collection_dropped")
            records = [r for r in workload["records"] if workload_failed or r["operation"] in ("entities/insert", "entities/query", "entities/search")]
            pending = {r["trace_id"]: r for r in records}
            traces = {}
            deadline = min(a.deadline, time.monotonic() + 35)
            while pending and time.monotonic() < deadline:
                for tid, r in list(pending.items()):
                    if time.monotonic() >= deadline:
                        break
                    found = get(query, "/api/traces/" + tid)
                    if found and any(s["spanID"] == r["span_id"] for s in found[0]["spans"]):
                        traces[tid] = found[0]
                        del pending[tid]
                if pending:
                    time.sleep(.25)
            # Parents/children can arrive in later exporter batches than the root.
            time.sleep(6)
            for tid in traces:
                refreshed = get(query, "/api/traces/" + tid)
                if refreshed:
                    traces[tid] = refreshed[0]
                save(out / (tid + ".trace.json"), traces[tid])
            if pending:
                report["missing_request_trace_ids"] = sorted(pending)
                raise RuntimeError("Some tagged request/client spans were not retained within the export budget; available raw traces saved")
            end = utc()
            if workload_failed:
                report["failed_workload_trace_ids"] = sorted(traces)
                for r in records:
                    flow = "read" if r["operation"] in ("entities/query", "entities/search") else "write"
                    try:
                        diagnose(a, config, r["trace_id"], flow, out, report["started_at"], end)
                    except Exception:
                        report["coverage_gaps"].append("Failed-workload diagnosis incomplete for " + r["trace_id"])
                raise RuntimeError("Actual workload, correctness, export or owned-data cleanup failed; retained attempted-request traces; inspect workload.json")
            batches = []
            if a.expect == "linked":
                for service, component in services.items():
                    if component != "streamingnode":
                        continue
                    params = urllib.parse.urlencode({"service": service, "operation": "Execute", "limit": 32,
                        "start": int(datetime.fromisoformat(report["started_at"]).timestamp() * 1e6), "end": int(time.time() * 1e6)})
                    batches.extend(get(query, "/api/traces?" + params))
                if len(batches) > 32:
                    raise RuntimeError("Batch candidate count exceeds its explicit budget")
            batch_diagnoses = {}
            for r in records:
                if time.monotonic() >= a.deadline:
                    raise RuntimeError("Acceptance execution budget exhausted")
                tid = r["trace_id"]
                raw = traces[tid]
                graph(raw)
                flow = "write" if r["operation"] == "entities/insert" else "read"
                outcomes = diagnose(a, config, tid, flow, out, report["started_at"], end)
                proof = child_proof(raw, services) if flow == "write" else None
                if flow == "write" and proof is None and a.expect == "linked":
                    for batch in batches:
                        proof = linked_proof(raw, batch, services)
                        if proof:
                            save(out / (batch["traceID"] + ".trace.json"), batch)
                            if batch["traceID"] not in batch_diagnoses:
                                batch_diagnoses[batch["traceID"]] = diagnose(a, config, batch["traceID"], "write", out, report["started_at"], end)
                            if "observed_stages_connected" in batch_diagnoses[batch["traceID"]]:
                                raise RuntimeError("A linked batch was incorrectly promoted to serial E2E ancestry")
                            break
                check = {"operation": r["operation"], "trace_id": tid, "client_latency_ms": r["client_latency_ms"],
                         "relationship_outcomes": outcomes, "proof": proof, "independent_composed_equal": True}
                report["checks"].append(check)
                if proof and proof["kind"] == "async_link":
                    check["batch_relationship_outcomes"] = batch_diagnoses[proof["batch_trace_id"]]
                if flow == "read" and "observed_stages_connected" not in outcomes:
                    raise RuntimeError("Strong query/search control lacks the supported retained downstream association")
                if flow == "write":
                    if a.expect == "partial":
                        if proof or "observed_stages_connected" in outcomes or "partial_observed_path" not in outcomes:
                            raise RuntimeError("Legacy partial control unexpectedly established a complete retained association")
                    elif not proof:
                        raise RuntimeError("Tagged Insert lacks the required Proxy/SN/WP parent or exact async-link proof")
                    elif proof["kind"] == "child_of" and "observed_stages_connected" not in outcomes and a.expect == "child":
                        raise RuntimeError("Diagnostic catalog did not recognize the verified child ancestry")
                    elif proof["kind"] == "child_of" and "observed_stages_connected" not in outcomes:
                        # A burst may retain one valid direct branch while other
                        # selected async/RPC parents are still absent. Preserve
                        # the source's stricter whole-trace coverage conclusion.
                        if "partial_observed_path" not in outcomes:
                            raise RuntimeError("Diagnostic did not retain an interpretable coverage outcome")
                        check["minimum_branch_only"] = True
            if a.expect == "linked" and not any(c.get("proof", {}).get("kind") == "async_link" for c in report["checks"] if c.get("proof")):
                raise RuntimeError("Coalesced batch control did not reproduce an actual async-linked request")
        report["final_builds"] = inventory(kube, a.release)
        if report["builds"] != report["final_builds"]:
            raise RuntimeError("Selected Pods changed or restarted during the trace scenario")
        report["passed"] = True
    except (Exception, KeyboardInterrupt) as error:
        report["failures"].append(str(error) or "Interrupted")
    finally:
        report["finished_at"] = utc()
        save(out / "acceptance.json", report)
        print(json.dumps({"passed": report["passed"], "report": str(out / "acceptance.json"), "failures": report["failures"]}), flush=True)
    return 0 if report["passed"] else 1


def arguments(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--execute", action="store_true")
    p.add_argument("--context", required=True)
    p.add_argument("--namespace", required=True)
    p.add_argument("--release", default="milvus")
    p.add_argument("--jaeger-service", default="full-stack-jaeger")
    p.add_argument("--services", type=Path, help="Optional exact service-name to diagnostic-role JSON mapping")
    p.add_argument("--expect", choices=("child", "linked", "partial"), default="child")
    p.add_argument("--requests", type=int, default=8)
    p.add_argument("--concurrency", type=int, default=1)
    p.add_argument("--rows", type=int, default=16)
    p.add_argument("--dimension", type=int, default=8)
    p.add_argument("--budget", type=int, default=600, help="Workload/diagnostic execution budget, 120..1800 seconds; cleanup has separate bounds")
    p.add_argument("--output", type=Path, default=Path(__file__).parent / ".local/full-stack")
    p.add_argument("--wpdiag", default=str(ROOT / "bin/wpdiag"))
    p.add_argument("--request-binary", default=str(ROOT / "bin/trace-requests"))
    a = p.parse_args(argv)
    if not 1 <= a.requests <= 32 or not 1 <= a.concurrency <= 8 or not 1 <= a.rows <= 64 or not 8 <= a.dimension <= 128 or not 120 <= a.budget <= 1800:
        p.error("Workload exceeds the request/concurrency/row bounds")
    for key in ("context", "namespace", "release", "jaeger_service"):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}", getattr(a, key)):
            p.error(key + " must be an explicit bounded identifier")
    a.wpdiag, a.request_binary = str(Path(a.wpdiag).resolve()), str(Path(a.request_binary).resolve())
    return a


def main(argv=None):
    a = arguments(argv)
    if not a.execute:
        print(json.dumps({"execute": False, "context": a.context, "namespace": a.namespace, "expected_association": a.expect,
            "workload": {"requests": a.requests, "concurrency": a.concurrency, "rows_per_request": a.rows, "dimension": a.dimension, "budget_seconds": a.budget},
            "required_namespace_label": LABEL + "=459", "cluster_lifecycle": "external",
            "mutations": "one unique owned collection; no deployment configuration or fault injection"}, indent=2))
        return 0
    return run(a)


if __name__ == "__main__":
    def interrupted(_sig, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
