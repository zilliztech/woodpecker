#!/usr/bin/env python3
"""Check Jaeger export and distinguish observed spans from complete E2E coverage."""
import argparse
import json
import os
from pathlib import Path
import random
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

JAEGER = os.getenv("JAEGER_URL", "http://127.0.0.1:" + os.getenv("JAEGER_PORT", "16686"))
MILVUS = os.getenv("MILVUS_URL", "http://127.0.0.1:" + os.getenv("MILVUS_PORT", "19530"))
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def get(path):
    with OPENER.open(JAEGER + path, timeout=15) as response:
        body = json.load(response)
    if body.get("errors"):
        raise RuntimeError(str(body["errors"]))
    return body.get("data", [])


def trace_summary(trace):
    processes = trace.get("processes", {})
    spans = trace.get("spans", [])
    services = sorted({p["serviceName"] for p in processes.values()})
    return {"trace_id": trace["traceID"], "spans": len(spans), "services": services,
            "url": JAEGER + "/trace/" + trace["traceID"],
            "operations": sorted({s["operationName"] for s in spans}),
            "scope": "Exported spans only; absence of an uninstrumented child is not zero latency."}


def coverage(operation, services):
    observed = set(services)
    required = {"proxy", "streamingnode"}
    if operation == "entities/insert":
        # One participating WP server is sufficient for a connected trace;
        # three exported service names alone cannot prove parentage or quorum.
        if not any(name.startswith("milvus-woodpecker-") for name in observed):
            required.add("WP server")
    missing = sorted(required - observed)
    return {"status": "partial" if missing else "observed",
            "missing_services": missing,
            "interpretation": "Service presence is a minimum coverage check, not proof that every wait, async task or file.Sync has a span."}


def call(path, payload, requests):
    trace_id, parent_id = uuid.uuid4().hex, uuid.uuid4().hex[:16]
    started = time.monotonic()
    record = {"operation": path, "trace_id": trace_id, "ok": False}
    requests.append(record)
    request = urllib.request.Request(MILVUS + "/v2/vectordb/" + path,
              data=json.dumps(payload).encode(), headers={"Content-Type": "application/json",
              "traceparent": "00-" + trace_id + "-" + parent_id + "-01"})
    try:
        with OPENER.open(request, timeout=90) as response:
            body = json.load(response)
        if body.get("code") != 0:
            raise RuntimeError(str(body))
        record["ok"] = True
        return body.get("data")
    finally:
        record["client_latency_ms"] = (time.monotonic() - started) * 1000


def smoke(report):
    name = "trace_smoke_" + str(time.time_ns())
    report.update(collection=name, requests=[], acknowledged_rows=0)
    requests = report["requests"]
    call("collections/create", {"collectionName": name, "dimension": 8, "autoID": False,
         "metricType": "COSINE", "consistencyLevel": "Strong", "params": {"shardsNum": 1}}, requests)
    rng = random.Random(42)
    rows = [{"id": i, "vector": [rng.random() for _ in range(8)]} for i in range(100)]
    inserted = call("entities/insert", {"collectionName": name, "data": rows}, requests)
    if inserted.get("insertCount") != len(rows):
        raise RuntimeError("Unexpected insert count: " + str(inserted))
    report["acknowledged_rows"] = len(rows)
    found = call("entities/query", {"collectionName": name, "filter": "id >= 0",
                 "outputFields": ["id"], "limit": 100, "consistencyLevel": "Strong"}, requests)
    if {r["id"] for r in found} != set(range(100)):
        raise RuntimeError("Strong query did not return all acknowledged IDs")
    result = call("entities/search", {"collectionName": name, "data": [rows[0]["vector"]],
                  "limit": 3, "outputFields": ["id"], "consistencyLevel": "Strong"}, requests)
    if not result or result[0].get("id") != 0:
        raise RuntimeError("Self vector was not the nearest search result")
    report["correctness"] = {"strong_query_ids": 100, "search": "passed", "collection_retained": True}


def capture(report, output, timeout=30):
    # Exporters batch asynchronously. Do not label a trace partial just because
    # its parent was exported before a child's next batch flush.
    time.sleep(6)
    pending = list(report["requests"])
    deadline = time.monotonic() + timeout
    while pending:
        for record in pending[:]:
            try:
                traces = get("/api/traces/" + record["trace_id"])
            except urllib.error.HTTPError as error:
                if error.code != 404:
                    raise
                continue
            if not traces:
                continue
            trace = traces[0]
            record["trace"] = trace_summary(trace)
            if record["operation"] != "collections/create":
                record["coverage"] = coverage(record["operation"], record["trace"]["services"])
            if output:
                folder = output.parent / (output.stem + "-traces")
                folder.mkdir(parents=True, exist_ok=True)
                path = folder / (record["trace_id"] + ".json")
                path.write_text(json.dumps(trace, indent=2) + "\n")
                record["trace_file"] = str(path)
            pending.remove(record)
        if not pending or time.monotonic() >= deadline:
            break
        time.sleep(1)
    report["missing_trace_ids"] = [r["trace_id"] for r in pending]
    report["coverage_gaps"] = [{"operation": r["operation"], "trace_id": r["trace_id"], **r["coverage"]}
                               for r in report["requests"] if r.get("coverage", {}).get("status") == "partial"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--smoke", action="store_true", help="Insert 100 vectors and trace Strong query/search")
    parser.add_argument("--require-e2e", action="store_true", help="Also fail if a write/query/search trace lacks required services")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    args.require_e2e = args.require_e2e or (args.smoke and os.getenv("TRACES_REQUIRE_E2E") == "1")
    if args.require_e2e and not args.smoke:
        parser.error("--require-e2e needs --smoke")
    report = {"checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "jaeger": JAEGER, "failures": [], "coverage_gaps": []}
    try:
        report["services"] = get("/api/services")
        if args.smoke:
            smoke(report)
            capture(report, args.output)
            report["services"] = get("/api/services")
            if report["missing_trace_ids"]:
                report["failures"].append("Tagged workload traces missing; inspect sampling, export, retention and traceparent propagation")
            if args.require_e2e and report["coverage_gaps"]:
                report["failures"].append("Incomplete E2E service coverage")
        required = {"proxy", "streamingnode", "milvus-woodpecker-0", "milvus-woodpecker-1", "milvus-woodpecker-2"}
        missing = sorted(required - set(report["services"]))
        report["missing_exporters"] = missing
        if missing:
            report["failures"].append("No retained spans from: " + ", ".join(missing))
    except Exception as error:
        report["failures"].append(str(error))
    report["status"] = "failed" if report["failures"] else "partial" if report["coverage_gaps"] else "export-verified"
    report["interpretation"] = "Export verification alone does not establish a complete user-to-storage trace. Inspect coverage_gaps and saved spans."
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return int(bool(report["failures"]))


if __name__ == "__main__":
    raise SystemExit(main())
