#!/usr/bin/env python3
"""Verify spans emitted by the actual WP queue/remote-gRPC regression in Jaeger.

Creates one disposable Jaeger; never changes Kubernetes or runs a Milvus workload.
The Go fixture acknowledges protocol messages without storing data. This is a
propagation check, not a disk/durability or complete Milvus E2E acceptance test.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[3]


def command(*args, **kwargs):
    proc = subprocess.run(args, capture_output=True, text=True, check=False, **kwargs)
    if proc.returncode:
        raise RuntimeError(f"{args[0]} failed ({proc.returncode}): {proc.stdout}\n{proc.stderr}")
    return proc.stdout.strip()


def get(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def retained(query, trace_id, minimum):
    deadline = time.monotonic() + 20
    traces = []
    while True:
        try:
            traces = get(query + "/api/traces/" + trace_id).get("data", [])
            if traces and len(traces[0]["spans"]) >= minimum:
                assert traces[0]["traceID"].lower().zfill(32) == trace_id
                return traces[0]
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
        if time.monotonic() > deadline:
            raise AssertionError("Actual emitted spans not retained: " + trace_id + " (last span count=" + str(len(traces[0]["spans"]) if traces else 0) + ")")
        time.sleep(.1)


def validate_trace(receipt, trace):
    """Validate emitted transport identity without a diagnostic CLI dependency."""
    trace_id = receipt["execution_trace_id"]
    assert receipt["case"] in {"single", "single-error", "batch"}
    assert trace["traceID"].lower().zfill(32) == trace_id
    spans = trace["spans"]
    span_ids = {s["spanID"] for s in spans}
    assert len(span_ids) == len(spans), "Duplicate retained span identities"
    for span in spans:
        assert span["traceID"].lower().zfill(32) == trace_id
        for ref in span.get("references", []):
            if ref["refType"] == "CHILD_OF":
                assert ref["traceID"].lower().zfill(32) == trace_id and ref["spanID"] in span_ids
    services = {p["serviceName"] for p in trace["processes"].values()}
    assert services == {"wp-client-trace-fixture", "wp-server-trace-fixture"}, services
    execution = [s for s in spans if s["operationName"] == "Execute"]
    assert len(execution) == 1
    refs = execution[0].get("references", [])
    if receipt["case"] == "batch":
        assert not any(r["refType"] == "CHILD_OF" for r in refs)
        actual = {(r["traceID"].lower().zfill(32), r["spanID"].lower().zfill(16)) for r in refs if r["refType"] == "FOLLOWS_FROM"}
        expected = {(s["trace_id"], s["span_id"]) for s in receipt["input_spans"]}
        assert actual == expected, (actual, expected)
        assert trace_id not in receipt["input_trace_ids"]
    else:
        assert receipt["input_trace_ids"] == [trace_id]
        expected = receipt["input_spans"][0]
        assert any(r["refType"] == "CHILD_OF" and r["traceID"].lower().zfill(32) == trace_id
                   and r["spanID"].lower().zfill(16) == expected["span_id"] for r in refs)
    # Exported server spans must retain their actual client parent, including
    # streams closed after their final acknowledgement. Transport cancellation
    # by itself is not a conclusion about application success or durability.
    servers = [s for s in spans if any(t["key"] == "span.kind" and t["value"] == "server" for t in s.get("tags", []))]
    assert servers, "No exported gRPC server span"
    for server in servers:
        assert trace["processes"][server["processID"]]["serviceName"] == "wp-server-trace-fixture"
        parents = [r["spanID"] for r in server.get("references", []) if r["refType"] == "CHILD_OF"]
        assert parents, "No retained server parent"
        assert any(s["spanID"] in parents and any(t["key"] == "span.kind" and t["value"] == "client" for t in s.get("tags", []))
                   and trace["processes"][s["processID"]]["serviceName"] == "wp-client-trace-fixture" for s in spans)
    return {"case": receipt["case"], "trace_id": trace_id, "spans": len(spans),
            "services": sorted(services), "references": refs, "propagation_status": "passed"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="jaegertracing/all-in-one:1.53.0")
    parser.add_argument("--output", type=Path, help="Retain fixture receipts, raw spans and propagation validation")
    args = parser.parse_args()
    name = "wp-trace-export-" + uuid.uuid4().hex[:10]
    started = False
    try:
        command("docker", "run", "-d", "--rm", "--name", name,
                "-e", "COLLECTOR_OTLP_ENABLED=true", "-e", "SPAN_STORAGE_TYPE=memory",
                "-p", "127.0.0.1::16686", "-p", "127.0.0.1::4318", args.image, timeout=60)
        started = True
        ports = json.loads(command("docker", "inspect", name, timeout=10))[0]["NetworkSettings"]["Ports"]
        query = "http://127.0.0.1:" + ports["16686/tcp"][0]["HostPort"]
        ingest = "127.0.0.1:" + ports["4318/tcp"][0]["HostPort"]
        for _ in range(100):
            try:
                get(query + "/api/services")
                break
            except (OSError, ValueError):
                time.sleep(.1)
        else:
            raise RuntimeError("Fixture Jaeger did not become ready")
        with tempfile.TemporaryDirectory(prefix="wp-trace-export-") as folder:
            root = Path(folder)
            receipts = root / "receipts"
            env = dict(os.environ, WP_TRACE_TEST_OTLP=ingest, WP_TRACE_TEST_RECEIPTS=str(receipts))
            go_output = command("go", "test", "./woodpecker/segment", "-run", "^TestAppendTrace(SingleRemote|BatchRemote)$",
                    "-count=1", "-timeout=90s", cwd=ROOT, env=env, timeout=120)
            if args.output:
                args.output.mkdir(parents=True, exist_ok=True)
                (args.output / "go-test.log").write_text(go_output + "\n")
            checks = []
            files = sorted(receipts.glob("*.json"))
            assert len(files) == 3, "Expected success, error and mixed-request batch receipts"
            for file in files:
                receipt = json.loads(file.read_text())
                trace_id = receipt["execution_trace_id"]
                trace = retained(query, trace_id, 3)
                check = validate_trace(receipt, trace)
                if args.output:
                    args.output.mkdir(parents=True, exist_ok=True)
                    for suffix, value in [("receipt", receipt), ("trace", trace)]:
                        (args.output / (receipt["case"] + "-" + suffix + ".json")).write_text(json.dumps(value, indent=2) + "\n")
                checks.append(check)
            result = {"schema_version": "wp.trace.propagation.validation/v1", "passed": True, "checks": checks,
                "limits": ["Protocol fixture only; no actual Milvus Insert, disk I/O or durability claim",
                           "Batch links connect independent traces; linked input traces are not fetched or merged",
                           "Always-sampled fixture does not establish lossless export or production sampling coverage",
                           "Raw propagation validation only; diagnostic CLI assessment is owned by Diagnostics CI"]}
            if args.output:
                (args.output / "validation.json").write_text(json.dumps(result, indent=2) + "\n")
            print(json.dumps(result, indent=2))
    finally:
        if started:
            subprocess.run(["docker", "stop", "--time", "1", name], capture_output=True, text=True, timeout=20)


if __name__ == "__main__":
    main()
