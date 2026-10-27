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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wpdiag", default="bin/wpdiag")
    parser.add_argument("--image", default="jaegertracing/all-in-one:1.53.0")
    parser.add_argument("--output", type=Path, help="Retain fixture receipts, raw spans and diagnostic reports")
    args = parser.parse_args()
    binary = str((ROOT / args.wpdiag).resolve())
    name = "wp-trace-459-" + uuid.uuid4().hex[:10]
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
        with tempfile.TemporaryDirectory(prefix="wp-trace-459-") as folder:
            root = Path(folder)
            receipts = root / "receipts"
            env = dict(os.environ, WP_TRACE_TEST_OTLP=ingest, WP_TRACE_TEST_RECEIPTS=str(receipts))
            go_output = command("go", "test", "./woodpecker/segment", "-run", "^TestAppendTrace(SingleRemote|BatchRemote)$",
                    "-count=1", "-timeout=90s", cwd=ROOT, env=env, timeout=120)
            if args.output:
                args.output.mkdir(parents=True, exist_ok=True)
                (args.output / "go-test.log").write_text(go_output + "\n")
            config = root / "wpdiag.json"
            config.write_text(json.dumps({"schema_version": "wpdiag.config/v1", "sources": {"trace": {
                "connections": {"fixture": {"settings": {"endpoint": query, "deployment": "trace-protocol-fixture",
                    "services": {"wp-client-trace-fixture": "wp_client", "wp-server-trace-fixture": "wp_server"}}}}}}}))
            checks = []
            files = sorted(receipts.glob("*.json"))
            assert len(files) == 3, "Expected success, error and mixed-request batch receipts"
            for file in files:
                receipt = json.loads(file.read_text())
                trace_id = receipt["execution_trace_id"]
                trace = retained(query, trace_id, 3)
                span_ids = {s["spanID"] for s in trace["spans"]}
                for span in trace["spans"]:
                    for ref in span.get("references", []):
                        if ref["refType"] == "CHILD_OF":
                            assert ref["traceID"].lower().zfill(32) == trace_id and ref["spanID"] in span_ids
                services = {p["serviceName"] for p in trace["processes"].values()}
                assert services == {"wp-client-trace-fixture", "wp-server-trace-fixture"}, services
                execution = [s for s in trace["spans"] if s["operationName"] == "Execute"]
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
                report = json.loads(command(binary, "trace", "diagnose", "--config", str(config),
                    "--connection", "fixture", "--scope", "wp", "--flow", "write", "--mode", "snapshot",
                    "--trace-id", trace_id, "-o", "json", timeout=20))
                assert report["status"] == "completed"
                unknown = {u for result in report["results"] for f in result["findings"] for u in f["unknown"]}
                assert "parent_not_retained" not in unknown
                assert "supported_span_evidence_absent" not in unknown
                # A cleanup cancellation alone cannot establish write failure.
                cancelled = any(any(t["key"] == "rpc.grpc.status_code" and str(t["value"]) == "1" for t in s.get("tags", []))
                                and any(t["key"] == "span.kind" and t["value"] == "client" for t in s.get("tags", []))
                                for s in trace["spans"])
                if cancelled:
                    assert any(f["outcome"] == "observed_rpc_cancellation" and f["severity"] == "unknown"
                               for result in report["results"] for f in result["findings"])
                if receipt["case"] == "batch":
                    assert "async_links_not_serial_children" in unknown
                if args.output:
                    args.output.mkdir(parents=True, exist_ok=True)
                    for suffix, value in [("receipt", receipt), ("trace", trace), ("diagnosis", report)]:
                        (args.output / (receipt["case"] + "-" + suffix + ".json")).write_text(json.dumps(value, indent=2) + "\n")
                checks.append({"case": receipt["case"], "trace_id": trace_id, "spans": len(trace["spans"]),
                               "services": sorted(services), "references": refs, "diagnostic_status": report["status"]})
            result = {"schema_version": "wp.trace.propagation.validation/v1", "passed": True, "checks": checks,
                "limits": ["Protocol fixture only; no actual Milvus Insert, disk I/O or durability claim",
                           "Batch links connect independent traces; wpdiag does not fetch or merge linked traces",
                           "Always-sampled fixture does not establish lossless export or production sampling coverage"]}
            if args.output:
                (args.output / "validation.json").write_text(json.dumps(result, indent=2) + "\n")
            print(json.dumps(result, indent=2))
    finally:
        if started:
            subprocess.run(["docker", "stop", "--time", "1", name], capture_output=True, text=True, timeout=20)


if __name__ == "__main__":
    main()
