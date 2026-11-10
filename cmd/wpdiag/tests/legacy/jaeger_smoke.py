#!/usr/bin/env python3
"""Exercise wpdiag against an isolated real Jaeger 1.53 OTLP/query instance.

Requires Docker and jaegertracing/all-in-one:1.53.0. The diagnostic binary only
performs reads; this fixture producer imports synthetic spans into its own
temporary container, which is removed on completion.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid


def command(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.PIPE).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wpdiag", default="bin/wpdiag")
    parser.add_argument("--image", default="jaegertracing/all-in-one:1.53.0")
    args = parser.parse_args()
    binary = str(Path(args.wpdiag).resolve())
    name = "wpdiag-jaeger-" + uuid.uuid4().hex[:10]
    namespace = name
    checks = []
    try:
        command("docker", "run", "-d", "--rm", "--name", name,
                "-e", "COLLECTOR_OTLP_ENABLED=true", "-e", "SPAN_STORAGE_TYPE=memory",
                "-p", "127.0.0.1::16686", "-p", "127.0.0.1::4318", args.image)
        ports = json.loads(command("docker", "inspect", name))[0]["NetworkSettings"]["Ports"]
        query = "http://127.0.0.1:" + ports["16686/tcp"][0]["HostPort"]
        ingest = "http://127.0.0.1:" + ports["4318/tcp"][0]["HostPort"]
        deadline = time.monotonic() + 30
        while True:
            try:
                with urllib.request.urlopen(query + "/api/services", timeout=1) as response:
                    json.load(response)
                break
            except (OSError, ValueError):
                if time.monotonic() > deadline:
                    raise
                time.sleep(.2)

        def publish(trace):
            resources = []
            for pid, process in trace["processes"].items():
                spans = []
                scope = next((t["value"] for t in process.get("tags", []) if t["key"] == "otel.library.name"), "fixture")
                for span in trace["spans"]:
                    if span["processID"] != pid:
                        continue
                    attributes = [{"key": "authorization", "value": {"stringValue": "sentinel-secret"}}]
                    for tag in span.get("tags", []):
                        kind = {"bool": "boolValue", "int64": "intValue"}.get(tag["type"], "stringValue")
                        value = tag["value"] if kind == "boolValue" else str(tag["value"])
                        attributes.append({"key": tag["key"], "value": {kind: value}})
                    item = {"traceId": trace["traceID"], "spanId": span["spanID"], "name": span["operationName"],
                            "startTimeUnixNano": str(span["startTime"] * 1000),
                            "endTimeUnixNano": str((span["startTime"] + span["duration"]) * 1000),
                            "attributes": attributes, "kind": 1}
                    for ref in span.get("references", []):
                        if ref["refType"] == "CHILD_OF":
                            item["parentSpanId"] = ref["spanID"]
                        else:
                            item.setdefault("links", []).append({"traceId": ref["traceID"], "spanId": ref["spanID"]})
                    spans.append(item)
                resources.append({"resource": {"attributes": [
                    {"key": "service.name", "value": {"stringValue": process["serviceName"]}},
                    {"key": "namespace", "value": {"stringValue": namespace}}]},
                    "scopeSpans": [{"scope": {"name": scope}, "spans": spans}]})
            request = urllib.request.Request(ingest + "/v1/traces", data=json.dumps({"resourceSpans": resources}).encode(),
                                             headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=5) as response:
                result = json.load(response)
            assert not result.get("partialSuccess", {}).get("rejectedSpans"), result
            deadline = time.monotonic() + 10
            while True:
                try:
                    with urllib.request.urlopen(query + "/api/traces/" + trace["traceID"], timeout=2) as response:
                        data = json.load(response).get("data", [])
                    if data:
                        break
                except urllib.error.HTTPError as error:
                    if error.code != 404:
                        raise
                if time.monotonic() > deadline:
                    raise AssertionError("OTLP fixture not retained")
                time.sleep(.1)

        def fixture(flow="write"):
            trace_id = uuid.uuid4().hex
            begin = time.time_ns() // 1000 - 10_000_000
            processes = {"p1": {"serviceName": "proxy"}, "p2": {"serviceName": "streamingnode"},
                         "p3": {"serviceName": "wp-client", "tags": [{"key": "otel.library.name", "value": "LogWriter"}]},
                         "p4": {"serviceName": "wp-server"}}
            operations = ["Proxy-Insert", "AppendMessages", "Write", "AddEntry"]
            if flow == "read":
                operations = ["Proxy-Search", "milvus.proto.query.QueryNode/Search", "ReadNext", "GetBatchEntriesAdv"]
                processes["p3"]["tags"][0]["value"] = "LogReader"
            spans = []
            for index, operation in enumerate(operations):
                refs = [] if index == 0 else [{"refType": "CHILD_OF", "traceID": trace_id, "spanID": f"{index:016x}"}]
                spans.append({"traceID": trace_id, "spanID": f"{index+1:016x}", "operationName": operation,
                              "processID": f"p{index+1}", "startTime": begin + index * 1000,
                              "duration": 2_000_000 - index * 10_000, "references": refs, "tags": []})
            return {"traceID": trace_id, "processes": processes, "spans": spans}

        with tempfile.TemporaryDirectory(prefix="wpdiag-jaeger-") as folder:
            path = Path(folder) / "wpdiag.json"
            settings = {"endpoint": query, "deployment": "fixture", "services": {
                "proxy": "proxy", "streamingnode": "streamingnode", "wp-client": "wp_client", "wp-server": "wp_server"},
                "match_tags": {"namespace": namespace}, "search": {"service": "proxy"}, "request_id_tag": "request_id"}

            def invoke(case, trace=None, scope="e2e", flow="write", extra=None, expected=0, outcome=None, changes=None):
                current = json.loads(json.dumps(settings))
                if changes:
                    current.update(changes)
                path.write_text(json.dumps({"schema_version": "wpdiag.config/v1", "sources": {"trace": {
                    "connections": {"local": {"settings": current}}}}}))
                argv = [binary, "trace", "diagnose", "--config", str(path), "--connection", "local",
                        "--scope", scope, "--flow", flow, "--budget", "5s", "-o", "json"]
                if trace:
                    argv += ["--mode", "snapshot", "--trace-id", trace["traceID"]]
                argv += extra or []
                proc = subprocess.run(argv, capture_output=True, text=True, timeout=8)
                report = json.loads(proc.stdout)
                assert proc.returncode == expected, (case, proc.returncode, report, proc.stderr)
                assert "sentinel-secret" not in proc.stdout + proc.stderr
                assert query not in proc.stdout
                findings = [f for result in report["results"] for f in result["findings"]]
                if outcome:
                    assert any(f["outcome"] == outcome for f in findings), (case, report)
                if case == "normal ReadNext":
                    assert not any(f["outcome"] == "observed_slow_span" for f in findings), report
                if case == "separate candidates":
                    assert not any(f["outcome"] == "observed_stages_connected" for f in findings), report
                    assert len({f["target"].get("trace_id") for f in findings}) == 2
                checks.append({"case": case, "exit": proc.returncode, "status": report["status"]})
                return argv, report

            write, read = fixture(), fixture("read")
            publish(write)
            publish(read)
            for scope in ["wp", "milvus", "e2e"]:
                invoke(scope + " write", write, scope=scope, outcome="observed_stages_connected")
                invoke(scope + " read", read, scope=scope, flow="read", outcome="observed_stages_connected")
            partial = json.loads((Path(__file__).resolve().parents[3] / "diagnostics/jaeger/testdata/partial-insert-v1.json").read_text())
            shift = time.time_ns() // 1000 - 10_000_000 - min(s["startTime"] for s in partial["spans"])
            for span in partial["spans"]:
                span["startTime"] += shift
            publish(partial)
            invoke("captured partial Insert", partial, outcome="partial_observed_path")
            for case in ["async link", "missing parent"]:
                trace = fixture()
                if case == "async link":
                    trace["spans"][2]["references"][0]["refType"] = "FOLLOWS_FROM"
                else:
                    trace["spans"][2]["references"][0]["spanID"] = "9999999999999999"
                publish(trace)
                invoke(case, trace, outcome="partial_observed_path")
            tail = fixture("read")
            tail["spans"] = tail["spans"][2:]
            tail["spans"][1]["duration"] = 1000
            publish(tail)
            invoke("normal ReadNext", tail, scope="wp", flow="read", outcome="no_supported_suspicion_in_observed_spans")
            error_trace = fixture()
            error_trace["spans"][3]["tags"] = [{"key": "error", "type": "bool", "value": True}]
            publish(error_trace)
            invoke("span error", error_trace, outcome="observed_span_error")
            start = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=5)).isoformat()
            end = datetime.datetime.now(datetime.timezone.utc).isoformat()
            historical = ["--mode", "historical", "--from", start, "--to", end]
            invoke("service/operation search", extra=historical, outcome="observed_stages_connected",
                   changes={"search": {"service": "proxy", "operation": "Proxy-Insert"}})
            invoke("no retained match", extra=historical + ["--request-id", "not-present"], outcome="inconclusive")
            for case, changes, extra in [
                ("span budget", {"limits": {"max_spans": 1}}, []),
                ("input budget", None, ["--max-input-bytes", "1024"]),
                ("output budget", None, ["--max-output-bytes", "4096"]),
            ]:
                invoke(case, write, changes=changes, extra=extra, expected=5)
            first, second = fixture(), fixture()
            first["spans"] = first["spans"][:1]
            second["spans"][0]["operationName"] = "unrelated-context"
            for trace in [first, second]:
                trace["spans"][0]["tags"] = [{"key": "request_id", "type": "string", "value": "ambiguous"}]
                publish(trace)
            invoke("separate candidates", extra=historical + ["--request-id", "ambiguous"], outcome="partial_observed_path")
            invoke("trace budget", extra=historical + ["--request-id", "ambiguous"], changes={"limits": {"max_traces": 1}}, expected=5)
            live_settings = json.loads(json.dumps(settings))
            live_settings["limits"] = {"export_delay_ms": 500}
            path.write_text(json.dumps({"schema_version": "wpdiag.config/v1", "sources": {"trace": {
                "connections": {"local": {"settings": live_settings}}}}}))
            argv = [binary, "trace", "diagnose", "--config", str(path), "--connection", "local",
                    "--scope", "e2e", "--flow", "write", "--mode", "live", "--window", "1200ms",
                    "--interval", "300ms", "--request-id", "live-case", "--budget", "5s", "-o", "json"]
            proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                time.sleep(.6)
                live_trace = fixture()
                begin = time.time_ns() // 1000 - 20000
                for index, span in enumerate(live_trace["spans"]):
                    span["startTime"], span["duration"] = begin + index * 1000, 15000 - index * 1000
                live_trace["spans"][0]["tags"] = [{"key": "request_id", "type": "string", "value": "live-case"}]
                publish(live_trace)
                stdout, stderr = proc.communicate(timeout=6)
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.communicate()
            live_report = json.loads(stdout)
            assert proc.returncode == 0, (stdout, stderr)
            assert any(f["outcome"] == "observed_stages_connected" for f in live_report["results"][0]["findings"]), live_report
            assert "sentinel-secret" not in stdout + stderr
            checks.append({"case": "live retained window", "exit": 0, "status": live_report["status"]})
            argv += ["--window", "2s", "--interval", "1s"]
            proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                time.sleep(.2)
                proc.send_signal(signal.SIGINT)
                stdout, stderr = proc.communicate(timeout=4)
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.communicate()
            assert proc.returncode == 7 and json.loads(stdout)["status"] == "cancelled", (stdout, stderr)
            checks.append({"case": "cancellation", "exit": 7, "status": "cancelled"})
        print(json.dumps({"passed": True, "backend": args.image, "checks": checks}, indent=2))
    finally:
        subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)


if __name__ == "__main__":
    main()
