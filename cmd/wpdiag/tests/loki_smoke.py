#!/usr/bin/env python3
"""Run read-only diagnosis against a temporary real Loki 3.5.7.

Only this fixture producer pushes logs, into its own container. No Kubernetes
context, application traffic or user's retained logs are used.
"""
import argparse
import datetime
import json
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.request
import uuid


def command(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.PIPE).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wpdiag", default="bin/wpdiag")
    parser.add_argument("--image", default="grafana/loki:3.5.7")
    args = parser.parse_args()
    binary = str(Path(args.wpdiag).resolve())
    name = "wpdiag-loki-" + uuid.uuid4().hex[:10]
    checks = []
    with tempfile.TemporaryDirectory(prefix="wpdiag-loki-") as folder:
        root = Path(folder)
        (root / "loki.yaml").write_text("""auth_enabled: false
server:
  http_listen_port: 3100
common:
  path_prefix: /tmp/loki
  replication_factor: 1
  ring:
    kvstore:
      store: inmemory
  storage:
    filesystem:
      chunks_directory: /tmp/loki/chunks
      rules_directory: /tmp/loki/rules
schema_config:
  configs:
    - from: "2024-01-01"
      store: tsdb
      object_store: filesystem
      schema: v13
      index:
        prefix: index_
        period: 24h
analytics:
  reporting_enabled: false
""")
        started = False
        try:
            command("docker", "run", "--rm", "-d", "--name", name, "-p", "127.0.0.1::3100",
                    "-v", str(root / "loki.yaml") + ":/etc/loki/test.yaml:ro", args.image,
                    "-config.file=/etc/loki/test.yaml")
            started = True
            endpoint = "http://" + command("docker", "port", name, "3100/tcp").splitlines()[0]
            deadline = time.monotonic() + 45
            while True:
                try:
                    with urllib.request.urlopen(endpoint + "/ready", timeout=1) as response:
                        assert response.status == 200
                    break
                except OSError:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(.2)
            begin = (time.time_ns() // 1_000_000_000 - 60) * 1_000_000_000
            end = begin + 30_000_000_000
            scenarios = {
                "positive": [("woodpecker", "add entry failed"), ("streamingnode", "append message into wal too slow"),
                             ("woodpecker", "get batch entries failed"), ("proxy", "failed to reduce search results")],
                "normal": [("woodpecker", "normal progress"), ("proxy", "query completed")],
                "format": [("woodpecker", "new version: add entry failed")],
                "recovery": [("woodpecker", "local WAL disk usage high: expand the PVC or scale out logstore nodes to bind new PVCs"),
                             ("woodpecker", "local WAL disk usage recovered below watermarks")],
            }
            streams = []
            for scenario, entries in scenarios.items():
                for index, (component, message) in enumerate(entries):
                    content = json.dumps({"msg": message, "requestID": "request-1", "error": "sentinel-secret",
                                          "traceID": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
                    streams.append({"stream": {"namespace": name, "cluster": "fixture", "scenario": scenario,
                                                "component": component, "pod": component + "-1", "container": "main"},
                                    "values": [[str(begin + (index + 1) * 1_000_000_000), content]]})
            # Tied timestamps exceed a page: the diagnostic must stop explicitly,
            # rather than advancing +1ns and silently skipping messages.
            streams.append({"stream": {"namespace": name, "cluster": "fixture", "scenario": "tied",
                                       "component": "woodpecker", "pod": "woodpecker-1"},
                            "values": [[str(begin + 1_000_000_000), json.dumps({"msg": "add entry failed", "requestID": f"r-{i}"})]
                                       for i in range(5)]})
            def add_stream(scenario, component, pod, values):
                streams.append({"stream": {"namespace": name, "cluster": "fixture", "scenario": scenario,
                                            "component": component, "pod": pod}, "values": values})

            # Known WARN after more ordinary lines than the default line budget.
            noise = [[str(begin + 1_000_000_000 + i * 100_000),
                      json.dumps({"msg": f"ordinary INFO progress {i}"})] for i in range(2500)]
            noise.append([str(begin + 3_000_000_000), json.dumps({"msg": "add entry failed"})])
            add_stream("noise-before-error", "woodpecker", "woodpecker-1", noise)
            # These streams even contain candidate text, but no rule applies to
            # their component. They must not consume the single selected target.
            for i in range(12):
                add_stream("unsupported-streams", "querynode", f"querynode-{i}",
                           [[str(begin + 1_000_000_000 + i), json.dumps({"msg": "append message into wal too slow"})]])
            add_stream("unsupported-streams", "streamingnode", "sn-1",
                       [[str(begin + 3_000_000_000), json.dumps({"msg": "append message into wal too slow"})]])
            real_trace, fake_trace = "a" * 32, "b" * 32
            text = ('[2026-10-26] [WARN] [logstore.go:295] ["add entry failed"] '
                    f'[traceID={real_trace}] [requestID=real] '
                    f'[error="failed [traceID={fake_trace}] [requestID=fake]"]')
            add_stream("error-body-id", "woodpecker", "wp-1", [[str(begin + 1_000_000_000), text]])
            add_stream("error-body-id", "streamingnode", "sn-1", [[str(begin + 2_000_000_000),
                       json.dumps({"msg": "append message into wal too slow", "traceID": fake_trace})]])
            for scenario in ["request-only-peer", "both-ids", "overlapping-ids"]:
                add_stream(scenario, "proxy", "proxy-1", [[str(begin + 1_000_000_000),
                           json.dumps({"msg": "failed to reduce search results", "requestID": "r-1", "traceID": real_trace})]])
                peer = {"msg": "read entries error"}
                if scenario != "overlapping-ids":
                    peer["requestID"] = "r-1"
                if scenario != "request-only-peer":
                    peer["traceID"] = real_trace
                add_stream(scenario, "streamingnode", "sn-1", [[str(begin + 2_000_000_000), json.dumps(peer)]])
                if scenario == "overlapping-ids":
                    add_stream(scenario, "woodpecker", "wp-1", [[str(begin + 3_000_000_000),
                               json.dumps({"msg": "get batch entries failed", "requestID": "r-1"})]])
            req = urllib.request.Request(endpoint + "/loki/api/v1/push", data=json.dumps({"streams": streams}).encode(),
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=5) as response:
                assert response.status == 204
            stamp = lambda ns: datetime.datetime.fromtimestamp(ns / 1e9, datetime.timezone.utc).isoformat()

            def invoke(case, scenario="positive", scope="wp", flow="write", expected=0, outcome=None, extra=None, limits=None):
                settings = {"endpoint": endpoint, "deployment": "fixture", "match_labels": {
                    "cluster": "fixture", "namespace": name, "scenario": scenario}}
                if limits:
                    settings["limits"] = limits
                path = root / "wpdiag.json"
                path.write_text(json.dumps({"schema_version": "wpdiag.config/v1", "sources": {"loki": {
                    "connections": {"test": {"settings": settings}}}}}))
                argv = [binary, "loki", "diagnose", "--config", str(path), "--connection", "test",
                        "--scope", scope, "--flow", flow, "--mode", "historical", "--from", stamp(begin), "--to", stamp(end),
                        "--budget", "5s", "-o", "json"] + (extra or [])
                proc = subprocess.run(argv, capture_output=True, text=True, timeout=8)
                report = json.loads(proc.stdout)
                assert proc.returncode == expected, (case, report, proc.stderr)
                assert "sentinel-secret" not in proc.stdout + proc.stderr
                assert endpoint not in proc.stdout
                findings = [f for result in report["results"] for f in result["findings"]]
                if outcome:
                    assert any(f["outcome"] == outcome for f in findings), (case, report)
                assert not any(f["outcome"] == "healthy" for f in findings), report
                checks.append({"case": case, "status": report["status"]})
                return report

            for scope in ["wp", "milvus", "e2e"]:
                for flow in ["write", "read"]:
                    invoke(scope + " " + flow, scope=scope, flow=flow, outcome="suspected_operation_problem")
            report = invoke("same observed request", scope="e2e", outcome="related_component_events")
            assert any(f["rule_id"] == "loki.e2e.observed_identity" for f in report["results"][0]["findings"])
            for scenario in ["normal", "idle", "format"]:
                invoke(scenario, scenario=scenario, outcome="inconclusive")
            invoke("wrong Pod", extra=["--instance", "unrelated"], outcome="inconclusive")
            invoke("wrong request", extra=["--operation-id", "unrelated"], outcome="inconclusive")
            invoke("wrong deployment", extra=["--deployment", "wrong"], expected=2)
            invoke("unmapped log", extra=["--log-id", "1"], expected=3)
            report = invoke("recovery", scenario="recovery", outcome="observed_recovery_event")
            assert any(f["contradicting_observations"] for f in report["results"][0]["findings"])
            invoke("pagination", limits={"page_size": 1}, expected=6)
            invoke("tied boundary", scenario="tied", limits={"page_size": 2}, expected=6)
            invoke("query budget", scenario="tied", limits={"page_size": 2, "max_queries": 1}, expected=5)
            report = invoke("noise does not hide known error", scenario="noise-before-error",
                            outcome="suspected_operation_problem")
            assert report["results"][0]["metadata"]["returned_lines"] == 1, report
            assert " |~ " in report["results"][0]["observations"][0]["query"]["expression"]
            invoke("unsupported components do not consume targets", scenario="unsupported-streams", scope="e2e",
                   outcome="suspected_operation_problem", extra=["--max-targets", "1"])
            report = invoke("error body cannot replace event ID", scenario="error-body-id", scope="e2e")
            assert not report["results"][0]["metadata"]["associations"], report
            invoke("error body cannot match operation selector", scenario="error-body-id",
                   extra=["--operation-id", "fake"], outcome="inconclusive")
            invoke("real event operation remains selectable", scenario="error-body-id",
                   extra=["--operation-id", "real"], outcome="suspected_operation_problem")
            for scenario, count, kinds in [("request-only-peer", 1, ["request"]),
                                           ("both-ids", 1, ["request", "trace"]),
                                           ("overlapping-ids", 2, None)]:
                report = invoke(scenario, scenario=scenario, scope="e2e", flow="read", outcome="related_component_events")
                associations = report["results"][0]["metadata"]["associations"]
                assert len(associations) == count, report
                if kinds:
                    assert associations[0]["identity_kinds"] == kinds, associations
                assert all(len(a["event_ids"]) == 2 for a in associations), associations
            print(json.dumps({"checks": checks, "count": len(checks)}, indent=2))
        finally:
            if started:
                command("docker", "rm", "-f", name)


if __name__ == "__main__":
    main()
