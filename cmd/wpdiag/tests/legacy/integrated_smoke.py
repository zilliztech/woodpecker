#!/usr/bin/env python3
"""Exercise integrated diagnosis with real Prometheus, wpdiag and wp binaries.

A temporary retained TSDB supplies a historical slow append/query. A read-only
Admin HTTP fixture supplies independent current native context. No Kubernetes,
Helm, running example stack, remote credentials or application data are used.
"""
import argparse
import http.server
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import threading
import time
import urllib.request
import uuid

from prometheus_smoke import command


def signature(report):
    findings = [f for a in report.get("assessments", []) for f in a["findings"]]
    if not report.get("assessments"):
        findings = [f for r in report["results"] for f in r["findings"]]
    return sorted((f["rule_id"], f["rule_version"], f["outcome"], f["severity"], f["evidence_strength"])
                  for f in findings)


def lineage(report):
    by_module = {run["module_id"]: run["report"] for run in report["runs"]}
    for view in report["summary"]["findings"]:
        child = by_module[view["module_id"]]
        original = [f for r in child["results"] for f in r["findings"]]
        original += [f for a in child.get("assessments", []) for f in a["findings"]]
        assert any(f["id"] == view["id"] for f in original), view
    for child in by_module.values():
        ids = {o["id"] for r in child["results"] for o in r["observations"]}
        originals = {f["id"] for r in child["results"] for f in r["findings"]}
        for f in [f for r in child["results"] for f in r["findings"]] + [
                f for a in child.get("assessments", []) for f in a["findings"]]:
            assert set(f["supporting_observations"] + f["contradicting_observations"]) <= ids
            assert set(f.get("derived_from_findings", [])) <= originals


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wpdiag", default="bin/wpdiag")
    parser.add_argument("--wp", default="bin/wp")
    parser.add_argument("--image", default="quay.io/prometheus/prometheus:v3.0.1")
    args = parser.parse_args()
    binary, native = str(Path(args.wpdiag).resolve()), str(Path(args.wp).resolve())
    begin = int(time.time()) - 3600
    end = begin + 60
    observed = threading.Event()
    state = {"requests": 0, "denied": 0, "pending": False}

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            state["requests"] += 1
            path = self.path.split("?", 1)[0]
            code = 200
            if path.startswith("/denied/"):
                state["denied"] += 1
                code, value = 401, "sentinel-secret"
            elif path == "/admin/memberlist":
                value = {"members": [{"id": "node-1", "service_addr": "internal:8080", "cluster_name": "fixture"}]}
            elif path == "/metrics":
                observed.set()
                value = ('# TYPE process_start_time_seconds gauge\nprocess_start_time_seconds 100\n'
                         '# TYPE woodpecker_server_logstore_operations_total counter\n'
                         'woodpecker_server_logstore_operations_total{node_id="node-1",log_id="2",log_ns="tenant-a",operation="add_entry",status="success"} 5\n')
            elif path == "/admin/runtime/ops":
                value = ([{"op_id": "pending", "op_type": "logstore.add_entry", "log_id": 2,
                           "segment_id": 7, "started_at": "2026-10-01T00:00:00Z"}]
                         if state["pending"] else [])
            elif path == "/admin/runtime/ops/stats":
                value = {"capacity": 1024, "in_use": int(state["pending"]), "evicted_total": 0}
            elif path == "/admin/env":
                value = {"build": {"version": "v0.1.46"}, "env": {"TOKEN": "sentinel-secret"}}
            else:
                code, value = 404, "unavailable"
            self.send_response(code)
            self.end_headers()
            try:
                self.wfile.write((value if isinstance(value, str) else json.dumps(value)).encode())
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    admin = "http://127.0.0.1:" + str(server.server_port)
    checks = []
    container = "wpdiag-integrated-smoke-" + uuid.uuid4().hex[:10]
    started = False
    try:
        with tempfile.TemporaryDirectory(prefix="wpdiag-integrated-") as folder:
            root = Path(folder)
            lines = []
            # Read the source-owned catalog; the fixture does not copy queries,
            # stage rules or thresholds into the integrated runner.
            for stage in ("append", "query"):
                catalog = json.loads(command(binary, "milvus", stage, "--list", "-o", "json"))["signals"]
                for spec in catalog:
                    labels = {"cluster": "fixture", "instance": spec["component"] + ":9091",
                              "component": spec["component"], "node_id": "node-1"}
                    labels.update(spec.get("match_labels", {}))
                    labels.update({k: v.split("|")[0] for k, v in spec.get("regex_labels", {}).items()})
                    if spec["id"].startswith("wp."):
                        labels.update(log_ns="tenant-a", log_id="2")
                    metric = spec["metric"]
                    if spec["kind"] == "histogram":
                        count = [i * 20 for i in range(6)]
                        latency = 5000 if spec["id"] in ("milvus.append.quota", "milvus.query") else 10
                        factor = spec.get("metric_to_milliseconds", 1)
                        rows = [(metric + "_count", {}, count), (metric + "_sum", {}, [n * latency / factor for n in count])]
                        for bound in (10, 100, 1000, 10000):
                            rows.append((metric + "_bucket", {"le": str(bound / factor)}, count if bound >= latency else [0] * 6))
                        rows.append((metric + "_bucket", {"le": "+Inf"}, count))
                    else:
                        rows = [(metric, {}, [0] * 6)]
                    for name, extra, values in rows:
                        identity = ",".join(k + "=" + json.dumps(v) for k, v in sorted(dict(labels, **extra).items()))
                        for i, value in enumerate(values):
                            lines.append(f"{name}{{{identity}}} {value} {begin + 5 + i * 10}")
            (root / "samples.openmetrics").write_text("\n".join(lines) + "\n# EOF\n")
            (root / "prometheus.yaml").write_text("global:\n  scrape_interval: 15s\nscrape_configs: []\n")
            identity = str(os.getuid()) + ":" + str(os.getgid())
            command("docker", "run", "--rm", "--user", identity, "-v", str(root) + ":/work", "--entrypoint", "/bin/promtool",
                    args.image, "tsdb", "create-blocks-from", "openmetrics", "/work/samples.openmetrics", "/work/data")
            command("docker", "run", "--rm", "-d", "--name", container, "--user", identity, "-p", "127.0.0.1::9090",
                    "-v", str(root) + ":/work", args.image, "--config.file=/work/prometheus.yaml", "--storage.tsdb.path=/work/data")
            started = True
            endpoint = "http://" + command("docker", "port", container, "9090/tcp").splitlines()[0]
            for _ in range(100):
                try:
                    with urllib.request.urlopen(endpoint + "/-/ready", timeout=1) as response:
                        if response.status == 200:
                            break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("Fixture Prometheus did not become ready")
            (root / "cli.yaml").write_text("contexts:\n  fixture:\n    endpoint: " + admin + "\n    node_admin_urls:\n      node-1: " + admin + "\n")
            config = {"schema_version": "wpdiag.config/v1", "sources": {
                "prometheus": {"connections": {"metrics": {"settings": {
                    "endpoint": endpoint, "deployment": "fixture", "match_labels": {"cluster": "fixture"}}}}},
                "wpcli": {"connections": {"native": {"settings": {"executable": native, "context": "fixture", "cli_config": "cli.yaml"}}}},
                "trace": {"connections": {"traces": {"settings": {"endpoint": admin + "/denied", "deployment": "fixture",
                    "services": {"proxy": "proxy", "sn": "streamingnode", "wp": "wp_server"}, "search": {"service": "proxy"}}}}},
                "loki": {"connections": {"logs": {"settings": {"endpoint": admin + "/denied", "deployment": "fixture", "match_labels": {"namespace": "fixture"}}}}}}}
            path = root / "wpdiag.yaml"
            path.write_text(json.dumps(config))
            common = ["--config", str(path), "--mode", "historical", "--from", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(begin)),
                      "--to", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(end))]

            def run(extra, expected=0):
                proc = subprocess.run([binary, "run"] + common + extra + ["-o", "json"], capture_output=True, text=True, timeout=15)
                report = json.loads(proc.stdout)
                assert proc.returncode == expected, (extra, proc.returncode, report["status"], proc.stderr)
                assert report["schema_version"] == ("wpdiag.plan/v1" if "--plan" in extra else "wpdiag.run/v1")
                assert all(secret not in proc.stdout + proc.stderr for secret in ("sentinel-secret", endpoint, admin))
                if "summary" in report and report.get("runs"):
                    lineage(report)
                checks.append({"selection": extra, "status": report["status"]})
                return report

            before = state["requests"]
            plan = run(["--flow", "write", "--plan"])
            assert len(plan["entries"]) == 4  # Historical auto omits current native.
            assert state["requests"] == before
            for scope in ("wp", "milvus", "e2e"):
                for flow in ("write", "read"):
                    selected = ["--scope", scope, "--flow", flow, "--sources", "prometheus"]
                    integrated = run(selected)
                    for child in integrated["runs"]:
                        command_args = ([binary, "prometheus", "diagnose", "--flow", flow] if child["module_id"] == "prometheus"
                                        else [binary, "milvus", child["module_id"].split(".")[1]])
                        standalone = json.loads(command(*command_args, *common, "--scope", scope, "--connection", "metrics", "-o", "json"))
                        assert signature(child["report"]) == signature(standalone), child["module_id"]
                    if scope != "milvus":
                        combined = run(selected + ["--sources", "prometheus,wpcli", "--current-native", "--target", "wpcli.node_id=node-1"])
                        current = [r for r in combined["runs"] if r["module_id"] == "wpcli"][0]
                        assert current["role"] == "current_context"
                        assert current["report"]["selection"]["window"]["mode"] == "snapshot"
                        standalone_native = json.loads(command(binary, "wpcli", "diagnose", "--config", str(path),
                            "--connection", "native", "--scope", "wp", "--flow", flow, "--mode", "snapshot", "--node-id", "node-1", "-o", "json"))
                        assert signature(current["report"]) == signature(standalone_native)
                        assert not combined["summary"]["associations"]
                        assert "current_runtime_not_historical_evidence" in {g["code"] for g in combined["summary"]["coverage_gaps"]}
                        if scope == "e2e" and flow == "write":
                            findings = combined["runs"][0]["report"]["assessments"][0]["findings"]
                            assert any(f["outcome"] == "suspected_quota_admission_wait" for f in findings)
                            assert not any(f["contradicting_observations"] for f in findings), "Current healthy native state must not clear past metric suspicion"
            partial = run(["--flow", "write", "--sources", "prometheus,trace,loki"], expected=6)
            statuses = {r["module_id"]: r["report"]["status"] for r in partial["runs"]}
            assert statuses == {"milvus.append": "completed", "milvus.persistence": "completed", "trace": "failed", "loki": "failed"}, statuses
            text = command(binary, "run", *common, "--flow", "write", "--sources", "prometheus", "--stages", "append")
            assert "suspected_quota_admission_wait" in text and "Next:" in text and "Uncertainty:" in text
            checks.append({"selection": "default text surfaces slow quota", "status": "completed"})
            assert state["denied"] >= 2, "Both optional source adapters must actually be invoked"
            run(["--flow", "write", "--sources", "wpcli"], expected=3)
            run(["--flow", "write", "--stages", "query"], expected=2)
            run(["--flow", "write", "--sources", "prometheus", "--stages", "append", "--max-output-bytes", "4096"], expected=5)
            # Real native live collection and shared cancellation, not only mock modules.
            state["pending"] = True
            observed.clear()
            proc = subprocess.Popen([binary, "run", "--config", str(path), "--scope", "wp", "--flow", "write", "--sources", "wpcli",
                                     "--window", "30s", "--interval", "1s", "--progress", "-o", "json"],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                assert observed.wait(5), "Native process did not start"
                proc.send_signal(signal.SIGINT)
                stdout, stderr = proc.communicate(timeout=5)
                report = json.loads(stdout)
                assert proc.returncode == 7 and report["status"] == "cancelled", (proc.returncode, stderr)
                assert report["runs"][0]["report"]["results"][0]["observations"], "Cancellation lost already collected evidence"
                checks.append({"selection": "real native cancellation", "status": report["status"]})
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.communicate()
            print(json.dumps({"passed": True, "count": len(checks), "checks": checks}, indent=2))
    finally:
        server.shutdown()
        server.server_close()
        if started:
            subprocess.run(["docker", "stop", "--time", "1", container], capture_output=True, text=True, timeout=15)


if __name__ == "__main__":
    main()
