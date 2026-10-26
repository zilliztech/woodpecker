#!/usr/bin/env python3
"""Real wpdiag -> wp -> local HTTP fixture smoke test; no cluster required."""

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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wp", default="bin/wp")
    parser.add_argument("--wpdiag", default="bin/wpdiag")
    args = parser.parse_args()
    wp, wpdiag = str(Path(args.wp).resolve()), str(Path(args.wpdiag).resolve())
    state = {"mode": "write", "requests": 0, "writer_reads": 0, "metric_reads": 0}
    observed = threading.Event()

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            state["requests"] += 1
            mode, code = state["mode"], 200
            path = self.path.split("?", 1)[0]
            if path == "/admin/memberlist":
                if mode == "slow":
                    time.sleep(0.5)
                if mode == "discovery-down":
                    code, body = 503, "sentinel-secret"
                else:
                    body = json.dumps({"members": [{"id": "node-1", "service_addr": "internal:8080", "cluster_name": "fixture"}]})
            elif path == "/metrics":
                observed.set()
                state["metric_reads"] += 1
                operation = "get_batch_entries" if mode == "read" or mode.startswith("native-read-") else "add_entry"
                body = ('# TYPE process_start_time_seconds gauge\nprocess_start_time_seconds 100\n'
                        '# TYPE woodpecker_server_logstore_operations_total counter\n'
                        'woodpecker_server_logstore_operations_total{node_id="node-1",log_id="2",log_ns="tenant-a",operation="'
                        + operation + '",status="success"} 5\n')
                if mode.startswith("native-"):
                    for name, value in [("disk_usage_ratio", .6), ("disk_watermark_soft", .8),
                                        ("disk_watermark_hard", .9), ("write_backpressure_state", 0)]:
                        metric = "woodpecker_server_logstore_" + name
                        body += f'# TYPE {metric} gauge\n{metric}{{node_id="node-1"}} {value}\n'
                if mode == "native-read-progress":
                    body = body.replace('} 5\n', '} ' + str(5 + state["metric_reads"]) + '\n')
                elif mode == "native-read-reset" and state["metric_reads"] == 2:
                    body = body.replace('} 5\n', '} 1\n')
                elif mode == "native-read-eof":
                    body += ('woodpecker_server_logstore_operations_total{node_id="node-1",log_id="2",log_ns="tenant-a",'
                             'operation="get_batch_entries",status="end_of_file"} ' + str(state["metric_reads"]) + '\n')
                elif mode == "native-read-ambiguous":
                    body += ('woodpecker_server_logstore_operations_total{node_id="node-1",log_id="2",log_ns="tenant-b",'
                             'operation="get_batch_entries",status="success"} 5\n')
                elif mode == "native-read-unavailable":
                    code, body = 501, "sentinel-secret"
            elif path == "/admin/runtime/ops":
                if mode == "ops-down":
                    code, body = 503, "sentinel-secret"
                else:
                    operation = "get_batch_entries" if mode == "read" or mode.startswith("native-read-") else "add_entry"
                    body = json.dumps([] if mode in ("idle", "native-idle", "native-read-idle") else [{"op_id": "pending", "op_type": "logstore." + operation,
                                      "log_id": 2, "segment_id": 7, "started_at": "2026-10-01T00:00:00Z"}])
            elif path == "/admin/runtime/ops/stats":
                body = json.dumps({"capacity": 1024, "in_use": 0 if mode in ("idle", "native-idle", "native-read-idle") else 1, "evicted_total": 0})
            elif path == "/admin/env":
                body = json.dumps({"build": {"version": "v0.1.46"}, "env": {"TOKEN": "sentinel-secret"}})
            elif path == "/admin/logstore/segments":
                writers = [{"log_id": 2, "segment_id": 7, "backend": "stagedstorage"}]
                if mode == "native-idle":
                    writers = []
                elif mode == "native-limit":
                    writers.append({"log_id": 2, "segment_id": 8, "backend": "stagedstorage"})
                elif mode == "native-unavailable":
                    code = 501
                body = json.dumps({"segments": writers})
            elif path == "/admin/logstore/segments/detail":
                state["writer_reads"] += 1
                durable = state["writer_reads"] if mode == "native-progress" else 5
                body = json.dumps({"log_id": 2, "segment_id": 7, "backend": "stagedstorage",
                                   "writable": True, "fenced": False, "finalized": False, "closed": False,
                                   "buffer_bytes": 1024, "buffer_entries": 8,
                                   "flush_queue_depth": 2, "flush_queue_capacity": 8, "last_entry_id": durable})
            else:
                code, body = 404, "unknown"
            self.send_response(code)
            self.end_headers()
            try:
                self.wfile.write(body.encode())
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = "http://127.0.0.1:" + str(server.server_port)
    results = []
    try:
        with tempfile.TemporaryDirectory(prefix="wpdiag smoke ") as directory:
            root = Path(directory)
            (root / "cli.yaml").write_text("current-context: unused\ncontexts:\n  fixture:\n    endpoint: " + url
                                          + "\n    node_admin_urls:\n      node-1: " + url + "\n")
            # JSON is also valid YAML; paths with spaces/special characters remain literal.
            config = {"schema_version": "wpdiag.config/v1", "sources": {"wpcli": {"connections": {"local": {
                "settings": {"executable": wp, "context": "fixture", "cli_config": "cli.yaml"}}}}}}
            (root / "wpdiag.yaml").write_text(json.dumps(config))
            env = dict(os.environ, WOODPECKER_ENDPOINT="http://ambient-secret.invalid", WOODPECKER_CLI_CONFIG="ambient-secret")
            base = [wpdiag, "wpcli", "diagnose", "--config", str(root / "wpdiag.yaml"), "--connection", "local",
                    "--node-id", "node-1", "--flow", "write", "--window", "30ms", "--interval", "10ms", "--budget", "2s", "-o", "json"]
            for name, mode, extra, expected, outcome in [
                ("write", "write", [], 0, "suspected_stall"),
                ("read", "read", ["--flow", "read"], 0, "suspected_stall"),
                ("idle", "idle", [], 0, "no_pending_work_observed"),
                ("snapshot", "write", ["--mode", "snapshot", "--window", "0s", "--interval", "0s"], 0, "inconclusive"),
                ("partial", "ops-down", [], 6, "inconclusive"),
                ("discovery", "discovery-down", [], 1, None),
                ("budget", "slow", ["--budget", "100ms"], 5, None),
                ("cancel", "write", ["--window", "1s", "--interval", "500ms"], 7, None),
                ("input bound", "write", ["--max-input-bytes", "1024"], 5, None),
                ("report bound", "write", ["--max-output-bytes", "4096"], 5, None),
            ]:
                state["mode"] = mode
                observed.clear()
                if name == "cancel":
                    proc = subprocess.Popen(base + extra, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    try:
                        assert observed.wait(3), "No sample before cancellation"
                        proc.send_signal(signal.SIGINT)
                        stdout, stderr = proc.communicate(timeout=3)
                    finally:
                        if proc.poll() is None:
                            proc.kill()
                            proc.communicate()
                else:
                    proc = subprocess.run(base + extra, env=env, capture_output=True, text=True, timeout=5)
                    stdout, stderr = proc.stdout, proc.stderr
                report = json.loads(stdout)
                assert proc.returncode == expected, (name, proc.returncode, report, stderr)
                assert report["schema_version"] == "wpdiag.report/v1"
                result = report["results"][0]
                if name == "cancel":
                    assert result["observations"], "Native partial cancellation evidence was lost"
                    assert result["metadata"]["native_exit_code"] == 7
                if outcome:
                    assert result["findings"][0]["outcome"] == outcome, (name, report)
                    assert result["metadata"]["service_context"] == "fixture"
                    assert result["metadata"]["native_schema_version"] == "wp.diagnosis/v1"
                    ids = {observation["id"] for observation in result["observations"]}
                    assert set(result["findings"][0]["supporting_observations"]) <= ids
                assert "sentinel-secret" not in stdout + stderr
                assert "ambient-secret" not in stdout + stderr
                assert url not in stdout
                results.append({"case": name, "exit": proc.returncode, "status": report["status"]})
            before = state["requests"]
            unsupported = [wpdiag, "wpcli", "diagnose", "--config", str(root / "wpdiag.yaml"), "--connection", "local",
                           "--flow", "write", "--mode", "historical", "--from", "2026-10-01T00:00:00Z", "--to", "2026-10-01T01:00:00Z", "-o", "json"]
            proc = subprocess.run(unsupported, capture_output=True, text=True, timeout=3)
            assert proc.returncode == 3 and json.loads(proc.stdout)["status"] == "unsupported"
            assert state["requests"] == before, "Historical request silently queried current state"
            results.append({"case": "historical makes no request", "exit": 3, "status": "unsupported"})
            # Exercise the expanded native scenario through the real binary.
            # The standalone adapter deliberately keeps its focused v1 profile.
            native_env = dict(os.environ, WOODPECKER_CLI_CONFIG=str(root / "cli.yaml"))
            native_env.pop("WOODPECKER_ENDPOINT", None)
            native_base = [wp, "--context", "fixture", "diagnose", "--scenario", "write", "--window", "30ms",
                           "--interval", "10ms", "--max-samples", "4", "--budget", "2s", "--max-writers", "1", "-o", "json"]
            for mode, expected, outcome in [("native-buildup", 0, "no_durable_progress_observed"),
                                            ("native-progress", 0, "no_buildup_observed"),
                                            ("native-idle", 0, "no_loaded_writer_observed"),
                                            ("native-limit", 6, "inconclusive"),
                                            ("native-unavailable", 6, "inconclusive")]:
                state["mode"], state["writer_reads"] = mode, 0
                proc = subprocess.run(native_base, env=native_env, capture_output=True, text=True, timeout=5)
                report = json.loads(proc.stdout)
                assert proc.returncode == expected, (mode, proc.returncode, report, proc.stderr)
                assert len(report["rules"]) == 4
                findings = {f["rule_id"]: f for f in report["findings"]}
                assert findings["flush_buildup"]["outcome"] == outcome, (mode, report)
                assert findings["disk_admission"]["outcome"] == "below_configured_watermarks"
                assert findings["replica_participation"]["outcome"] == "inconclusive"
                assert state["writer_reads"] <= 4, "Writer detail request bound exceeded"
                ids = {o["id"] for o in report["observations"]}
                assert all(set(f["supporting_observations"]) <= ids for f in report["findings"])
                assert "sentinel-secret" not in proc.stdout + proc.stderr
                assert url not in proc.stdout
                results.append({"case": mode, "exit": proc.returncode, "status": report["collection_status"]})
            native_env = dict(os.environ, WOODPECKER_CLI_CONFIG=str(root / "cli.yaml"))
            native_env.pop("WOODPECKER_ENDPOINT", None)
            native_base = [wp, "--context", "fixture", "diagnose", "--scenario", "read", "--window", "30ms",
                           "--interval", "10ms", "--max-samples", "4", "--budget", "2s", "-o", "json"]
            for mode, expected, outcome, extra in [
                ("native-read-pending", 0, "pending_read_expectation_unknown", []),
                ("native-read-progress", 0, "forward_progress_observed", []),
                ("native-read-eof", 0, "eof_observed", []),
                ("native-read-idle", 0, "no_pending_read_observed", []),
                ("native-read-reset", 0, "inconclusive", []),
                ("native-read-ambiguous", 0, "inconclusive", []),
                ("native-read-unavailable", 6, "inconclusive", []),
                ("native-read-snapshot", 0, "inconclusive", ["--mode", "snapshot"]),
            ]:
                state["mode"], state["metric_reads"] = mode, 0
                proc = subprocess.run(native_base + extra, env=native_env, capture_output=True, text=True, timeout=5)
                report = json.loads(proc.stdout)
                assert proc.returncode == expected, (mode, proc.returncode, report, proc.stderr)
                assert len(report["rules"]) == 3
                findings = {f["rule_id"]: f for f in report["findings"]}
                assert findings["read_activity"]["outcome"] == outcome, (mode, report)
                assert findings["reader_progress"]["outcome"] == "inconclusive"
                assert findings["read_quorum_visibility"]["outcome"] == "inconclusive"
                ids = {o["id"] for o in report["observations"]}
                assert all(set(f["supporting_observations"]) <= ids for f in report["findings"])
                assert "sentinel-secret" not in proc.stdout + proc.stderr
                assert url not in proc.stdout
                results.append({"case": mode, "exit": proc.returncode, "status": report["collection_status"]})
        print(json.dumps({"passed": True, "checks": results}, indent=2))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


if __name__ == "__main__":
    main()
