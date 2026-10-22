#!/usr/bin/env python3
"""Exercise concurrent writes, business errors and diagnostic failures with local fakes."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("perf", Path(__file__).resolve().parents[1] / "scripts/perf.py")
perf = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(perf)


class PerfTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.rows = {}
        self.duplicates = []
        self.inserts = 0
        self.traceparents = []
        self.business_error = False
        self.lost_row = False
        self.metrics_down = False
        self.lock = threading.Lock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, body, code=200):
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(body).encode())

            def do_GET(self):
                if "/api/v2/alerts" in self.path:
                    return self.reply([])
                if owner.metrics_down:
                    return self.reply({"error": "monitoring unavailable"}, 503)
                if "ALERTS" in urllib.parse.unquote(self.path):
                    return self.reply({"status": "success", "data": {"result": []}})
                values = [[0, str(len(owner.rows))]]
                if "/query_range?" in self.path:
                    return self.reply({"status": "success", "data": {"resultType": "matrix", "result": [
                        {"metric": {}, "values": values}]}})
                self.reply({"status": "success", "data": {"resultType": "vector", "result": [
                    {"metric": {}, "value": values[0]}]}})

            def do_POST(self):
                data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                path = self.path.rsplit("/", 1)[-1]
                if path == "create":
                    return self.reply({"code": 0, "data": {}})
                if path == "describe":
                    return self.reply({"code": 0, "data": {"collectionID": 12345}})
                if path == "insert":
                    with owner.lock:
                        owner.inserts += 1
                        owner.traceparents.append(self.headers.get("traceparent"))
                        if owner.business_error:
                            return self.reply({"code": 65535, "message": "simulated append failure"})
                        for row in data["data"]:
                            if row["id"] in owner.rows:
                                owner.duplicates.append(row["id"])
                            owner.rows[row["id"]] = row
                        if owner.lost_row:
                            owner.rows.pop(data["data"][-1]["id"], None)
                    return self.reply({"code": 0, "data": {"insertCount": len(data["data"])}})
                if path == "query":
                    if data["outputFields"] == ["count(*)"]:
                        return self.reply({"code": 0, "data": [{"count(*)": len(owner.rows)}]})
                    ids = json.loads(data["filter"].removeprefix("id in "))
                    return self.reply({"code": 0, "data": [{"id": i} for i in ids if i in owner.rows]})
                if path == "search":
                    wanted = int(data["filter"].removeprefix("id == "))
                    return self.reply({"code": 0, "data": [{"id": wanted}] if wanted in owner.rows else []})
                self.reply({"code": 1, "message": "unexpected endpoint"})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = "http://127.0.0.1:" + str(self.server.server_port)
        binaries = self.root / "bin"
        binaries.mkdir()
        backend = binaries / "kubectl"
        backend.write_text("#!" + sys.executable + "\n" + '''import json, sys
if "pods" in sys.argv:
    print(json.dumps({"items": [{"metadata": {"name": "milvus-proxy-test", "uid": "test-pod", "labels": {
        "app.kubernetes.io/instance": "milvus"}}, "status": {}, "spec": {"containers": [{
        "name": "proxy", "image": "test:1", "env": [{"name": "PASSWORD", "value": "must-not-export"}]}]}}]}))
else:
    print("test diagnostic output")
''')
        backend.chmod(0o755)
        self.environment = {"PATH": str(binaries) + os.pathsep + os.environ["PATH"]}

    def run_benchmark(self, *extra):
        args = perf.arguments(["--milvus", self.url, "--prometheus", self.url, "--alertmanager", self.url,
                               "--output", str(self.root / "runs"), "--duration", "1", "--warmup", "0",
                               "--cooldown", "0", "--batch-size", "4", "--dimension", "4",
                               "--rps", "10", "--concurrency", "3", "--max-errors", "1", *extra])
        with patch.dict(os.environ, self.environment), contextlib.redirect_stdout(io.StringIO()):
            benchmark = perf.Benchmark(args)
            code = benchmark.run()
        return code, benchmark, json.loads((benchmark.out / "report.json").read_text())

    def test_concurrent_warmup_measurement_and_correctness(self):
        code, benchmark, report = self.run_benchmark("--warmup", "0.2")
        self.assertEqual(code, 0, report["failures"] + report["diagnostic_errors"])
        self.assertTrue(report["passed"])
        self.assertEqual(self.duplicates, [])
        self.assertEqual(report["correctness"]["strong_read_count"], len(self.rows))
        self.assertEqual(report["phases"]["measure"]["acknowledged_rows"], 40)
        self.assertEqual(report["phases"]["warmup"]["acknowledged_rows"], 8)
        self.assertEqual(report["phases"]["measure"]["success_latency_ms"]["samples"], 10)
        records = [json.loads(line) for line in (benchmark.out / "requests.jsonl").read_text().splitlines()]
        self.assertEqual(len({r["request_id"] for r in records}), len(records))
        self.assertEqual({p.split("-")[1] for p in self.traceparents}, {r["trace_id"] for r in records})
        self.assertTrue(all(p.endswith("-01") for p in self.traceparents))
        self.assertTrue(all(r["trace_sampled"] and r["trace_url"].endswith(r["trace_id"]) for r in records))
        self.assertTrue((benchmark.out / "milvus-proxy-test-proxy.log").exists())
        self.assertNotIn("must-not-export", (benchmark.out / "pods.json").read_text())
        self.assertTrue((benchmark.out / "metric-server_appends.json").exists())
        self.assertNotIn("alerts", report["coverage_gaps"])
        self.assertFalse(report["alert_states_observed"])

    def test_business_failure_is_not_retried_or_reported_as_success(self):
        self.business_error = True
        code, benchmark, report = self.run_benchmark("--concurrency", "1")
        self.assertEqual(code, 1)
        self.assertFalse(report["passed"])
        self.assertEqual(self.inserts, 1)
        records = [json.loads(line) for line in (benchmark.out / "requests.jsonl").read_text().splitlines()]
        self.assertIn("Milvus code 65535", records[0]["error"])
        self.assertIsNone(report["phases"]["measure"]["success_latency_ms"])
        self.assertTrue((benchmark.out / "pods.json").exists())

    def test_trace_sampling_can_be_disabled_for_performance_comparisons(self):
        code, benchmark, report = self.run_benchmark("--trace-sample-fraction", "0")
        self.assertEqual(code, 0, report["failures"])
        self.assertTrue(all(p.endswith("-00") for p in self.traceparents))
        self.assertTrue(all(not r["trace_sampled"] and r["trace_url"] is None for r in benchmark.records))

    def test_missing_acknowledged_rows_fails_strong_read_check(self):
        self.lost_row = True
        code, _, report = self.run_benchmark()
        self.assertEqual(code, 1)
        self.assertTrue(any("Strong read count" in failure for failure in report["failures"]))

    def test_missing_monitoring_fails_and_keeps_workload_and_logs(self):
        self.metrics_down = True
        code, benchmark, report = self.run_benchmark()
        self.assertEqual(code, 1)
        self.assertTrue(report["correctness"]["search_passed"])
        self.assertTrue(report["diagnostic_errors"])
        self.assertTrue((benchmark.out / "milvus-proxy-test-proxy.log").exists())

    def test_configured_latency_gate_fails_without_hiding_successful_writes(self):
        code, _, report = self.run_benchmark("--max-p99-ms", "0.000001")
        self.assertEqual(code, 1)
        self.assertTrue(report["correctness"]["search_passed"])
        self.assertTrue(any("p99" in failure for failure in report["failures"]))

    def test_invalid_configuration_does_not_create_run_artifacts(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            perf.arguments(["--duration", "nan", "--output", str(self.root / "bad")])
        self.assertFalse((self.root / "bad").exists())


if __name__ == "__main__":
    unittest.main()
