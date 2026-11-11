"""Offline/browser-tool counterexamples; these are not deployed UI receipts."""
import contextlib
import copy
import hashlib
import http.server
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request

import webui_acceptance as ui
from webui_probe import Probe


def request():
    return {"selection": {"scope": "wp", "flow": "write", "sources": [], "target": {},
        "window": {"mode": "historical", "from": "2026-10-28T10:00:00.000Z", "to": "2026-10-28T10:01:00.000Z"}},
        "modules": ["prometheus"], "connections": {"prometheus": "actual"}, "source_targets": {},
        "limits": {"budget_ms": 60000, "max_input_bytes": 8388608, "max_output_bytes": 16777216, "max_targets": 8}}


class BrowserAcceptanceTests(unittest.TestCase):
    def stored_case(self, folder):
        q = request()
        observation = {"id": "original-observation", "data": {"timestamp_ns": 1793182000000000001}}
        child = {"results": [{"observations": [observation], "findings": [{"id": "original-finding",
            "supporting_observations": ["original-observation"]}]}]}
        plan = {"request": q, "problems": []}
        report = {"status": "completed", "plan": plan, "runs": [{"module_id": "prometheus", "report": child}],
                  "summary": {"findings": [{"id": "original-finding"}]}}
        row = {"report": "report.json", "plan": "plan.json", "request": "request.json", "status": "completed",
               "rendered_ids": ["original-finding"], "evidence": [{"module": "prometheus", "id": observation["id"], "file": "evidence.json"}]}
        for name, data in (("report.json", report), ("plan.json", plan), ("request.json", q), ("evidence.json", observation)):
            (folder / name).write_text(json.dumps(data))
        row["sha256"] = hashlib.sha256((folder / row["report"]).read_bytes()).hexdigest()
        return row, report

    def test_stored_report_preserves_ids_bytes_and_large_evidence_integer(self):
        with tempfile.TemporaryDirectory() as root:
            folder = Path(root)
            row, report = self.stored_case(folder)
            self.assertEqual(ui.verify_stored_run(folder, row), report)
            for kind in ("bytes", "finding", "omitted", "integer", "window", "status"):
                row, report = self.stored_case(folder)
                if kind == "bytes":
                    (folder / "report.json").write_text("{}")
                elif kind == "finding":
                    row["rendered_ids"] = ["fabricated-finding"]
                elif kind == "omitted":
                    row["rendered_ids"] = []
                elif kind == "integer":
                    evidence = json.loads((folder / "evidence.json").read_text())
                    evidence["data"]["timestamp_ns"] = 1793182000000000000
                    (folder / "evidence.json").write_text(json.dumps(evidence))
                elif kind == "window":
                    q = request()
                    q["selection"]["window"]["to"] = "2026-10-28T10:02:00Z"
                    (folder / "request.json").write_text(json.dumps(q))
                else:
                    row["status"] = "failed"
                with self.subTest(kind=kind), self.assertRaises(RuntimeError):
                    ui.verify_stored_run(folder, row)

    def test_plan_does_not_execute_or_write_artifacts(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "absent"
            with patch.object(subprocess, "run", side_effect=AssertionError("Offline plan executed")), \
                 patch.object(subprocess, "Popen", side_effect=AssertionError("Offline plan spawned")), \
                 contextlib.redirect_stdout(io.StringIO()) as text:
                self.assertEqual(ui.main(["--context", "explicit", "--namespace", "disposable", "--output", str(output)]), 0)
            plan = json.loads(text.getvalue())
            self.assertFalse(plan["execute"])
            self.assertEqual(set(plan["cases"]), ui.CASES)
            self.assertFalse(output.exists())

    def test_time_format_normalization_preserves_selection(self):
        before = request()
        after = copy.deepcopy(before)
        after["selection"]["sources"] = ["prometheus"]
        after["selection"]["window"]["from"] = "2026-10-28T10:00:00Z"
        after["selection"]["window"]["to"] = "2026-10-28T10:01:00+00:00"
        ui.verify_request(before, after)

    def test_changed_selection_window_target_and_limits_cannot_pass(self):
        for field in ("scope", "flow", "profile", "stage", "target", "window", "modules", "connections", "source_targets", "limits", "sources"):
            before, after = request(), request()
            if field in ("scope", "flow", "profile", "stage"):
                after["selection"][field] = "other"
            elif field == "target":
                after["selection"]["target"] = {"deployment": "another"}
            elif field == "window":
                after["selection"]["window"]["to"] = "2026-10-28T10:01:01Z"
            elif field == "sources":
                before["selection"]["sources"] = ["prometheus"]
                after["selection"]["sources"] = ["trace"]
            else:
                after[field] = {"changed": True}
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                ui.verify_request(before, after)

    def test_artifacts_cannot_escape_owned_folder(self):
        with tempfile.TemporaryDirectory() as root:
            folder = Path(root) / "browser"
            folder.mkdir()
            outside = Path(root) / "other.json"
            outside.write_text("{}")
            (folder / "symlink.json").symlink_to(outside)
            for name in ("../other.json", str(outside), "symlink.json"):
                with self.subTest(name=name), self.assertRaises(RuntimeError):
                    ui.checked_artifact(folder, name)

    def test_boolean_success_without_cases_is_not_acceptance(self):
        for result in ({"passed": False}, {"passed": True, "uncaught_exceptions": ["UI crashed"]},
                       {"passed": True, "uncaught_exceptions": [], "runs": []}):
            with self.subTest(result=result), self.assertRaises(RuntimeError):
                ui.verify_browser(Path("/unused"), result)

    def test_cli_uses_exact_window_and_named_source_connections(self):
        q = request()
        argv = ui.cli_arguments(q, "private.json", "wpdiag")
        self.assertIn("--connections", argv)
        self.assertIn("prometheus=actual", argv)
        self.assertEqual(argv[argv.index("--from") + 1], q["selection"]["window"]["from"])
        self.assertIn("--modules", argv)
        self.assertNotIn("--source-connection", argv)

    def test_browser_process_group_exits_on_timeout(self):
        with self.assertRaises(subprocess.TimeoutExpired):
            ui.run_browser([sys.executable, "-c", "import time; time.sleep(60)"], os.environ, timeout=.1)

    def test_server_startup_failure_reaps_owned_process(self):
        with tempfile.TemporaryDirectory() as root:
            binary = Path(root) / "fake-server"
            binary.write_text("#!/bin/sh\nexit 1\n")
            binary.chmod(0o700)
            server = ui.BrowserServer(str(binary), "unused", Path(root), os.environ)
            with self.assertRaises(RuntimeError):
                server.__enter__()
            self.assertIsNotNone(server.process.poll())
            self.assertTrue(server.log.closed)

    def test_interruption_during_server_startup_reaps_process(self):
        with tempfile.TemporaryDirectory() as root:
            binary = Path(root) / "fake-server"
            binary.write_text("#!" + sys.executable + "\nimport time\ntime.sleep(60)\n")
            binary.chmod(0o700)
            server = ui.BrowserServer(str(binary), "unused", Path(root), os.environ)
            with patch.object(ui.queue.Queue, "get", side_effect=KeyboardInterrupt()), self.assertRaises(KeyboardInterrupt):
                server.__enter__()
            self.assertIsNotNone(server.process.poll())
            self.assertTrue(server.log.closed)


class ProbeTests(unittest.TestCase):
    def setUp(self):
        self.body = b'{"data":{"integer":9223372036854775807},"status":"success"}'
        self.calls = []
        outer = self
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass
            def do_GET(self):
                outer.calls.append((self.path, self.headers.get("Authorization")))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(outer.body)
        self.backend = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.backend.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = "http://127.0.0.1:" + str(self.backend.server_port)

    def tearDown(self):
        self.backend.shutdown()
        self.backend.server_close()
        self.thread.join(timeout=2)

    def test_relay_preserves_real_response_bytes_and_never_records_secret(self):
        with Probe({"prometheus": self.endpoint}, "private-token", "private-error") as probe:
            path = "/api/v1/query_range?query=actual_counter&start=1&end=2&step=1"
            req = urllib.request.Request(probe.url + "/normal/prometheus" + path, headers={"Authorization": "Bearer private-token"})
            with urllib.request.urlopen(req) as response:
                self.assertEqual(response.read(), self.body)
            self.assertEqual(self.calls, [(path, "Bearer private-token")])
            rows = probe.snapshot()
            self.assertTrue(rows[0]["authenticated"])
            self.assertNotIn("private-token", json.dumps(rows))
            self.assertEqual(rows[0]["response_bytes"], len(self.body))

    def test_write_and_unregistered_routes_never_reach_backend(self):
        with Probe({"prometheus": self.endpoint}, "token", "error") as probe:
            for path, method, status in (("/normal/prometheus/api/v1/admin/tsdb/delete_series", "GET", 403),
                                        ("/normal/prometheus/api/v1/query", "POST", 501),
                                        ("/unknown/prometheus/api/v1/query", "GET", 404)):
                with self.subTest(path=path), self.assertRaises(urllib.error.HTTPError) as error:
                    urllib.request.urlopen(urllib.request.Request(probe.url + path, method=method))
                self.assertEqual(error.exception.code, status)
            self.assertEqual(self.calls, [])

    def test_explicit_401_is_labeled_synthetic_transport(self):
        with Probe({"prometheus": self.endpoint}, "token", "private-error") as probe:
            with self.assertRaises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(probe.url + "/deny/prometheus/api/v1/query")
            self.assertEqual(error.exception.read(), b"private-error")
            rows = probe.snapshot()
            self.assertTrue(rows[0]["controlled_transport"])
            self.assertEqual(self.calls, [])
            self.assertNotIn("private-error", json.dumps(rows))

    def test_held_response_records_actual_query_then_socket_cancellation(self):
        with Probe({"prometheus": self.endpoint}, "token", "error") as probe:
            port = probe.server.server_port
            client = socket.create_connection(("127.0.0.1", port), timeout=2)
            try:
                client.sendall(b"GET /hold/prometheus/api/v1/query?query=up HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n")
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline and not any(r.get("backend_finished_at") for r in probe.snapshot()):
                    time.sleep(.01)
                self.assertEqual(len(self.calls), 1)
                self.assertTrue(probe.snapshot()[0].get("backend_finished_at"))
            finally:
                client.close()
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline and not any(r.get("client_disconnected_at") for r in probe.snapshot()):
                time.sleep(.01)
            self.assertTrue(probe.snapshot()[0].get("client_disconnected_at"))
            self.assertFalse(probe.snapshot()[0].get("hold_expired"))

    def test_external_endpoint_is_rejected(self):
        with self.assertRaises(ValueError):
            Probe({"prometheus": "http://external.invalid:9090"}, "token", "error")


if __name__ == "__main__":
    unittest.main()
