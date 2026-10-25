#!/usr/bin/env python3
"""Check log evidence gates without pushing synthetic records into Loki."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("check_logs", ROOT / "scripts/check-logs.py")
logs = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(logs)


class LoggingTests(unittest.TestCase):
    def test_query_escapes_namespace_pod_and_unique_marker(self):
        expression = logs.query('a"b', 'collection\\name', 'proxy"0')
        self.assertEqual(expression, '{namespace="a\\"b",pod="proxy\\"0"} |= "collection\\\\name"')

    def test_successful_workload_without_source_log_is_not_delivery_evidence(self):
        replies = [subprocess.CompletedProcess([], 0, "passed", ""),
                   subprocess.CompletedProcess([], 0, json.dumps({"items": [{"metadata": {"name": "proxy-0"}}]}), ""),
                   subprocess.CompletedProcess([], 0, "an unrelated log", "")]
        with patch.object(logs.subprocess, "run", side_effect=replies), self.assertRaisesRegex(RuntimeError, "not found"):
            logs.smoke("test-context", "test-namespace")

    def test_source_workload_and_kubernetes_commands_share_explicit_target(self):
        def run(args, **kwargs):
            if "workload.py" in args[-1]:
                self.assertEqual(kwargs["env"]["BATCH_SIZE"], "10")
                self.assertEqual(kwargs["env"]["MILVUS_URL"], "http://127.0.0.1:29530")
                return subprocess.CompletedProcess(args, 0, "passed", "")
            self.assertEqual(args[:5], ["kubectl", "--context", "test-context", "-n", "test-namespace"])
            output = json.dumps({"items": [{"metadata": {"name": "proxy-0"}}]}) if "get" in args else "collection=log_smoke_123"
            return subprocess.CompletedProcess(args, 0, output, "")
        with patch.object(logs.time, "time_ns", return_value=123), patch.dict(logs.os.environ, {"MILVUS_URL": "http://127.0.0.1:29530"}), patch.object(logs.subprocess, "run", side_effect=run):
            source = logs.smoke("test-context", "test-namespace")
        self.assertEqual(source["marker"], "log_smoke_123")
        self.assertEqual(source["pod"], "proxy-0")

    def test_ready_loki_with_no_collected_logs_fails(self):
        response = Mock(status=200)
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        with patch.object(sys, "argv", ["check-logs.py"]), patch.object(logs.OPENER, "open", return_value=response), patch.object(logs, "fetch", return_value=[]), contextlib.redirect_stdout(io.StringIO()) as output:
            result = logs.main()
        report = json.loads(output.getvalue())
        self.assertEqual(result, 1)
        self.assertTrue(report["ready"])
        self.assertFalse(report["passed"])
        self.assertIn("No matching", report["failures"][0])


if __name__ == "__main__":
    unittest.main()
