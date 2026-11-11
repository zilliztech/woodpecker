#!/usr/bin/env python3
"""Tracing coverage must not confuse exporter presence with a connected request."""
import importlib.util
import contextlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("check_traces", ROOT / "scripts/check-traces.py")
tracing = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(tracing)


class TraceTests(unittest.TestCase):
    def test_proxy_insert_alone_is_not_e2e(self):
        result = tracing.coverage("entities/insert", ["proxy", "mixture"])
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["missing_services"], ["WP server", "streamingnode"])

    def test_write_needs_a_wp_server_in_the_same_trace(self):
        result = tracing.coverage("entities/insert", ["proxy", "streamingnode"])
        self.assertEqual(result["missing_services"], ["WP server"])
        self.assertEqual(tracing.coverage("entities/insert", ["proxy", "streamingnode", "milvus-woodpecker-1"])["status"], "observed")

    def test_query_does_not_require_historical_wp_read(self):
        self.assertEqual(tracing.coverage("entities/query", ["proxy", "streamingnode", "segcore"])["status"], "observed")

    def test_capture_keeps_partial_trace_and_raw_evidence(self):
        trace = {"traceID": "abc", "processes": {"p1": {"serviceName": "proxy"}},
                 "spans": [{"operationName": "Insert", "processID": "p1"}]}
        report = {"requests": [{"operation": "entities/insert", "trace_id": "abc"}]}
        with tempfile.TemporaryDirectory() as folder, patch.object(tracing, "get", return_value=[trace]), patch.object(tracing.time, "sleep"):
            tracing.capture(report, Path(folder) / "probe.json")
            self.assertEqual(report["missing_trace_ids"], [])
            self.assertEqual(len(report["coverage_gaps"]), 1)
            self.assertTrue(Path(report["requests"][0]["trace_file"]).exists())

    def test_missing_trace_is_not_reported_as_success(self):
        report = {"requests": [{"operation": "entities/insert", "trace_id": "abc"}]}
        with patch.object(tracing, "get", return_value=[]), patch.object(tracing.time, "sleep"):
            tracing.capture(report, None, timeout=0)
        self.assertEqual(report["missing_trace_ids"], ["abc"])

    def test_strict_mode_fails_on_a_gap_even_when_all_exporters_are_present(self):
        services = ["proxy", "streamingnode", "milvus-woodpecker-0", "milvus-woodpecker-1", "milvus-woodpecker-2"]
        def capture(report, output):
            report.update(missing_trace_ids=[], coverage_gaps=[{"operation": "entities/insert", "status": "partial"}])
        for strict, expected in ((False, 0), (True, 1)):
            argv = ["check-traces.py", "--smoke"] + (["--require-e2e"] if strict else [])
            with patch("sys.argv", argv), patch.dict(tracing.os.environ, {"TRACES_REQUIRE_E2E": "0"}), \
                    patch.object(tracing, "get", return_value=services), patch.object(tracing, "smoke"), \
                    patch.object(tracing, "capture", side_effect=capture), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(tracing.main(), expected)


if __name__ == "__main__":
    unittest.main()
