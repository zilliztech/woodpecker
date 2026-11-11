#!/usr/bin/env python3
"""Smoke success requires flush completion and correct post-flush reads."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("workload", Path(__file__).resolve().parents[1] / "scripts/workload.py")
workload = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(workload)


class WorkloadTests(unittest.TestCase):
    def run_workload(self, lost_rows=False, flush_error=False):
        rows = []
        flushed = False

        def call(path, payload):
            nonlocal flushed
            if path == "entities/insert":
                rows.extend(payload["data"])
                return {"insertCount": len(payload["data"])}
            if path == "collections/flush":
                if flush_error:
                    raise RuntimeError("flush rejected")
                flushed = True
                if lost_rows:
                    rows.pop()
            if path == "entities/query":
                self.assertEqual(payload["consistencyLevel"], "Strong")
                if payload["outputFields"] == ["count(*)"]:
                    self.assertTrue(flushed)
                    return [{"count(*)": len(rows)}]
                return [{"id": rows[0]["id"]}]
            if path == "entities/search":
                return [{"id": rows[0]["id"]}]
            return {}

        output = io.StringIO()
        with patch.dict(os.environ, BATCHES="2", BATCH_SIZE="3", INTERVAL="0"), \
                patch.object(workload, "call", side_effect=call), contextlib.redirect_stdout(output):
            workload.main()
        return [json.loads(line) for line in output.getvalue().splitlines()]

    def test_small_workload_flushes_and_checks_complete_rows(self):
        events = self.run_workload()
        self.assertEqual(events[-2], {"stage": "flushed", "rows": 6, "query_and_search": "passed"})
        self.assertEqual(events[-1]["stage"], "passed")

    def test_successful_insert_does_not_hide_missing_post_flush_rows(self):
        with self.assertRaisesRegex(RuntimeError, "row count after flush"):
            self.run_workload(lost_rows=True)

    def test_flush_failure_does_not_report_success(self):
        with self.assertRaisesRegex(RuntimeError, "flush rejected"):
            self.run_workload(flush_error=True)


if __name__ == "__main__":
    unittest.main()
