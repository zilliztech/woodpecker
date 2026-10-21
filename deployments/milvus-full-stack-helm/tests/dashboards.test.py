#!/usr/bin/env python3
"""Verify E2E isolation, timer semantics and stage-specific coverage failures."""
import importlib.util
import json
from pathlib import Path
import re
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


dashboards = module("e2e_dashboards", "e2e_dashboards.py")
checker = module("check", "check.py")


class DashboardTests(unittest.TestCase):
    def test_checked_in_dashboards_match_authored_steps(self):
        for key, title, flow, stages in dashboards.definitions():
            with self.subTest(chain=key):
                expected = dashboards.build_dashboard(key, title, flow, stages)
                actual = json.loads((ROOT / "charts/full-stack-support/dashboards" / ("e2e-" + key + ".json")).read_text())
                self.assertEqual(actual, expected)
                rows = [panel["title"] for panel in actual["panels"] if panel["type"] == "row"]
                self.assertEqual(rows, [stage["title"] for stage in stages])
                self.assertEqual(len({panel["id"] for panel in actual["panels"]}), len(actual["panels"]))
                self.assertTrue(all(not panel.get("collapsed") for panel in actual["panels"]))

    def test_deployment_tenant_and_channel_scopes_remain_separate(self):
        expressions = [target["expr"] for _, title, flow, stages in dashboards.definitions()
                       for stage in stages for plot in stage["plots"] for target in plot["targets"]]
        for expression in expressions:
            self.assertIn(dashboards.SCOPE, expression)
            expanded = checker.expand(expression, {})
            self.assertNotIn("$", expanded)
            if re.search(r"woodpecker_(client|server)_", expression):
                self.assertNotIn("channel_name", expression)
            if "log_ns=" in expression:
                self.assertIn("log_id=", expression)
            self.assertNotIn("or vector(0)", expression)
            self.assertNotIn('component="querynode"', expression)

    def test_timestamp_differences_match_pod_channel_before_subtracting(self):
        expression = dashboards.delta("milvus_wal_last_allocated_time_tick", "milvus_wal_last_confirmed_time_tick")
        self.assertIn("- on (pod,channel_name)", expression)
        self.assertIn("max by (pod,channel_name)", expression)
        self.assertTrue(expression.startswith("clamp_min("))

    def test_second_histograms_convert_to_milliseconds_and_keep_sample_scope(self):
        spec = dashboards.latency("Example", "duration_seconds", "pod,channel_name", dashboards.CHANNEL, seconds=True)
        self.assertEqual(spec["unit"], "ms")
        for target in spec["targets"]:
            self.assertIn("sum by (le,pod,channel_name)", target["expr"])
            self.assertTrue(target["expr"].endswith(" * 1000"))
        self.assertNotIn(" * 1000", dashboards.latency("Example", "wp_latency")["targets"][0]["expr"])

    def test_required_coverage_cannot_pass_with_only_up_or_other_stage_data(self):
        stage = dashboards.stage("wal", "WAL", "Inspect backend", [], [dashboards.probe("milvus_wal_append_message_total", required=True)])
        with patch.object(checker, "query", return_value={"status": "success", "data": {"result": []}}):
            result, failures = checker.stage_coverage([stage])
        self.assertEqual(result[0]["status"], "missing-required")
        self.assertIn("milvus_wal_append_message_total", failures[0])

    def test_optional_idle_or_version_specific_family_does_not_fail_validation(self):
        stage = dashboards.stage("read", "Read", "Inspect mode", [], [dashboards.probe("remote_only")])
        with patch.object(checker, "query", return_value={"status": "success", "data": {"result": []}}):
            result, failures = checker.stage_coverage([stage])
        self.assertFalse(failures)
        self.assertEqual(result[0]["status"], "partial")
        self.assertEqual(result[0]["missing_metrics"], ["remote_only"])

    def test_rejected_optional_query_is_an_error_not_a_coverage_gap(self):
        stage = dashboards.stage("read", "Read", "Inspect mode", [], [dashboards.probe("remote_only")])
        with patch.object(checker, "query", return_value={"status": "error", "error": "bad selector"}):
            _, failures = checker.stage_coverage([stage])
        self.assertIn("rejected coverage queries", failures[0])

    def test_normal_eof_and_local_consumption_are_explained_without_false_error_sum(self):
        definitions = {key: stages for key, _, _, stages in dashboards.definitions()}
        text = " ".join(stage["guidance"] for stage in definitions["consume"])
        for term in ("EOF", "local", "read-frontier", "StreamingNode", "blocking read_next"):
            self.assertIn(term, text)
        plots = [plot for stage in definitions["consume"] for plot in stage["plots"]]
        self.assertTrue(any("{{status}}" in target["legendFormat"] for plot in plots for target in plot["targets"]))

    def test_all_dashboard_links_exist_and_e2e_links_preserve_filter_values(self):
        uids = {json.loads(path.read_text())["uid"] for path in (ROOT / "charts/full-stack-support/dashboards").glob("*.json")}
        for link in dashboards.navigation():
            self.assertIn(link["url"].removeprefix("/d/"), uids)
            self.assertTrue(link["keepTime"])
            self.assertEqual(link["includeVars"], link["url"].startswith("/d/milvus-e2e-"))


if __name__ == "__main__":
    unittest.main()
