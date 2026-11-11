"""Offline semantic/lifecycle counterexamples, not deployed evidence."""
import copy
import contextlib
import io
import json
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest
from unittest.mock import patch

import source_acceptance as s
import source_fixture as f

OWNER = "source-20261028t060000z-1234abcd"


def correlated():
    event = {"pattern": "wp.append_failure", "event_id": "actual-event", "trace_id": "1" * 32, "timestamp_ns": "101000"}
    span = {"span_id": "actual-span", "trace_id": "1" * 32, "selected": True, "start_us": 100, "duration_us": 2}
    report = lambda kind, data: {"results": [{"observations": [{"kind": kind, "status": "observed", "data": data}]}]}
    return {"runs": [{"module_id": "loki", "report": report("loki_log_page", {"events": [event]})},
                     {"module_id": "trace", "report": report("jaeger_trace", {"spans": [span]})}],
            "summary": {"associations": [{"identity": "1" * 32, "observations": [
                {"module_id": "loki", "event_id": "actual-event"}, {"module_id": "trace", "span_id": "actual-span"}]}]}}


class SourceTests(unittest.TestCase):
    def test_expired_budget_cannot_create_or_start_another_wait(self):
        with tempfile.TemporaryDirectory() as folder:
            fixture = f.Fixture("context", "fixture", folder, {})
            fixture.deadline = 0
            with patch.object(subprocess, "run", side_effect=AssertionError("Expired work must not execute")), self.assertRaises(RuntimeError):
                fixture.command(["create", "pod", OWNER])
            run = object.__new__(s.Run)
            run.deadline = 0
            with patch.object(s.time, "sleep", side_effect=AssertionError("Expired work must not wait")), self.assertRaises(RuntimeError):
                run.pause(2)

    def test_plan_is_offline_without_creating_artifacts(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "must-not-exist"
            with patch.object(sys, "argv", ["source_acceptance", "--context", "test", "--namespace", "test", "--output", str(output)]), \
                    patch.object(subprocess, "run", side_effect=AssertionError("Plan must not execute")), contextlib.redirect_stdout(io.StringIO()) as text:
                self.assertEqual(s.main(), 0)
            self.assertFalse(json.loads(text.getvalue())["execute"])
            self.assertFalse(output.exists())

    def test_create_journal_survives_ambiguous_api_outcome(self):
        with tempfile.TemporaryDirectory() as folder:
            receipt = {"run_id": OWNER, "resources": {}}
            fixture = f.Fixture("context", "fixture", folder, receipt)
            obj = f.manifests(OWNER, "fixture")[0]
            with patch.object(fixture, "command", side_effect=subprocess.TimeoutExpired("create", 30)), self.assertRaises(subprocess.TimeoutExpired):
                fixture.create(obj)
            journal = json.loads((Path(folder) / "fixture.json").read_text())
            self.assertEqual(journal["resources"]["configmap/" + OWNER]["intent"], obj)
            self.assertNotIn("uid", journal["resources"]["configmap/" + OWNER])

    def test_cleanup_keeps_lock_on_foreign_owner_before_any_delete(self):
        lock = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": f.LOCK, "uid": "lock",
                "labels": {f.RUN_LABEL: OWNER}}, "data": {"run-id": OWNER, "profile": f.PROFILE}}
        pod = f.manifests(OWNER, "fixture")[-1]
        wanted = copy.deepcopy(pod)
        pod["metadata"]["uid"] = "replacement"
        receipt = {"run_id": OWNER, "resources": {"configmap/" + f.LOCK: {"intent": lock, "uid": "lock"},
                                                  "pod/" + OWNER: {"intent": wanted, "uid": "original"}}}
        with tempfile.TemporaryDirectory() as folder:
            fixture = f.Fixture("context", "fixture", folder, receipt)
            commands = []
            def command(argv):
                commands.append(argv)
                return copy.deepcopy(lock if argv[1] in ("configmap", "configmap/" + f.LOCK) else pod)
            with patch.object(fixture, "namespace_check"), patch.object(fixture, "command", side_effect=command), self.assertRaises(RuntimeError):
                fixture.cleanup()
            self.assertTrue(all(argv[0] == "get" for argv in commands))
            self.assertNotIn("cleaned", receipt)

    def test_missing_lock_only_resolves_when_every_owned_name_is_absent(self):
        for remaining in (False, True):
            with self.subTest(remaining=remaining), tempfile.TemporaryDirectory() as folder:
                receipt = {"run_id": OWNER, "resources": {"configmap/" + f.LOCK: {"intent": {}}, "pod/" + OWNER: {"intent": {}}}}
                fixture = f.Fixture("context", "fixture", folder, receipt)
                commands = []
                def command(argv):
                    commands.append(argv)
                    return {"metadata": {"uid": "foreign"}} if remaining and argv[1] == "pod/" + OWNER else None
                with patch.object(fixture, "namespace_check"), patch.object(fixture, "command", side_effect=command):
                    if remaining:
                        with self.assertRaises(RuntimeError):
                            fixture.cleanup()
                    else:
                        fixture.cleanup()
                        self.assertTrue(receipt["cleaned"])
                self.assertTrue(all(argv[0] == "get" for argv in commands))

    def test_equivalence_checks_source_findings_not_only_stage_assessments(self):
        f = {"id": "finding", "rule_id": "trace.test", "rule_version": 1, "flow": "write", "stage": "append", "target": {},
             "outcome": "inconclusive", "severity": "unknown", "evidence_strength": "insufficient", "summary": "Partial",
             "unknown": ["parent_not_retained"], "supporting_observations": ["observation"]}
        r = {"results": [{"observations": [{"id": "observation"}], "findings": [f]}]}
        s.equivalent(r, copy.deepcopy(r))
        for key, value in (("unknown", []), ("severity", "info"), ("target", {"deployment": "other"}), ("outcome", "healthy")):
            changed = copy.deepcopy(r)
            changed["results"][0]["findings"][0][key] = value
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                s.equivalent(r, changed)
        for changed in ({"status": "failed", "results": r["results"]},
                        {"results": [{**r["results"][0], "coverage_gaps": [{"code": "new-gap"}]}]}):
            with self.assertRaises(RuntimeError):
                s.equivalent(r, changed)

    def test_positive_requires_original_event_span_and_time(self):
        self.assertEqual(s.positive_gate(correlated(), "1" * 32)["event_ids"], ["actual-event"])
        for name in ("wrong_id", "wrong_event", "wrong_span", "outside_time", "metrics", "duplicate_join", "no_join"):
            r = correlated()
            join = r["summary"]["associations"][0]
            if name == "wrong_id":
                r["runs"][0]["report"]["results"][0]["observations"][0]["data"]["events"][0]["trace_id"] = "2" * 32
            elif name == "wrong_event":
                join["observations"][0]["event_id"] = "unrelated-event"
            elif name == "wrong_span":
                join["observations"][1]["span_id"] = "unrelated-span"
            elif name == "outside_time":
                r["runs"][0]["report"]["results"][0]["observations"][0]["data"]["events"][0]["timestamp_ns"] = "103000"
            elif name == "metrics":
                join["observations"].append({"module_id": "prometheus"})
            elif name == "duplicate_join":
                r["summary"]["associations"].append(copy.deepcopy(join))
            else:
                r["summary"]["associations"] = []
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                s.positive_gate(r, "1" * 32)

    def test_owned_collector_does_not_rewrite_content_or_trace_identity(self):
        objects = f.manifests(OWNER, "fixture")
        config = objects[0]["data"]
        self.assertIn("loki.source.kubernetes", config["config.alloy"])
        self.assertIn(f.RUN_LABEL + "=" + OWNER, config["config.alloy"])
        self.assertNotIn("loki.process", config["config.alloy"])
        self.assertNotIn("traceID", config["config.alloy"])
        self.assertEqual(json.loads(config["wp.yaml"])["trace"]["sampleFraction"], 1)
        self.assertEqual(len(objects[-1]["spec"]["containers"]), 7)
        self.assertFalse(objects[-1]["spec"]["automountServiceAccountToken"])
        self.assertTrue(all(v.get("emptyDir", {}).get("sizeLimit") == "256Mi" for v in objects[-1]["spec"]["volumes"] if "emptyDir" in v))

    def test_cleanup_rejects_changed_uid_label_settings_and_owner(self):
        cm = f.manifests(OWNER, "fixture")[0]
        cm["metadata"]["uid"] = "original"
        receipt = {"run_id": OWNER, "resources": {"configmap/" + OWNER: {"intent": cm, "uid": "original"}}}
        with tempfile.TemporaryDirectory() as path:
            fixture = f.Fixture("context", "fixture", path, receipt)
            self.assertEqual(fixture.original("configmap/" + OWNER, cm), "original")
            for modify in (lambda r: r["metadata"].update(uid="foreign"),
                           lambda r: r["metadata"]["labels"].update({f.RUN_LABEL: "foreign"}),
                           lambda r: r["data"].update({"wp.yaml": "changed"})):
                row = copy.deepcopy(cm)
                modify(row)
                with self.assertRaises(RuntimeError):
                    fixture.original("configmap/" + OWNER, row)

    def test_namespace_uid_change_blocks_recovery(self):
        receipt = {"namespace_uid": "original"}
        with tempfile.TemporaryDirectory() as path:
            fixture = f.Fixture("context", "fixture", path, receipt)
            with patch.object(fixture, "command", return_value={"metadata": {"uid": "replacement", "labels": {f.LABEL: f.PROFILE}}}):
                with self.assertRaises(RuntimeError):
                    fixture.namespace_check()

    def test_malformed_names_cannot_provision(self):
        for owner in ("milvus", OWNER + "/../foreign", "source-20261028t060000z-ZZZZZZZZ"):
            with self.assertRaises(ValueError):
                f.manifests(owner, "fixture")


if __name__ == "__main__":
    unittest.main()
