"""Source/transport/lifecycle counterexamples; these are not deployment receipts."""
import contextlib
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import cli_acceptance as shared
import query_acceptance as q
import query_recover as recovery


def evidence(rule, outcome, operations=("query", "search")):
    observations, source, stage = [], [], []
    for operation in operations:
        oid, pop = "obs-" + operation, "pop-" + operation
        original = oid + ":" + pop
        observations.append({"id": oid, "status": "observed", "kind": "prometheus_matrix", "data": {"facts": [
            {"population_id": pop, "labels": {"query_type": operation}, "observed_completions": 40}]}})
        source.append({"id": original})
        stage.append({"id": original + ":stage", "rule_id": rule, "outcome": outcome, "derived_from_findings": [original]})
    return {"results": [{"observations": observations, "findings": source}], "assessments": [{"findings": stage}]}


def workload(offset=2000):
    records = []
    for i in range(2):
        ts = 470000000000000123 + i
        records.append({"operation": "insert", "expected_id": 64 + i, "ok": True, "ack_timestamp": str(ts)})
        for op in ("query", "search"):
            for control in (False, True):
                delta = offset if not control else 0
                records.append({"operation": op, "strong_control": control, "expected_id": 64 + i, "ok": True,
                    "ack_timestamp": str(ts), "guarantee_timestamp": str(ts + (delta << 18)) if delta else "0",
                    "consistency": "Customized" if delta else "Strong", "latency_ms": 2100 if delta else 20})
    return {"passed": True, "records": records}


class GateTests(unittest.TestCase):
    def test_missing_single_operation_or_broken_lineage_is_not_qualified(self):
        report = evidence("milvus.query.proxy_completion", "within_configured_latency_for_observed_completions")
        self.assertEqual(set(q.populations(report, "milvus.query.proxy_completion", {"within_configured_latency_for_observed_completions"})), {"query", "search"})
        for changed in (evidence("milvus.query.proxy_completion", "inconclusive"),
                        evidence("milvus.query.proxy_completion", "within_configured_latency_for_observed_completions", ("query",))):
            with self.assertRaises(RuntimeError):
                q.metric_gate(changed, "baseline")
        report["assessments"][0]["findings"][1]["derived_from_findings"] = ["unrelated"]
        with self.assertRaises(RuntimeError):
            q.populations(report, "milvus.query.proxy_completion", {"within_configured_latency_for_observed_completions"})

    def test_idle_requires_observed_idle_populations_not_absent_series(self):
        report = evidence("milvus.query.proxy_completion", "idle_or_no_completions")
        self.assertEqual(set(q.metric_gate(report, "idle")), {"query", "search"})
        report["results"][0]["observations"] = []
        with self.assertRaises(RuntimeError):
            q.metric_gate(report, "idle")

    def test_server_timestamp_precision_and_strong_control_boundary(self):
        valid = workload()
        self.assertGreater(q.workload_gate(valid, 2, 2000)["query_target"]["p95"], 2000)
        for mutation in ("wrong_fence", "strong_changed", "wrong_marker", "failed_rpc", "no_contrast"):
            changed = copy.deepcopy(valid)
            if mutation == "wrong_fence":
                changed["records"][1]["guarantee_timestamp"] = str(int(changed["records"][1]["guarantee_timestamp"]) + 1)
            elif mutation == "strong_changed":
                changed["records"][2]["consistency"] = "Customized"
            elif mutation == "wrong_marker":
                changed["records"][1]["expected_id"] = 999
            elif mutation == "failed_rpc":
                changed["records"][1]["ok"] = False
            else:
                for r in changed["records"]:
                    if r["operation"] != "insert":
                        r["latency_ms"] = 20
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                q.workload_gate(changed, 2, 2000)

    def test_findings_equivalence_keeps_unknowns_and_targets(self):
        f = {"id": "first", "rule_id": "rule", "rule_version": 1, "outcome": "inconclusive", "severity": "unknown",
             "evidence_strength": "inconclusive", "flow": "read", "stage": "query", "target": {"instance": "node"},
             "summary": "Missing identity", "unknown": ["no_mapping"]}
        a = {"assessments": [{"findings": [f]}]}
        b = copy.deepcopy(a)
        b["assessments"][0]["findings"][0]["id"] = "another-run"
        self.assertEqual(q.signature(a), q.signature(b))
        b["assessments"][0]["findings"][0]["unknown"] = []
        self.assertNotEqual(q.signature(a), q.signature(b))


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.folder = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)

    def test_plan_is_offline_and_bounded(self):
        target = self.folder / "absent"
        with patch.object(q, "Acceptance", side_effect=AssertionError("offline")), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(q.main(["--context", "test", "--namespace", "test", "--output", str(target)]), 0)
        self.assertFalse(target.exists())
        for extra in (["--budget", "nan"], ["--batches", "1000"], ["--future-ms", "100000"], ["--context", "--invalid"]):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                q.arguments(["--context", "test", "--namespace", "test"] + extra)

    def test_failure_cleans_only_owned_data_and_original_lock(self):
        a = q.Acceptance(q.arguments(["--context", "test", "--namespace", "test", "--output", str(self.folder)]))
        calls = []
        def setup():
            a.owned, a.lock_uid, a.milvus = True, "owned-lock", "http://localhost"
            raise RuntimeError("request failed after creation")
        def command(argv, **kwargs):
            if "delete" in argv:
                calls.append("delete_lock")
            return json.dumps({"metadata": {"uid": "owned-lock"}, "data": {"run-id": a.run_id}})
        with patch.object(a, "setup", side_effect=setup), patch.object(q, "drop_owned", side_effect=lambda endpoint, name: calls.append(name)), \
             patch.object(shared, "command", side_effect=command), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(a.run(), 1)
        self.assertEqual(calls, [a.name, "delete_lock"])
        self.assertTrue(a.report["collection_dropped"])
        self.assertFalse(a.report["passed"])

    def test_failed_cleanup_preserves_lock(self):
        a = q.Acceptance(q.arguments(["--context", "test", "--namespace", "test", "--output", str(self.folder)]))
        a.owned, a.lock_uid, a.milvus = True, "owned-lock", "http://localhost"
        with patch.object(q, "drop_owned", side_effect=RuntimeError("unavailable")), patch.object(shared, "command", side_effect=AssertionError("keep lock")):
            a.finish()
        self.assertFalse(a.report["passed"])
        self.assertNotIn("lock_released", a.report)

    def test_recovery_refuses_wrong_target_and_unowned_name_offline(self):
        receipt = {"schema_version": "wpdiag.query-acceptance/v1", "profile": q.PROFILE, "run_id": "20261027T000000Z-aabbccdd",
            "selection": {"context": "test", "namespace": "test", "release": "milvus"}, "collection_owned": True,
            "collection": "query_accept_20261027T000000Z_aabbccdd", "lock_uid": "owned"}
        shared.save(self.folder / "acceptance.json", receipt)
        with patch.object(shared, "command", side_effect=AssertionError("offline")):
            with self.assertRaisesRegex(RuntimeError, "target"):
                recovery.load(self.folder, "foreign", "test")
            receipt["collection"] = "unrelated"
            shared.save(self.folder / "acceptance.json", receipt)
            with self.assertRaisesRegex(RuntimeError, "unowned"):
                recovery.main(["--run", str(self.folder), "--context", "test", "--namespace", "test", "--execute"])

    def test_already_absent_owned_collection_is_idempotent(self):
        with patch.object(shared, "http", return_value={"data": {"has": False}}) as http:
            q.drop_owned("http://localhost", "query_accept_20261027T000000Z_aabbccdd")
            self.assertEqual(http.call_count, 1)
            with self.assertRaises(RuntimeError):
                q.drop_owned("http://localhost", "existing-user-collection")
            self.assertEqual(http.call_count, 1)

    def test_crash_recovery_preserves_failed_receipt_and_releases_only_original_lock(self):
        self.crash_recovery(owned=True)

    def test_recovery_never_drops_a_name_that_preflight_did_not_own(self):
        self.crash_recovery(owned=False)

    def crash_recovery(self, owned):
        receipt = {"schema_version": "wpdiag.query-acceptance/v1", "profile": q.PROFILE, "run_id": "20261027T000000Z-aabbccdd",
            "selection": {"context": "test", "namespace": "test", "release": "milvus"}, "collection_owned": owned,
            "collection": "query_accept_20261027T000000Z_aabbccdd", "lock_uid": "original", "passed": False}
        shared.save(self.folder / "acceptance.json", receipt)
        calls = []
        def command(argv):
            if "namespace" in argv:
                return json.dumps({"metadata": {"labels": {q.LABEL: q.PROFILE}}})
            if "delete" in argv:
                calls.append("release-lock")
            return json.dumps({"metadata": {"uid": "original"}, "data": {"run-id": receipt["run_id"], "profile": q.PROFILE}})
        class Forward:
            def __init__(self, *args): pass
            def __enter__(self): return "http://localhost"
            def __exit__(self, *args): pass
        with patch.object(shared, "command", side_effect=command), patch.object(shared, "Forward", Forward), \
             patch.object(q, "drop_owned", side_effect=lambda endpoint, name: calls.append(name)):
            recovery.recover(self.folder, "test", "test")
        self.assertEqual(calls, ([receipt["collection"]] if owned else []) + ["release-lock"])
        self.assertEqual(json.loads((self.folder / "acceptance.json").read_text()), receipt)
        self.assertTrue(json.loads((self.folder / "manual-recovery.json").read_text())["lock_released"])


if __name__ == "__main__":
    unittest.main()
