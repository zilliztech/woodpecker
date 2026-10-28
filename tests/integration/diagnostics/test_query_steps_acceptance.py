"""Offline source/lineage/ownership counterexamples, not deployment receipts."""
import contextlib
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import query_steps_acceptance as s
import query_acceptance as original
import query_steps_recover as recovery


def evidence(phase="execution"):
    normal = "within_configured_latency_for_observed_completions"
    definitions = [("proxy_completion", normal, "proxy"), ("tsafe", normal, "qn"),
        ("queue", "suspected_scheduling_pressure" if phase == "scheduling" else normal, "qn"),
        ("core", "suspected_execution_work" if phase == "execution" else normal, "qn"),
        ("segment", "observed_slow_segment_work", "qn"), ("ready", "persistent_ready_backlog_observed" if phase == "scheduling" else "no_ready_backlog_observed", "qn")]
    observations, source, stage = [], [], []
    for sig, outcome, exporter in definitions:
        oid, population = "obs-" + sig, "population-" + sig
        fact = {"population_id": population, "exporter_identity": exporter, "labels": {} if sig == "ready" else {"query_type": "search"}, "observed_completions": 24}
        if sig == "ready":
            value = 4 if phase == "scheduling" else 0
            fact["gauge"] = {"minimum": value, "peak": value, "last": value}
        observations.append({"id": oid, "kind": "prometheus_matrix", "status": "observed", "data": {"facts": [fact]}})
        source.append({"id": oid + ":" + population})
        stage.append({"id": "stage-" + sig, "rule_id": "milvus.query." + sig, "outcome": outcome, "derived_from_findings": [source[-1]["id"]]})
    if phase == "scheduling":
        stage[2]["derived_from_findings"].append(source[-1]["id"])
    return {"status": "completed", "results": [{"observations": observations, "findings": source}], "assessments": [{"findings": stage}]}


class EvidenceTests(unittest.TestCase):
    def test_execution_requires_qualified_core_segment_and_same_exporter_controls(self):
        report = evidence()
        self.assertIn("core", s.metric_gate(report, "execution"))
        for mutation in ("missing_core", "wrong_exporter_queue", "missing_tsafe", "wrong_exporter_ready", "positive_ready", "missing_segment", "partial"):
            changed = copy.deepcopy(report)
            observations = changed["results"][0]["observations"]
            if mutation == "missing_core":
                changed["assessments"][0]["findings"][3]["outcome"] = "inconclusive"
            elif mutation == "missing_segment":
                changed["assessments"][0]["findings"][4]["outcome"] = "inconclusive"
            elif mutation == "partial":
                changed["status"] = "partial"
            elif mutation == "missing_tsafe":
                changed["assessments"][0]["findings"][1]["outcome"] = "inconclusive"
            elif mutation == "positive_ready":
                observations[5]["data"]["facts"][0]["gauge"]["peak"] = 2
            else:
                index = {"wrong_exporter_queue": 2, "wrong_exporter_ready": 5}[mutation]
                observations[index]["data"]["facts"][0]["exporter_identity"] = "unrelated-sn"
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                s.metric_gate(changed, "execution")

    def test_tsafe_remains_independent_when_exported_on_the_streaming_delegator(self):
        report = evidence()
        report["results"][0]["observations"][1]["data"]["facts"][0]["exporter_identity"] = "streaming-delegator"
        self.assertIn("tsafe", s.metric_gate(report, "execution"))

    def test_scheduling_needs_full_existing_pressure_outcome_and_real_associated_gauge(self):
        report = evidence("scheduling")
        self.assertIn("queue", s.metric_gate(report, "scheduling"))
        for mutation in ("recovered", "missing_ready", "zero_minimum", "zero_last", "wrong_exporter", "dangling_reference"):
            changed = copy.deepcopy(report)
            queue = changed["assessments"][0]["findings"][2]
            ready = changed["results"][0]["observations"][-1]["data"]["facts"][0]
            if mutation == "recovered":
                queue["outcome"] = "slow_queue_completions_with_backlog_recovery"
            elif mutation == "missing_ready":
                changed["results"][0]["observations"].pop()
            elif mutation == "zero_minimum":
                ready["gauge"]["minimum"] = 0
            elif mutation == "zero_last":
                ready["gauge"]["last"] = 0
            elif mutation == "wrong_exporter":
                ready["exporter_identity"] = "foreign-node"
            else:
                queue["derived_from_findings"].append("invented-source")
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                s.metric_gate(changed, "scheduling")

    def test_idle_is_observed_zero_not_missing_and_baseline_is_normal(self):
        report = evidence("baseline")
        s.metric_gate(report, "baseline")
        report["assessments"][0]["findings"][0]["outcome"] = "observed_slow_proxy_completions"
        with self.assertRaises(RuntimeError):
            s.metric_gate(report, "baseline")
        report["assessments"][0]["findings"][0]["outcome"] = "idle_or_no_completions"
        fact = report["results"][0]["observations"][0]["data"]["facts"][0]
        fact["observed_completions"] = 0
        s.metric_gate(report, "idle")
        fact["observed_completions"] = 1
        with self.assertRaises(RuntimeError):
            s.metric_gate(report, "idle")
        report["results"][0]["observations"] = []
        with self.assertRaises(RuntimeError):
            s.metric_gate(report, "idle")

    def test_workload_needs_actual_successful_verified_nq_and_offered_accounting(self):
        workload = {"passed": True, "mode": "search", "nq": 8192, "metadata": {"offered_requests": 20},
                    "records": [{"operation": "search", "ok": True, "verified_nq": 8192, "latency_ms": 900} for _ in range(20)]}
        s.workload_gate(workload, 20)
        for mutation in ("failure", "missing_nq", "wrong_count", "no_work"):
            w = copy.deepcopy(workload)
            if mutation == "failure":
                w["records"][0]["ok"] = False
            elif mutation == "missing_nq":
                w["records"][0]["verified_nq"] = 0
            elif mutation == "wrong_count":
                w["metadata"]["offered_requests"] = 19
            else:
                w["records"] = []
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                s.workload_gate(w, 20)


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.folder = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)

    def acceptance(self):
        args = s.arguments(["--context", "test", "--namespace", "test", "--output", str(self.folder)])
        return s.Acceptance(args)

    def test_plan_is_offline_and_rejects_unbounded_work(self):
        output = self.folder / "absent"
        argv = ["--context", "test", "--namespace", "test", "--output", str(output)]
        with patch.object(s, "Acceptance", side_effect=AssertionError("offline")), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(s.main(argv), 0)
        self.assertFalse(output.exists())
        for extra in (["--rows", "999999"], ["--parallel", "999"], ["--execution-nq", "999999"], ["--duration", "0"],
                      ["--budget", "nan"], ["--measurement", "90", "--duration", "60"]):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                s.arguments(argv + extra)

    def test_drop_refuses_changed_lock_or_owner_description_or_collection_id(self):
        a = self.acceptance()
        a.milvus = "http://127.0.0.1:1"
        a.report["collection_id"] = "original-id"
        description = {"description": "wpdiag query steps acceptance " + a.name, "collectionID": "original-id"}
        for mutation in ("lock", "description", "id"):
            changed = copy.deepcopy(description)
            if mutation == "description":
                changed["description"] = "foreign-owner"
            elif mutation == "id":
                changed["collectionID"] = "replacement"
            with patch.object(a, "lock_check", side_effect=RuntimeError("foreign-lock") if mutation == "lock" else None), \
                    patch.object(a, "call", side_effect=[{"has": True}, changed]), \
                    patch.object(original, "drop_owned", side_effect=AssertionError("must preserve foreign data")), \
                    self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                a.drop_owned()

    def test_owned_cleanup_uses_uid_preconditions_and_lock_last(self):
        a = self.acceptance()
        a.lock_uid, a.owned = "original-lock", True
        calls = []
        def command(argv, **_):
            self.assertIn("--raw", argv)
            options = json.loads(Path(argv[-1]).read_text())
            self.assertEqual(options["preconditions"], {"uid": "original-lock"})
            calls.append("delete-lock")
            return "{}"
        with patch.object(a, "lock_check"), patch.object(a, "drop_owned", side_effect=lambda: calls.append("drop-owned")), \
                patch.object(s.shared, "command", side_effect=command):
            a.finish()
        self.assertEqual(calls, ["drop-owned", "delete-lock"])
        self.assertTrue(a.report["lock_released"])

    def test_failed_cleanup_keeps_lock_and_failure_receipt(self):
        a = self.acceptance()
        a.lock_uid, a.owned = "original-lock", True
        with patch.object(a, "lock_check"), patch.object(a, "drop_owned", side_effect=RuntimeError("unavailable")), \
                patch.object(s.shared, "command", side_effect=AssertionError("keep lock")):
            a.finish()
        self.assertFalse(a.report["passed"])
        self.assertNotIn("lock_released", a.report)
        self.assertIn("unavailable", a.report["failures"][0])

    def test_ambiguous_lock_create_cannot_remove_another_run(self):
        a = self.acceptance()
        a.report["lock_create_intent"] = True
        foreign = {"metadata": {"uid": "foreign"}, "data": {"run-id": "another", "profile": s.PROFILE}}
        with patch.object(s.shared, "command", return_value=json.dumps(foreign)) as command:
            a.finish()
        self.assertEqual(command.call_count, 1)
        self.assertNotIn("lock_released", a.report)
        self.assertTrue(a.report["failures"])

    def recovery_receipt(self):
        a = self.acceptance()
        a.owned, a.rows, a.lock_uid = True, 65536, "original-lock"
        a.report.update(namespace_uid="original-namespace", lock_uid=a.lock_uid, collection_id="original-collection")
        a.checkpoint()
        return a

    def test_recovery_refuses_wrong_target_or_unowned_name_before_network(self):
        a = self.recovery_receipt()
        with patch.object(s.shared, "command", side_effect=AssertionError("offline")):
            with self.assertRaises(RuntimeError):
                recovery.load(a.out, "foreign", "test")
            receipt = json.loads((a.out / "acceptance.json").read_text())
            receipt["collection"] = "query_accept_20261028T000000Z_aabbccdd"
            s.shared.save(a.out / "acceptance.json", receipt)
            with self.assertRaises(RuntimeError):
                recovery.load(a.out, "test", "test")

    def test_recovery_preserves_original_failed_receipt_and_uses_original_uid(self):
        a = self.recovery_receipt()
        before = (a.out / "acceptance.json").read_bytes()
        calls = []
        def command(argv, **_):
            self.assertIn("--raw", argv)
            options = json.loads(Path(argv[-1]).read_text())
            self.assertEqual(options["preconditions"], {"uid": "original-lock"})
            calls.append("delete-original-lock")
            return "{}"
        with patch.object(s.Acceptance, "namespace_check"), patch.object(s.Acceptance, "lock_check"), \
                patch.object(s.Acceptance, "drop_owned", side_effect=lambda: calls.append("drop-owned")), \
                patch.object(s.shared, "Forward", return_value=contextlib.nullcontext("http://127.0.0.1:1")), \
                patch.object(s.shared, "command", side_effect=command):
            result = recovery.recover(a.out, "test", "test")
        self.assertEqual(calls, ["drop-owned", "delete-original-lock"])
        self.assertTrue(result["lock_released"])
        self.assertEqual((a.out / "acceptance.json").read_bytes(), before)

    def test_recovery_foreign_lock_prevents_any_data_or_lock_mutation(self):
        a = self.recovery_receipt()
        with patch.object(s.Acceptance, "namespace_check"), patch.object(s.Acceptance, "lock_check", side_effect=RuntimeError("replaced lock")), \
                patch.object(s.Acceptance, "drop_owned", side_effect=AssertionError("preserve data")), \
                patch.object(s.shared, "command", side_effect=AssertionError("preserve lock")), self.assertRaises(RuntimeError):
            recovery.recover(a.out, "test", "test")

    def test_already_cleaned_recovery_is_offline_and_idempotent(self):
        a = self.recovery_receipt()
        a.report.update(lock_released=True, collection_dropped=True)
        a.checkpoint()
        with patch.object(s.shared, "command", side_effect=AssertionError("already clean")):
            self.assertTrue(recovery.recover(a.out, "test", "test")["already_cleaned"])


if __name__ == "__main__":
    unittest.main()
