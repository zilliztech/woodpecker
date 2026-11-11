"""Semantic/lifecycle fixtures, not deployed consume acceptance receipts."""
import contextlib
import copy
import io
import json
import socket
from pathlib import Path
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

import cli_acceptance as shared
import consume_acceptance as c
import consume_recover as recovery
from test_acceptance import ConfigBackend


def source(signal, facts):
    return {"status": "completed", "results": [{"observations": [{"status": "observed", "kind": "prometheus_matrix",
             "data": {"signal": signal, "facts": facts}}]}], "assessments": [{"findings": []}]}


class EvidenceTests(unittest.TestCase):
    def test_timeout_classification_is_structured_and_never_accepts_connection_failure(self):
        for error, kind in ((socket.timeout("sentinel"), "timeout"), (TimeoutError("sentinel"), "timeout"),
                            (urllib.error.URLError(socket.timeout("sentinel")), "timeout"),
                            (urllib.error.URLError(ConnectionRefusedError("sentinel")), "request_failed")):
            with self.subTest(error=type(error).__name__), patch.object(shared.urllib.request, "build_opener") as opener:
                opener.return_value.open.side_effect = error
                with self.assertRaises(shared.HTTPRequestError) as raised:
                    shared.http("http://127.0.0.1:1", "/probe")
                self.assertEqual(raised.exception.kind, kind)
                self.assertNotIn("sentinel", str(raised.exception))

    def test_missing_idle_wrong_collection_cannot_pass_working_gate(self):
        fact = {"labels": {"collection_id": "9007199254740993"}, "counter": {"increase": 10}}
        report = source("milvus.consume.packs", [fact])
        self.assertEqual(len(c.working_gate(report, "9007199254740993")), 1)
        for wrong in (source("milvus.consume.packs", []), source("milvus.consume.packs", [{"labels": fact["labels"], "counter": {"increase": 0}}]),
                      source("milvus.consume.packs", [{"labels": {"collection_id": "9007199254740992"}, "counter": {"increase": 10}}]),
                      source("milvus.consume.pending", [fact])):
            with self.subTest(report=wrong), self.assertRaises(RuntimeError):
                c.working_gate(wrong, "9007199254740993")
        report["results"][0]["observations"][0]["status"] = "unavailable"
        with self.assertRaises(RuntimeError):
            c.working_gate(report, "9007199254740993")

    def test_pause_requires_retained_persistence_and_does_not_invent_pressure(self):
        report = source("milvus.consume.paused", [{"gauge": {"minimum": 1, "peak": 1}}])
        self.assertEqual(len(c.pause_gate(report)), 1)
        for minimum in (0, .5):
            with self.assertRaises(RuntimeError):
                c.pause_gate(source("milvus.consume.paused", [{"gauge": {"minimum": minimum, "peak": 1}}]))
        report["assessments"][0]["findings"] = [{"outcome": "suspected_scanner_downstream_backpressure"}]
        with self.assertRaisesRegex(RuntimeError, "processing pressure"):
            c.pause_gate(report)

    def test_equivalence_preserves_uncertainty_and_identity_not_run_uuid(self):
        finding = {"id": "one", "rule_id": "step", "rule_version": 1, "outcome": "inconclusive", "severity": "unknown",
                   "evidence_strength": "inconclusive", "flow": "read", "stage": "consume", "target": {"instance": "sn"},
                   "summary": "No mapping", "unknown": ["wp_reader_mapping_unavailable"]}
        first = {"assessments": [{"findings": [finding]}]}
        second = copy.deepcopy(first)
        second["assessments"][0]["findings"][0]["id"] = "another-run"
        self.assertEqual(c.signatures(first), c.signatures(second))
        second["assessments"][0]["findings"][0]["unknown"] = []
        self.assertNotEqual(c.signatures(first), c.signatures(second))


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.folder = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)

    def backend(self, source="FileSource"):
        backend = ConfigBackend()
        self.addCleanup(backend.close)
        backend.original = {c.KEY: {"key": c.KEY, "value": "false", "source": source}}
        backend.rows = copy.deepcopy(backend.original)
        return backend

    def test_custom_control_is_journaled_and_exact_override_absence_restored(self):
        for origin in ("FileSource", "EtcdSource"):
            with self.subTest(origin=origin):
                backend = self.backend(origin)
                fault = c.ConsumeFault(backend.endpoint, self.folder)
                fault.apply()
                journal = json.loads((self.folder / "restoration.json").read_text())
                self.assertEqual(journal["profile"], c.PROFILE)
                self.assertEqual(journal["fault"], c.CONTROLS)
                self.assertEqual(set(backend.writes[0]["configs"][0]), {"key", "value"})
                fault.restore()
                self.assertEqual(backend.rows, backend.original)
                self.assertEqual("value" in backend.writes[-1]["configs"][0], origin == "EtcdSource")

    def test_external_change_refuses_restore_and_preserves_unresolved_journal(self):
        backend = self.backend()
        fault = c.ConsumeFault(backend.endpoint, self.folder)
        fault.apply()
        backend.rows[c.KEY]["value"] = "external-value"
        with self.assertRaisesRegex(RuntimeError, "outside this run"):
            fault.restore()
        self.assertEqual(len(backend.writes), 1)
        self.assertFalse(fault.restored)
        self.assertFalse(json.loads((self.folder / "restoration.json").read_text())["restored"])

    def receipt(self):
        run_id = "20261027T000000Z-aabbccdd"
        receipt = {"schema_version": "wpdiag.consume-acceptance/v1", "profile": c.PROFILE, "run_id": run_id,
                   "selection": {"context": "test", "namespace": "test", "release": "milvus"}, "lock_uid": "owned-lock",
                   "coordinator": {"pod": "coord", "uid": "coord-uid"}, "passed": False,
                   "owned_collections": {"consume_accept_20261027T000000Z_aabbccdd_paused": {"dropped": False}}}
        shared.save(self.folder / "acceptance.json", receipt)
        return receipt

    def test_explicit_crash_recovery_drops_only_original_owned_data_and_preserves_failure(self):
        self.crash_recovery(inject=True)

    def test_process_loss_before_fault_has_a_restorable_owned_data_journal(self):
        self.crash_recovery(inject=False)

    def crash_recovery(self, inject):
        backend = self.backend()
        fault = c.ConsumeFault(backend.endpoint, self.folder)
        fault.prepare()
        self.assertFalse(backend.writes)
        if inject:
            fault.apply()
        receipt = self.receipt()
        lock = {"metadata": {"uid": "owned-lock"}, "data": {"run-id": receipt["run_id"], "profile": c.PROFILE}}
        class Forward:
            def __init__(self, *args): pass
            def __enter__(self): return backend.endpoint
            def __exit__(self, *args): pass
        original_http = shared.http
        drops = []
        exists = True
        def http(endpoint, path, payload=None, timeout=10):
            nonlocal exists
            if path.endswith("collections/drop"):
                drops.append(payload["collectionName"])
                exists = False
                return {"data": {}}
            if path.endswith("collections/has"):
                return {"data": {"has": exists}}
            return original_http(endpoint, path, payload, timeout)
        with patch.object(c.Acceptance, "inventory", return_value=[{"pod": "coord", "uid": "coord-uid"}]), \
             patch.object(shared, "Forward", Forward), patch.object(shared, "http", side_effect=http), \
             patch.object(shared, "command", return_value=json.dumps(lock)) as command:
            recovery.recover(self.folder, "test", "test")
        self.assertEqual(drops, list(receipt["owned_collections"]))
        self.assertEqual(backend.rows, backend.original)
        self.assertEqual(json.loads((self.folder / "acceptance.json").read_text()), receipt)
        self.assertTrue(json.loads((self.folder / "manual-recovery.json").read_text())["lock_released"])
        self.assertIn("delete", command.call_args.args[0])

    def test_already_absent_owned_data_is_idempotent_and_unrelated_names_are_refused(self):
        name = "consume_accept_20261027T000000Z_aabbccdd_paused"
        with patch.object(shared, "http", return_value={"data": {"has": False}}) as http:
            c.drop_owned_collection("http://localhost", name)
            self.assertEqual(http.call_count, 1)
            self.assertTrue(http.call_args.args[1].endswith("collections/has"))
            with self.assertRaisesRegex(RuntimeError, "collection name"):
                c.drop_owned_collection("http://localhost", "existing-production-data")
            self.assertEqual(http.call_count, 1)

    def test_unpaused_controls_need_positive_retained_state_not_missing_metrics(self):
        for report in (source("milvus.consume.paused", []), source("milvus.consume.paused", [{"gauge": {"last_two_zero": False}}])):
            with self.assertRaises(RuntimeError):
                c.resumed_gate(report)
        self.assertEqual(len(c.resumed_gate(source("milvus.consume.paused", [{"gauge": {"last_two_zero": True}}]))), 1)

    def test_wrong_target_or_unowned_collection_refuses_recovery_offline(self):
        backend = self.backend()
        c.ConsumeFault(backend.endpoint, self.folder).apply()
        receipt = self.receipt()
        with patch.object(shared, "command", side_effect=AssertionError("offline")):
            with self.assertRaisesRegex(RuntimeError, "target"):
                recovery.load(self.folder, "another", "test")
            receipt["owned_collections"]["someone-elses-data"] = {}
            shared.save(self.folder / "acceptance.json", receipt)
            with self.assertRaisesRegex(RuntimeError, "unowned"):
                recovery.main(["--run", str(self.folder), "--context", "test", "--namespace", "test", "--execute"])

    def test_recovery_refuses_replaced_lock_before_any_mutation(self):
        backend = self.backend()
        c.ConsumeFault(backend.endpoint, self.folder).apply()
        receipt = self.receipt()
        foreign = {"metadata": {"uid": "someone-elses-lock"}, "data": {"run-id": receipt["run_id"], "profile": c.PROFILE}}
        with patch.object(c.Acceptance, "inventory", return_value=[{"pod": "coord", "uid": "coord-uid"}]), \
             patch.object(shared, "command", return_value=json.dumps(foreign)), \
             patch.object(shared, "Forward", side_effect=AssertionError("No mutation connection")):
            with self.assertRaisesRegex(RuntimeError, "lock no longer belongs"):
                recovery.recover(self.folder, "test", "test")
        self.assertEqual(len(backend.writes), 1)
        self.assertEqual(backend.rows[c.KEY]["value"], "true")

    def test_failed_restoration_keeps_lock_and_skips_data_mutation(self):
        args = c.arguments(["--context", "test", "--namespace", "test", "--output", str(self.folder)])
        acceptance = c.Acceptance(args)
        acceptance.fault = unittest.mock.Mock(changed=True, restored=False)
        acceptance.fault.restore.side_effect = RuntimeError("backend unavailable")
        acceptance.lock_uid = "owned-lock"
        acceptance.owned["consume_accept_20261027T000000Z_aabbccdd_paused"] = {"dropped": False, "ids": []}
        with patch.object(shared, "command", side_effect=AssertionError("Keep lock")), \
             patch.object(shared, "http", side_effect=AssertionError("No data mutation")):
            acceptance.finish()
        self.assertFalse(acceptance.report["configuration_restored"])
        self.assertTrue(any("RESTORATION FAILED" in f for f in acceptance.report["failures"]))

    def test_failed_artifact_save_cannot_skip_restoration(self):
        args = c.arguments(["--context", "test", "--namespace", "test", "--output", str(self.folder)])
        acceptance = c.Acceptance(args)
        backend = self.backend()
        fault = c.ConsumeFault(backend.endpoint, acceptance.out)
        fault.apply()
        acceptance.fault = fault
        original_save = acceptance.save
        def save(name, value):
            if name == "all-requests.json":
                raise OSError("disk full")
            return original_save(name, value)
        with patch.object(acceptance, "setup", side_effect=RuntimeError("interrupted after injection")), \
             patch.object(acceptance, "save", side_effect=save), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(acceptance.run(), 1)
        self.assertTrue(fault.restored)
        self.assertEqual(backend.rows, backend.original)

    def test_plan_is_offline_without_artifacts(self):
        absent = self.folder / "absent"
        with patch.object(c, "Acceptance", side_effect=AssertionError("No execution")), \
             patch.object(shared, "command", side_effect=AssertionError("No subprocess")), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(c.main(["--context", "test", "--namespace", "test", "--output", str(absent)]), 0)
        self.assertFalse(absent.exists())

    def test_bounds_reject_nonfinite_and_oversized_workload(self):
        for extra in (["--duration", "nan"], ["--rps", "inf"], ["--duration", "90", "--rps", "8", "--rows", "64"], ["--context", "--invalid"]):
            with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                c.arguments(["--context", "test", "--namespace", "test"] + extra)


if __name__ == "__main__":
    unittest.main()
