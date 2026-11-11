"""Evidence and Kubernetes/ownership protocol fixtures, not deployed receipts."""
import contextlib
import copy
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import cli_acceptance as shared
import persistence_acceptance as p
import persistence_recover as recovery
from persistence_fault import StorageFault, PROFILE, resources_applied, quota_resources


def report(outcome=p.NORMAL):
    return {"results": [{"observations": [{"id": "obs", "kind": "prometheus_matrix", "status": "observed",
        "data": {"signal": p.EXECUTE, "facts": [{"population_id": "population", "observed_completions": 24}]}}]}],
        "assessments": [{"findings": [{"rule_id": "milvus.persistence.step." + p.EXECUTE,
            "outcome": outcome, "derived_from_findings": ["obs:population"]}]}]}


def workload():
    records = []
    for i in range(2):
        identity, stamp = 64 + i * 16, 470000000000000123 + i
        common = {"ok": True, "expected_id": identity, "ack_timestamp": str(stamp), "latency_ms": 10}
        records.append({**common, "operation": "insert", "partition": "persist_00", "rows": 16, "ids": [str(j) for j in range(identity, identity + 16)]})
        records.append({**common, "operation": "flush", "flush_timestamp": str(stamp + 1), "segment_ids": [str(9007199254740993 + i * 16 + j) for j in range(16)]})
        records.extend({**common, "operation": op, "consistency": "Strong", "guarantee_timestamp": "0"} for op in ("query", "search"))
    return {"passed": True, "mode": "flush", "records": records}


class Backend:
    def __init__(self):
        self.sts = {"metadata": {"uid": "storage-uid"}, "spec": {"replicas": 1,
            "persistentVolumeClaimRetentionPolicy": {"whenScaled": "Retain"},
            "volumeClaimTemplates": [{"metadata": {"name": "data"}}], "template": {"version": "original"}}}
        self.claim = {"metadata": {"uid": "claim-uid"}, "status": {"phase": "Bound"}, "spec": {"volumeName": "original-volume"}}
        self.calls = []
        self.resources = {"limits": {"cpu": "1", "memory": "512Mi"}, "requests": {"cpu": "100m", "memory": "128Mi"}}
        self.resize_supported = True
        self.missing_status_resources = False
        self.fail_after_commit = False
        self.before_mutation = lambda: None

    def command(self, argv, **kwargs):
        self.calls.append(argv)
        if "patch" in argv:
            self.before_mutation()
            operations = json.loads(argv[-1])
            if operations[0] != {"op": "test", "path": "/metadata/uid", "value": "storage-pod"}:
                raise RuntimeError("Atomic controller UID guard failed")
            if operations[1]["value"] != self.resources:
                raise RuntimeError("Atomic resource guard failed")
            self.resources = operations[2]["value"]
            if self.fail_after_commit:
                self.fail_after_commit = False
                raise RuntimeError("ambiguous network outcome after commit")
            return "patched"
        if "statefulset" in argv:
            return json.dumps(self.sts)
        if "exec" in argv:
            cpu = self.resources["limits"].get("cpu")
            quota = "max" if not cpu else str(int(float(cpu[:-1]) * 100 if cpu.endswith("m") else float(cpu) * 100000))
            return quota + " 100000"
        if "--raw" in argv:
            return json.dumps({"resources": [{"name": "pods/resize", "verbs": ["get", "patch"]}] if self.resize_supported else []})
        if "pvc" in argv:
            return json.dumps(self.claim)
        if "pods" in argv:
            pods = [] if self.sts["spec"]["replicas"] == 0 else [{"metadata": {"name": "minio-0", "uid": "storage-pod",
                "ownerReferences": [{"uid": "storage-uid", "controller": True}]}, "spec": {"containers": [{"resources": self.resources}]},
                "status": {"conditions": [{"type": "Ready", "status": "True"}], "containerStatuses": [{"resources": None if self.missing_status_resources else self.resources}]}}]
            return json.dumps({"items": pods})
        raise AssertionError(argv)

    def mutations(self):
        return [c for c in self.calls if "patch" in c]


class EvidenceTests(unittest.TestCase):
    def test_enforced_cpu_limit_is_distinct_from_retained_request_status(self):
        wanted = {"limits": {"cpu": "25m", "memory": "512Mi"}, "requests": {"cpu": "25m", "memory": "128Mi"}}
        row = {"resources": copy.deepcopy(wanted), "applied_resources": copy.deepcopy(wanted), "kernel_cpu_available": True, "kernel_cpu_millicores": 25}
        row["applied_resources"]["requests"]["cpu"] = "100m"
        self.assertTrue(resources_applied(row, wanted))
        row["applied_resources"]["limits"]["cpu"] = "1"
        self.assertTrue(resources_applied(row, wanted))
        row["kernel_cpu_millicores"] = 1000
        self.assertFalse(resources_applied(row, wanted))
        row["kernel_cpu_millicores"] = 25
        row["applied_resources"]["requests"]["memory"] = "1Gi"
        self.assertFalse(resources_applied(row, wanted))

    def test_mounted_binding_precedence_and_wrong_store_fail_closed(self):
        p.storage_binding({"minio": {"address": "old", "port": 9000}}, {"minio": {"address": "minio"}}, {"minio"})
        for defaults, override in (({"minio": {"address": "other", "port": 9000}}, {}),
                                  ({"minio": {"address": "minio", "port": 9000}}, {"minio": {"port": 9001}}),
                                  ({"minio": "unsupported"}, {})):
            with self.assertRaises(RuntimeError):
                p.storage_binding(defaults, override, {"minio"})

    def test_gate_requires_source_owned_qualified_population_and_lineage(self):
        for phase, outcome in (("baseline", p.NORMAL), ("recovery", p.NORMAL), ("slow_storage", p.SLOW), ("idle", "idle_or_no_completions")):
            self.assertTrue(p.gate(report(outcome), phase))
        for mutation in ("missing", "lineage", "wrong_signal", "inconclusive", "idle"):
            changed = report()
            if mutation == "missing":
                changed["results"] = []
            elif mutation == "lineage":
                changed["assessments"][0]["findings"][0]["derived_from_findings"] = ["other:population"]
            elif mutation == "wrong_signal":
                changed["results"][0]["observations"][0]["data"]["signal"] = "unrelated"
            else:
                changed["assessments"][0]["findings"][0]["outcome"] = "inconclusive" if mutation == "inconclusive" else "idle_or_no_completions"
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                p.gate(changed, "baseline")

    def test_flush_boundary_exact_ids_and_large_segment_precision(self):
        valid = workload()
        result = p.workload_gate(valid, 2, 64)
        self.assertEqual(len(result["sealed_segments"]), 32)
        self.assertEqual(result["sealed_segments"][0], "9007199254740993")
        for mutation in ("wrong_fence", "wrong_id", "reused_segment", "number_segment", "customized", "rpc_failed", "missing_rpc"):
            changed = copy.deepcopy(valid)
            if mutation == "wrong_fence":
                changed["records"][1]["flush_timestamp"] = "1"
            elif mutation == "wrong_id":
                changed["records"][1]["expected_id"] = 999
            elif mutation == "reused_segment":
                changed["records"][5]["segment_ids"] = changed["records"][1]["segment_ids"]
            elif mutation == "number_segment":
                changed["records"][1]["segment_ids"] = [9007199254740993]
            elif mutation == "customized":
                changed["records"][2]["consistency"] = "Customized"
            elif mutation == "rpc_failed":
                changed["records"][1]["ok"] = False
            else:
                changed["records"].pop()
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                p.workload_gate(changed, 2, 64)


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.folder = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.backend = Backend()
        self.mock = patch.object(shared, "command", side_effect=self.backend.command)
        self.mock.start()
        self.addCleanup(self.mock.stop)
        self.fault = StorageFault(["kubectl", "--context", "test", "-n", "test"], "minio", self.folder)

    def test_original_claims_and_journal_precede_atomic_resize_and_exact_restore(self):
        self.fault.prepare()
        def before():
            journal = json.loads((self.folder / "storage-restoration.json").read_text())
            self.assertTrue(journal["mutation_started"])
            self.assertEqual(journal["before"]["claims"][0]["uid"], "claim-uid")
        self.backend.before_mutation = before
        self.fault.apply()
        self.assertEqual(self.backend.resources["limits"]["cpu"], "25m")
        self.fault.restore()
        self.fault.restore()
        self.assertEqual(len(self.backend.mutations()), 2)
        self.assertEqual(self.backend.resources["limits"]["cpu"], "1")
        self.assertEqual(self.backend.resources["requests"]["cpu"], "100m")
        self.assertTrue(json.loads((self.folder / "storage-restoration.json").read_text())["restored"])

    def test_ambiguous_committed_resize_is_restored(self):
        self.fault.prepare()
        self.backend.fail_after_commit = True
        with self.assertRaisesRegex(RuntimeError, "ambiguous"):
            self.fault.apply()
        self.assertTrue(self.fault.changed)
        self.assertEqual(self.backend.resources["limits"]["cpu"], "25m")
        self.fault.restore()
        self.assertEqual(self.backend.resources["limits"]["cpu"], "1")

    def test_journal_failure_prevents_resize(self):
        self.fault.prepare()
        with patch.object(shared, "save", side_effect=OSError("full")), self.assertRaises(OSError):
            self.fault.apply()
        self.assertFalse(self.backend.mutations())

    def test_retention_or_ephemeral_storage_is_rejected_without_mutation(self):
        self.backend.sts["spec"]["persistentVolumeClaimRetentionPolicy"]["whenScaled"] = "Delete"
        with self.assertRaisesRegex(RuntimeError, "retain"):
            self.fault.prepare()
        self.assertFalse(self.backend.mutations())
        self.backend.sts["spec"]["volumeClaimTemplates"] = []
        with self.assertRaisesRegex(RuntimeError, "PVC"):
            self.fault.prepare()

    def test_external_claim_template_and_replica_changes_are_not_overwritten(self):
        self.fault.prepare()
        self.fault.apply()
        for mutation in ("claim", "template", "replicas", "controller", "resources"):
            old_sts, old_claim, old_resources = copy.deepcopy(self.backend.sts), copy.deepcopy(self.backend.claim), copy.deepcopy(self.backend.resources)
            if mutation == "claim":
                self.backend.claim["metadata"]["uid"] = "foreign"
            elif mutation == "template":
                self.backend.sts["spec"]["template"]["version"] = "foreign"
            elif mutation == "replicas":
                self.backend.sts["spec"]["replicas"] = 2
            elif mutation == "controller":
                self.backend.sts["metadata"]["uid"] = "foreign"
            else:
                self.backend.resources["limits"]["memory"] = "1Gi"
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                self.fault.restore()
            self.backend.sts, self.backend.claim = old_sts, old_claim
            self.backend.resources = old_resources
        self.assertEqual(len(self.backend.mutations()), 1)
        self.assertFalse(json.loads((self.folder / "storage-restoration.json").read_text())["restored"])

    def test_pre_injection_recovery_does_not_overwrite_unrelated_resize(self):
        self.fault.prepare()
        self.fault.restore()
        self.assertFalse(self.backend.mutations())
        self.backend.resources = self.fault.desired()
        with self.assertRaisesRegex(RuntimeError, "before this run"):
            self.fault.restore()
        self.assertFalse(self.backend.mutations())

    def test_unsupported_or_unapplied_preflight_keeps_no_mutation_ownership(self):
        for missing in ("api", "status", "already_quota", "unlimited"):
            self.backend.resize_supported = missing != "api"
            self.backend.missing_status_resources = missing == "status"
            if missing == "already_quota":
                self.backend.resources["limits"]["cpu"] = "25m"
                self.backend.resources["requests"]["cpu"] = "25m"
            if missing == "unlimited":
                self.backend.resources["limits"].pop("cpu", None)
            fault = StorageFault(["kubectl", "--context", "test", "-n", "test"], "minio", self.folder)
            with self.subTest(missing=missing), self.assertRaises(RuntimeError):
                fault.prepare()
            self.assertIsNone(fault.before)
        self.assertFalse(self.backend.mutations())


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.folder = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)

    def acceptance(self):
        return p.Acceptance(p.arguments(["--context", "test", "--namespace", "test", "--output", str(self.folder)]))

    def test_plan_is_offline_and_invalid_bounds_fail_before_io(self):
        target = self.folder / "absent"
        with patch.object(p, "Acceptance", side_effect=AssertionError("offline")), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(p.main(["--context", "test", "--namespace", "test", "--output", str(target)]), 0)
        self.assertFalse(target.exists())
        for flags in (["--budget", "nan"], ["--outage-hold", "1000"], ["--batches", "10000"]):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                p.arguments(["--context", "test", "--namespace", "test"] + flags)

    def test_failed_storage_restore_preserves_data_and_lock(self):
        a = self.acceptance()
        a.owned, a.lock_uid = True, "lock"
        a.fault = SimpleNamespace(before={"replicas": 1}, restore=lambda: (_ for _ in ()).throw(RuntimeError("changed")))
        with patch.object(a, "assert_lock"), patch.object(p, "drop_owned", side_effect=AssertionError("keep data")):
            a.finish()
        self.assertFalse(a.report["passed"])
        self.assertNotIn("lock_released", a.report)

    def test_replaced_lock_prevents_restoration_or_data_mutations(self):
        a = self.acceptance()
        a.lock_uid = "lock"
        a.fault = SimpleNamespace(before={"replicas": 1}, restore=lambda: self.fail("foreign lock"))
        with patch.object(a, "assert_lock", side_effect=RuntimeError("ownership changed")):
            a.finish()
        self.assertFalse(a.report["passed"])
        self.assertNotIn("storage_final", a.report)

    def test_failed_run_restores_before_owned_cleanup_and_keeps_failed_receipt(self):
        a = self.acceptance()
        calls = []
        def setup():
            a.owned, a.lock_uid, a.milvus = True, "lock", "http://localhost"
            a.fault = SimpleNamespace(before={"replicas": 1}, restore=lambda: calls.append("restore"))
            raise RuntimeError("failed after mutation")
        with patch.object(a, "setup", side_effect=setup), patch.object(a, "assert_lock"), \
             patch.object(p, "drop_owned", side_effect=lambda *args: calls.append("drop")), \
             patch.object(shared, "command", side_effect=lambda *args, **kw: calls.append("release")), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(a.run(), 1)
        self.assertEqual(calls, ["restore", "drop", "release"])
        self.assertFalse(json.loads((a.out / "acceptance.json").read_text())["passed"])

    def test_recovery_checks_original_target_and_names_offline(self):
        self.write_receipt()
        with patch.object(shared, "command", side_effect=AssertionError("offline")):
            receipt, _ = recovery.load(self.folder, "test", "test")
            with self.assertRaisesRegex(RuntimeError, "target"):
                recovery.load(self.folder, "foreign", "test")
            receipt["collection"] = "unrelated"
            shared.save(self.folder / "acceptance.json", receipt)
            with self.assertRaisesRegex(RuntimeError, "Unowned"):
                recovery.load(self.folder, "test", "test")

    def write_receipt(self, owned=True):
        before = {"name": "minio", "uid": "controller", "replicas": 1, "spec_sha256": "a" * 64, "claims": [{"uid": "claim"}],
            "resources": {"limits": {"cpu": "1", "memory": "512Mi"}, "requests": {"cpu": "100m", "memory": "128Mi"}}, "pods": [{"uid": "original-pod"}]}
        receipt = {"schema_version": "wpdiag.persistence-acceptance/v1", "profile": PROFILE, "run_id": "20261027T000000Z-aabbccdd",
            "passed": False, "collection_owned": owned, "collection": "persist_accept_20261027T000000Z_aabbccdd", "storage_before": before, "lock_uid": "lock",
            "selection": {"context": "test", "namespace": "test", "release": "milvus", "storage_statefulset": "minio", "storage_service": "minio"}}
        shared.save(self.folder / "acceptance.json", receipt)
        shared.save(self.folder / "storage-restoration.json", {"schema_version": "wpdiag.storage-resize/v1", "profile": PROFILE,
            "before": before, "fault_resources": quota_resources(before["resources"]), "mutation_started": True, "restored": False})

    def test_actual_recovery_preserves_failed_receipt_and_preflight_unowned_name(self):
        for owned in (False, True):
            self.write_receipt(owned)
            original = (self.folder / "acceptance.json").read_bytes()
            calls = []
            def command(argv, **kwargs):
                if "namespace" in argv:
                    return json.dumps({"metadata": {"labels": {p.LABEL: PROFILE}}})
                calls.append("release")
            with patch.object(p.Acceptance, "inventory", side_effect=AssertionError("recovery must not wait for affected workloads")), patch.object(p.Acceptance, "assert_lock"), \
                 patch.object(StorageFault, "restore", side_effect=lambda: calls.append("restore")), \
                 patch.object(shared, "Forward", return_value=contextlib.nullcontext("http://localhost")), \
                 patch.object(p, "drop_owned", side_effect=lambda *args: calls.append("drop")), \
                 patch.object(shared, "command", side_effect=command):
                recovery.recover(self.folder, "test", "test")
            self.assertEqual(calls, ["restore"] + (["drop"] if owned else []) + ["release"])
            self.assertEqual((self.folder / "acceptance.json").read_bytes(), original)

    def test_preflight_process_loss_before_receipt_update_is_recoverable_only_without_mutation(self):
        self.write_receipt(False)
        receipt = json.loads((self.folder / "acceptance.json").read_text())
        receipt.pop("storage_before")
        shared.save(self.folder / "acceptance.json", receipt)
        journal = json.loads((self.folder / "storage-restoration.json").read_text())
        journal["mutation_started"] = False
        shared.save(self.folder / "storage-restoration.json", journal)
        recovery.load(self.folder, "test", "test")
        journal["mutation_started"] = True
        shared.save(self.folder / "storage-restoration.json", journal)
        with self.assertRaisesRegex(RuntimeError, "differs"):
            recovery.load(self.folder, "test", "test")


if __name__ == "__main__":
    unittest.main()
