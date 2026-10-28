"""Offline protocol/lifecycle counterexamples, not a deployed success receipt."""
import contextlib
import copy
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import native_acceptance as n
import native_fixture as fixture
import native_recover as recovery

OWNER = "native-20261028t000000z-aabbccdd"
IMAGES = {"gate_image": "gate:test", "wp_image": "wp:test", "minio_image": "minio:test", "prometheus_image": "prom:test"}


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.native = json.loads((n.ROOT / "cmd/wpcli/diagnosis/testdata/scoped-write-v1.json").read_text())
        self.imported = json.loads((n.ROOT / "diagnostics/wpcli/testdata/scoped-write-import-v1.json").read_text())

    def test_actual_contract_fixtures_keep_all_original_lineage(self):
        self.assertTrue(n.native_lineage(self.native, self.imported))

    def test_changed_findings_targets_references_or_observation_times_fail(self):
        for mutation in ("id", "rule", "target", "strength", "support", "unknown", "time", "source"):
            changed = copy.deepcopy(self.imported)
            result = changed["results"][0]
            finding = result["findings"][0]
            if mutation == "id":
                finding["id"] = "replacement"
            elif mutation == "rule":
                finding["rule_id"] = "invented"
            elif mutation == "target":
                finding["target"]["operation_id"] = "another-operation"
            elif mutation == "strength":
                finding["evidence_strength"] = "confirmed"
            elif mutation == "support":
                finding["supporting_observations"] = []
            elif mutation == "unknown":
                finding["unknown"] = []
            elif mutation == "time":
                result["observations"][0]["started_at"] = "2020-01-01T00:00:00Z"
            else:
                result["observations"][0]["source_id"] = "new-source"
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                n.native_lineage(self.native, changed)

    def pending_report(self):
        report = copy.deepcopy(self.native)
        original = copy.deepcopy(report["findings"][0])
        report["findings"] = []
        for obs in report["observations"]:
            if obs["kind"] == "ops":
                op = copy.deepcopy(obs["ops"][0])
                obs["ops"] = [{**op, "op_id": "pending-" + str(i)} for i in range(20)]
        for i in range(20):
            finding = copy.deepcopy(original)
            finding["target"]["operation_id"] = "pending-" + str(i)
            finding["id"] = "finding-" + str(i)
            report["findings"].append(finding)
        return report

    def test_suspicion_requires_real_identity_start_and_completion_coverage(self):
        valid = self.pending_report()
        log = valid["findings"][0]["target"]["log_id"]
        self.assertEqual(len(n.stall_gate(valid, "write", log)), 20)
        for mutation in ("snapshot", "absent_ops", "changed_start", "missing_counter", "unqualified", "duplicate"):
            report = copy.deepcopy(valid)
            if mutation == "snapshot":
                report["collection_status"] = "partial"
            elif mutation == "unqualified":
                report["findings"][0]["outcome"] = "inconclusive"
            elif mutation == "duplicate":
                report["findings"][1] = copy.deepcopy(report["findings"][0])
            elif mutation == "missing_counter":
                for obs in report["observations"]:
                    obs.pop("counters", None)
            else:
                obs = next(o for o in report["observations"] if o["kind"] == "ops")
                if mutation == "absent_ops":
                    obs["ops"] = []
                else:
                    obs["ops"][0]["started_at"] = "2020-01-01T00:00:00Z"
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                n.stall_gate(report, "write", log)


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.folder = Path(self.tmp.name)
        self.receipt = {"schema_version": "wpdiag.native.fixture/v1", "profile": fixture.PROFILE, "run_id": OWNER,
                        "selection": {"context": "test", "namespace": "test"}, "resources": {}, "lock_uid": "lock-uid",
                        "namespace_uid": "namespace-uid"}
        self.f = fixture.Fixture("test", "test", self.folder, self.receipt)
        self.rows = {"namespace/test": {"metadata": {"uid": "namespace-uid", "labels": {fixture.LABEL: fixture.PROFILE}}}}
        lock = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": fixture.LOCK, "labels": {fixture.RUN_LABEL: OWNER}},
                "data": {"run-id": OWNER}}
        for obj in (*fixture.manifests(OWNER, "test", **IMAGES), lock):
            key = obj["kind"].lower() + "/" + obj["metadata"]["name"]
            self.receipt["resources"][key] = {"intent": obj, "uid": "uid-" + key, "created": True}
            self.rows[key] = copy.deepcopy(obj)
            self.rows[key]["metadata"]["uid"] = "uid-" + key
        self.rows["configmap/" + fixture.LOCK]["metadata"]["uid"] = "lock-uid"
        self.receipt["resources"]["configmap/" + fixture.LOCK]["uid"] = "lock-uid"
        self.deleted = []

    def command(self, args, *_, **__):
        if args[0] == "get":
            key = args[1] if "/" in args[1] else args[1] + "/" + args[2]
            return copy.deepcopy(self.rows.get(key))
        if args[0] == "delete":
            options = json.loads(Path(args[-1]).read_text())
            kind, name = args[2].split("/")[-2:]
            key = ("pod" if kind == "pods" else "configmap") + "/" + name
            self.assertEqual(options["preconditions"], {"uid": self.rows[key]["metadata"]["uid"]})
            self.deleted.append(key)
            del self.rows[key]
            return {"kind": "Status", "status": "Success"}
        self.fail("Unexpected operation " + str(args))

    def test_offline_plan_does_not_create_directory_or_call_cluster(self):
        output = self.folder / "absent"
        args = ["--context", "test", "--namespace", "test", "--gate-image", "gate:test", "--output", str(output)]
        with patch.object(n, "Acceptance", side_effect=AssertionError("offline")), patch.object(sys, "argv", ["native_acceptance.py", *args]), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(n.main(), 0)
        self.assertFalse(output.exists())
        for extra in (["--namespace", "--invalid"], ["--total-budget", "0"], ["--gate-image", "unsafe\nimage"]):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                n.arguments(args + extra)

    def test_fixture_has_no_existing_resources_metadata_mounts_or_cluster_actions(self):
        cm, pod = fixture.manifests(OWNER, "test", **IMAGES)
        self.assertEqual(cm["metadata"]["name"], OWNER)
        self.assertEqual([c["name"] for c in pod["spec"]["containers"]], ["woodpecker", "gate", "minio", "prometheus"])
        self.assertFalse(pod["spec"]["automountServiceAccountToken"])
        self.assertTrue(all("configMap" in v or "emptyDir" in v for v in pod["spec"]["volumes"]))
        self.assertEqual(json.loads(cm["data"]["wp.yaml"])["woodpecker"]["storage"]["type"], "minio")
        with self.assertRaises(ValueError):
            fixture.manifests("existing-tenant", "test", **IMAGES)

    def test_uid_preconditions_and_pod_first_lock_last(self):
        with patch.object(self.f, "command", side_effect=self.command):
            self.f.cleanup()
        self.assertTrue(self.receipt["cleaned"])
        self.assertEqual(self.deleted[0], "pod/" + OWNER)
        self.assertEqual(self.deleted[-1], "configmap/" + fixture.LOCK)

    def test_foreign_namespace_lock_pod_owner_or_settings_preserves_everything(self):
        original = copy.deepcopy(self.rows)
        for mutation in ("namespace", "label", "lock", "pod", "settings", "config", "owner"):
            self.rows = copy.deepcopy(original)
            if mutation == "namespace":
                self.rows["namespace/test"]["metadata"]["uid"] = "replacement"
            elif mutation == "label":
                self.rows["namespace/test"]["metadata"]["labels"] = {}
            elif mutation == "lock":
                self.rows["configmap/" + fixture.LOCK]["data"]["run-id"] = "another-run"
            elif mutation == "pod":
                self.rows["pod/" + OWNER]["metadata"]["uid"] = "replacement"
            elif mutation == "settings":
                self.rows["pod/" + OWNER]["spec"]["containers"][0]["image"] = "foreign:latest"
            elif mutation == "config":
                self.rows["configmap/" + OWNER]["data"]["empty.yaml"] = "changed"
            else:
                self.rows["pod/" + OWNER]["metadata"]["ownerReferences"] = [{"uid": "foreign"}]
            with self.subTest(mutation=mutation), patch.object(self.f, "command", side_effect=self.command), self.assertRaises(RuntimeError):
                self.f.cleanup()
            self.assertEqual(self.deleted, [])

    def test_ambiguous_create_resolves_only_original_label_settings(self):
        self.receipt["resources"]["pod/" + OWNER].pop("uid")
        with patch.object(self.f, "command", side_effect=self.command):
            self.f.cleanup()
        self.assertTrue(self.receipt["cleaned"])

    def test_failed_pod_cleanup_keeps_lock_for_recovery(self):
        original = self.command
        def unavailable(args, *a, **kw):
            if args[0] == "delete":
                raise RuntimeError("API unavailable")
            return original(args, *a, **kw)
        with patch.object(self.f, "command", side_effect=unavailable), self.assertRaises(RuntimeError):
            self.f.cleanup()
        self.assertFalse(self.deleted)
        self.assertIn("configmap/" + fixture.LOCK, self.rows)

    def test_recovery_refuses_target_and_unregistered_resource_before_api(self):
        n.shared.save(self.folder / "fixture.json", self.receipt)
        with patch.object(fixture.Fixture, "command", side_effect=AssertionError("offline")):
            with self.assertRaises(ValueError):
                recovery.load(self.folder, "foreign", "test")
            self.receipt["resources"]["pod/existing"] = {}
            n.shared.save(self.folder / "fixture.json", self.receipt)
            with self.assertRaises(ValueError):
                recovery.load(self.folder, "test", "test")

    def test_recovery_preserves_original_failed_acceptance(self):
        n.shared.save(self.folder / "fixture.json", self.receipt)
        n.shared.save(self.folder / "acceptance.json", {"passed": False, "failures": ["interrupted"]})
        before = (self.folder / "acceptance.json").read_bytes()
        with patch.object(fixture.Fixture, "command", side_effect=self.command):
            self.assertEqual(recovery.main(["--run", str(self.folder), "--context", "test", "--namespace", "test", "--execute"]), 0)
        self.assertEqual((self.folder / "acceptance.json").read_bytes(), before)
        self.assertTrue(json.loads((self.folder / "manual-recovery.json").read_text())["cleaned"])

    def test_capture_keeps_unmodified_native_bytes_and_nonzero_exit(self):
        binary = self.folder / "wp"
        body = '{"schema_version":"wp.diagnosis/v1","collection_status":"partial"}\n'
        binary.write_text("#!" + sys.executable + "\nimport sys\nsys.stdout.write(" + repr(body) + ")\nsys.exit(6)\n")
        binary.chmod(0o700)
        import os
        env = dict(os.environ, WP_NATIVE_ACCEPTANCE_BINARY=str(binary), WP_NATIVE_ACCEPTANCE_CAPTURE=str(self.folder))
        p = subprocess.run([sys.executable, str(Path(n.__file__).with_name("native_capture.py")), "diagnose"], env=env, capture_output=True, timeout=5)
        self.assertEqual(p.returncode, 6)
        self.assertEqual(p.stdout, body.encode())
        self.assertEqual(next(self.folder.glob("*.native.json")).read_bytes(), p.stdout)

    def test_unavailable_gate_control_still_cleans_original_owned_fixture(self):
        args = n.arguments(["--context", "test", "--namespace", "test", "--gate-image", "gate:test", "--output", str(self.folder)])
        acceptance = n.Acceptance(args)
        def setup():
            acceptance.folder.mkdir()
            acceptance.control = "http://127.0.0.1:1"
            acceptance.report["gate_restore_required"] = True
            raise KeyboardInterrupt("interrupted")
        def cleanup():
            acceptance.fixture_receipt["cleaned"] = True
        with patch.object(acceptance, "setup", side_effect=setup), \
                patch.object(acceptance, "gate", side_effect=RuntimeError("control unavailable")), \
                patch.object(acceptance.fixture, "cleanup", side_effect=cleanup) as remove, \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(acceptance.run(), 1)
        remove.assert_called_once()
        self.assertFalse(acceptance.report["passed"])
        self.assertTrue(acceptance.report["owned_fixture_cleaned"])
        self.assertFalse(acceptance.report["gate_restore_required"])

    def test_artifact_limit_fails_before_later_output_is_written(self):
        args = n.arguments(["--context", "test", "--namespace", "test", "--gate-image", "gate:test", "--output", str(self.folder)])
        acceptance = n.Acceptance(args)
        acceptance.folder.mkdir()
        with self.assertRaises(RuntimeError):
            acceptance.artifact_budget(97 << 20)


if __name__ == "__main__":
    unittest.main()
