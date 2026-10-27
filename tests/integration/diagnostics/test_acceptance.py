"""Fast lifecycle/lineage contract tests, not deployed acceptance receipts."""
import contextlib
import copy
import http.server
import io
import json
from pathlib import Path
import signal
import tempfile
import threading
import unittest
from unittest.mock import patch

import cli_acceptance as a
import recover


class ConfigBackend:
    def __init__(self):
        self.original = {key: {"key": key, "value": str(index + 1), "source": "FileSource"}
                         for index, key in enumerate(a.FAULT)}
        first = next(iter(self.original))
        self.original[first]["source"] = "EtcdSource"
        self.rows = copy.deepcopy(self.original)
        self.writes = []
        self.fail_after_commit = False
        self.unavailable = False
        backend = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                rows = list(backend.rows.values())
                if backend.unavailable:
                    rows = [{"key": key, "error": "key not found"} for key in a.FAULT]
                self.send_response(200)
                self.end_headers()
                self.wfile.write(json.dumps({"configs": rows}).encode())

            def do_POST(self):
                data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                backend.writes.append(data)
                for item in data["configs"]:
                    if "value" in item:
                        backend.rows[item["key"]] = {"key": item["key"], "value": item["value"], "source": "EtcdSource"}
                    else:
                        backend.rows[item["key"]] = copy.deepcopy(backend.original[item["key"]])
                self.send_response(503 if backend.fail_after_commit else 200)
                self.end_headers()
                self.wfile.write(b'{"msg":"sentinel-secret"}')

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.endpoint = "http://127.0.0.1:" + str(self.server.server_port)

    def close(self):
        self.server.shutdown()
        self.server.server_close()


class RestorationTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.output = Path(self.folder.name)
        self.backend = ConfigBackend()
        self.addCleanup(self.folder.cleanup)
        self.addCleanup(self.backend.close)

    def test_exact_override_and_original_absence_are_restored(self):
        fault = a.ConfigFault(self.backend.endpoint, self.output)
        before = copy.deepcopy(self.backend.rows)
        fault.apply()
        self.assertTrue(fault.changed)
        self.assertFalse(fault.restored)
        journal = json.loads((self.output / "restoration.json").read_text())
        self.assertEqual(journal["before"], before)
        self.assertFalse(journal["restored"])
        fault.restore()
        self.assertTrue(fault.restored)
        self.assertEqual(self.backend.rows, before)
        writes = self.backend.writes[-1]["configs"]
        self.assertIn("value", writes[0], "Existing etcd override must be restored")
        self.assertTrue(all("value" not in item for item in writes[1:]), "File-backed baseline must not become etcd overrides")
        self.assertTrue(json.loads((self.output / "restoration.json").read_text())["restored"])

    def test_unknown_parameters_never_mutate(self):
        self.backend.unavailable = True
        fault = a.ConfigFault(self.backend.endpoint, self.output)
        with self.assertRaisesRegex(RuntimeError, "restorable"):
            fault.apply()
        self.assertFalse(fault.changed)
        self.assertFalse(self.backend.writes)

    def test_changed_baseline_refuses_injection(self):
        fault = a.ConfigFault(self.backend.endpoint, self.output)
        fault.before = fault.read()
        self.backend.rows[next(iter(a.FAULT))]["value"] = "another-owner"
        with self.assertRaisesRegex(RuntimeError, "changed since preflight"):
            fault.apply()
        self.assertFalse(self.backend.writes)

    def test_unknown_configuration_source_refuses_mutation(self):
        self.backend.rows[next(iter(a.FAULT))]["source"] = "UnknownSource"
        with self.assertRaisesRegex(RuntimeError, "restorable"):
            a.ConfigFault(self.backend.endpoint, self.output).apply()
        self.assertFalse(self.backend.writes)

    def test_write_journal_precedes_even_ambiguous_commit_failure(self):
        self.backend.fail_after_commit = True
        fault = a.ConfigFault(self.backend.endpoint, self.output)
        with self.assertRaisesRegex(RuntimeError, "503") as error:
            fault.apply()
        self.assertNotIn("sentinel-secret", str(error.exception))
        self.assertTrue(fault.changed)
        self.assertTrue((self.output / "restoration.json").exists())
        self.backend.fail_after_commit = False
        fault.restore()
        self.assertEqual(self.backend.rows, self.backend.original)

    def test_journal_failure_prevents_mutation(self):
        fault = a.ConfigFault(self.backend.endpoint, self.output)
        with patch.object(a, "save", side_effect=OSError("disk unavailable")):
            with self.assertRaises(OSError):
                fault.apply()
        self.assertFalse(fault.changed)
        self.assertFalse(self.backend.writes)

    def test_failed_restoration_keeps_journal_unresolved(self):
        fault = a.ConfigFault(self.backend.endpoint, self.output)
        fault.apply()
        self.backend.fail_after_commit = True
        with self.assertRaises(RuntimeError):
            fault.restore()
        self.assertFalse(fault.restored)
        self.assertFalse(json.loads((self.output / "restoration.json").read_text())["restored"])

    def recover_fixture(self, changed=False):
        fault = a.ConfigFault(self.backend.endpoint, self.output)
        fault.apply()
        builds = [{"pod": "mixcoord", "uid": "original-uid"}]
        a.save(self.output / "acceptance.json", {"profile": a.PROFILE, "builds": builds,
            "selection": {"context": "test", "namespace": "owned", "release": "milvus"},
            "lock_uid": "lock-uid", "run_id": "run", "collection": "diag_accept_run"})
        if changed:
            self.backend.rows[next(iter(a.FAULT))]["value"] = "external-update"
        commands = []
        def inventory(harness):
            harness.report["builds"] = builds
            return "mixcoord", []
        def command(argv):
            commands.append(argv)
            return json.dumps({"metadata": {"uid": "lock-uid"}, "data": {"run-id": "run"}})
        @contextlib.contextmanager
        def forward(*_):
            yield self.backend.endpoint
        with patch.object(a.Acceptance, "inventory", inventory), patch.object(a, "Forward", forward), \
                patch.object(a, "command", side_effect=command):
            if changed:
                with self.assertRaisesRegex(RuntimeError, "outside this run"):
                    recover.recover(self.output, "test", "owned")
                self.assertEqual(len(self.backend.writes), 1)
                self.assertFalse(any("delete" in argv for argv in commands))
            else:
                recover.recover(self.output, "test", "owned")
                self.assertEqual(self.backend.rows, self.backend.original)
                self.assertTrue(any("delete" in argv for argv in commands))
                self.assertTrue((self.output / "manual-recovery.json").exists())

    def test_explicit_recovery_restores_original_config_and_releases_lock(self):
        self.recover_fixture()

    def test_explicit_recovery_never_overwrites_external_changes(self):
        self.recover_fixture(changed=True)


class HarnessTests(unittest.TestCase):
    def test_not_ready_inventory_retains_restart_and_termination_evidence(self):
        pods = []
        for role in ("mixcoord", "woodpecker"):
            pods.append({"metadata": {"name": role, "uid": role, "labels": {"component": role}},
                "status": {"conditions": [{"type": "Ready", "status": "False"}], "containerStatuses": [{"name": role,
                    "image": "fixture", "restartCount": 1, "lastState": {"terminated": {"reason": "OOMKilled", "exitCode": 137,
                    "message": "private-sentinel"}}}]}})
        harness = a.Acceptance(a.arguments(["--context", "test", "--namespace", "owned"]))
        responses = [json.dumps({"metadata": {"labels": {a.LABEL: a.PROFILE}}}), json.dumps({"items": pods})]
        with patch.object(a, "command", side_effect=responses):
            with self.assertRaisesRegex(RuntimeError, "must already be Ready"):
                harness.inventory()
        evidence = harness.report["builds"][0]
        self.assertFalse(evidence["ready"])
        self.assertEqual(evidence["containers"][0]["last_termination"], {"reason": "OOMKilled", "exitCode": 137})
        self.assertNotIn("private-sentinel", json.dumps(harness.report))

    def test_missing_idle_or_unreferenced_metrics_cannot_pass_traffic_gate(self):
        rule = "milvus.query.proxy_completion"
        for outcome, refs in (("inconclusive", ["o"]), ("idle_or_no_completions", ["o"]),
                              ("within_configured_latency_for_observed_completions", [])):
            with self.subTest(outcome=outcome, refs=refs):
                report = {"assessments": [{"findings": [{"rule_id": rule, "outcome": outcome, "supporting_observations": refs}]}]}
                with self.assertRaisesRegex(RuntimeError, "positively observed"):
                    a.completed_population(report, rule)
        report = {"assessments": [{"findings": [{"id": "f", "rule_id": rule,
            "outcome": "observed_slow_proxy_completions", "supporting_observations": ["o"]}]}]}
        self.assertEqual(a.completed_population(report, rule), ["f"])

    def test_plan_is_offline_and_does_not_create_output(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "absent"
            buf = io.StringIO()
            with patch.object(a.subprocess, "run", side_effect=AssertionError("No process in plan")), \
                    patch.object(a, "http", side_effect=AssertionError("No HTTP in plan")), contextlib.redirect_stdout(buf):
                code = a.main(["--context", "test", "--namespace", "owned", "--output", str(output)])
            self.assertEqual(code, 0)
            self.assertFalse(output.exists())
            plan = json.loads(buf.getvalue())
            self.assertFalse(plan["execute"])
            self.assertEqual(plan["temporary_controls"], a.FAULT)

    def test_invalid_sizes_and_unbounded_identifiers_fail_before_execute(self):
        for extra in (["--duration", "nan"], ["--dimension", "512", "--batch-size", "1024"],
                      ["--rps", "50", "--duration", "120", "--batch-size", "256"], ["--context", "--unsafe"]):
            with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    a.arguments(["--context", "test", "--namespace", "owned"] + extra)

    def test_lineage_rejects_dangling_refs_and_signatures_ignore_new_collection_ids(self):
        report = {"results": [{"observations": [{"id": "o1"}], "findings": [{"id": "source", "supporting_observations": ["o1"]}]}],
                  "assessments": [{"findings": [{"id": "stage", "rule_id": "rule", "rule_version": 1, "outcome": "inconclusive", "severity": "unknown",
                      "evidence_strength": "insufficient", "supporting_observations": ["o1"], "derived_from_findings": ["source"]}]}]}
        a.lineage(report)
        again = copy.deepcopy(report)
        again["assessments"][0]["findings"][0]["id"] = "different-run-uuid"
        self.assertEqual(a.signature(report), a.signature(again))
        again["assessments"][0]["findings"][0]["supporting_observations"] = ["missing"]
        with self.assertRaisesRegex(RuntimeError, "Dangling"):
            a.lineage(again)

    def test_cleanup_protects_restoration_from_second_interrupt(self):
        before = signal.getsignal(signal.SIGINT)
        with a.cleanup_signals():
            self.assertEqual(signal.getsignal(signal.SIGINT), signal.SIG_IGN)
        self.assertEqual(signal.getsignal(signal.SIGINT), before)

    def test_failure_restores_before_cleanup_and_releases_owned_lock(self):
        with tempfile.TemporaryDirectory() as folder:
            args = a.arguments(["--context", "test", "--namespace", "owned", "--output", folder])
            calls = []
            class Fault:
                changed = True
                restored = False
                def restore(self):
                    calls.append("restore")
                    self.restored = True
            class Work:
                out = Path(folder)
                collection = "diag_accept_fixture"
                collection_created = True
                trace = io.StringIO()
                signals = io.StringIO()
            harness = a.Acceptance(args)
            def setup():
                harness.workload = Work()
                harness.fault = Fault()
                harness.lock_id = "owned-run"
                harness.report["lock_uid"] = "owned-uid"
                harness.milvus = "http://127.0.0.1:1"
                raise KeyboardInterrupt
            def request(_base, path, *_args):
                calls.append(path)
                return {"data": {"has": True}}
            def command(argv, **_kwargs):
                calls.append(argv[-1])
                return json.dumps({"metadata": {"uid": "owned-uid"}, "data": {"run-id": "owned-run"}})
            with patch.object(harness, "setup", side_effect=setup), patch.object(a, "http", side_effect=request), \
                    patch.object(a, "command", side_effect=command), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(harness.run(), 1)
            report = json.loads((Path(folder) / "acceptance.json").read_text())
            self.assertTrue(report["configuration_restored"])
            self.assertFalse(report["collection_retained"])
            self.assertEqual(calls[0], "restore")
            self.assertFalse(report["passed"])

    def test_failed_restoration_keeps_owned_lock(self):
        with tempfile.TemporaryDirectory() as folder:
            args = a.arguments(["--context", "test", "--namespace", "owned", "--output", folder])
            harness = a.Acceptance(args)
            class Fault:
                changed = True
                restored = False
                def restore(self):
                    raise RuntimeError("offline")
            class Work:
                out = Path(folder)
                collection_created = False
                trace = io.StringIO()
                signals = io.StringIO()
            harness.workload, harness.fault, harness.lock_id = Work(), Fault(), "run"
            with patch.object(a, "command", side_effect=AssertionError("Must preserve lock")), contextlib.redirect_stdout(io.StringIO()):
                harness.finish()
            self.assertFalse(harness.report["configuration_restored"])
            self.assertIn("RESTORATION FAILED", harness.report["failures"][0])

    def test_recovery_refuses_wrong_target_or_unregistered_controls_offline(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)
            a.save(path / "acceptance.json", {"profile": a.PROFILE, "selection": {"context": "test", "namespace": "owned"}})
            a.save(path / "restoration.json", {"profile": a.PROFILE, "schema_version": "wpdiag.acceptance.restore/v1",
                "fault": a.FAULT, "before": {"unregistered": {"source": "FileSource", "value": "bad"}}})
            with self.assertRaisesRegex(RuntimeError, "target"):
                recover.load(path, "wrong", "owned")
            with self.assertRaisesRegex(RuntimeError, "unregistered"):
                recover.load(path, "test", "owned")


if __name__ == "__main__":
    unittest.main()
