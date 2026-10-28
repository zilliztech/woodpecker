"""Offline counterexamples for #493; never substituted for deployed evidence."""
import contextlib
import copy
import io
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import discontinuity_acceptance as x
import source_acceptance


def identity():
    return {"uid": "original-pod", "spec_sha256": "unchanged", "containers": [
        {"name": "woodpecker", "image": "wp:version", "image_id": "wp-digest", "container_id": "original-process", "restarts": 0},
        {"name": "prometheus", "image": "prom:version", "image_id": "prom-digest", "container_id": "retained-tsdb", "restarts": 0}]}


def report(code, points):
    return {"results": [{"observations": [{"id": "original-observation", "kind": "prometheus_matrix", "status": "observed",
        "data": {"series": [{"series_id": "real-population", "labels": {"__name__": "read_count", "operation": "get_batch_entries", "status": "success"},
            "samples": [{"time_ms": t, "value": value} for t, value in points]}]}}],
        "findings": [{"id": "original-finding", "rule_id": "wp.wal_read.latency", "outcome": "inconclusive", "severity": "unknown",
                      "supporting_observations": ["original-observation"], "unknown": [code]}]}]}


class DiscontinuityTests(unittest.TestCase):
    def test_offline_plan_has_no_subprocess_or_artifact_write(self):
        with tempfile.TemporaryDirectory() as root:
            out = Path(root) / "absent"
            with patch.object(subprocess, "run", side_effect=AssertionError("Plan executed")), \
                 patch.object(subprocess, "Popen", side_effect=AssertionError("Plan spawned")), \
                 contextlib.redirect_stdout(io.StringIO()) as text:
                self.assertEqual(x.main(["--context", "explicit", "--namespace", "disposable", "--output", str(out)]), 0)
            plan = json.loads(text.getvalue())
            self.assertFalse(plan["execute"])
            self.assertIn("external", plan["cluster_lifecycle"])
            self.assertFalse(out.exists())

    def test_one_restart_excludes_other_process_and_configuration_changes(self):
        old = identity()
        good = copy.deepcopy(old)
        good["containers"][0].update(container_id="restarted", restarts=1)
        x.one_restart(old, good)
        for case in ("no_restart", "twice", "pod", "spec", "image", "other", "missing"):
            changed = copy.deepcopy(good)
            if case == "no_restart":
                changed = copy.deepcopy(old)
            elif case == "twice":
                changed["containers"][0]["restarts"] = 2
            elif case == "pod":
                changed["uid"] = "replacement"
            elif case == "spec":
                changed["spec_sha256"] = "mutated"
            elif case == "image":
                changed["containers"][0]["image_id"] = "other"
            elif case == "other":
                changed["containers"][1]["container_id"] = "lost-tsdb"
            else:
                changed["containers"] = []
            with self.subTest(case=case), self.assertRaises(RuntimeError):
                x.one_restart(old, changed)

    def test_original_source_and_webui_still_reject_any_process_change(self):
        run = object.__new__(source_acceptance.Run)
        run.receipt = {"pod_before": identity(), "pod_after": identity()}
        run.verify_fixture_identity()
        run.receipt["pod_after"]["containers"][0]["restarts"] = 1
        with self.assertRaises(RuntimeError):
            run.verify_fixture_identity()

    def test_counter_reset_requires_actual_decrease(self):
        code = "counter_reset_or_restart"
        proof = x.discontinuity(report(code, [(0, 3), (2000, 4), (8000, 1), (10000, 1)]), code)
        self.assertEqual(len(proof["reset_pairs"]), 1)
        self.assertEqual(proof["gap_pairs"], [])
        with self.assertRaises(RuntimeError):
            x.discontinuity(report(code, [(0, 3), (2000, 4), (8000, 5)]), code)

    def test_scrape_gap_requires_actual_gap_without_reset(self):
        code = "scrape_gap"
        proof = x.discontinuity(report(code, [(0, 3), (2000, 3), (44000, 3), (46000, 4)]), code)
        self.assertEqual(len(proof["gap_pairs"]), 1)
        for points in ([(0, 3), (2000, 3)], [(0, 3), (44000, 1)]):
            with self.subTest(points=points), self.assertRaises(RuntimeError):
                x.discontinuity(report(code, points), code)

    def test_missing_or_healthy_or_wrong_code_never_satisfies_gap_gate(self):
        baseline = report("scrape_gap", [(0, 3), (44000, 4)])
        for case in ("healthy", "no_code", "empty_samples", "two_populations", "dangling"):
            changed = copy.deepcopy(baseline)
            row = changed["results"][0]
            finding = row["findings"][0]
            if case == "healthy":
                finding.update(severity="none", outcome="healthy")
            elif case == "no_code":
                finding["unknown"] = ["insufficient_observations"]
            elif case == "empty_samples":
                row["observations"][0]["data"]["series"][0]["samples"] = []
            elif case == "two_populations":
                row["observations"][0]["data"]["series"] *= 2
            else:
                finding["supporting_observations"] = ["invented"]
            with self.subTest(case=case), self.assertRaises(RuntimeError):
                x.discontinuity(changed, "scrape_gap")

    def signal_run(self):
        run = object.__new__(x.Run)
        run.owner = "source-owned"
        run.receipt = {"actions": []}
        run.checkpoint = Mock()
        run.fixture = SimpleNamespace(deadline=100, kube=["kubectl", "--context", "test"], checkpoint=Mock())
        run.remaining = Mock(return_value=20)
        run.checked_pod = Mock(return_value={"metadata": {"uid": "original"}, "status": {"containerStatuses": [
            {"name": "woodpecker", "containerID": "original-process", "restartCount": 0, "state": {"running": {"startedAt": "2026-10-28T00:00:00Z"}}}]}})
        run.runtime_process = Mock(return_value=dict(run.process(run.checked_pod.return_value), pid=25, proc_start_ticks="12345"))
        return run

    def test_ambiguous_signal_is_journaled_before_exec_and_never_assumed_failed(self):
        run = self.signal_run()
        def timeout(*_, **kwargs):
            self.assertEqual(run.receipt["actions"][0]["outcome"], "pending")
            self.assertTrue(run.checkpoint.called)
            self.assertTrue(run.fixture.checkpoint.called)
            raise subprocess.TimeoutExpired("exec", 10)
        with patch.object(subprocess, "run", side_effect=timeout):
            run.send_signal("KILL")
        self.assertEqual(run.receipt["actions"][0]["outcome"], "ambiguous_timeout")
        self.assertEqual(run.receipt["actions"][0]["target"]["container_id"], "original-process")

    def test_restoration_has_its_own_budget_and_refuses_replaced_process(self):
        run = self.signal_run()
        run.fixture.deadline = 0
        run.remaining = Mock(side_effect=AssertionError("Restoration used expired work budget"))
        with patch.object(subprocess, "run", return_value=SimpleNamespace(returncode=0)):
            run.send_signal("CONT", {"container_id": "original-process", "pid": 25, "proc_start_ticks": "12345"}, restore=True)
        self.assertEqual(run.fixture.deadline, 0)
        with patch.object(subprocess, "run", side_effect=AssertionError("Signalled replacement")), self.assertRaises(RuntimeError):
            run.send_signal("CONT", {"container_id": "replaced"}, restore=True)

    def test_foreign_target_is_rejected_before_action_or_exec(self):
        run = self.signal_run()
        run.checked_pod.side_effect = RuntimeError("Foreign resource identity")
        with patch.object(subprocess, "run", side_effect=AssertionError("Foreign signal")), self.assertRaises(RuntimeError):
            run.send_signal("STOP")
        self.assertEqual(run.receipt["actions"], [])

    def test_only_discontinuity_fixture_shares_owned_process_namespace(self):
        with tempfile.TemporaryDirectory() as folder:
            receipt = {"run_id": "source-20261028t110000z-1234abcd"}
            normal = x.fixture.Fixture("ctx", "ns", folder, receipt).objects()
            controlled = x.OwnedFixture("ctx", "ns", folder, receipt).objects()
            self.assertNotIn("shareProcessNamespace", normal[-1]["spec"])
            self.assertTrue(controlled[-1]["spec"]["shareProcessNamespace"])
            self.assertFalse(controlled[-1]["spec"].get("hostPID", False))

    def test_same_container_with_reused_pid_is_not_signalled(self):
        run = self.signal_run()
        with patch.object(subprocess, "run", side_effect=AssertionError("Reused PID signalled")), self.assertRaises(RuntimeError):
            run.send_signal("STOP", {"container_id": "original-process", "pid": 25, "proc_start_ticks": "older"})
        self.assertEqual(run.receipt["actions"], [])

    def test_runtime_discovery_requires_unique_non_init_pid_and_owned_argv(self):
        run = self.signal_run()
        del run.runtime_process
        fields = ["25", "(woodpecker)", "R"] + ["0"] * 18 + ["12345"]
        argv = ["/woodpecker/bin/woodpecker", "server", "--node-name", run.owner, "--cluster", run.owner]
        raw = (" ".join(fields) + "\n" + "\0".join(argv) + "\0").encode()
        with patch.object(subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout=raw)):
            target = run.runtime_process(run.checked_pod())
        self.assertEqual((target["pid"], target["proc_start_ticks"]), (25, "12345"))
        for case, changed in (("init", raw.replace(b"25 ", b"1 ", 1)), ("foreign", raw.replace(run.owner.encode(), b"foreign")),
                              ("multiple", raw + raw), ("wrong binary", raw.replace(b"/woodpecker/bin/woodpecker", b"/bin/sh"))):
            with self.subTest(case=case), patch.object(subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout=changed)), self.assertRaises(RuntimeError):
                run.runtime_process(run.checked_pod())


if __name__ == "__main__":
    unittest.main()
