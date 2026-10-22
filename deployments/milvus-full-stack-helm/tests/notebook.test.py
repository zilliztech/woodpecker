#!/usr/bin/env python3
"""Verify notebook guard cells and ownership rules without contacting Kubernetes."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("notebook_helpers", ROOT / "scripts/notebook_helpers.py")
helpers = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(helpers)


class NotebookTests(unittest.TestCase):
    def lab(self, root):
        with patch.dict(os.environ, {"STATE": str(Path(root) / ".local")}):
            return helpers.StackLab(root, "test-context", "test-namespace",
                                    {"GRAFANA": 13000, "PROMETHEUS": 19099, "MILVUS": 29530, "ALERTMANAGER": 19093}, execute=True)

    def test_source_is_unexecuted_and_lifecycle_cells_are_inert_by_default(self):
        notebook = json.loads((ROOT / "notebooks/full-stack-walkthrough.ipynb").read_text())
        for cell in notebook["cells"]:
            if cell["cell_type"] != "code":
                continue
            self.assertIsNone(cell["execution_count"])
            self.assertEqual(cell["outputs"], [])
            source = "".join(cell["source"])
            compile(source, cell["id"], "exec")
            if "lifecycle" in cell["metadata"].get("tags", []):
                lab = Mock()
                with contextlib.redirect_stdout(io.StringIO()):
                    exec(source, {"lab": lab})
                self.assertEqual(lab.mock_calls, [], cell["id"])

    def test_snapshots_are_immutable_when_source_changes(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "stack.sh").write_text("echo original\n")
            lab = self.lab(root)
            first = lab.snapshot()
            (root / "stack.sh").write_text("echo changed\n")
            second = lab.snapshot()
            self.assertNotEqual(first, second)
            self.assertEqual(first.read_text(), "echo original\n")
            self.assertEqual(second.read_text(), "echo changed\n")

    def test_command_failure_propagates(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "stack.sh").write_text("echo diagnostic-output\nexit 7\n")
            lab = self.lab(root)
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(subprocess.CalledProcessError) as error:
                lab.run("status")
            self.assertEqual(error.exception.returncode, 7)
            log, = (lab.state / "notebook").glob("*-status.log")
            self.assertIn("diagnostic-output", log.read_text())

    def test_reusing_healthy_external_forwards_does_not_spawn_or_kill(self):
        with tempfile.TemporaryDirectory() as folder:
            lab = self.lab(folder)
            with patch.object(lab, "occupied", return_value=True), patch.object(lab, "ready", return_value=True), \
                    patch.object(helpers.subprocess, "Popen") as spawn, patch.object(helpers.os, "killpg") as kill:
                lab.open_forwards()
                lab.close_forwards()
                spawn.assert_not_called()
                kill.assert_not_called()

    def test_partial_ports_delegate_ownership_checks_without_killing_external_processes(self):
        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / "stack.sh").write_text("echo offline fixture\n")
            lab = self.lab(folder)
            with patch.object(lab, "occupied", side_effect=[True, False, False, False]), \
                    patch.object(helpers.subprocess, "Popen", return_value=Mock(poll=Mock(return_value=17))) as spawn, patch.object(helpers.os, "killpg") as kill:
                with self.assertRaisesRegex(RuntimeError, "Forward process exited"):
                    lab.open_forwards()
                spawn.assert_called_once()
                kill.assert_not_called()

    def test_preview_mode_does_not_start_forwards(self):
        with tempfile.TemporaryDirectory() as folder:
            lab = helpers.StackLab(folder, "test-context", "test-namespace", {}, execute=False)
            with patch.object(lab, "run") as run, patch.object(helpers.subprocess, "Popen") as spawn:
                lab.open_forwards()
                run.assert_called_once_with("forward")
                spawn.assert_not_called()

    def test_notebook_execution_flag_is_explicit(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "stack.sh").write_text('printf "%s\\n" "$@"\n')
            for execute in (False, True):
                lab = helpers.StackLab(root, "test-context", "test-namespace", {}, execute=execute)
                with contextlib.redirect_stdout(io.StringIO()) as output:
                    lab.run("install")
                argv = output.getvalue().splitlines()[0]
                self.assertEqual(" -x" in argv, execute)
                self.assertIn("--with all", argv)

    def test_monitoring_waits_for_transient_errors_but_times_out_on_persistent_errors(self):
        with tempfile.TemporaryDirectory() as folder:
            lab = self.lab(folder)
            targets = {"data": {"activeTargets": [{"labels": {"namespace": "test-namespace", "woodpecker_id": "milvus",
                       "component": "woodpecker" if i < 3 else "proxy"}, "health": "up"} for i in range(8)]}}
            rules = [{"name": f"{prefix}{i}", "type": "alerting", "labels": {"rule_id": f"{prefix}{i}"}, "health": "ok"}
                     for prefix, count in (("R", 7), ("F", 9), ("S", 6), ("C", 9)) for i in range(1, count + 1)]
            good = {"data": {"groups": [{"name": "woodpecker-full-stack", "rules": rules}]}}
            bad = json.loads(json.dumps(good))
            bad["data"]["groups"][0]["rules"][0].update(health="err", lastError="temporary rollout conflict")
            with patch.object(lab, "get_json", side_effect=[targets, bad, targets, good, targets, good]), \
                    patch.object(helpers.time, "sleep"), contextlib.redirect_stdout(io.StringIO()):
                lab.wait_monitoring()
            with patch.object(lab, "get_json", side_effect=[targets, bad]), \
                    patch.object(helpers.time, "sleep"), patch.object(helpers.time, "monotonic", side_effect=[0, 0, 181]), \
                    contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(TimeoutError, "temporary rollout conflict"):
                lab.wait_monitoring()


if __name__ == "__main__":
    unittest.main()
