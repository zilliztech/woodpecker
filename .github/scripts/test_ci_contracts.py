"""CI ownership, migration completeness and real label-script regressions."""
import fnmatch
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import yaml

import ci_packages as packages
import stack_tests

ROOT = packages.ROOT
WORKFLOWS = ROOT / ".github/workflows"
PRIMARY = ("unit-test.yaml", "wpcli-ci.yaml", "diagnostics-ci.yaml")


def workflow(name):
    return yaml.load((WORKFLOWS / name).read_text(), Loader=yaml.BaseLoader)


def array(script, name):
    return shlex.split(re.search(r"\b" + name + r"=\((.*?)^\)", script, re.S | re.M)[1])


class OwnershipTests(unittest.TestCase):
    def test_broken_tool_import_is_retained_for_its_owner(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "go.mod").write_text("module " + packages.MODULE.rstrip("/") + "\n\ngo 1.25.0\n")
            (root / "server").mkdir()
            (root / "server/server.go").write_text("package server\n")
            (root / "cmd/wpcli").mkdir(parents=True)
            (root / "cmd/wpcli/broken.go").write_text('package main\nimport _ "missing_local_package"\n')
            with patch.object(packages, "ROOT", root):
                actual = packages.packages()
            self.assertEqual(packages.select(actual, "core"), [packages.MODULE + "server"])
            self.assertEqual(packages.select(actual, "wpcli"), [packages.MODULE + "cmd/wpcli"])
            failed = subprocess.run(["go", "test", "./cmd/wpcli"], cwd=root, capture_output=True, timeout=15)
            self.assertNotEqual(failed.returncode, 0, "Broken package must fail its own suite, not disappear")

    def test_each_root_package_has_one_owner_including_future_core_packages(self):
        p = [packages.MODULE + path for path in ("server", "server/storage", "new/runtime", "cmd/wpcli", "cmd/wpcli/diagnosis",
            "cmd/wpcli-extra", "diagnostics", "diagnostics/wpcli", "cmd/wpdiag", "cmd/wpdiag/webui", "tests/integration")]
        self.assertEqual(len(set.union(*(set(packages.select(p, s)) for s in packages.SCOPES))), len(p) - 1)
        self.assertEqual(packages.owner(packages.MODULE + "cmd/wpcli-extra"), "core")
        self.assertEqual(packages.owner(packages.MODULE + "cmd/wpcli/diagnosis"), "wpcli")
        self.assertEqual(packages.owner(packages.MODULE + "diagnostics/wpcli"), "diagnostics")
        with self.assertRaises(ValueError):
            packages.select(p + [p[0]], "core")
        with self.assertRaises(ValueError):
            packages.select([packages.MODULE + "server"], "wpcli")

    def test_sdk_suite_excludes_all_diagnostic_and_trace_helpers(self):
        p = [packages.MODULE + "tests/integration" + suffix for suffix in ("", "/sdk/future", "/diagnostics/nativeclient",
            "/diagnostics/sourceclient", "/diagnostics/newhelper", "/tracing/cmd/requests")]
        self.assertEqual(len(packages.select(p, "sdk-integration")), 2)
        self.assertEqual(len(packages.select(p, "diagnostic-helpers")), 3)

    def test_dotted_stack_tests_are_all_loaded_and_empty_suite_fails(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)
            with self.assertRaises(RuntimeError):
                stack_tests.suite(path)
            (path / "some-case.test.py").write_text("import unittest\nclass T(unittest.TestCase):\n def test_real(self): pass\n")
            self.assertEqual(stack_tests.suite(path).countTestCases(), 1)
            (path / "empty.test.py").write_text("# no tests\n")
            with self.assertRaises(RuntimeError):
                stack_tests.suite(path)


class WorkflowTests(unittest.TestCase):
    def test_primary_triggers_stay_identical_for_complete_coverage_uploads(self):
        configs = [workflow(n) for n in PRIMARY]
        self.assertTrue(all(c["on"] == configs[0]["on"] for c in configs))
        self.assertEqual({c["name"] for c in configs}, {"Core Unit Test", "WPCLI CI", "Diagnostics CI"})
        self.assertEqual(workflow("unit-test.yaml")["jobs"]["unit-test"]["name"], "Core Unit Test")
        for scope, name in zip(packages.SCOPES, PRIMARY):
            jobs = workflow(name)["jobs"]
            steps = jobs["unit-test" if scope == "core" else "unit"]["steps"]
            upload = next(s for s in steps if s.get("uses", "").startswith("codecov/"))
            self.assertEqual(upload["with"]["files"], "coverage-" + scope + ".out")
            self.assertEqual(upload["with"]["flags"], scope)
        coverage = yaml.safe_load((ROOT / "codecov.yml").read_text())
        self.assertEqual(coverage["codecov"]["notify"]["after_n_builds"], 3)
        self.assertEqual(coverage["coverage"]["status"]["patch"]["default"]["target"], "80%")

    def test_every_existing_smoke_has_one_matrix_or_cli_browser_owner(self):
        diag = workflow("diagnostics-ci.yaml")
        matrix = diag["jobs"]["backends"]["strategy"]["matrix"]["include"]
        actual = {row["fixture"] + "_smoke.py" for row in matrix}
        expected = {p.name for p in (ROOT / "cmd/wpdiag/tests").glob("*_smoke.py")} - {"webui_smoke.py"}
        self.assertEqual(actual, expected)
        self.assertEqual(len(matrix), len(actual))
        self.assertEqual(diag["jobs"]["backends"]["strategy"]["fail-fast"], "false")
        self.assertNotIn("needs", diag["jobs"]["unit"])
        self.assertNotIn("needs", diag["jobs"]["harness"])
        text = (WORKFLOWS / "diagnostics-ci.yaml").read_text()
        for command in ("tests/smoke.py", "tests/webui_smoke.py", "ci_packages.py diagnostic-helpers", "queryclient test -race ./...", "stack_tests.py"):
            self.assertIn(command, text)
        self.assertNotIn("--execute", text)
        self.assertIn("shell_terminal_test.py", (WORKFLOWS / "wpcli-ci.yaml").read_text())
        component = (WORKFLOWS / "integration-test-components.yaml").read_text()
        for command in ("ci_packages.py sdk-integration", "append_propagation.py", "tracing/cmd/requests", "Trace Export Integration"):
            self.assertIn(command, component)
        self.assertNotIn("./tests/integration/...", component)

    def test_rerun_guide_and_label_include_every_primary_check(self):
        script = workflow("pr-ci-label.yaml")["jobs"]["Label"]["steps"][0]["run"]
        checks = set(array(script, "ROOT_CHECKS"))
        actual = set()
        for filename in PRIMARY:
            for job in workflow(filename)["jobs"].values():
                if "strategy" in job:
                    actual.update(job["name"].replace("${{ matrix.fixture }}", row["fixture"]) for row in job["strategy"]["matrix"]["include"])
                else:
                    actual.add(job["name"])
        self.assertEqual(checks, actual)
        trigger = workflow("pr-ci-label.yaml")["on"]["workflow_run"]["workflows"]
        self.assertTrue({workflow(f)["name"] for f in PRIMARY} <= set(trigger))
        rerun = (WORKFLOWS / "rerun-ci.yaml").read_text()
        guide = (WORKFLOWS / "pr-ci-guide.yaml").read_text()
        for command, file in (("/rerun-unit-test", PRIMARY[0]), ("/rerun-wpcli", PRIMARY[1]), ("/rerun-diagnostics", PRIMARY[2])):
            self.assertIn('WORKFLOW_MAP["' + command + '"]="' + file + '"', rerun)
            self.assertIn(command, guide)
        # The privileged controller must continue to use API metadata only.
        self.assertNotIn("actions/checkout", (WORKFLOWS / "pr-ci-label.yaml").read_text())


class LabelTests(unittest.TestCase):
    def setUp(self):
        self.script = workflow("pr-ci-label.yaml")["jobs"]["Label"]["steps"][0]["run"]
        self.names = array(self.script, "ROOT_CHECKS")

    def label(self, checks, paths=("cmd/wpcli/cmd/root.go",), jobs=(), current="head", api_error=False):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "state.json").write_text(json.dumps({"checks": checks, "paths": paths, "jobs": jobs, "current": current, "api_error": api_error}))
            (root / "gh").write_text('''#!/usr/bin/env python3
import json,os,pathlib,sys
p=pathlib.Path(os.environ['CI_FIXTURE']);s=json.loads((p/'state.json').read_text());a=sys.argv[1:]
if a[:2]==['pr','view']: print(s['current'])
elif a[:2]==['pr','checks']:
 if s['api_error']: sys.exit(1)
 print(json.dumps(s['checks']))
elif a[:2]==['pr','edit']: (p/'effect').write_text('add' if '--add-label' in a else 'remove')
elif a[0]=='api':
 if '/commits/' in a[1]: print('123')
 elif '/jobs' in a[1]: print(json.dumps(s['jobs']))
 elif '/files' in a[1]: print('\\n'.join(s['paths']))
 else: sys.exit('Unexpected API request')
else: sys.exit('Unexpected gh command')
''')
            (root / "gh").chmod(0o755)
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ["PATH"], CI_FIXTURE=str(root),
                       REPO="fixture/repo", HEAD_SHA="head", RUN_ID="1", RUN_CONCLUSION="success")
            subprocess.run(["bash", "-euo", "pipefail", "-c", self.script], env=env, cwd=root, capture_output=True, text=True, check=True, timeout=10)
            return (root / "effect").read_text() if (root / "effect").exists() else "unchanged"

    def test_only_complete_success_can_add_label(self):
        checks = [{"name": n, "bucket": "pass"} for n in self.names]
        self.assertEqual(self.label(checks), "add")
        self.assertEqual(self.label(checks[:-1]), "remove")
        self.assertEqual(self.label(checks[:1]), "remove")
        self.assertEqual(self.label([]), "remove")

    def test_failure_cancellation_skip_and_api_failure_cannot_pass(self):
        checks = [{"name": n, "bucket": "pass"} for n in self.names]
        for bucket in ("fail", "cancel", "skipping", "pending"):
            with self.subTest(bucket=bucket):
                self.assertEqual(self.label(checks[:-1] + [{"name": self.names[-1], "bucket": bucket}]), "remove")
        self.assertEqual(self.label(checks, api_error=True), "remove")
        self.assertEqual(self.label(checks, jobs=[{"name": self.names[0], "conclusion": "failure"}]), "remove")

    def test_path_policy_matches_workflow_filters_and_stale_head_is_ignored(self):
        paths = workflow(PRIMARY[0])["on"]["pull_request"]["paths"]
        samples = ["server/storage/writer.go", "go.mod", "go.sum", "config/woodpecker.yaml", "cmd/wpcli/tests/shell_terminal_test.py",
            "cmd/wpdiag/webui/page.html", "diagnostics/README.md", "tests/integration/diagnostics/test_source_acceptance.py",
            "tests/integration/tracing/full_stack.py", "deployments/milvus-full-stack-helm/tests/cli.test.py",
            "docs/stack-examples/_e2e.html", "Makefile", "codecov.yml", ".github/scripts/ci_packages.py",
            ".github/scripts/test_ci_contracts.py", ".github/scripts/stack_tests.py", ".github/scripts/requirements-ci.txt",
            ".github/workflows/wpcli-ci.yaml", "README.md", "docs/something.md", "deployments/operator/README.md"]
        for path in samples:
            with self.subTest(path=path):
                expected = "remove" if any(fnmatch.fnmatchcase(path, pattern) for pattern in paths) else "add"
                self.assertEqual(self.label([], paths=[path]), expected)
        self.assertEqual(self.label([], current="newer-head"), "unchanged")


if __name__ == "__main__":
    unittest.main()
