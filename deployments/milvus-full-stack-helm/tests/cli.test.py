#!/usr/bin/env python3
"""Verify lifecycle scoping and cleanup without stopping or uninstalling a real stack."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

STACK = Path(__file__).resolve().parents[1] / "stack.sh"


class LifecycleCLI(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        binaries = root / "bin"
        binaries.mkdir()
        self.trace = root / "calls.jsonl"
        self.state = root / "state"
        self.pvcs = root / "pvcs.json"
        self.pvcs.write_text(json.dumps([
            {"name": "wp-0", "labels": {"app.kubernetes.io/instance": "milvus"}},
            {"name": "wp-13", "labels": {"app.kubernetes.io/instance": "milvus"}},
            {"name": "prometheus", "labels": {"app.kubernetes.io/instance": "monitoring-kube-prometheus-prometheus"}},
            {"name": "alertmanager", "labels": {"app.kubernetes.io/instance": "monitoring-kube-prometheus-alertmanager"}},
            {"name": "grafana", "labels": {"app.kubernetes.io/instance": "monitoring"}},
            {"name": "etcd", "labels": {"app": "full-stack-etcd"}},
            {"name": "minio", "labels": {"app": "full-stack-minio"}},
            {"name": "loki", "labels": {"app": "full-stack-loki"}},
            {"name": "other-milvus", "labels": {"app.kubernetes.io/instance": "other-milvus", "app.kubernetes.io/name": "milvus"}},
            {"name": "other-prometheus", "labels": {"app.kubernetes.io/instance": "other-prometheus", "app.kubernetes.io/name": "prometheus"}},
            {"name": "unlabeled", "labels": {}},
        ]))
        self.env = dict(os.environ, PATH=str(binaries) + os.pathsep + os.environ["PATH"],
                        STATE=str(self.state), PROFILE="test-profile", CONTEXT="test-context",
                        NAMESPACE="test-namespace", TIMEOUT="2m", CLI_TEST_TRACE=str(self.trace),
                        CLI_TEST_PVCS=str(self.pvcs))
        # Backends only record requests. They never contact Docker or Kubernetes.
        stub = "#!" + sys.executable + "\n" + '''import json, os, re, sys
tool = os.path.basename(sys.argv[0])
args = sys.argv[1:]
with open(os.environ["CLI_TEST_TRACE"], "a") as output:
    output.write(json.dumps([tool] + args) + "\\n")
if tool == "helm" and "list" in args:
    print(json.dumps([{"name": n} for n in os.environ.get("CLI_TEST_RELEASES", "").split(",") if n]))
if tool == "helm" and "get" in args:
    release = args[args.index("values") + 1]
    values = {"components": {n: True for n in ("grafana", "prometheus", "alertmanager", "jaeger", "loki")}} if release == "full-stack" else {}
    if release == "milvus":
        values = {"extraConfigFiles": {"user.yaml": "trace: {exporter: otlp, sampleFraction: 1, otlp: {endpoint: 'full-stack-jaeger:4317', method: grpc, secure: false}}"}}
    print(json.dumps(values))
if tool == "kubectl" and "deployments,statefulsets" in args:
    print('{"items": []}')
if tool == "helm" and "uninstall" in args:
    release = args[args.index("uninstall") + 1]
    if release == os.environ.get("CLI_TEST_FAIL_RELEASE"):
        sys.exit(17)
if tool == "kubectl" and "delete" in args:
    if os.environ.get("CLI_TEST_FAIL_PVC_DELETE"):
        sys.exit(19)
    assert args[args.index("delete") + 1] == "pvc"
    selector = args[args.index("--selector") + 1]
    match = re.fullmatch(r"([^ ]+) in \\(([^)]+)\\)", selector)
    assert match, selector
    key, values = match.groups()
    values = {value.strip() for value in values.split(",")}
    path = os.environ["CLI_TEST_PVCS"]
    with open(path) as source:
        claims = json.load(source)
    with open(path, "w") as output:
        json.dump([pvc for pvc in claims if pvc["labels"].get(key) not in values], output)
'''
        for tool in ("minikube", "helm", "kubectl"):
            binary = binaries / tool
            binary.write_text(stub)
            binary.chmod(0o755)

    def run_cli(self, *args):
        return subprocess.run(["bash", str(STACK), *args], env=self.env,
                              capture_output=True, text=True, timeout=10)

    def calls(self):
        return [json.loads(line) for line in self.trace.read_text().splitlines()] if self.trace.exists() else []

    def test_help_needs_no_backends_and_creates_no_state(self):
        for args in ((), ("help",), ("-h",), ("--help",), ("deploy", "--help"), ("clear", "--help")):
            with self.subTest(args=args):
                result = self.run_cli(*args)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("Cluster lifecycle", result.stdout)
                self.assertIn("Stack lifecycle", result.stdout)
                self.assertIn("test-context", result.stdout)
                self.assertIn("delete their persistent data", result.stdout)
                self.assertNotIn("Compatibility alias", result.stdout)
        self.assertEqual(self.calls(), [])
        self.assertFalse(self.state.exists())

    def test_invalid_input_has_no_side_effects(self):
        for args in (("unknown",), ("down",), ("stop", "--unknown"), ("deploy", "--values"),
                     ("status", "--json"), ("connections", "--wpcli"), ("forward", "--wpcli-config"),
                     ("connections", "--json", "--wpcli-config")):
            with self.subTest(args=args):
                self.assertEqual(self.run_cli(*args).returncode, 2)
        self.assertEqual(self.calls(), [])
        self.assertFalse(self.state.exists())

    def test_stop_targets_only_the_selected_minikube_profile(self):
        result = self.run_cli("stop")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.calls(), [["minikube", "stop", "-p", "test-profile"]])

    def test_start_resumes_the_selected_profile_with_configured_resources(self):
        self.env.update(CPUS="4", MEMORY="4096")
        result = self.run_cli("start")
        self.assertEqual(result.returncode, 0, result.stderr)
        call, = self.calls()
        self.assertEqual(call[:4], ["minikube", "start", "-p", "test-profile"])
        self.assertIn("--cpus=4", call)
        self.assertIn("--memory=4096", call)

    def test_clear_deletes_all_stack_storage_and_keeps_unrelated_claims(self):
        for action in ("clear", "clear"):
            result = self.run_cli(action)
            self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls()
        self.assertEqual(len(calls), 10)
        for index, call in enumerate(calls):
            if index % 5 < 3:
                self.assertEqual(call[:6], ["helm", "--kube-context", "test-context", "-n", "test-namespace", "uninstall"])
                self.assertEqual(call[6], ("milvus", "full-stack", "monitoring")[index % 5])
                self.assertEqual(call[-2:], ["--timeout", "2m"])
            else:
                self.assertEqual(call[:7], ["kubectl", "--context", "test-context", "-n", "test-namespace", "delete", "pvc"])
                self.assertNotIn("--all", call)
                self.assertIn("--timeout=2m", call)
            self.assertIn("--ignore-not-found", call)
            self.assertIn("--wait", call)
        remaining = {pvc["name"] for pvc in json.loads(self.pvcs.read_text())}
        self.assertEqual(remaining, {"other-milvus", "other-prometheus", "unlabeled"})

    def cached_charts(self):
        charts = self.state / "charts"
        charts.mkdir(parents=True)
        for name in ("milvus-5.0.31.tgz", "kube-prometheus-stack-67.4.0.tgz"):
            (charts / name).write_text("offline test fixture")

    def test_first_deploy_installs_only_core_without_monitoring(self):
        self.cached_charts()
        result = self.run_cli("deploy")
        self.assertEqual(result.returncode, 0, result.stderr)
        upgrades = [c for c in self.calls() if c[0] == "helm" and "upgrade" in c]
        self.assertEqual([c[c.index("--install") + 1] for c in upgrades], ["full-stack", "milvus"])
        self.assertFalse(any("pull" in c for c in self.calls()))

    def test_adding_loki_does_not_upgrade_core(self):
        self.cached_charts()
        self.env["CLI_TEST_RELEASES"] = "milvus,full-stack,monitoring"
        result = self.run_cli("addons-deploy", "--with", "loki")
        self.assertEqual(result.returncode, 0, result.stderr)
        upgrades = [c for c in self.calls() if c[0] == "helm" and "upgrade" in c]
        self.assertEqual([c[c.index("--install") + 1] for c in upgrades], ["monitoring", "full-stack"])

    def test_removing_loki_keeps_core_and_other_addons(self):
        self.cached_charts()
        self.env["CLI_TEST_RELEASES"] = "milvus,full-stack,monitoring"
        result = self.run_cli("addons-clear", "--with", "loki")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(any("uninstall" in c for c in self.calls()))
        self.assertFalse(any("upgrade" in c and "milvus" in c for c in self.calls()))
        removed = [c for c in self.calls() if c[0] == "kubectl" and "delete" in c]
        self.assertEqual(len(removed), 1)
        self.assertIn("app in (full-stack-loki)", removed[0])
        remaining = {p["name"] for p in json.loads(self.pvcs.read_text())}
        self.assertNotIn("loki", remaining)
        self.assertIn("wp-0", remaining)
        self.assertIn("prometheus", remaining)

    def test_clear_stops_on_backend_failure(self):
        self.env["CLI_TEST_FAIL_RELEASE"] = "milvus"
        result = self.run_cli("clear")
        self.assertEqual(result.returncode, 17, result.stderr)
        self.assertEqual(len(self.calls()), 1)
        self.assertEqual(len(json.loads(self.pvcs.read_text())), 11)

    def test_clear_reports_storage_failure_and_can_be_retried(self):
        self.env["CLI_TEST_FAIL_PVC_DELETE"] = "1"
        result = self.run_cli("clear")
        self.assertEqual(result.returncode, 19, result.stderr)
        self.assertEqual(len(self.calls()), 4)
        self.assertNotIn("Stack releases and PVCs deleted", result.stdout)
        del self.env["CLI_TEST_FAIL_PVC_DELETE"]
        result = self.run_cli("clear")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual({pvc["name"] for pvc in json.loads(self.pvcs.read_text())},
                         {"other-milvus", "other-prometheus", "unlabeled"})


if __name__ == "__main__":
    unittest.main()
