#!/usr/bin/env python3
"""Native pause/resume, controller ownership, data retention and repeatability."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("lifecycle", ROOT / "scripts/lifecycle.py")
lifecycle = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(lifecycle)
K = ["kubectl", "--context", "production context", "-n", "example"]


def workload(kind, name, replicas=1, release="milvus"):
    return {"kind": kind, "metadata": {"name": name, "uid": "uid-" + name,
            "annotations": {"meta.helm.sh/release-name": release, "meta.helm.sh/release-namespace": "example"}},
            "spec": {"replicas": replicas, "selector": {"matchLabels": {"app": name}}, "template": {"spec": {}}}}


def fixtures():
    items = [workload("Deployment", "milvus-proxy", 3), workload("Deployment", "milvus-mixcoord"), workload("Deployment", "milvus-streamingnode"), workload("StatefulSet", "milvus-woodpecker", 3),
             workload("StatefulSet", "full-stack-etcd", release="full-stack"), workload("StatefulSet", "full-stack-minio", release="full-stack"),
             workload("Deployment", "monitoring-operator", release="monitoring"), workload("Deployment", "monitoring-grafana", 0, "monitoring"),
             workload("Prometheus", "monitoring-prometheus", 2, "monitoring"), workload("Alertmanager", "monitoring-alertmanager", 1, "monitoring"),
             workload("DaemonSet", "full-stack-alloy", release="full-stack")]
    children = []
    for i in items:
        if i["kind"] in ("Prometheus", "Alertmanager"):
            child = workload("StatefulSet", i["metadata"]["name"] + "-controlled")
            child["metadata"]["ownerReferences"] = [{"uid": i["metadata"]["uid"]}]
            children.append(child)
    return items, children


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.out = Path(self.temp.name)
        self.items, self.children = fixtures()
        self.state = None
        self.hpas = []
        self.pods = [{"metadata": {"name": i["metadata"]["name"] + "-0", "labels": i["spec"]["selector"]["matchLabels"]}} for i in self.items + self.children if i["kind"] not in ("Prometheus", "Alertmanager")]

    def build(self, action):
        def read(args):
            return {"items": self.hpas if "hpa" in args else self.pods}
        with patch.object(lifecycle, "inventory", return_value=(K, self.items, self.children)), patch.object(lifecycle, "saved_state", side_effect=lambda _: self.state), patch.object(lifecycle, "read", side_effect=read):
            return lifecycle.plan(action, "production context", "example", self.out)

    def save(self):
        self.state = lifecycle.snapshot(self.items, self.children, "example")

    def test_stop_only_plans_native_scales_and_controller_patches(self):
        commands = self.build("stop")
        self.assertEqual(commands[0][len(K)], "create")
        self.assertTrue(all(c[:len(K)] == K for c in commands))
        self.assertFalse(any("uninstall" in c or "pvc" in c or "minikube" in c for c in commands))
        scales = [c for c in commands if "scale" in c]
        self.assertTrue(all(c[-1] == "--replicas=0" for c in scales))
        self.assertFalse(any("-controlled" in c[len(K)+1] for c in scales))
        prom = next(c for c in commands if "prometheuses.monitoring.coreos.com/monitoring-prometheus" in c)
        self.assertEqual(json.loads(prom[-1]), {"spec": {"replicas": 0}})
        operator = next(i for i, c in enumerate(commands) if "deployment/monitoring-operator" in c)
        last_prom_wait = max(i for i, c in enumerate(commands) if "--selector=app=monitoring-prometheus-controlled" in c)
        self.assertGreater(operator, last_prom_wait)
        stored = json.loads(yaml.safe_load((self.out / "lifecycle-state.yaml").read_text())["data"]["state.json"])
        self.assertEqual(next(w["replicas"] for w in stored["workloads"] if w["name"] == "milvus-proxy"), 3)

    def test_start_restores_exact_previous_counts_and_removes_only_its_pause_selector(self):
        self.save()
        commands = self.build("start")
        self.assertIn("deployment/monitoring-operator", commands[0])
        self.assertIn([*K, "scale", "deployment/milvus-proxy", "--replicas=3"], commands)
        self.assertIn([*K, "scale", "deployment/monitoring-grafana", "--replicas=0"], commands)
        self.assertFalse(any("rollout" in c and "deployment/monitoring-grafana" in c for c in commands))
        ds = next(c for c in commands if "patch" in c and "daemonset/full-stack-alloy" in c)
        self.assertEqual(json.loads(ds[-1])["spec"]["template"]["spec"]["nodeSelector"], {lifecycle.PAUSE_KEY: None})
        self.assertEqual(commands[-1], [*K, "delete", "configmap", lifecycle.STATE_NAME])
        self.assertFalse(any("pvc" in c for c in commands))
        self.assertTrue(any("--for=jsonpath={.spec.replicas}=2" in c for c in commands))

    def test_start_schedules_interdependent_milvus_roles_before_waiting_for_them(self):
        self.save()
        commands = self.build("start")
        roles = {"deployment/milvus-mixcoord", "deployment/milvus-streamingnode", "deployment/milvus-proxy"}
        scales = [i for i,c in enumerate(commands) if "scale" in c and roles.intersection(c)]
        waits = [i for i,c in enumerate(commands) if "rollout" in c and roles.intersection(c)]
        self.assertEqual(len(scales), 3)
        self.assertLess(max(scales), min(waits))

    def test_repeated_stop_does_not_overwrite_saved_counts_with_zero(self):
        self.save()
        for i in self.items:
            if i["kind"] != "DaemonSet": i["spec"]["replicas"] = 0
        self.items[-1]["spec"]["template"]["spec"]["nodeSelector"] = {lifecycle.PAUSE_KEY: self.state["pause_token"]}
        commands = self.build("stop")
        self.assertFalse(any("create" in c for c in commands))
        self.assertEqual(next(w["replicas"] for w in self.state["workloads"] if w["name"] == "milvus-proxy"), 3)

    def test_start_without_stop_state_changes_nothing(self):
        with contextlib.redirect_stdout(io.StringIO()): self.assertEqual(self.build("start"), [])

    def test_scale_down_retention_delete_is_rejected_including_operator_children(self):
        for saved in (False, True):
            self.children[0]["spec"].pop("persistentVolumeClaimRetentionPolicy", None)
            if saved: self.save()
            self.children[0]["spec"]["persistentVolumeClaimRetentionPolicy"] = {"whenScaled": "Delete"}
            with self.assertRaisesRegex(ValueError, "would delete PVCs"): self.build("stop")

    def test_recreated_workload_cannot_receive_stale_replica_state(self):
        self.save()
        self.items[0]["metadata"]["uid"] = "replacement-owner"
        with self.assertRaisesRegex(ValueError, "identity changed"): self.build("start")

    def test_hpa_cannot_silently_reconcile_paused_workloads(self):
        self.hpas = [{"spec": {"scaleTargetRef": {"kind": "Deployment", "name": "milvus-proxy"}}}]
        with self.assertRaisesRegex(ValueError, "HPA"): self.build("stop")
        self.assertFalse((self.out / "lifecycle-state.yaml").exists())

    def test_printed_command_round_trips_with_spaces_and_no_shell_execution(self):
        argv = [*K, "patch", "daemonset/example", "--type=merge", "-p", '{"key":"$(never-execute)"}']
        with patch.object(lifecycle.subprocess, "run") as run, contextlib.redirect_stdout(io.StringIO()) as output:
            lifecycle.command(argv, False)
            run.assert_not_called()
        self.assertEqual(shlex.split(output.getvalue()), argv)
        with patch.object(lifecycle.subprocess, "run") as run, contextlib.redirect_stdout(io.StringIO()):
            lifecycle.command(argv, True)
            run.assert_called_once_with(argv, check=True)

    def test_main_preview_never_runs_the_prepared_native_writes(self):
        commands = [K + ["scale", "deployment/example", "--replicas=0"]]
        with patch.object(sys, "argv", ["lifecycle.py", "stop", "--context", "test", "--namespace", "example", "--output", str(self.out)]), \
                patch.object(lifecycle, "plan", return_value=commands), patch.object(lifecycle.subprocess, "run") as run, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(lifecycle.main(), 0)
            run.assert_not_called()
        self.assertEqual(json.loads((self.out / "commands.json").read_text()), commands)

    def test_execute_stops_on_failure_and_retains_the_plan_for_recovery(self):
        commands = [K + ["scale", "deployment/one", "--replicas=0"], K + ["scale", "deployment/two", "--replicas=0"]]
        with patch.object(sys, "argv", ["lifecycle.py", "stop", "--context", "test", "--namespace", "example", "--output", str(self.out), "--execute"]), \
                patch.object(lifecycle, "plan", return_value=commands), patch.object(lifecycle.subprocess, "run", side_effect=subprocess.CalledProcessError(17, commands[0])) as run, contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(subprocess.CalledProcessError): lifecycle.main()
            run.assert_called_once_with(commands[0], check=True)
        self.assertEqual(json.loads((self.out / "commands.json").read_text()), commands)

    def test_inventory_uses_helm_manifests_and_ownership_to_exclude_other_releases(self):
        foreign = workload("Deployment", "someone-else", release="other")
        def read(args, json_output=True):
            if "list" in args: return [{"name": r} for r in ("milvus", "full-stack", "monitoring", "other")]
            if "manifest" in args:
                return "\n---\n".join(yaml.safe_dump(i) for i in self.items if i["metadata"]["annotations"]["meta.helm.sh/release-name"] == args[-1])
            return {"items": self.items + self.children + [foreign]}
        with patch.object(lifecycle, "read", side_effect=read):
            _, items, children = lifecycle.inventory("production context", "example")
        self.assertEqual(len(items), len(self.items))
        self.assertEqual(len(children), 2)
        self.assertNotIn("someone-else", [i["metadata"]["name"] for i in items])


if __name__ == "__main__": unittest.main()
