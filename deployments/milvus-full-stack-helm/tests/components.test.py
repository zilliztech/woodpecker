#!/usr/bin/env python3
"""Independent addon selection, safe late changes and CRD-free core rendering."""
import contextlib
import importlib.util
import io
import itertools
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("components", ROOT / "scripts/components.py")
components = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(components)


class ComponentTests(unittest.TestCase):
    def base(self):
        return tuple(components.load_values([ROOT / path]) for path in (
            "charts/full-stack-support/values.yaml", "values/monitoring.yaml", "values/milvus.yaml"))

    def test_each_optional_component_can_be_selected_independently(self):
        support, mon, mv = self.base()
        for mask in itertools.product((False, True), repeat=5):
            selected = {name for name, enabled in zip(components.NAMES, mask) if enabled}
            with self.subTest(selected=selected):
                s, m, v, _ = components.build(selected, support, mon, mv, "test")
                for name in ("grafana", "prometheus", "alertmanager"):
                    self.assertEqual(m[name]["enabled"], name in selected)
                self.assertEqual(m["prometheusOperator"]["enabled"], bool(selected & {"prometheus", "alertmanager"}))
                self.assertEqual(s["jaeger"]["enabled"], "jaeger" in selected)
                self.assertEqual(s["loki"]["enabled"], "loki" in selected)
                self.assertEqual({d["uid"] for d in m["grafana"]["additionalDataSources"]}, selected & {"jaeger", "loki"})
                self.assertEqual(m["grafana"]["sidecar"]["datasources"]["defaultDatasourceEnabled"], "prometheus" in selected)
                self.assertEqual(yaml.safe_load(v["extraConfigFiles"]["user.yaml"])["trace"]["exporter"], "otlp" if "jaeger" in selected else "noop")

    def test_late_loki_install_preserves_live_core_overrides_and_trace(self):
        support, mon, mv = self.base()
        mv["image"]["all"]["tag"] = "private-user-build"
        mv["streamingNode"]["resources"]["limits"]["memory"] = "3Gi"
        _, _, traced, _ = components.build({"jaeger"}, support, mon, mv, "test")
        live = {"milvus": traced, "full-stack": {"minio": {"secretKey": "custom-secret"}}, "monitoring": {"grafana": {"adminPassword": "custom-password"}}}
        with tempfile.TemporaryDirectory() as folder, patch.object(components, "deployed", return_value=(live, {"jaeger"})), \
                patch("sys.argv", ["components.py", "addons-deploy", "--with", "loki", "--output", folder]), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(components.main(), 0)
            result = yaml.safe_load((Path(folder) / "milvus.yaml").read_text())
            self.assertEqual(result["image"]["all"]["tag"], "private-user-build")
            self.assertEqual(result["streamingNode"]["resources"]["limits"]["memory"], "3Gi")
            self.assertEqual(yaml.safe_load((Path(folder) / "support.yaml").read_text())["minio"]["secretKey"], "custom-secret")
            self.assertEqual(yaml.safe_load((Path(folder) / "monitoring.yaml").read_text())["grafana"]["adminPassword"], "custom-password")
            self.assertFalse(json.loads((Path(folder) / "plan.json").read_text())["trace_changed"])

    def test_plain_deploy_keeps_previously_selected_addons(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(components, "deployed", return_value=({}, {"grafana", "loki"})), \
                patch("sys.argv", ["components.py", "deploy", "--output", folder]), contextlib.redirect_stdout(io.StringIO()):
            components.main()
            self.assertEqual(json.loads((Path(folder) / "plan.json").read_text())["selected"], ["grafana", "loki"])

    def test_core_manifest_contains_no_monitoring_crd_or_optional_workload(self):
        output = subprocess.check_output(["helm", "template", "full-stack", str(ROOT / "charts/full-stack-support")], text=True)
        docs = [d for d in yaml.safe_load_all(output) if d]
        self.assertFalse({d["kind"] for d in docs} & {"PodMonitor", "PrometheusRule", "Prometheus", "Alertmanager"})
        names = {d["metadata"]["name"] for d in docs}
        self.assertIn("full-stack-etcd", names)
        self.assertIn("full-stack-minio", names)
        self.assertFalse(names & {"full-stack-jaeger", "full-stack-loki", "full-stack-alloy"})

    def test_grafana_only_needs_no_monitoring_crds(self):
        chart = ROOT / ".local/charts/kube-prometheus-stack-67.4.0.tgz"
        if not chart.exists():
            self.skipTest("Run ./stack.sh charts --with grafana to cache the pinned chart")
        support, mon, mv = self.base()
        _, monitoring, _, _ = components.build({"grafana"}, support, mon, mv, "test")
        with tempfile.TemporaryDirectory() as folder:
            values = Path(folder) / "values.yaml"
            values.write_text(yaml.safe_dump(monitoring))
            output = subprocess.check_output(["helm", "template", "monitoring", str(chart), "--include-crds", "-f", str(values)], text=True)
        docs = [d for d in yaml.safe_load_all(output) if d]
        self.assertFalse(any(d["apiVersion"].startswith("monitoring.coreos.com/") for d in docs))
        self.assertTrue(any(d["kind"] == "Deployment" and "grafana" in d["metadata"]["name"] for d in docs))

    def test_removed_jaeger_disables_export_without_touching_other_user_settings(self):
        support, mon, mv = self.base()
        _, _, traced, _ = components.build({"jaeger"}, support, mon, mv, "test")
        _, _, disabled, changed = components.build(set(), support, mon, traced, "test")
        self.assertTrue(changed)
        user = yaml.safe_load(disabled["extraConfigFiles"]["user.yaml"])
        self.assertEqual(user["trace"]["exporter"], "noop")
        self.assertEqual(user["woodpecker"]["meta"]["prefix"], "woodpecker")
        self.assertEqual(disabled["image"], traced["image"])

    def test_unknown_components_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "Unknown components"):
            components.selection("loki,promethues")


if __name__ == "__main__":
    unittest.main()
