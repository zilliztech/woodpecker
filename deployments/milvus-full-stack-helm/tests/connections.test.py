#!/usr/bin/env python3
"""Connection provenance, target scoping, output and forward ownership regressions."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("connections", ROOT / "scripts/connections.py")
connections = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(connections)


def resources():
    services = [{"metadata": {"name": name}, "spec": {"type": "ClusterIP", "clusterIP": "10.0.0.1",
                 "ports": [{"port": port, "protocol": "TCP"}]}} for name, port in
                [(service, port) for _, service, port, _, _ in connections.SERVICE_PORTS] + [("full-stack-etcd", 2379)]]
    pods = [{"metadata": {"name": "milvus-woodpecker-" + str(i), "namespace": "test-ns"}, "spec": {
        "containers": [{"env": [
            {"name": "NODE_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
            {"name": "ADVERTISE_SERVICE_ADDR", "value": "$(NODE_NAME).milvus-woodpecker-headless:18080"},
            {"name": "MINIO_SECRET_KEY", "value": "never-export-this-secret"}],
            "ports": [{"name": "metrics", "containerPort": 9091}]}]}} for i in range(3)]
    cm = {"data": {"default.yaml": """etcd:
  endpoints: [full-stack-etcd:2379]
minio:
  bucketName: example-bucket
  rootPath: example-root
  secretAccessKey: never-export-this-secret
""", "user.yaml": """etcd:
  rootPath: example-meta
woodpecker:
  meta:
    prefix: woodpecker
"""}}
    return {"items": services}, {"items": pods}, cm


def inventory():
    services, pods, cm = resources()
    result = connections.Connections("test-context", "test-ns")
    result.services = {s["metadata"]["name"]: s for s in services["items"]}
    result.pods, result.fields, result.forwards = pods["items"], connections.config_fields(cm), []
    result.refine_mappings = Mock()
    return result


class ConnectionTests(unittest.TestCase):
    def setUp(self):
        variables = ("GRAFANA_PORT", "PROMETHEUS_PORT", "MILVUS_PORT", "ALERTMANAGER_PORT", "WP_ADMIN_PORT_BASE", "ETCD_PORT")
        self.environment = patch.dict(os.environ, {key: str(value) for key, value in zip(variables, (3000, 9099, 19530, 9093, 19091, 32379))})
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def test_configuration_has_explicit_metadata_and_wp_storage_root(self):
        with patch.object(connections, "listening", return_value=False):
            report = inventory().report()
        self.assertFalse(report["wpcli"]["ready"])
        self.assertEqual(report["tenant"]["meta_prefix"], "example-meta/woodpecker")
        self.assertEqual(report["tenant"]["milvus_storage_root"], "example-root")
        self.assertEqual(report["tenant"]["root_path"], "example-root/wp")
        self.assertEqual(report["tenant"]["etcd_endpoints"], ["127.0.0.1:32379"])
        context = report["wpcli"]["config"]["contexts"]["test-context/test-ns"]
        self.assertEqual(set(context), {"endpoint", "node_admin_urls", "k8s"})
        self.assertEqual(context["node_admin_urls"]["milvus-woodpecker-2.milvus-woodpecker-headless:18080"], "http://127.0.0.1:19093")
        self.assertNotIn("never-export-this-secret", json.dumps(report))
        self.assertIn("--root-path example-root/wp", report["tenant"]["connect_command"])
        self.assertTrue(all(e["state"] == "not-forwarded" for e in report["endpoints"]))

    def test_deployed_yaml_override_and_stable_pod_ordinal_ports(self):
        result = inventory()
        _, _, cm = resources()
        cm["data"]["user.yaml"] = "etcd.rootPath: tenant-b\nwoodpecker.meta.prefix: wal\nminio: {bucketName: bucket-b, rootPath: data-b}\n"
        result.fields = connections.config_fields(cm)
        result.pods = [result.pods[0], result.pods[2]]
        with patch.object(connections, "listening", return_value=False), patch.dict(os.environ, WP_ADMIN_PORT_BASE="21000"):
            report = result.report()
        self.assertEqual(report["tenant"]["meta_prefix"], "tenant-b/wal")
        self.assertEqual(report["tenant"]["root_path"], "data-b/wp")
        self.assertEqual([n["admin"]["local_port"] for n in report["woodpecker_nodes"]], [21000, 21002])

    def test_discovery_only_reads_selected_cluster_and_namespace(self):
        services, pods, cm = resources()
        calls = []
        def run(context, namespace, *args):
            self.assertEqual((context, namespace), ("test-context", "test-ns"))
            self.assertEqual(args[0], "get")
            calls.append(args[1])
            return {"services": services, "pods": pods, "configmap": cm}[args[1]]
        with patch.object(connections, "kubectl", side_effect=run), patch.object(connections, "process_forwards", return_value=[]):
            result = connections.Connections("test-context", "test-ns").discover()
        self.assertCountEqual(calls, ["services", "pods", "configmap"])
        self.assertEqual(len(result.pods), 3)

    def test_processes_require_exact_target_and_ipv4_loopback(self):
        lines = "\n".join([
            "11 kubectl --context=test-context --namespace=test-ns port-forward --address 127.0.0.1 svc/milvus 29530:19530",
            "12 kubectl --context test-context -n test-ns port-forward pod/milvus-woodpecker-0 21000:9091",
            "13 kubectl --context other -n test-ns port-forward svc/milvus 19530:19530",
            "14 kubectl --context test-context -n other port-forward svc/milvus 19530:19530",
            "15 kubectl --context test-context -n test-ns port-forward --address 0.0.0.0 svc/milvus 19530:19530",
            "16 kubectl --context test-context -n test-ns port-forward --address ::1 svc/milvus 19530:19530",
            "17 kubectl -n test-ns port-forward svc/milvus 19530:19530",
        ])
        with patch.object(connections.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, lines, "")):
            result = connections.process_forwards("test-context", "test-ns")
        self.assertEqual([f["pid"] for f in result], [11, 12])

    def test_observed_nondefault_ports_are_exported_instead_of_assumed_defaults(self):
        result = inventory()
        with patch.object(connections, "listening", return_value=False):
            base = result.report()
        for endpoint in base["endpoints"]:
            result.forwards.append({"pid": 100, "resource": endpoint["resource"], "remote_port": endpoint["remote_port"], "local_port": endpoint["local_port"] + 100})
        ports = {f["local_port"] for f in result.forwards}
        with patch.object(connections, "listening", side_effect=lambda p: p in ports):
            report = result.report()
        self.assertTrue(report["wpcli"]["ready"])
        context = report["wpcli"]["config"]["contexts"]["test-context/test-ns"]
        self.assertEqual(context["endpoint"], "http://127.0.0.1:19191")
        self.assertNotIn("etcd", context)
        self.assertEqual(report["tenant"]["etcd_endpoints"], ["127.0.0.1:32479"])
        self.assertTrue(report["tenant"]["ready"])

    def test_an_unrelated_listener_never_makes_wpcli_ready(self):
        with patch.object(connections, "listening", return_value=True):
            report = inventory().report()
        self.assertFalse(report["wpcli"]["ready"])
        self.assertTrue(all(e["state"] == "occupied-unverified" for e in report["endpoints"]))

    def test_external_etcd_is_never_replaced_with_the_local_stack_etcd(self):
        result = inventory()
        result.fields["etcd.endpoints"] = ["external.example:2379"]
        with patch.object(connections, "listening", return_value=False):
            report = result.report()
        self.assertEqual(report["tenant"]["etcd_endpoints"], ["external.example:2379"])
        self.assertIn("external/custom", " ".join(report["warnings"]))
        self.assertFalse(report["wpcli"]["ready"])

    def test_auth_and_tls_are_not_exported_or_reported_as_ready(self):
        result = inventory()
        result.fields["etcd.auth.enabled"] = "false"
        result.fields["etcd.ssl.enabled"] = "false"
        with patch.object(connections, "listening", return_value=False):
            report = result.report()
        self.assertFalse(report["tenant"]["authentication_configured"])
        result.fields.update({"etcd.auth.enabled": True, "etcd.auth.password": "never-export-this-secret"})
        with patch.object(connections, "listening", return_value=False):
            report = result.report()
        self.assertTrue(report["tenant"]["authentication_configured"])
        self.assertNotIn("never-export-this-secret", json.dumps(report))
        result.fields["etcd.endpoints"] = ["https://user:never-export-this-secret@example.com:2379"]
        with self.assertRaises(RuntimeError) as error:
            result.tenant({"url": "127.0.0.1:32379", "remote_port": 2379})
        self.assertNotIn("never-export-this-secret", str(error.exception))

    def test_config_export_fails_cleanly_when_forwards_are_missing(self):
        result = inventory()
        with patch.object(connections.Connections, "discover", return_value=result), \
                patch.object(connections, "listening", return_value=False), \
                patch.object(connections.sys, "argv", ["connections.py", "--context", "test-context", "--namespace", "test-ns", "--format", "wpcli"]):
            stdout, stderr = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                self.assertEqual(connections.main(), 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("not ready", stderr.getvalue())

    def test_json_and_yaml_exports_are_machine_readable_without_a_banner(self):
        import yaml
        result = inventory()
        with patch.object(connections, "listening", return_value=False):
            base = result.report()
        result.forwards = [{"pid": 100, "resource": endpoint["resource"], "remote_port": endpoint["remote_port"],
                            "local_port": endpoint["local_port"]} for endpoint in base["endpoints"]]
        for format in ("json", "wpcli"):
            with self.subTest(format=format), patch.object(connections.Connections, "discover", return_value=result), \
                    patch.object(connections, "listening", return_value=True), \
                    patch.object(connections.sys, "argv", ["connections.py", "--context", "test-context", "--namespace", "test-ns", "--format", format]):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    self.assertEqual(connections.main(), 0)
                if format == "json":
                    self.assertTrue(json.loads(stdout.getvalue())["wpcli"]["ready"])
                else:
                    config = yaml.safe_load(stdout.getvalue())
                    self.assertEqual(config["current-context"], "test-context/test-ns")
                    self.assertEqual(config, result.report()["wpcli"]["config"])
                    self.assertNotIn("etcd", config["contexts"]["test-context/test-ns"])
                    self.assertIn("# 1. WP service cluster context (cli.yaml)", stdout.getvalue())
                    self.assertIn("\n\n# 2. Milvus instance WP client metadata connection (outside cli.yaml)", stdout.getvalue())
                    self.assertIn("#   connect test-ns --etcd 127.0.0.1:32379", stdout.getvalue())

    def test_live_memberlist_refines_only_known_node_mappings(self):
        result = inventory()
        del result.refine_mappings
        response = io.StringIO(json.dumps({"members": [
            {"id": "milvus-woodpecker-0", "service_addr": "milvus-woodpecker-0.milvus-woodpecker-headless:18080", "gossip_addr": "10.244.1.2:17946"},
            {"id": "unmapped-node", "service_addr": "other:18080", "gossip_addr": "10.244.9.9:17946"}]}))
        opener = Mock()
        opener.open.return_value = response
        result.forwards = [{"pid": 100, "resource": "pod/milvus-woodpecker-0", "remote_port": 9091, "local_port": 19091}]
        with patch.object(connections, "listening", side_effect=lambda p: p == 19091), \
                patch.object(connections.urllib.request, "build_opener", return_value=opener):
            report = result.report()
        mappings = report["wpcli"]["config"]["contexts"]["test-context/test-ns"]["node_admin_urls"]
        self.assertEqual(mappings["10.244.1.2:17946"], "http://127.0.0.1:19091")
        self.assertNotIn("10.244.9.9:17946", mappings)
        self.assertEqual(report["woodpecker_nodes"][0]["identity_source"], "live Admin memberlist")

    def test_missing_prefix_never_falls_back_to_a_different_tenant(self):
        result = inventory()
        del result.fields["etcd.rootpath"]
        with patch.object(connections, "listening", return_value=False):
            report = result.report()
        self.assertIsNone(report["tenant"]["meta_prefix"])
        self.assertIsNotNone(report["wpcli"]["config"])
        self.assertFalse(report["tenant"]["ready"])
        self.assertIsNone(report["tenant"]["connect_command"])

    def test_service_context_export_is_independent_of_metadata_readiness(self):
        import yaml
        for state in ("not-forwarded", "missing-prefix", "missing-endpoints", "external", "auth", "tls"):
            with self.subTest(metadata=state):
                result = inventory()
                if state == "missing-prefix":
                    del result.fields["etcd.rootpath"]
                elif state == "missing-endpoints":
                    del result.fields["etcd.endpoints"]
                elif state == "external":
                    result.fields["etcd.endpoints"] = ["external.example:2379"]
                elif state in ("auth", "tls"):
                    result.fields["etcd." + ("auth" if state == "auth" else "ssl") + ".enabled"] = True
                result.forwards = [{"pid": 100, "resource": "pod/" + pod["metadata"]["name"],
                                    "remote_port": 9091, "local_port": 19091 + i}
                                   for i, pod in enumerate(result.pods)]
                if state in ("auth", "tls"):
                    result.forwards.append({"pid": 100, "resource": "svc/full-stack-etcd", "remote_port": 2379, "local_port": 32379})
                ports = {f["local_port"] for f in result.forwards}
                with patch.object(connections.Connections, "discover", return_value=result), \
                        patch.object(connections, "listening", side_effect=lambda p: p in ports), \
                        patch.object(connections.sys, "argv", ["connections.py", "--context", "test-context", "--namespace", "test-ns", "--format", "wpcli"]):
                    report = result.report()
                    self.assertTrue(report["wpcli"]["ready"])
                    self.assertFalse(report["tenant"]["ready"])
                    stdout = io.StringIO()
                    with contextlib.redirect_stdout(stdout):
                        self.assertEqual(connections.main(), 0)
                    self.assertEqual(yaml.safe_load(stdout.getvalue()), report["wpcli"]["config"])

    def test_text_report_separates_service_context_from_milvus_metadata(self):
        with patch.object(connections, "listening", return_value=False):
            report = inventory().report()
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            connections.print_report(report)
        service, metadata = stdout.getvalue().split("2. Milvus instance WP client metadata connection", 1)
        self.assertIn("1. WP service cluster context (cli.yaml)", service)
        self.assertIn("current-context: test-context/test-ns", service)
        self.assertNotIn("example-meta/woodpecker", service)
        self.assertIn("example-meta/woodpecker", metadata)
        self.assertIn("connect test-ns --etcd", metadata)

    def test_metadata_comments_cannot_introduce_yaml_settings(self):
        import yaml
        with patch.object(connections, "listening", return_value=False):
            report = inventory().report()
        report["tenant"]["connect_command"] = "connect 'tenant\ninjected: true'"
        self.assertEqual(yaml.safe_load(connections.wpcli_export(report)), report["wpcli"]["config"])

    def test_port_conflict_preflight_has_no_process_side_effects(self):
        with patch.object(connections, "listening", return_value=False):
            report = inventory().report()
        with patch.object(connections, "listening", return_value=True), patch.object(connections.subprocess, "Popen") as spawn:
            with self.assertRaisesRegex(RuntimeError, "occupied"):
                connections.forward(report)
            spawn.assert_not_called()

    def test_forward_reuses_borrowed_processes_and_cleans_up_only_owned_children(self):
        result = inventory()
        with patch.object(connections, "listening", return_value=False):
            base = result.report()
        for endpoint in base["endpoints"][:4]:
            result.forwards.append({"pid": 100, "resource": endpoint["resource"], "remote_port": endpoint["remote_port"], "local_port": endpoint["local_port"]})
        ports = {f["local_port"] for f in result.forwards}
        with patch.object(connections, "listening", side_effect=lambda p: p in ports):
            report = result.report()
        children, commands = [], []
        def start(command):
            commands.append(command)
            ports.add(int(command[-1].split(":")[0]))
            child = Mock()
            child.poll.return_value = None
            children.append(child)
            return child
        with patch.object(connections, "listening", side_effect=lambda p: p in ports), \
                patch.object(connections.subprocess, "Popen", side_effect=start), \
                patch.object(connections.time, "sleep", side_effect=RuntimeError("bounded fixture stop")), \
                contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError, "bounded fixture stop"):
                connections.forward(report)
        self.assertEqual(len(children), 4)
        self.assertTrue(all("svc/monitoring-grafana" not in command for command in commands))
        for child in children:
            child.terminate.assert_called_once()
            child.wait.assert_called_once()


if __name__ == "__main__":
    unittest.main()
