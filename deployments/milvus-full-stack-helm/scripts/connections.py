#!/usr/bin/env python3
"""Discover this stack's connection targets and manage optional wpcli forwards."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
import posixpath
import re
import shlex
import signal
import socket
import subprocess
import sys
import time
import urllib.parse
import urllib.request


SERVICE_PORTS = (
    ("Grafana", "monitoring-grafana", 80, "GRAFANA_PORT", 3000),
    ("Prometheus", "monitoring-kube-prometheus-prometheus", 9090, "PROMETHEUS_PORT", 9099),
    ("Alertmanager", "monitoring-kube-prometheus-alertmanager", 9093, "ALERTMANAGER_PORT", 9093),
    ("Milvus", "milvus", 19530, "MILVUS_PORT", 19530),
)


def kubectl(context, namespace, *args):
    result = subprocess.run(["kubectl", "--context", context, "-n", namespace,
                             "--request-timeout=5s", *args], capture_output=True, text=True, timeout=8)
    if result.returncode:
        raise RuntimeError("kubectl failed for " + " ".join(args[:3]) + ": " + result.stderr.strip())
    return json.loads(result.stdout)


def port_value(name, default):
    value = int(os.getenv(name, default))
    if not 1 <= value <= 65535:
        raise ValueError(name + " must be between 1 and 65535")
    return value


def listening(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=.15):
            return True
    except OSError:
        return False


def process_forwards(context, namespace):
    """Only recognize explicit loopback forwards for the exact context/namespace."""
    result = subprocess.run(["ps", "-axo", "pid=,command="], capture_output=True, text=True, timeout=3)
    if result.returncode:
        raise RuntimeError("Cannot inspect local port-forward processes")
    found = []
    for line in result.stdout.splitlines():
        try:
            pid, command = line.strip().split(None, 1)
            tokens = shlex.split(command)
            start = next(i for i, token in enumerate(tokens) if os.path.basename(token) == "kubectl")
            args = tokens[start + 1:]
            if "port-forward" not in args:
                continue
            options, positionals = {}, []
            index = 0
            while index < len(args):
                arg = args[index]
                if arg == "port-forward":
                    index += 1
                    continue
                if arg.startswith("-"):
                    name, separator, value = arg.partition("=")
                    if name in ("--context", "--namespace", "-n", "--address", "--kubeconfig", "--pod-running-timeout", "--request-timeout"):
                        if not separator:
                            index += 1
                            value = args[index]
                        options[name] = value
                    index += 1
                    continue
                positionals.append(arg)
                index += 1
            if options.get("--context") != context or options.get("--namespace", options.get("-n")) != namespace:
                continue
            addresses = set(options.get("--address", "localhost").split(","))
            if not addresses <= {"127.0.0.1", "localhost", "::1"} or not addresses & {"127.0.0.1", "localhost"}:
                continue
            resource, *mappings = positionals
            kind, separator, name = resource.partition("/")
            if not separator:
                kind, name = "pod", resource
            kind = {"service": "svc", "services": "svc", "pods": "pod"}.get(kind, kind)
            if kind not in ("svc", "pod"):
                continue
            for mapping in mappings:
                local, remote = mapping.split(":")
                if local.isdigit() and remote.isdigit() and 0 < int(local) <= 65535 and 0 < int(remote) <= 65535:
                    found.append({"pid": int(pid), "resource": kind + "/" + name,
                                  "local_port": int(local), "remote_port": int(remote)})
        except (ValueError, IndexError, StopIteration):
            continue
    return found


def config_fields(configmap):
    try:
        import yaml
    except ImportError as error:
        raise RuntimeError("Connection discovery requires PyYAML: python3 -m pip install -r scripts/requirements-connections.txt") from error
    fields = {}

    def flatten(value, prefix=""):
        if not isinstance(value, dict):
            raise ValueError("Milvus configuration must be a mapping")
        for key, item in value.items():
            path = (prefix + "." + str(key)).strip(".").lower()
            if isinstance(item, dict):
                flatten(item, path)
            else:
                fields[path] = item

    for filename in ("default.yaml", "user.yaml"):
        try:
            flatten(yaml.safe_load(configmap.get("data", {}).get(filename, "")) or {})
        except (yaml.YAMLError, ValueError) as error:
            # YAML parser diagnostics can include secret-bearing source lines.
            raise RuntimeError("Invalid deployed Milvus configuration in ConfigMap milvus/" + filename) from error
    return fields


def pod_environment(pod):
    values = {}
    for container in pod.get("spec", {}).get("containers", []):
        for env in container.get("env", []):
            value = env.get("value")
            if value is None:
                field = env.get("valueFrom", {}).get("fieldRef", {}).get("fieldPath")
                value = {"metadata.name": pod["metadata"]["name"],
                         "metadata.namespace": pod["metadata"].get("namespace", ""),
                         "status.podIP": pod.get("status", {}).get("podIP", "")}.get(field)
            if value is not None:
                values[env["name"]] = re.sub(r"\$\(([^)]+)\)", lambda m: values.get(m[1], m[0]), str(value))
    return values


def pod_sort(pod):
    name = pod["metadata"]["name"]
    match = re.search(r"-(\d+)$", name)
    return (name[:match.start()] if match else name, int(match[1]) if match else 0)


class Connections:
    def __init__(self, context, namespace, context_name=None):
        self.context, self.namespace = context, namespace
        self.context_name = context_name or context + "/" + namespace
        self.warnings = []

    def discover(self):
        with ThreadPoolExecutor(max_workers=3) as pool:
            jobs = [pool.submit(kubectl, self.context, self.namespace, *args) for args in (
                ("get", "services", "-o", "json"),
                ("get", "pods", "-l", "app.kubernetes.io/instance=milvus,component=woodpecker", "-o", "json"),
                ("get", "configmap", "milvus", "-o", "json"))]
            services, pods, cm = [job.result() for job in jobs]
        self.services = {s["metadata"]["name"]: s for s in services["items"]}
        self.pods = sorted(pods["items"], key=pod_sort)
        self.fields = config_fields(cm)
        try:
            self.forwards = process_forwards(self.context, self.namespace)
        except (OSError, subprocess.TimeoutExpired, RuntimeError) as error:
            self.forwards = []
            self.warnings.append(str(error) + "; local endpoints are unverified")
        return self

    def endpoint(self, label, resource, remote, local, scheme="http", aliases=()):
        matches = [f for f in self.forwards if f["resource"] in (resource, *aliases)
                   and f["remote_port"] == remote and listening(f["local_port"])]
        matches.sort(key=lambda f: (f["local_port"] != local, f["local_port"]))
        port = matches[0]["local_port"] if matches else local
        state = "forwarded" if matches else "occupied-unverified" if listening(local) else "not-forwarded"
        kind, name = resource.split("/", 1)
        service = self.services.get(name, {}) if kind == "svc" else {}
        if kind == "svc" and (not service or not any(p["port"] == remote for p in service.get("spec", {}).get("ports", []))):
            state, matches = "resource-missing", []
        return {"name": label, "resource": resource, "remote_port": remote,
                "local_port": port, "suggested_port": local, "state": state,
                "url": scheme + "://127.0.0.1:" + str(port) if scheme else "127.0.0.1:" + str(port),
                "service_type": service.get("spec", {}).get("type"),
                "cluster_ip": service.get("spec", {}).get("clusterIP"),
                "external_ips": service.get("spec", {}).get("externalIPs", []),
                "load_balancer_ingress": service.get("status", {}).get("loadBalancer", {}).get("ingress", []),
                "service_ports": [{k: p[k] for k in ("port", "nodePort", "protocol") if k in p}
                                  for p in service.get("spec", {}).get("ports", [])],
                "forward_command": shlex.join(["kubectl", "--context", self.context, "-n", self.namespace,
                                                 "port-forward", "--address", "127.0.0.1", resource,
                                                 str(local) + ":" + str(remote)]),
                "active_forwards": matches}

    def report(self, mail_capture=False):
        endpoints = [self.endpoint(name, "svc/" + service, remote, port_value(env, default))
                     for name, service, remote, env, default in SERVICE_PORTS]
        etcd_port = port_value("ETCD_PORT", 32379)
        etcd = self.endpoint("etcd", "svc/full-stack-etcd", 2379, etcd_port, scheme="")
        endpoints.append(etcd)
        if mail_capture or "full-stack-mailpit" in self.services:
            endpoints.append(self.endpoint("Mailpit", "svc/full-stack-mailpit", 8025, port_value("MAILPIT_PORT", 8025)))
        nodes, mappings = [], {}
        base = port_value("WP_ADMIN_PORT_BASE", 19091)
        for index, pod in enumerate(self.pods):
            name = pod["metadata"]["name"]
            ordinal = re.search(r"-(\d+)$", name)
            port = base + (int(ordinal[1]) if ordinal else index)
            if port > 65535:
                raise ValueError("WP_ADMIN_PORT_BASE plus Pod ordinal exceeds 65535")
            env = pod_environment(pod)
            admin_port = next((p["containerPort"] for c in pod["spec"]["containers"] for p in c.get("ports", [])
                               if p.get("name") in ("admin", "metrics")), 9091)
            endpoint = self.endpoint("WP Admin " + name, "pod/" + name, admin_port, port, aliases=("pod/" + name,))
            endpoints.append(endpoint)
            identity = env.get("NODE_NAME", name)
            advertised = env.get("ADVERTISE_SERVICE_ADDR", "")
            aliases = {identity, name}
            if advertised and "$(" not in advertised:
                aliases.add(advertised)
            for key in aliases:
                mappings[key] = endpoint["url"]
            nodes.append({"pod": name, "node_id": identity, "advertised_service_addr": advertised,
                          "admin": endpoint, "identity_source": "Pod environment"})
        tenant, etcd_is_local = self.tenant(etcd)
        self.refine_mappings(nodes, mappings)
        configured = bool(nodes)
        connection = {"endpoint": nodes[0]["admin"]["url"] if nodes else None,
                      "node_admin_urls": dict(sorted(mappings.items())),
                      "k8s": {"namespace": self.namespace, "kube_context": self.context}}
        ready = configured and all(n["admin"]["state"] == "forwarded" for n in nodes)
        config = {"current-context": self.context_name, "contexts": {self.context_name: connection}} if configured else None
        metadata_configured = bool(tenant["meta_prefix"] and tenant["etcd_endpoints"])
        tenant["purpose"] = "Milvus instance WP client metadata; attach explicitly inside wp shell, not a cli.yaml setting"
        tenant["ready"] = bool(metadata_configured and etcd_is_local and etcd["state"] == "forwarded"
                               and not tenant["authentication_configured"] and not tenant["tls_configured"])
        connect = None
        if metadata_configured:
            args = ["connect", self.namespace, "--etcd", ",".join(tenant["etcd_endpoints"]), "--meta-prefix", tenant["meta_prefix"]]
            if tenant["bucket"] and tenant["root_path"]:
                args += ["--bucket", tenant["bucket"], "--root-path", tenant["root_path"]]
            connect = shlex.join(args)
        tenant["connect_command"] = connect
        if not nodes:
            self.warnings.append("No Woodpecker Pods found for this stack")
        if not metadata_configured:
            self.warnings.append("Milvus instance metadata settings are incomplete; no metadata connect command was generated")
        if tenant["authentication_configured"] or tenant["tls_configured"]:
            self.warnings.append("Etcd authentication/TLS is configured; supply your host-side credentials/certificates separately (not exported)")
        if not etcd_is_local:
            self.warnings.append("Etcd endpoints are external/custom; host reachability has not been checked")
        return {"schema_version": 1, "context": self.context, "namespace": self.namespace,
                "mail_capture_requested": mail_capture,
                "configuration_source": {"kind": "ConfigMap", "name": "milvus", "files": ["default.yaml", "user.yaml"],
                                         "scope": "deployed configuration; runtime overrides are not inspected"},
                "endpoints": endpoints, "woodpecker_nodes": nodes, "tenant": tenant,
                "wpcli": {"purpose": "WP service cluster context (cli.yaml)", "ready": bool(ready), "config": config,
                           "shell_command": shlex.join(["wp", "shell", "--context", self.context_name])},
                "warnings": self.warnings}

    def tenant(self, etcd):
        fields = self.fields
        endpoints = fields.get("etcd.endpoints") or []
        if isinstance(endpoints, str):
            endpoints = [value.strip() for value in endpoints.split(",")]
        for endpoint in endpoints:
            parsed = urllib.parse.urlsplit(str(endpoint) if "://" in str(endpoint) else "//" + str(endpoint))
            if parsed.username or parsed.password or parsed.query or parsed.fragment:
                raise RuntimeError("Etcd endpoints contain credentials or query parameters; configure host-side authentication separately")
        local_hosts = {"full-stack-etcd" + suffix for suffix in ("", "." + self.namespace,
                       "." + self.namespace + ".svc", "." + self.namespace + ".svc.cluster.local")}
        parsed = urllib.parse.urlsplit("//" + str(endpoints[0]).split("://")[-1]) if len(endpoints) == 1 else None
        local = bool(parsed and parsed.hostname in local_hosts and (parsed.port or 2379) == etcd["remote_port"])
        root, prefix = fields.get("etcd.rootpath"), fields.get("woodpecker.meta.prefix")
        meta_prefix = posixpath.normpath(str(root).rstrip("/") + "/" + str(prefix).lstrip("/")) if root is not None and prefix else None
        storage_root = fields.get("minio.rootpath")
        return {"name": self.namespace, "etcd_cluster_endpoints": endpoints,
                "etcd_endpoints": [etcd["url"]] if local else endpoints,
                "meta_prefix": meta_prefix, "bucket": fields.get("minio.bucketname"),
                "milvus_storage_root": storage_root,
                "root_path": str(storage_root).rstrip("/") + "/wp" if storage_root is not None else None,
                "storage_root_derivation": "Milvus WAL adapter: minio.rootPath + /wp",
                "authentication_configured": str(fields.get("etcd.auth.enabled", False)).lower() == "true",
                "tls_configured": str(fields.get("etcd.ssl.enabled", False)).lower() == "true" or any(str(e).startswith("https://") for e in endpoints)}, local

    def refine_mappings(self, nodes, mappings):
        seed = next((n for n in nodes if n["admin"]["state"] == "forwarded"), None)
        if seed is None:
            return
        request = urllib.request.Request(seed["admin"]["url"] + "/admin/memberlist", headers={"Accept": "application/json"})
        try:
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(request, timeout=1) as response:
                members = json.load(response)["members"]
            if not isinstance(members, list) or not all(isinstance(member, dict) for member in members):
                raise ValueError("unexpected memberlist shape")
            for node in nodes:
                member = next((m for m in members if m.get("id") == node["node_id"] or
                               m.get("service_addr") and m["service_addr"] == node["advertised_service_addr"]), None)
                if member:
                    node["identity_source"] = "live Admin memberlist"
                    for key in (member.get("id"), member.get("service_addr"), member.get("gossip_addr")):
                        if key:
                            mappings[key] = node["admin"]["url"]
        except (OSError, ValueError, KeyError, TypeError):
            self.warnings.append("Live Admin memberlist unavailable; node mappings are derived from Pod environment")


def print_report(report):
    print("Stack: " + report["context"] + " / " + report["namespace"])
    print("Source: deployed ConfigMap milvus (runtime overrides not inspected)")
    print("\nSERVICE                         LOCAL ADDRESS                    K8S PORT   STATE")
    for endpoint in report["endpoints"]:
        print(f'{endpoint["name"]:31} {endpoint["url"]:32} {endpoint["remote_port"]:<10} {endpoint["state"]}')
    print("\nOnly 'forwarded' addresses have a matching local kubectl process and a listening socket.")
    print("Other addresses are proposed ports, not verified connections.")
    print("\n1. WP service cluster context (cli.yaml)")
    print("Connects wpcli to WP service nodes through their Admin endpoints.")
    if report["wpcli"]["config"]:
        print("Local Admin forwards: " + ("ready" if report["wpcli"]["ready"] else "not ready"))
        print(cli_yaml(report["wpcli"]["config"]))
        print("\nSave a ready configuration without changing your personal cli.yaml:")
        print("  ./stack.sh connections --wpcli-config > .local/wpcli.yaml")
        print("  WOODPECKER_CLI_CONFIG=.local/wpcli.yaml " + report["wpcli"]["shell_command"])
    else:
        print("Unavailable: no WP service nodes discovered.")
    print("\n" + "\n".join(metadata_lines(report["tenant"])))
    for warning in report["warnings"]:
        print("Note: " + warning)
    if not report["wpcli"]["ready"] or not report["tenant"]["ready"]:
        print("\nOpen local forwards if needed: ./stack.sh forward --wpcli")


def metadata_lines(tenant):
    lines = ["2. Milvus instance WP client metadata connection (outside cli.yaml)",
             "Connects to metadata managed by this Milvus instance's WP client.",
             "This is separate from the WP service cluster context above.",
             "Local etcd forward: " + ("ready" if tenant["ready"] else "not verified; check endpoints/authentication/TLS")]
    for label, value in (("Milvus instance / tenant", tenant["name"]),
                         ("Etcd (inside Kubernetes)", tenant["etcd_cluster_endpoints"]),
                         ("Etcd (host)", tenant["etcd_endpoints"]), ("Complete metadata prefix", tenant["meta_prefix"]),
                         ("Bucket", tenant["bucket"]), ("Milvus storage root", tenant["milvus_storage_root"]),
                         ("Woodpecker root path", tenant["root_path"])):
        lines.append(f"  {label:26} " + (json.dumps(value, ensure_ascii=False) if value is not None else "(unknown)"))
    if tenant["connect_command"]:
        lines += ["Inside wp shell, attach this Milvus instance explicitly:", "  " + tenant["connect_command"],
                  "Then run status --check to validate the Admin and etcd connections."]
    else:
        lines.append("Metadata settings are incomplete; no connect command is available.")
    return lines


def cli_yaml(config):
    import yaml
    return yaml.safe_dump(config, sort_keys=False, allow_unicode=True).rstrip()


def wpcli_export(report):
    # Keep the entire output safe to redirect to cli.yaml: metadata instructions
    # are comments, never context settings or a second YAML document.
    notes = metadata_lines(report["tenant"]) + ["Note: " + warning for warning in report["warnings"]]
    return ("# 1. WP service cluster context (cli.yaml)\n"
            "# Connects wpcli to WP service nodes through their Admin endpoints.\n"
            + cli_yaml(report["wpcli"]["config"]) + "\n\n"
            + "\n".join("# " + line for note in notes for line in note.splitlines()))


def forward(report):
    # Preflight all ports before starting anything. Reuse only a verified forward
    # to this exact context/namespace/resource/remote port; never kill external owners.
    if not report["woodpecker_nodes"]:
        raise RuntimeError("No Woodpecker Pods found; deploy the stack before opening wpcli forwards")
    endpoints = [e for e in report["endpoints"]
                 if (e["name"] != "etcd" or report["tenant"]["etcd_endpoints"] == [e["url"]])
                 and (e["name"] != "Mailpit" or report["mail_capture_requested"])]
    ports = [e["suggested_port"] for e in endpoints]
    if len(ports) != len(set(ports)):
        raise RuntimeError("Local forward ports overlap; change the *_PORT or WP_ADMIN_PORT_BASE settings")
    start = []
    for endpoint in endpoints:
        if endpoint["state"] == "resource-missing":
            raise RuntimeError("Missing Kubernetes target " + endpoint["resource"] + ":" + str(endpoint["remote_port"]))
        exact = any(f["local_port"] == endpoint["suggested_port"] for f in endpoint["active_forwards"])
        if exact:
            print("Reusing " + endpoint["name"] + " at " + str(endpoint["suggested_port"]), flush=True)
        elif listening(endpoint["suggested_port"]):
            raise RuntimeError("Port " + str(endpoint["suggested_port"]) + " is occupied without a matching stack forward; existing processes were not touched")
        else:
            start.append(endpoint)
    children = []
    stopping = {"signal": None}
    previous = {sig: signal.signal(sig, lambda value, _: stopping.update(signal=value)) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        for endpoint in start:
            children.append(subprocess.Popen(shlex.split(endpoint["forward_command"])))
        print("Forwarding this stack, including per-node WP Admin and tenant etcd. Ctrl+C closes only newly started forwards.", flush=True)
        deadline = time.monotonic() + 30
        while not stopping["signal"]:
            if any(child.poll() is not None for child in children):
                raise RuntimeError("A port-forward exited; inspect the kubectl output above")
            if all(listening(port) for port in ports):
                print("All requested endpoints are listening. Run ./stack.sh connections in another terminal.", flush=True)
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("Port-forward readiness timed out")
            time.sleep(.2)
        while not stopping["signal"]:
            if any(child.poll() is not None for child in children):
                raise RuntimeError("A port-forward exited; rerun forward after inspecting Pod readiness")
            time.sleep(.5)
        return 128 + stopping["signal"]
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
        for child in children:
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=3)
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--context-name", default=os.getenv("WP_CONTEXT_NAME"))
    parser.add_argument("--format", choices=("text", "json", "wpcli"), default="text")
    parser.add_argument("--forward", action="store_true")
    parser.add_argument("--mail-capture", action="store_true")
    args = parser.parse_args()
    try:
        report = Connections(args.context, args.namespace, args.context_name).discover().report(args.mail_capture)
        if args.forward:
            return forward(report)
        if args.format == "json":
            print(json.dumps(report, indent=2))
        elif args.format == "wpcli":
            if not report["wpcli"]["ready"]:
                raise RuntimeError("wpcli connections are not ready. Run ./stack.sh connections for details and ./stack.sh forward --wpcli to open the missing forwards")
            print(wpcli_export(report))
        else:
            print_report(report)
        return 0
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        print("connections: " + str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
