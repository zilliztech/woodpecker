#!/usr/bin/env python3
"""Resolve an existing kubeconfig context and explain configuration failures."""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys


def invoke(argv, timeout=15):
    return subprocess.run(argv, capture_output=True, text=True, timeout=timeout)


def config_paths():
    value = os.getenv("KUBECONFIG")
    return [Path(p).expanduser() for p in value.split(os.pathsep) if p] if value else [Path.home() / ".kube/config"]


def file_status(path):
    try:
        return "found" if path.is_file() else "missing"
    except OSError:
        return "unreadable"


def explain(message, contexts, paths, action, namespace, selected=None, reason=None):
    print("Kubernetes configuration: " + message, file=sys.stderr)
    print("Kubeconfig files (contents/credentials are not displayed):", file=sys.stderr)
    for path in paths:
        print("  " + str(path) + " (" + file_status(path) + ")", file=sys.stderr)
    print("Stack namespace: " + namespace, file=sys.stderr)
    if contexts:
        print("Available contexts:", file=sys.stderr)
        for entry in contexts:
            data = entry.get("context", {})
            print("  " + entry["name"] + "  cluster=" + data.get("cluster", "?") + "  namespace=" + data.get("namespace", "default"), file=sys.stderr)
        print("Use one for this invocation (does not switch your current context):", file=sys.stderr)
        for entry in contexts:
            print("  " + shlex.join(["./stack.sh", "status", "--context", entry["name"], "--namespace", namespace]), file=sys.stderr)
        print("Or choose a default yourself: kubectl config use-context CONTEXT_NAME", file=sys.stderr)
    elif contexts is None:
        print("The context list could not be read. Fix kubeconfig paths/permissions or YAML syntax, then inspect:", file=sys.stderr)
        print("  kubectl config get-contexts", file=sys.stderr)
    else:
        print("No contexts are available. Obtain a kubeconfig from your cluster administrator/provider, then run:", file=sys.stderr)
        print("  export KUBECONFIG=/absolute/path/to/your/kubeconfig", file=sys.stderr)
        print("  kubectl config get-contexts", file=sys.stderr)
        print("  kubectl config use-context CONTEXT_NAME", file=sys.stderr)
        print("  " + shlex.join(["./stack.sh", "status", "--namespace", namespace]), file=sys.stderr)
    if selected:
        print("Selected context: " + selected, file=sys.stderr)
    if reason:
        print("Connection check: " + reason, file=sys.stderr)
        print("Inspect the kubectl error locally:", file=sys.stderr)
        print("  " + shlex.join(["kubectl", "--context", selected, "version", "--request-timeout=10s"]), file=sys.stderr)
        print("  " + shlex.join(["kubectl", "--context", selected, "get", "pods", "-n", namespace]), file=sys.stderr)
    if action != "status":
        print("After status succeeds, retry ./stack.sh " + action + " with your original options.", file=sys.stderr)
    print("No context was switched. No Kubernetes resources were changed. Manage the cluster outside stack.sh.", file=sys.stderr)


def connection_reason(error):
    error = error.lower()
    if any(s in error for s in ("unauthorized", "must be logged in", "credentials", "getting credentials", "executable", "exec plugin")):
        return "authentication failed or a credential plugin is unavailable; refresh your provider login/credentials"
    if "forbidden" in error:
        return "access was denied; check Kubernetes permissions for this identity"
    if any(s in error for s in ("x509", "certificate", "tls handshake")):
        return "TLS/certificate verification failed; check kubeconfig CA, credentials and API endpoint"
    if any(s in error for s in ("connection refused", "no such host", "timeout", "timed out", "network is unreachable", "no route to host")):
        return "the Kubernetes API is unreachable; check endpoint, network/VPN and whether your cluster is running"
    return "kubectl could not connect or authenticate; run the diagnostic command below for the full error"


def resolve(context, namespace, action):
    paths = config_paths()
    try:
        result = invoke(["kubectl", "config", "view", "-o", "json"])
        if result.returncode:
            raise ValueError("Cannot read kubeconfig. Check the file paths, permissions and YAML syntax.")
        config = json.loads(result.stdout)
    except (ValueError, OSError, subprocess.TimeoutExpired):
        explain("Cannot read kubeconfig; check the file paths, permissions and YAML syntax.", None, paths, action, namespace)
        return None
    contexts = config.get("contexts") or []
    selected = context or config.get("current-context", "")
    if not contexts:
        message = "No kubeconfig file was found." if all(file_status(p) == "missing" for p in paths) else "The loaded kubeconfig contains no contexts."
        explain(message, [], paths, action, namespace, selected or None)
        return None
    if not selected:
        explain("Contexts exist, but none is selected as current-context.", contexts, paths, action, namespace)
        return None
    if selected not in {c["name"] for c in contexts}:
        explain("The requested/current context is not in the loaded kubeconfig.", contexts, paths, action, namespace, selected)
        return None
    try:
        result = invoke(["kubectl", "--context", selected, "version", "--request-timeout=10s", "-o", "json"])
        if not result.returncode:
            return selected
        reason = connection_reason(result.stderr)
    except subprocess.TimeoutExpired:
        reason = connection_reason("timeout")
    except OSError:
        reason = "kubectl could not be executed; check its installation and PATH"
    explain("The selected context exists, but its Kubernetes API check failed.", contexts, paths, action, namespace, selected, reason)
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", default="")
    parser.add_argument("--namespace", default="milvus-stack")
    parser.add_argument("--action", default="status")
    args = parser.parse_args()
    context = resolve(args.context, args.namespace, args.action)
    if context is None:
        return 2
    print(context)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
