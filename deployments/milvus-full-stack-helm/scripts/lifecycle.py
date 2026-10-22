#!/usr/bin/env python3
"""Plan native kubectl pause/resume commands for this stack's Helm-owned workloads."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import shlex
import subprocess
import uuid

import yaml

STATE_NAME = "milvus-full-stack-lifecycle"
PAUSE_KEY = "woodpecker.io/stack-paused"
KINDS = {"Deployment": "deployment", "StatefulSet": "statefulset", "DaemonSet": "daemonset",
         "Prometheus": "prometheuses.monitoring.coreos.com", "Alertmanager": "alertmanagers.monitoring.coreos.com"}
RELEASES = {"milvus", "full-stack", "monitoring"}


def read(argv, json_output=True):
    result = subprocess.run(argv, capture_output=True, text=True, timeout=60)
    if result.returncode:
        raise RuntimeError(shlex.join(argv[:6]) + ": " + result.stderr.strip())
    return json.loads(result.stdout) if json_output else result.stdout


def command(argv, execute):
    print(shlex.join(argv), flush=True)
    if execute:
        subprocess.run(argv, check=True)


def inventory(context, namespace):
    k = ["kubectl", "--context", context, "-n", namespace]
    h = ["helm", "--kube-context", context, "-n", namespace]
    releases = {r["name"] for r in read(h + ["list", "--all", "-o", "json"]) if r["name"] in RELEASES}
    if "milvus" not in releases:
        raise ValueError("No Milvus release here; review ./stack.sh deploy first")
    def manifest(name):
        return name, list(yaml.safe_load_all(read(h + ["get", "manifest", name], False)))
    with ThreadPoolExecutor(max_workers=3) as pool:
        manifests = dict(pool.map(manifest, sorted(releases)))
    expected = {(d["kind"], d["metadata"]["name"]): release for release, docs in manifests.items()
                for d in docs if d and d.get("kind") in KINDS and d["metadata"].get("namespace", namespace) == namespace}
    kinds = ["deployments", "statefulsets", "daemonsets"] + [KINDS[kind] for kind in ("Prometheus", "Alertmanager") if any(k == kind for k, _ in expected)]
    all_items = read(k + ["get", ",".join(kinds), "-o", "json"])["items"]
    items = []
    for item in all_items:
        identity = item["kind"], item["metadata"]["name"]
        if identity not in expected:
            continue
        annotations = item["metadata"].get("annotations", {})
        if annotations.get("meta.helm.sh/release-name") != expected[identity] or annotations.get("meta.helm.sh/release-namespace") != namespace:
            raise ValueError("Helm ownership does not match for " + "/".join(identity))
        items.append(item)
    cr_uids = {i["metadata"]["uid"] for i in items if i["kind"] in ("Prometheus", "Alertmanager")}
    children = [i for i in all_items if i["kind"] == "StatefulSet" and any(o["uid"] in cr_uids for o in i["metadata"].get("ownerReferences", []))]
    return k, items, children


def priority(item):
    name = item["metadata"]["name"]
    if "operator" in name: return 0
    if name in ("full-stack-etcd", "full-stack-minio", "full-stack-jaeger", "full-stack-loki"): return 1
    if "woodpecker" in name: return 2
    if "mixcoord" in name: return 3
    if any(s in name for s in ("streamingnode", "datanode", "querynode")): return 4
    if "proxy" in name: return 5
    return 6


def saved_state(k):
    result = read(k + ["get", "configmap", STATE_NAME, "--ignore-not-found", "-o", "json"], False)
    return json.loads(json.loads(result)["data"]["state.json"]) if result.strip() else None


def snapshot(items, children, namespace, pause_token=None):
    for i in items + children:
        spec = i["spec"]
        if i["kind"] == "StatefulSet" and spec.get("persistentVolumeClaimRetentionPolicy", {}).get("whenScaled") == "Delete":
            raise ValueError(i["metadata"]["name"] + ": whenScaled=Delete would delete PVCs; configure Retain in its owning chart/controller first")
        if i["kind"] in ("Prometheus", "Alertmanager") and spec.get("paused"):
            raise ValueError(i["metadata"]["name"] + ": its Operator reconciliation is paused; resolve that before stack stop")
        if i["kind"] == "DaemonSet" and PAUSE_KEY in spec["template"]["spec"].get("nodeSelector", {}) and spec["template"]["spec"]["nodeSelector"][PAUSE_KEY] != pause_token:
            raise ValueError("Reserved pause selector already exists on " + i["metadata"]["name"])
    return {"version": 1, "namespace": namespace, "pause_token": uuid.uuid4().hex,
            "workloads": [{"kind": i["kind"], "name": i["metadata"]["name"], "uid": i["metadata"]["uid"],
                           "replicas": i["spec"].get("replicas", 1)} for i in items]}


def plan(action, context, namespace, output):
    k, items, children = inventory(context, namespace)
    saved = saved_state(k)
    if saved and (saved.get("version") != 1 or saved.get("namespace") != namespace):
        raise ValueError("Unexpected lifecycle state; inspect ConfigMap " + STATE_NAME)
    if action == "start" and not saved:
        print("# No saved stop state: no replica counts changed. Use deploy to install/upgrade.")
        return []
    # Revalidate PVC retention even on a repeated stop.
    fresh = snapshot(items, children, namespace, saved.get("pause_token") if saved else None) if action == "stop" else None
    state = saved or fresh
    actual = {(i["kind"], i["metadata"]["name"]): i for i in items}
    for w in state["workloads"]:
        live = actual.get((w["kind"], w["name"]))
        if not live or live["metadata"]["uid"] != w["uid"]:
            raise ValueError("Workload identity changed since stop: " + w["name"] + "; inspect saved state before resuming")
    if len(actual) != len(state["workloads"]):
        raise ValueError("Workload set changed while paused; inspect saved state before changing replicas")
    hpas = read(k + ["get", "hpa", "-o", "json"])["items"]
    if any((h["spec"]["scaleTargetRef"]["kind"], h["spec"]["scaleTargetRef"]["name"]) in actual for h in hpas):
        raise ValueError("A stack workload has an HPA; suspend autoscaling through its owning configuration before pause/resume")
    commands = []
    if action == "stop" and not saved:
        output.mkdir(parents=True, exist_ok=True)
        path = output / "lifecycle-state.yaml"
        path.write_text(yaml.safe_dump({"apiVersion": "v1", "kind": "ConfigMap", "immutable": True,
            "metadata": {"name": STATE_NAME, "namespace": namespace, "labels": {"app.kubernetes.io/part-of": "milvus-full-stack"}},
            "data": {"state.json": json.dumps(state)}}, sort_keys=False))
        commands.append(k + ["create", "-f", str(path)])
    def waits(item):
        # kubectl wait --for=delete also succeeds with no matching resources.
        # Discover Pods at execution time, rather than baking stale Pod names into a preview.
        selector = item["spec"]["selector"]
        if selector.get("matchExpressions"):
            raise ValueError("Pause requires a matchLabels-only Pod selector: " + item["metadata"]["name"])
        labels = selector.get("matchLabels", {})
        if not labels: raise ValueError("Missing workload selector")
        return [k + ["wait", "--for=delete", "pod", "--selector=" + ",".join(a + "=" + b for a, b in sorted(labels.items())), "--timeout=" + TIMEOUT]]
    replicas = {(w["kind"], w["name"]): w["replicas"] for w in state["workloads"]}
    normal = sorted([i for i in items if i["kind"] in ("Deployment", "StatefulSet")], key=lambda i: (priority(i), i["metadata"]["name"]))
    operators = [i for i in normal if priority(i) == 0]
    normal = [i for i in normal if priority(i) != 0]
    crs = [i for i in items if i["kind"] in ("Prometheus", "Alertmanager")]
    daemonsets = [i for i in items if i["kind"] == "DaemonSet"]
    stop_normal = list(reversed([i for i in normal if priority(i) < 6])) + [i for i in normal if priority(i) == 6]
    order = [*stop_normal, *daemonsets, *crs, *operators] if action == "stop" else [*operators, *normal, *crs, *daemonsets]
    pending_rollouts = []
    previous_group = None
    for item in order:
        # Start interdependent Milvus roles together before waiting for readiness.
        # Dependencies/WP and the Operator still form earlier readiness barriers.
        group = (3 if 3 <= priority(item) <= 5 else priority(item))
        if item["kind"] in ("Prometheus", "Alertmanager"): group = 7
        if item["kind"] == "DaemonSet": group = 8
        if action == "start" and previous_group != group:
            commands.extend(pending_rollouts)
            pending_rollouts = []
        previous_group = group
        kind, name = item["kind"], item["metadata"]["name"]
        target = KINDS[kind] + "/" + name
        count = 0 if action == "stop" else replicas[kind, name]
        if kind == "DaemonSet":
            patch = {"spec": {"template": {"spec": {"nodeSelector": {PAUSE_KEY: state["pause_token"] if action == "stop" else None}}}}}
            commands.append(k + ["patch", target, "--type=merge", "-p", json.dumps(patch, separators=(",", ":"))])
        elif kind in ("Prometheus", "Alertmanager"):
            commands.append(k + ["patch", target, "--type=merge", "-p", json.dumps({"spec": {"replicas": count}})])
        else:
            commands.append(k + ["scale", target, "--replicas=" + str(count)])
        if action == "stop":
            if kind in ("Prometheus", "Alertmanager"):
                for child in children:
                    if any(o["uid"] == item["metadata"]["uid"] for o in child["metadata"].get("ownerReferences", [])):
                        commands.extend(waits(child))
            else: commands.extend(waits(item))
        elif count > 0:
            targets = ["statefulset/" + c["metadata"]["name"] for c in children
                       if any(o["uid"] == item["metadata"]["uid"] for o in c["metadata"].get("ownerReferences", []))] if kind in ("Prometheus", "Alertmanager") else [target]
            for t in targets:
                if kind in ("Prometheus", "Alertmanager"):
                    pending_rollouts.append(k + ["wait", "--for=jsonpath={.spec.replicas}=" + str(count), t, "--timeout=" + TIMEOUT])
                pending_rollouts.append(k + ["rollout", "status", t, "--timeout=" + TIMEOUT])
    commands.extend(pending_rollouts)
    if action == "start": commands.append(k + ["delete", "configmap", STATE_NAME])
    return commands


TIMEOUT = "12m"


def main():
    global TIMEOUT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop"))
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", default="12m")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    TIMEOUT = args.timeout
    commands = plan(args.action, args.context, args.namespace, args.output)
    # Finish discovery/validation before the first write, including in execute mode.
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
    for argv in commands: command(argv, args.execute)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, RuntimeError, subprocess.SubprocessError) as error:
        raise SystemExit(str(error))
