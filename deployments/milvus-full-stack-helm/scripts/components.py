#!/usr/bin/env python3
"""Plan optional components without replacing live core overrides on addon changes."""
import argparse
import copy
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import subprocess

import yaml

ROOT = Path(__file__).resolve().parents[1]
NAMES = ("grafana", "prometheus", "alertmanager", "jaeger", "loki")
WORKLOADS = {"grafana": "monitoring-grafana", "prometheus": "prometheus-monitoring-kube-prometheus-prometheus",
             "alertmanager": "alertmanager-monitoring-kube-prometheus-alertmanager", "jaeger": "full-stack-jaeger", "loki": "full-stack-loki"}


def selection(value):
    names = {v.strip() for v in value.split(",") if v.strip()}
    unknown = names - set(NAMES) - {"all"}
    if unknown:
        raise ValueError("Unknown components: " + ", ".join(sorted(unknown)))
    return set(NAMES) if "all" in names else names


def merge(left, right):
    result = copy.deepcopy(left)
    for key, value in right.items():
        result[key] = merge(result[key], value) if isinstance(value, dict) and isinstance(result.get(key), dict) else copy.deepcopy(value)
    return result


def load_values(paths, initial=None):
    result = copy.deepcopy(initial or {})
    for path in paths:
        result = merge(result, yaml.safe_load(Path(path).read_text()) or {})
    return result


def run(argv):
    completed = subprocess.run(argv, text=True, capture_output=True)
    if completed.returncode:
        raise RuntimeError(" ".join(argv[:5]) + ": " + completed.stderr.strip())
    return json.loads(completed.stdout)


def deployed(context, namespace):
    h = ["helm", "--kube-context", context, "-n", namespace]
    releases = {r["name"] for r in run(h + ["list", "--all", "-o", "json"])}
    def values(name):
        return name, (run(h + ["get", "values", name, "-o", "json"]) or {})
    with ThreadPoolExecutor(max_workers=3) as pool:
        live = dict(pool.map(values, sorted(releases & {"milvus", "monitoring", "full-stack"})))
    if "components" in live.get("full-stack", {}):
        selected = {n for n, enabled in live["full-stack"]["components"].items() if n in NAMES and enabled}
    else:
        # Migration from the earlier always-on stack; inspect actual workload names.
        workloads = run(["kubectl", "--context", context, "-n", namespace, "get", "deployments,statefulsets", "-o", "json"])
        names = {r["metadata"]["name"] for r in workloads["items"]}
        selected = {key for key, name in WORKLOADS.items() if name in names}
    return live, selected


def build(selected, support, monitoring, milvus, namespace):
    support = copy.deepcopy(support)
    support["components"] = {name: name in selected for name in NAMES}
    support.setdefault("jaeger", {})["enabled"] = "jaeger" in selected
    support.setdefault("loki", {})["enabled"] = "loki" in selected
    mon = copy.deepcopy(monitoring)
    for name in ("grafana", "prometheus", "alertmanager"):
        mon.setdefault(name, {})["enabled"] = name in selected
    mon.setdefault("prometheusOperator", {})["enabled"] = bool(selected & {"prometheus", "alertmanager"})
    mon.setdefault("crds", {})["enabled"] = bool(selected & {"prometheus", "alertmanager"})
    mon.setdefault("kubeStateMetrics", {})["enabled"] = "prometheus" in selected
    mon["grafana"].setdefault("serviceMonitor", {})["enabled"] = "prometheus" in selected
    for name in ("alertmanager", "prometheusOperator"):
        mon[name].setdefault("serviceMonitor", {})["selfMonitor"] = "prometheus" in selected
    mon.setdefault("kubelet", {})["enabled"] = "prometheus" in selected
    datasources = mon["grafana"].setdefault("sidecar", {}).setdefault("datasources", {})
    datasources["defaultDatasourceEnabled"] = "prometheus" in selected
    datasources.setdefault("alertmanager", {})["enabled"] = "alertmanager" in selected
    extras = []
    if "jaeger" in selected:
        extras.append({"name": "Jaeger", "uid": "jaeger", "type": "jaeger", "access": "proxy",
                       "url": "http://full-stack-jaeger:16686", "editable": False, "jsonData": {"nodeGraph": {"enabled": True}}})
    if "loki" in selected:
        data = {"maxLines": 1000}
        if "jaeger" in selected:
            data["derivedFields"] = [{"name": "TraceID", "matcherRegex": r"\[traceID=([a-fA-F0-9]{32})\]",
                                      "datasourceUid": "jaeger", "url": "${__value.raw}", "urlDisplayLabel": "View trace in Jaeger"}]
        extras.append({"name": "Loki", "uid": "loki", "type": "loki", "access": "proxy",
                       "url": "http://full-stack-loki:3100", "editable": False, "jsonData": data})
    # Preserve unrelated user datasources; remove only the ones managed by this example.
    mon["grafana"]["additionalDataSources"] = [d for d in mon["grafana"].get("additionalDataSources", [])
                                               if d.get("uid") not in ("jaeger", "loki")] + extras
    mv = copy.deepcopy(milvus)
    user = yaml.safe_load(mv.setdefault("extraConfigFiles", {}).get("user.yaml", "")) or {}
    old_trace = user.get("trace", {})
    trace = copy.deepcopy(old_trace)
    if "jaeger" in selected:
        fraction = 1.0 if old_trace.get("exporter", "noop") == "noop" else trace.get("sampleFraction", 1.0)
        trace.update(exporter="otlp", sampleFraction=fraction,
                     otlp={"endpoint": "full-stack-jaeger:4317", "method": "grpc", "secure": False})
    else:
        trace.update(exporter="noop", sampleFraction=0)
    user["trace"] = trace
    mv["extraConfigFiles"]["user.yaml"] = yaml.safe_dump(user, sort_keys=False)
    return support, mon, mv, trace != old_trace


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("validate", "render", "install", "addons-install", "addons-uninstall", "status", "flags"))
    parser.add_argument("--with", dest="components", default="")
    parser.add_argument("--context", default="")
    parser.add_argument("--namespace", default="milvus-stack")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--milvus-values", action="append", default=[])
    parser.add_argument("--monitoring-values", action="append", default=[])
    parser.add_argument("--support-values", action="append", default=[])
    args = parser.parse_args()
    requested = selection(args.components)
    if args.mode == "validate":
        return 0
    live, previous = ({}, set()) if args.mode == "render" else deployed(args.context, args.namespace)
    if args.mode in ("status", "flags"):
        if args.mode == "flags":
            for name in NAMES:
                print("HAS_" + name.upper() + "=" + str(name in previous).lower())
        else:
            print("Optional components (selected in this context/namespace):")
            for name in NAMES:
                print(f"  {name:14} " + ("enabled" if name in previous else "not installed"))
        return 0
    if args.mode.startswith("addons-") and (not requested or "milvus" not in live):
        raise ValueError("Addon changes require an existing Milvus release and --with <components>")
    selected = previous - requested if args.mode == "addons-uninstall" else previous | requested
    support = merge(load_values([ROOT / "charts/full-stack-support/values.yaml"]), live.get("full-stack", {}))
    support = load_values(args.support_values, support)
    monitoring = merge(load_values([ROOT / "values/monitoring.yaml"]), live.get("monitoring", {}))
    monitoring = load_values(args.monitoring_values, monitoring)
    milvus = (live["milvus"] if args.mode.startswith("addons-") else
              load_values([ROOT / "values/milvus.yaml", *args.milvus_values]))
    support, monitoring, milvus, trace_changed = build(selected, support, monitoring, milvus, args.namespace)
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    for name, values in (("support", support), ("monitoring", monitoring), ("milvus", milvus)):
        (output / (name + ".yaml")).write_text(yaml.safe_dump(values, sort_keys=False))
    flags = {"NEED_MONITORING": bool(selected & {"grafana", "prometheus", "alertmanager"}),
             "HAD_MONITORING": "monitoring" in live, "NEED_MILVUS": args.mode in ("render", "install") or trace_changed,
             "TRACE_CHANGED": trace_changed}
    flags.update({"HAS_" + n.upper(): n in selected for n in NAMES})
    flags.update({"REMOVE_" + n.upper(): n in previous - selected for n in NAMES})
    (output / "flags.env").write_text("".join(k + "=" + str(v).lower() + "\n" for k, v in flags.items()))
    (output / "plan.json").write_text(json.dumps({"context": args.context, "namespace": args.namespace,
                "previous": sorted(previous), "selected": sorted(selected), "trace_changed": trace_changed}, indent=2) + "\n")
    print("# Core: Milvus + WP + etcd/MinIO; addons: " + (", ".join(sorted(selected)) or "none"))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, RuntimeError) as error:
        raise SystemExit(str(error))
