#!/usr/bin/env python3
"""Import the supplied Milvus export and sync the repository's WP templates.

Run explicitly when updating sources; deployment uses the checked-in JSON assets.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]
DEST = ROOT / "charts/full-stack-support/dashboards"


def normalize(value):
    if isinstance(value, list):
        return [normalize(item) for item in value]
    if not isinstance(value, dict):
        return value
    if value.get("type") == "logs" and "targets" in value:
        return {"id": value["id"], "gridPos": value["gridPos"], "type": "text",
                "title": "Kubernetes events",
                "options": {"mode": "markdown", "content":
                    "Kubernetes events are included in `./stack.sh collect` artifacts. "
                    "The production Loki event panel requires a separate logging stack."}}
    result = {}
    for key, item in value.items():
        if key == "datasource" and isinstance(item, dict):
            # Production exports contain a mix of variable and hardcoded UIDs.
            result[key] = item if item.get("type") in ("grafana", "__expr__") else {
                "type": "prometheus", "uid": "${datasource}"
            }
        elif key == "datasource" and isinstance(item, str):
            result[key] = "${datasource}"
        elif key == "links":
            # Replace private production navigation with local dashboard links below.
            result[key] = []
        else:
            result[key] = normalize(item)
    # Correct queries from the production export for standard Prometheus.
    for target in result.get("targets", []):
        expression = target.get("expr", "")
        expression = expression.replace("increase_prometheus(", "increase(")
        if result.get("title") == "Pod Restarts":
            expression = ('sum by (pod) (increase(kube_pod_container_status_restarts_total'
                          '{cluster="$cluster",namespace="$namespace",pod=~"$pod"}[$__rate_interval]))')
        if result.get("title") == "Pod IOPS":
            expression = re.sub(r"rate\((container_fs_(?:reads|writes)_total\{[^}]*\})\)",
                                r"rate(\1[$__rate_interval])", expression, flags=re.S)
        if "expr" in target:
            target["expr"] = expression
    return result


def open_row(dashboard, title):
    """Keep all sections, with one useful overview expanded by default."""
    groups = []
    for panel in dashboard["panels"]:
        if panel.get("type") == "row":
            groups.append((panel, list(panel.get("panels", []))))
        elif groups:
            groups[-1][1].append(panel)
        else:
            raise ValueError("Expected a row before dashboard panels")
    output, y = [], 0
    for row, children in groups:
        row["gridPos"] = {"x": 0, "y": y, "w": 24, "h": 1}
        expanded = row.get("title") == title
        row["collapsed"] = not expanded
        height = 0
        if children:
            original_y = min(child["gridPos"]["y"] for child in children)
            for child in children:
                child["gridPos"]["y"] += y + 1 - original_y
                height = max(height, child["gridPos"]["y"] + child["gridPos"]["h"] - y - 1)
        row["panels"] = [] if expanded else children
        output.append(row)
        if expanded:
            output.extend(children)
        y += 1 + (height if expanded else 0)
    dashboard["panels"] = output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--milvus-source", type=Path, required=True)
    args = parser.parse_args()
    raw = args.milvus_source.read_bytes()
    milvus = normalize(json.loads(raw))
    milvus.update(id=None, uid="milvus-full-stack", title="Milvus - Full stack",
                  version=1, refresh="15s", time={"from": "now-30m", "to": "now"})
    variables = milvus["templating"]["list"]
    # app_name must be available before the instance query that refers to it.
    app_name = next(v for v in variables if v["name"] == "app_name")
    variables.remove(app_name)
    variables.insert(0, app_name)
    for variable in variables:
        if variable["type"] == "datasource":
            variable["current"] = {"text": "Prometheus", "value": "prometheus"}
            variable.pop("queryValue", None)
        elif variable["type"] == "query":
            variable["current"] = {}
            variable["options"] = []
            if variable.get("includeAll"):
                variable["current"] = {"text": "All", "value": "$__all"}
                variable["allValue"] = ".*"
            scoped = {
                "cluster": 'label_values(go_info{app_kubernetes_io_name="$app_name"}, cluster)',
                "namespace": 'label_values(go_info{app_kubernetes_io_name="$app_name",cluster="$cluster"}, namespace)',
                "pod": 'label_values(go_info{app_kubernetes_io_name="$app_name",app_kubernetes_io_instance="$instance",namespace="$namespace",cluster="$cluster",component!="woodpecker"}, pod)',
            }.get(variable["name"])
            if scoped:
                variable["definition"] = scoped
                variable["query"] = {"query": scoped, "refId": "StandardVariableQuery"}
            if variable["name"] == "pod":
                variable["allValue"] = "$instance-(datanode|mixcoord|proxy|querynode|streamingnode).*"
    open_row(milvus, "Service Quality")
    dashboards = [("milvus.json", milvus)]
    for kind in ("client", "server"):
        path = REPO / f"tests/k8s/monitor/grafana/templates/dashboard_{kind}_k8s.json"
        dashboard = json.loads(path.read_text())
        dashboard["id"] = None
        dashboard["time"] = {"from": "now-30m", "to": "now"}
        dashboard["refresh"] = "15s"
        datasource = next(v for v in dashboard["templating"]["list"] if v["type"] == "datasource")
        datasource["current"] = {"text": "Prometheus", "value": "prometheus"}
        # Select namespaces from WP series, rather than unrelated Go processes.
        for variable in dashboard["templating"]["list"]:
            if variable["name"] == "namespace":
                variable["query"] = 'label_values(go_info{woodpecker_id!=""}, namespace)'
                variable["allValue"] = ".*"
            elif variable["name"] == "server_job":
                variable["query"] = 'label_values(go_info{namespace=~"$namespace",woodpecker_id=~"$woodpecker_id",component="woodpecker"}, job)'
        if kind == "server":
            open_row(dashboard, "Engine Layer — LogStore")
        dashboards.append((f"woodpecker-{kind}.json", dashboard))
    links = [{"title": title, "type": "link", "url": f"/d/{uid}",
              "keepTime": True, "targetBlank": False}
             for title, uid in [("Milvus", "milvus-full-stack"),
                                ("Woodpecker client", "woodpecker-client-k8s"),
                                ("Woodpecker server", "woodpecker-server-k8s")]]
    DEST.mkdir(parents=True, exist_ok=True)
    for name, dashboard in dashboards:
        dashboard["links"] = links
        (DEST / name).write_text(json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n")
    provenance = {
        "milvus": {"source": "User-supplied production export: milvus_grafana_dashboard.json",
                   "sha256": hashlib.sha256(raw).hexdigest(),
                   "original_uid": json.loads(raw).get("uid")},
        "woodpecker": "tests/k8s/monitor/grafana/templates/dashboard_{client,server}_k8s.json",
        "changes": ["Local datasource and dashboard navigation", "Clear production variable selections",
                    "Provide stable dashboard UIDs", "Select namespaces with Woodpecker metrics",
                    "Scope Milvus namespace/pod discovery to the deployed application",
                    "Correct restart, slow-query, and IOPS queries for standard Prometheus",
                    "Open service quality and server overview sections by default",
                    "Replace the production Loki events panel with local evidence collection guidance"]
    }
    (ROOT / "dashboards-sources.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print(f"Updated {len(dashboards)} dashboards in {DEST}")


if __name__ == "__main__":
    main()
