#!/usr/bin/env python3
"""Verify live scrape targets, core metrics, and provisioned dashboard queries."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
from pathlib import Path
import re
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
PROM = os.getenv("PROMETHEUS_URL", "http://127.0.0.1:" + os.getenv("PROMETHEUS_PORT", "9099"))
GRAFANA = os.getenv("GRAFANA_URL", "http://127.0.0.1:" + os.getenv("GRAFANA_PORT", "3000"))
NAMESPACE = os.getenv("NAMESPACE", "milvus-stack")


def get(url):
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        return {"status": "error", "error": error.read().decode()[:1000]}


def query(expression):
    return get(PROM + "/api/v1/query?" + urllib.parse.urlencode({"query": expression}))


def panels(items):
    for panel in items:
        yield panel
        yield from panels(panel.get("panels", []))


def expand(expression, defaults):
    now = int(time.time())
    variables = {"namespace": NAMESPACE, "cluster": "local", "instance": "milvus",
                 "app_name": "milvus", "collection": ".*",
                 "pod": "milvus-(datanode|mixcoord|proxy|querynode|streamingnode).*",
                 "woodpecker_id": "milvus", "log_ns": ".*", "log_id": ".*",
                 "node_id": ".*", "__rate_interval": "1m", "__interval": "1m",
                 "__interval_ms": "15000", "__from": str((now - 1800) * 1000),
                 "__to": str(now * 1000), "__fromTime": str(now - 1800),
                 "__toTime": str(now), "show_p99": "or", "show_avg": "or",
                 "show_min": "or", "show_max": "or"}
    variables.update(defaults)
    return re.sub(r"\$\{(\w+)(?::[^}]+)?\}|\$(\w+)",
                  lambda match: variables.get(match[1] or match[2], match[0]), expression)


def check_target(target):
    title, expression, defaults = target
    result = query(expand(expression, defaults))
    data = result.get("data", {}).get("result", [])
    if not isinstance(data, list):
        data = [data]
    finite = sum(1 for item in data if isinstance(item, dict)
                 and "value" in item and math.isfinite(float(item["value"][1])))
    return {"panel": title, "expression": expression,
            "status": result.get("status"), "series": len(data), "finite_series": finite,
            **({"error": result.get("error")} if result.get("status") != "success" else {})}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    failures = []
    report = {"namespace": NAMESPACE, "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    targets = get(PROM + "/api/v1/targets").get("data", {}).get("activeTargets", [])
    app_targets = [t for t in targets if t.get("labels", {}).get("woodpecker_id") == "milvus"
                   and t.get("labels", {}).get("namespace") == NAMESPACE]
    report["targets"] = [{"labels": t["labels"], "health": t["health"],
                          "lastError": t.get("lastError")} for t in app_targets]
    servers = [t for t in app_targets if t["labels"].get("component") == "woodpecker"]
    clients = [t for t in app_targets if t["labels"].get("component") != "woodpecker"]
    if len(servers) != 3 or len(clients) != 5 or any(t["health"] != "up" for t in app_targets):
        failures.append(f"Expected 3 Woodpecker and 5 Milvus targets UP; got {len(servers)} and {len(clients)}")
    ns = '{namespace="' + NAMESPACE + '"}'
    core = {
        "milvus_inserted_vectors": 'sum(milvus_proxy_insert_vectors_count' + ns + ')',
        "milvus_searches": 'sum(milvus_proxy_search_vectors_count' + ns + ')',
        "woodpecker_client_appends": 'sum(woodpecker_client_append_requests_total' + ns + ')',
        "woodpecker_server_appends": 'sum(woodpecker_server_logstore_operations_total{namespace="' + NAMESPACE + '",operation=~"add_entry|add_entry_batch",status="success"})',
        "pod_inventory": 'count(kube_pod_info{namespace="' + NAMESPACE + '",cluster="local"})',
        "container_cpu": 'count(container_cpu_usage_seconds_total{namespace="' + NAMESPACE + '",cluster="local"})',
    }
    report["core_metrics"] = {}
    for name, expression in core.items():
        result = query(expression)
        values = result.get("data", {}).get("result", [])
        report["core_metrics"][name] = values
        if result.get("status") != "success" or not values or not any(float(v["value"][1]) > 0 for v in values):
            failures.append(f"Missing/non-positive core metric: {name}")
    report["dashboards"] = {}
    # The default local credentials are also used to verify server-side provisioning.
    import base64
    token = base64.b64encode((os.getenv("GRAFANA_USER", "admin") + ":" +
                              os.getenv("GRAFANA_PASSWORD", "admin")).encode()).decode()
    opener = urllib.request.build_opener()
    opener.addheaders = [("Authorization", "Basic " + token)]
    for path in sorted((ROOT / "charts/full-stack-support/dashboards").glob("*.json")):
        dashboard = json.loads(path.read_text())
        try:
            with opener.open(GRAFANA + "/api/dashboards/uid/" + dashboard["uid"], timeout=15) as response:
                provisioned = json.load(response)
            if not provisioned.get("meta", {}).get("provisioned"):
                failures.append(f"Dashboard is not provisioned: {dashboard['uid']}")
        except Exception as error:
            failures.append(f"Dashboard unavailable: {dashboard['uid']}: {error}")
        defaults = {v["name"]: str(v.get("current", {}).get("value", v.get("query", "")))
                    for v in dashboard.get("templating", {}).get("list", [])
                    if v.get("type") in ("textbox", "constant")}
        if any(v["name"] == "server_job" for v in dashboard.get("templating", {}).get("list", [])):
            defaults["server_job"] = NAMESPACE + "/full-stack-woodpecker"
        expressions = [(p.get("title"), t["expr"], defaults) for p in panels(dashboard.get("panels", []))
                       for t in p.get("targets", []) if t.get("expr") and not t.get("hide")]
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(check_target, expressions))
        errors = [r for r in results if r["status"] != "success"]
        populated = sum(r["finite_series"] > 0 for r in results)
        report["dashboards"][dashboard["uid"]] = {
            "title": dashboard["title"], "queries": len(results), "queries_with_data": populated,
            "errors": errors, "results": results}
        if errors:
            failures.append(f"{dashboard['uid']}: {len(errors)} rejected queries")
        if populated == 0:
            failures.append(f"{dashboard['uid']}: no queries have finite samples")
        print(f"{dashboard['title']}: {populated}/{len(results)} queries have data, {len(errors)} query errors")
    report["failures"] = failures
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(f"Detailed query coverage: {args.output}")
    if failures:
        raise SystemExit("Verification failed:\n" + "\n".join(failures))
    print("PASS: scrape targets, workload metrics, and all three dashboards verified")


if __name__ == "__main__":
    main()
