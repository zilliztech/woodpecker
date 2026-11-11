#!/usr/bin/env python3
"""Run independent query-stage diagnosis against a temporary real Prometheus."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.request
import uuid

from prometheus_smoke import command


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wpdiag", default="bin/wpdiag")
    parser.add_argument("--image", default="quay.io/prometheus/prometheus:v3.0.1")
    args = parser.parse_args()
    binary = str(Path(args.wpdiag).resolve())
    catalog = json.loads(command(binary, "milvus", "query", "--list", "-o", "json"))["signals"]
    cases = [
        ("visibility", "milvus.query.tsafe", "suspected_visibility_wait"),
        ("queue", "milvus.query.queue", "suspected_scheduling_pressure"),
        ("recovery", "milvus.query.queue", "slow_queue_completions_with_backlog_recovery"),
        ("execution", "milvus.query.core", "suspected_execution_work"),
        ("loading", "milvus.query.loading", "observed_slow_segment_loading"),
        ("return", "milvus.query.wait_result", "suspected_downstream_result_wait"),
        ("healthy", "milvus.query.core", "within_configured_latency_for_observed_completions"),
        ("idle", "milvus.query.core", "idle_or_no_completions"),
        ("reset", "milvus.query.core", "inconclusive"),
        ("missing", "milvus.query.core", "inconclusive"),
        ("mixed", "milvus.query", "observed_slow_proxy_completions"),
    ]
    begin = int(time.time()) - 3600
    end = begin + 60
    checks = []
    with tempfile.TemporaryDirectory(prefix="wpdiag-query-") as folder:
        root = Path(folder)
        lines = []
        for scenario, selected, _ in cases:
            for spec in catalog:
                if scenario == "missing" and spec["id"] == selected:
                    continue
                component = "querynode" if spec["component"] == "consumer" else spec["component"]
                labels = {"cluster": "fixture", "scenario": scenario, "instance": component + ":9091",
                          "component": component, "node_id": component, "job": component}
                metric = spec["metric"]
                if spec["kind"] == "histogram":
                    if spec["id"] != "milvus.query.loading":
                        labels["query_type"] = "Search"
                    if spec["id"] == "milvus.query.queue":
                        labels.update(db_name="default", rg="default")
                    count = [i * 20 for i in range(6)]
                    if scenario == "idle":
                        count = [0] * 6
                    elif scenario == "reset":
                        count = [0, 20, 40, 0, 20, 40]
                    slow = scenario not in ("healthy", "idle", "reset", "missing") and spec["id"] == selected
                    latency = 5000 if slow else 10
                    rows = [(metric + "_count", {}, count), (metric + "_sum", {}, [n * latency for n in count])]
                    for bound in [10, 100, 1000, 10000]:
                        rows.append((metric + "_bucket", {"le": str(bound)}, count if bound >= latency else [0] * 6))
                    rows.append((metric + "_bucket", {"le": "+Inf"}, count))
                else:
                    values = [0] * 6
                    if spec["id"] == "milvus.query.ready":
                        if scenario == "queue":
                            values = [2, 2, 3, 4, 4, 5]
                        elif scenario == "recovery":
                            values = [3, 3, 2, 1, 0, 0]
                    if spec["id"] == "milvus.query.concurrency":
                        values = [2] * 6
                    rows = [(metric, {}, values)]
                for name, extra, values in rows:
                    identity = ",".join(k + "=" + json.dumps(v) for k, v in sorted(dict(labels, **extra).items()))
                    for index, value in enumerate(values):
                        lines.append(f"{name}{{{identity}}} {value} {begin + 5 + index * 10}")
        (root / "samples.openmetrics").write_text("\n".join(lines) + "\n# EOF\n")
        (root / "prometheus.yaml").write_text("global:\n  scrape_interval: 15s\nscrape_configs: []\n")
        identity = str(os.getuid()) + ":" + str(os.getgid())
        command("docker", "run", "--rm", "--user", identity, "-v", str(root) + ":/work", "--entrypoint", "/bin/promtool",
                args.image, "tsdb", "create-blocks-from", "openmetrics", "/work/samples.openmetrics", "/work/data")
        container = "wpdiag-query-smoke-" + uuid.uuid4().hex[:10]
        started = False
        try:
            command("docker", "run", "--rm", "-d", "--name", container, "--user", identity, "-p", "127.0.0.1::9090",
                    "-v", str(root) + ":/work", args.image, "--config.file=/work/prometheus.yaml", "--storage.tsdb.path=/work/data")
            started = True
            endpoint = "http://" + command("docker", "port", container, "9090/tcp").splitlines()[0]
            for _ in range(100):
                try:
                    with urllib.request.urlopen(endpoint + "/-/ready", timeout=1) as response:
                        if response.status == 200:
                            break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("Fixture Prometheus did not become ready")
            for scenario, selected, expected in cases:
                config = {"schema_version": "wpdiag.config/v1", "sources": {"prometheus": {"connections": {"fixture": {"settings": {
                    "endpoint": endpoint, "deployment": "fixture", "match_labels": {"cluster": "fixture", "scenario": scenario}}}}}}}
                path = root / "wpdiag.yaml"
                path.write_text(json.dumps(config))
                for scope in ["milvus", "e2e"]:
                    proc = subprocess.run([binary, "milvus", "query", "--config", str(path), "--connection", "fixture", "--scope", scope,
                                           "--mode", "historical", "--from", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(begin)),
                                           "--to", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(end)), "-o", "json"],
                                          capture_output=True, text=True, timeout=15)
                    report = json.loads(proc.stdout)
                    assert proc.returncode == 0, (scenario, report, proc.stderr)
                    assessment = report["assessments"][0]
                    target = selected if selected != "milvus.query" else "milvus.query.proxy_completion"
                    findings = [f for f in assessment["findings"] if f["rule_id"] == target]
                    assert findings and findings[0]["outcome"] == expected, (scenario, expected, findings)
                    source_ids = {f["id"] for f in report["results"][0]["findings"]}
                    assert all(set(f["derived_from_findings"]) <= source_ids for f in assessment["findings"])
                    assert endpoint not in proc.stdout
                    assert all("woodpecker_server" not in o["query"]["expression"] for o in report["results"][0]["observations"])
                    if scope == "e2e":
                        assert "wp_visibility_mapping_unavailable" in {g["code"] for g in assessment["coverage_gaps"]}
                    checks.append({"scenario": scenario, "scope": scope, "outcome": expected})
            print(json.dumps({"passed": True, "checks": checks}, indent=2))
        finally:
            if started:
                subprocess.run(["docker", "stop", "--time", "1", container], capture_output=True, text=True, timeout=15)


if __name__ == "__main__":
    main()
