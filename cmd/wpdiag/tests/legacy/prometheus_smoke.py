#!/usr/bin/env python3
"""Exercise wpdiag against real Prometheus with deterministic retained scrapes.

Requires Docker and quay.io/prometheus/prometheus:v3.0.1 (the Helm example's
version). Creates and removes one isolated container and a temporary TSDB.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.request
import uuid


def command(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.PIPE).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wpdiag", default="bin/wpdiag")
    parser.add_argument("--image", default="quay.io/prometheus/prometheus:v3.0.1")
    args = parser.parse_args()
    binary = str(Path(args.wpdiag).resolve())
    name = "wpdiag-prom-smoke-" + uuid.uuid4().hex[:10]
    image = args.image
    begin = int(time.time()) - 3600
    end = begin + 60
    outcomes = []
    with tempfile.TemporaryDirectory(prefix="wpdiag-prom-") as folder:
        root = Path(folder)
        points = [begin + 5 + 10 * index for index in range(6)]
        lines = []
        families = [
            ("woodpecker_server_logstore_operation_latency", "woodpecker", "add_entry", 1),
            ("woodpecker_server_logstore_operation_latency", "woodpecker", "get_batch_entries", 1),
            ("milvus_proxy_mutation_latency", "proxy", "insert", 1),
            ("milvus_proxy_sq_latency", "proxy", "Search", 1),
            ("milvus_wal_append_message_duration_seconds", "streamingnode", "", 1000),
        ]
        for scenario in ["slow", "healthy", "idle", "reset", "sparse"]:
            for metric, component, operation, factor in families:
                labels = {"cluster": "fixture", "scenario": scenario, "instance": component + ":9091", "component": component}
                if component == "woodpecker":
                    labels.update(node_id="node-1", log_ns="tenant-a", log_id="2", operation=operation, status="success")
                elif metric == "milvus_proxy_mutation_latency":
                    labels["msg_type"] = operation
                elif metric == "milvus_proxy_sq_latency":
                    labels["query_type"] = operation
                latency = (50 if scenario == "healthy" else 3000) / factor
                count = [index * 20 for index in range(6)]
                if scenario == "idle":
                    count = [0] * 6
                elif scenario == "reset":
                    count = [0, 20, 40, 0, 20, 40]
                values = [(metric + "_count", {}, count),
                          (metric + "_sum", {}, [n * latency for n in count])]
                for upper in [100, 256, 2048, 4096]:
                    values.append((metric + "_bucket", {"le": str(upper / factor)},
                                   count if upper >= latency * factor else [0] * 6))
                values.append((metric + "_bucket", {"le": "+Inf"}, count))
                for family, extra, samples in values:
                    identity = ",".join(k + "=" + json.dumps(v) for k, v in sorted(dict(labels, **extra).items()))
                    for index, timestamp in enumerate(points):
                        if scenario == "sparse" and index not in (0, 5):
                            continue
                        lines.append(f"{family}{{{identity}}} {samples[index]} {timestamp}")
            for timestamp in points:
                lines.append('woodpecker_server_logstore_write_backpressure_state{cluster="fixture",scenario="'
                             + scenario + '",instance="woodpecker:9091",component="woodpecker",node_id="node-1"} '
                             + ("2" if scenario == "slow" else "0") + " " + str(timestamp))
        (root / "samples.openmetrics").write_text("\n".join(lines) + "\n# EOF\n")
        (root / "prometheus.yaml").write_text("global:\n  scrape_interval: 15s\nscrape_configs: []\n")
        identity = str(os.getuid()) + ":" + str(os.getgid())
        command("docker", "run", "--rm", "--user", identity, "-v", str(root) + ":/work", "--entrypoint", "/bin/promtool",
                image, "tsdb", "create-blocks-from", "openmetrics", "/work/samples.openmetrics", "/work/data")
        started = False
        try:
            command("docker", "run", "--rm", "-d", "--name", name, "--user", identity, "-p", "127.0.0.1::9090",
                    "-v", str(root) + ":/work", image, "--config.file=/work/prometheus.yaml", "--storage.tsdb.path=/work/data")
            started = True
            address = command("docker", "port", name, "9090/tcp").splitlines()[0]
            endpoint = "http://" + address
            for _ in range(100):
                try:
                    with urllib.request.urlopen(endpoint + "/-/ready", timeout=1) as response:
                        if response.status == 200:
                            break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("Fixture Prometheus did not become ready")
            for scenario, expected in [("slow", "suspected_high_latency"),
                                       ("healthy", "within_configured_latency_for_observed_completions"),
                                       ("idle", "idle_or_no_completions"), ("reset", "inconclusive"), ("sparse", "inconclusive")]:
                config = {"schema_version": "wpdiag.config/v1", "sources": {"prometheus": {"connections": {"fixture": {
                    "settings": {"endpoint": endpoint, "deployment": "fixture", "match_labels": {"cluster": "fixture", "scenario": scenario}}}}}}}
                (root / "wpdiag.yaml").write_text(json.dumps(config))
                for scope, flow in [("wp", "write"), ("wp", "read"), ("milvus", "write"), ("milvus", "read"), ("e2e", "write"), ("e2e", "read")]:
                    args_run = [binary, "prometheus", "diagnose", "--config", str(root / "wpdiag.yaml"), "--connection", "fixture",
                                "--scope", scope, "--flow", flow, "--mode", "historical", "--from", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(begin)),
                                "--to", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(end)), "-o", "json"]
                    result = subprocess.run(args_run, capture_output=True, text=True, timeout=10)
                    report = json.loads(result.stdout)
                    assert result.returncode == 0, (scenario, scope, flow, report, result.stderr)
                    findings = report["results"][0]["findings"]
                    latency = [f for f in findings if f["rule_id"].endswith(".latency")]
                    assert latency and all(f["outcome"] == expected for f in latency), (scenario, scope, flow, findings)
                    assert endpoint not in result.stdout
                    outcomes.append({"scenario": scenario, "scope": scope, "flow": flow, "outcome": expected})
            print(json.dumps({"passed": True, "image": image, "checks": outcomes}, indent=2))
        finally:
            if started:
                subprocess.run(["docker", "stop", "--time", "1", name], capture_output=True, text=True, timeout=15)


if __name__ == "__main__":
    main()
