#!/usr/bin/env python3
"""Run independent consume-stage diagnosis against a temporary real Prometheus."""
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
    catalog = json.loads(command(binary, "milvus", "consume", "--list", "-o", "json"))["signals"]
    cases = [
        ("scanner-pressure", "milvus.consume.paused", "suspected_scanner_downstream_backpressure"),
        ("scanner-recovery", "milvus.consume.paused", "scanner_pause_and_pending_recovery_observed"),
        ("processing-pressure", "milvus.consume.processing", "suspected_consumer_processing_pressure"),
        ("processing-recovery", "milvus.consume.processing", "slow_processing_with_backlog_recovery"),
        ("processing-idle-queue", "milvus.consume.processing", "slow_processing_without_persistent_waiting_work"),
        ("lag-active", "milvus.consume.lag", "lag_increased_during_observed_msgpack_consumption"),
        ("lag-idle", "milvus.consume.lag", "consumer_time_tick_lag_increased"),
        ("lag-reset", "milvus.consume.lag", "consumer_time_tick_lag_increased"),
        ("lag-other-collection", "milvus.consume.lag", "consumer_time_tick_lag_increased"),
        ("tailing", "milvus.consume.mode", "scanner_mode_population_observed"),
        ("local", "milvus.consume.subscription", "subscription_access_population_observed"),
        ("unavailable", "milvus.consume.unavailable", "persistent_unavailable_consumer_observed"),
        ("unavailable-recovery", "milvus.consume.unavailable", "unavailable_consumer_recovery_observed"),
        ("progress", "milvus.consume.scanned", "scan_activity_observed"),
        ("passed", "milvus.consume.passed", "passed_message_activity_observed"),
        ("bytes", "milvus.consume.delivered", "delivered_bytes_observed"),
        ("healthy", "milvus.consume.processing", "within_configured_latency_for_observed_completions"),
        ("idle", "milvus.consume.processing", "idle_or_no_completions"),
        ("reset", "milvus.consume.scanned", "inconclusive"),
        ("missing", "milvus.consume.processing", "inconclusive"),
        ("blocking-read-next", "wp.read_next", "blocking_read_next_completion_time_observed"),
        ("batch", "wp.read_batch", "observed_slow_wp_batch_reads"),
        ("direct", "wp.direct_read", "observed_slow_wp_direct_reads"),
        ("eof", "wp.eof", "server_eof_observed"),
        ("no-rpc", "wp.quorum_bytes", "no_quorum_read_bytes_observed"),
    ]
    begin = int(time.time()) - 3600
    end = begin + 60
    checks = []
    with tempfile.TemporaryDirectory(prefix="wpdiag-consume-") as folder:
        root = Path(folder)
        lines = []
        for scenario, selected, _ in cases:
            for spec in catalog:
                if scenario == "missing" and spec["id"] == selected:
                    continue
                if scenario == "local" and spec["id"] == "milvus.consume.remote":
                    continue
                sid = spec["id"]
                component = "streamingnode" if spec["component"] == "consumer" else spec["component"]
                labels = {"cluster": "fixture", "scenario": scenario, "instance": component + ":9091",
                          "component": component, "node_id": component, "job": component, "private": "sentinel-secret"}
                labels.update(spec.get("match_labels", {}))
                for key, value in spec.get("regex_labels", {}).items():
                    labels[key] = value.split("|")[0]
                if sid in ("milvus.consume.mode", "milvus.consume.scanned", "milvus.consume.passed"):
                    labels["scanner_model"] = "tailing" if scenario == "tailing" else "catchup"
                if sid in ("milvus.consume.scanned", "milvus.consume.passed"):
                    labels["message_type"] = "Insert"
                if sid.startswith("milvus.") and "querynode_" not in spec["metric"]:
                    labels["channel_name"] = "channel-a"
                if sid == "milvus.consume.subscription":
                    labels["access_model"] = "local"
                if sid in ("milvus.consume.waiting", "milvus.consume.processing"):
                    labels["msg_type"] = "Insert"
                if sid in ("milvus.consume.lag", "milvus.consume.packs"):
                    labels["collection_id"] = "8" if scenario == "lag-other-collection" and sid.endswith("packs") else "7"
                if sid.startswith("wp."):
                    labels.update(log_ns="wp-client-tenant", log_id="9007199254740993")
                if sid == "wp.quorum_bytes":
                    labels["az_scope"] = "local"
                metric = spec["metric"]
                count = [i * 20 for i in range(6)]
                if scenario in ("idle", "tailing", "no-rpc") or (scenario == "lag-idle" and sid.endswith("packs")):
                    count = [0] * 6
                elif scenario == "reset" or (scenario == "lag-reset" and sid.endswith("packs")):
                    count = [0, 20, 40, 0, 20, 40]
                if spec["kind"] == "histogram":
                    slow = scenario in ("processing-pressure", "processing-recovery", "processing-idle-queue",
                                        "blocking-read-next", "batch", "direct") and sid == selected
                    latency = 5000 if slow else 10
                    rows = [(metric + "_count", {}, count), (metric + "_sum", {}, [n * latency for n in count])]
                    for bound in [10, 100, 1000, 10000]:
                        rows.append((metric + "_bucket", {"le": str(bound)}, count if bound >= latency else [0] * 6))
                    rows.append((metric + "_bucket", {"le": "+Inf"}, count))
                elif spec["kind"].startswith("counter_"):
                    rows = [(metric, {}, count)]
                else:
                    values = [0] * 6
                    if sid in ("milvus.consume.mode", "milvus.consume.subscription"):
                        values = [1] * 6
                    if sid in ("milvus.consume.pending", "milvus.consume.paused"):
                        if scenario == "scanner-pressure":
                            values = [1] * 6
                        elif scenario == "scanner-recovery":
                            values = [1, 1, 1, 1, 0, 0]
                    if sid == "milvus.consume.waiting":
                        if scenario == "processing-pressure":
                            values = [1, 2, 3, 4, 5, 6]
                        elif scenario == "processing-recovery":
                            values = [3, 3, 2, 1, 0, 0]
                    if sid == "milvus.consume.unavailable":
                        if scenario == "unavailable":
                            values = [1] * 6
                        elif scenario == "unavailable-recovery":
                            values = [1, 1, 1, 1, 0, 0]
                    if sid == "milvus.consume.lag" and scenario.startswith("lag-"):
                        values = [100, 200, 300, 400, 500, 600]
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
        container = "wpdiag-consume-smoke-" + uuid.uuid4().hex[:10]
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
                for scope in (["e2e"] if selected.startswith("wp.") else ["milvus", "e2e"]):
                    proc = subprocess.run([binary, "milvus", "consume", "--config", str(path), "--connection", "fixture", "--scope", scope,
                                           "--mode", "historical", "--from", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(begin)),
                                           "--to", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(end)), "-o", "json"],
                                          capture_output=True, text=True, timeout=15)
                    report = json.loads(proc.stdout)
                    assert proc.returncode == 0, (scenario, report, proc.stderr)
                    assessment = report["assessments"][0]
                    target = "milvus.consume.step." + selected
                    findings = [f for f in assessment["findings"] if f["rule_id"] == target]
                    assert findings and findings[0]["outcome"] == expected, (scenario, expected, findings)
                    source_ids = {f["id"] for f in report["results"][0]["findings"]}
                    assert all(set(f["derived_from_findings"]) <= source_ids for f in assessment["findings"])
                    assert endpoint not in proc.stdout and "sentinel-secret" not in proc.stdout
                    if scope == "milvus":
                        assert all("woodpecker_" not in o["query"]["expression"] for o in report["results"][0]["observations"])
                    if scope == "e2e":
                        assert "wp_reader_mapping_unavailable" in {g["code"] for g in assessment["coverage_gaps"]}
                    if scenario == "bytes":
                        facts = [o["data"]["facts"] for o in report["results"][0]["observations"]
                                 if o["source_id"] == "prometheus:milvus.consume.delivered"][0]
                        assert facts[0]["counter"]["increase"] == 100
                        assert facts[0]["counter"]["per_second"] == 2
                    if scenario == "lag-idle":
                        assert "no_new_packs_no_stall_verdict" in findings[0]["unknown"]
                        assert findings[0]["contradicting_observations"]
                    checks.append({"scenario": scenario, "scope": scope, "outcome": expected})
            print(json.dumps({"passed": True, "checks": checks}, indent=2))
        finally:
            if started:
                subprocess.run(["docker", "stop", "--time", "1", container], capture_output=True, text=True, timeout=15)


if __name__ == "__main__":
    main()
