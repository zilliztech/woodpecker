#!/usr/bin/env python3
"""Run independent persistence-stage diagnosis against a temporary real Prometheus."""
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
    catalog = json.loads(command(binary, "milvus", "persistence", "--list", "-o", "json"))["signals"]
    cases = [
        ("queue", "milvus.persistence.queue", "suspected_sync_scheduling_pressure"),
        ("queue-recovery", "milvus.persistence.queue", "slow_queue_completions_with_pending_recovery"),
        ("execute", "milvus.persistence.execute", "observed_slow_sync_execution"),
        ("save", "milvus.persistence.save", "observed_slow_save_completions"),
        ("put", "milvus.persistence.put", "observed_slow_storage_put_completions"),
        ("growing-pressure", "milvus.persistence.pressure", "suspected_growing_flush_pressure"),
        ("growing-recovery", "milvus.persistence.pressure", "growing_pressure_below_hwm_after_breach"),
        ("snapshot-delay", "milvus.persistence.persisted", "suspected_recovery_snapshot_persistence_delay"),
        ("snapshot-idle", "milvus.persistence.persisted", "recovery_frontiers_equal_at_final_scrapes"),
        ("snapshot-recovery", "milvus.persistence.persisted", "recovery_frontiers_equal_at_final_scrapes"),
        ("term-mismatch", "milvus.persistence.persisted", "frontier_unchanged_observed"),
        ("busy-recovered", "milvus.persistence.persisted", "persisted_recovery_frontier_unchanged"),
        ("checkpoint-idle", "milvus.persistence.checkpoint", "frontier_unchanged_observed"),
        ("seen", "milvus.persistence.seen", "frontier_advanced_observed"),
        ("normal", "milvus.persistence.execute", "within_configured_latency_for_observed_completions"),
        ("idle", "milvus.persistence.execute", "idle_or_no_completions"),
        ("reset", "milvus.persistence.execute", "inconclusive"),
        ("missing", "milvus.persistence.execute", "inconclusive"),
        ("wp-flush", "wp.flush", "observed_slow_wp_local_flush"),
        ("wp-compaction", "wp.compaction", "observed_slow_wp_compaction"),
        ("wp-storage", "wp.storage", "observed_slow_wp_storage_completions"),
        ("wp-disk", "wp.disk_state", "exported_wp_disk_admission_pressure_observed"),
    ]
    begin = int(time.time()) - 3600
    end = begin + 60
    checks = []
    with tempfile.TemporaryDirectory(prefix="wpdiag-persistence-") as folder:
        root = Path(folder)
        lines = []
        for scenario, selected, _ in cases:
            for spec in catalog:
                if scenario == "missing" and spec["id"] == selected:
                    continue
                component = "streamingnode" if spec["component"] == "persistor" else spec["component"]
                labels = {"cluster": "fixture", "scenario": scenario, "instance": component + ":9091",
                          "component": component, "node_id": component, "job": component}
                metric = spec["metric"]
                labels.update(spec.get("match_labels", {}))
                labels.update({k: v.split("|")[0] for k, v in spec.get("regex_labels", {}).items()})
                if spec["id"].startswith("wp."):
                    labels.update(log_ns="wp-client-tenant", log_id="9007199254740993")
                if spec["kind"] == "unix_seconds" or spec["id"] in ("milvus.persistence.flusher", "milvus.persistence.busy"):
                    labels.update(channel_name="channel-a")
                    if spec["id"] != "milvus.persistence.checkpoint":
                        labels["term"] = "1"
                if spec["id"] == "milvus.persistence.flusher":
                    labels["state"] = "working"
                if scenario == "term-mismatch" and spec["id"] == "milvus.persistence.memory":
                    labels["term"] = "2"
                if spec["kind"] == "histogram":
                    count = [i * 20 for i in range(6)]
                    if scenario == "idle":
                        count = [0] * 6
                    elif scenario == "reset":
                        count = [0, 20, 40, 0, 20, 40]
                    slow = scenario not in ("normal", "idle", "reset", "missing") and spec["id"] == selected
                    latency = 5000 if slow else 10
                    factor = spec["metric_to_milliseconds"]
                    rows = [(metric + "_count", {}, count), (metric + "_sum", {}, [n * latency / factor for n in count])]
                    for bound in [10, 100, 1000, 10000]:
                        rows.append((metric + "_bucket", {"le": str(bound / factor)}, count if bound >= latency else [0] * 6))
                    rows.append((metric + "_bucket", {"le": "+Inf"}, count))
                else:
                    values = [0] * 6
                    if spec["kind"] == "unix_seconds":
                        values = [begin - 10 + i * 2 for i in range(6)]
                    if spec["id"] == "milvus.persistence.hwm":
                        values = [100] * 6
                    if spec["id"] == "milvus.persistence.flusher":
                        values = [1] * 6
                    if spec["id"] == "milvus.persistence.pending":
                        if scenario == "queue":
                            values = [2, 2, 3, 4, 4, 5]
                        elif scenario == "queue-recovery":
                            values = [3, 3, 2, 1, 0, 0]
                    if spec["id"] == "milvus.persistence.pressure":
                        if scenario == "growing-pressure":
                            values = [120] * 6
                        elif scenario == "growing-recovery":
                            values = [120, 120, 110, 80, 20, 10]
                    if spec["id"] == "milvus.persistence.persisted" and scenario in ("snapshot-delay", "snapshot-idle", "term-mismatch", "busy-recovered"):
                        values = [begin - 20] * 6
                    if spec["id"] == "milvus.persistence.memory" and scenario == "snapshot-idle":
                        values = [begin - 20] * 6
                    if spec["id"] == "milvus.persistence.persisted" and scenario == "snapshot-recovery":
                        values = [begin - 20, begin - 18, begin - 16, begin - 14, begin - 2, begin]
                    if spec["id"] == "milvus.persistence.busy" and scenario in ("snapshot-delay", "snapshot-idle", "term-mismatch", "busy-recovered"):
                        values = [1] * 6
                        if scenario == "busy-recovered":
                            values = [1, 1, 1, 1, 0, 0]
                    if spec["id"] == "milvus.persistence.checkpoint" and scenario == "checkpoint-idle":
                        values = [begin - 100] * 6
                    if spec["id"] == "wp.disk_state" and scenario == "wp-disk":
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
        container = "wpdiag-persistence-smoke-" + uuid.uuid4().hex[:10]
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
                    proc = subprocess.run([binary, "milvus", "persistence", "--config", str(path), "--connection", "fixture", "--scope", scope,
                                           "--mode", "historical", "--from", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(begin)),
                                           "--to", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(end)), "-o", "json"],
                                          capture_output=True, text=True, timeout=15)
                    report = json.loads(proc.stdout)
                    assert proc.returncode == 0, (scenario, report, proc.stderr)
                    assessment = report["assessments"][0]
                    target = "milvus.persistence.step." + selected
                    findings = [f for f in assessment["findings"] if f["rule_id"] == target]
                    assert findings and findings[0]["outcome"] == expected, (scenario, expected, findings)
                    source_ids = {f["id"] for f in report["results"][0]["findings"]}
                    assert all(set(f["derived_from_findings"]) <= source_ids for f in assessment["findings"])
                    assert endpoint not in proc.stdout
                    if scope == "milvus":
                        assert all("woodpecker_" not in o["query"]["expression"] for o in report["results"][0]["observations"])
                    if scope == "e2e":
                        assert "wp_maintenance_mapping_unavailable" in {g["code"] for g in assessment["coverage_gaps"]}
                    checks.append({"scenario": scenario, "scope": scope, "outcome": expected})
            print(json.dumps({"passed": True, "count": len(checks), "checks": checks}, indent=2))
        finally:
            if started:
                subprocess.run(["docker", "stop", "--time", "1", container], capture_output=True, text=True, timeout=15)


if __name__ == "__main__":
    main()
