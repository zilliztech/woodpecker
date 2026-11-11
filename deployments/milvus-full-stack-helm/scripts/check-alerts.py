#!/usr/bin/env python3
"""Check alert evaluation, notification profile, and optional local lifecycle probes."""
import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.parse
import urllib.request
import uuid

PROM = os.getenv("PROMETHEUS_URL", "http://127.0.0.1:" + os.getenv("PROMETHEUS_PORT", "9099"))
AM = os.getenv("ALERTMANAGER_URL", "http://127.0.0.1:" + os.getenv("ALERTMANAGER_PORT", "9093"))
NS = os.getenv("NAMESPACE", "milvus-stack")
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def request(base, path, data=None):
    headers = {"Content-Type": "application/json"}
    req = urllib.request.Request(base + path, data=None if data is None else json.dumps(data).encode(), headers=headers)
    with OPENER.open(req, timeout=20) as response:
        body = response.read()
    return json.loads(body) if body else None


def query(expr):
    result = request(PROM, "/api/v1/query?" + urllib.parse.urlencode({"query": expr}))
    if result["status"] != "success":
        raise RuntimeError(result)
    return result["data"]["result"]


def wait_for(name, condition, timeout=240):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            print("PASS:", name, flush=True)
            return
        time.sleep(3)
    raise RuntimeError("Timed out: " + name)


def timestamp(offset=0):
    return (datetime.now(timezone.utc) + timedelta(seconds=offset)).isoformat()


def notification_mode():
    config = request(AM, "/api/v2/status")["config"]["original"]
    receivers = {r["name"] for r in request(AM, "/api/v2/receivers")}
    integrations = re.findall(r"^\s*([a-zA-Z_]+_configs)\s*:", config, re.M)
    if not integrations and receivers == {"local-only"}:
        return "local-only"
    hosts = re.findall(r"^\s*smarthost:\s*['\"]?([^\s'\"]+)", config, re.M)
    if (set(integrations) == {"email_configs"} and hosts and
            set(hosts) == {"full-stack-mailpit:1025"} and receivers == {"local-only", "smtp-capture"}):
        return "smtp-capture"
    return "external"


def smoke(context, report):
    run = uuid.uuid4().hex[:10]
    name = "full-stack-alert-probe-" + run
    labels = {"alert_set": "full-stack-probe", "test_run": run, "cluster": "probe-" + run,
              "namespace": NS, "woodpecker_id": "milvus", "priority": "P2", "severity": "info"}
    resource = {"apiVersion": "monitoring.coreos.com/v1", "kind": "PrometheusRule",
                "metadata": {"name": name, "namespace": NS, "labels": {"full-stack": "milvus"}},
                "spec": {"groups": [{"name": name, "interval": "5s", "rules": [{
                    "alert": "FullStackAlertPipelineProbe", "expr": "vector(1)", "for": "15s",
                    "labels": labels, "annotations": {"summary": "Local pipeline verification; no external notifications"}}]}]}}
    cmd = ["kubectl", "--context", context, "-n", NS]
    def apply():
        subprocess.run(cmd + ["apply", "-f", "-"], input=json.dumps(resource), text=True, check=True, timeout=30)
    def prom_alerts():
        return [a for a in request(PROM, "/api/v1/alerts")["data"]["alerts"] if a["labels"].get("test_run") == run]
    def am_alerts():
        return [a for a in request(AM, "/api/v2/alerts?active=true&silenced=true&inhibited=true")
                if a["labels"].get("test_run") == run]
    injected = []
    try:
        apply()
        wait_for("Prometheus probe enters firing", lambda: any(a["state"] == "firing" for a in prom_alerts()))
        wait_for("Alertmanager receives Prometheus probe", lambda: any(a["labels"]["alertname"] == "FullStackAlertPipelineProbe" for a in am_alerts()))
        resource["spec"]["groups"][0]["rules"][0]["expr"] = "vector(0) == 1"
        apply()
        wait_for("Prometheus probe resolves", lambda: not prom_alerts())
        wait_for("Alertmanager probe resolves", lambda: not am_alerts())
        report["pipeline_probe"] = "firing -> received -> resolved"

        # Exercise real inhibition matchers in an isolated synthetic cluster.
        cases = [("node-down", "F1", "server", "node-a", None),
                 ("same-node-flush", "F4", "server", "node-a", True),
                 ("other-node-flush", "F4", "server", "node-b", False),
                 ("object-down", "F8", "server", "node-c", None),
                 ("cleanup", "S6", "server", "node-c", True),
                 ("other-cleanup", "S6", "server", "node-d", False),
                 ("compaction", "C8", "client", "", True),
                 ("disk-hard", "R5", "server", "node-e", None),
                 ("append-errors", "S3", "server", "node-e", True),
                 ("reader-stuck", "C6", "client", "", None),
                 ("slow-read", "C5", "client", "", True)]
        expected = {}
        for case, rule_id, scope, node_id, inhibited in cases:
            alert_labels = dict(labels, alertname="FullStackProbe_" + case, rule_id=rule_id, alert_scope=scope)
            if node_id:
                alert_labels["node_id"] = node_id
            if rule_id in ("C5", "C6"):
                alert_labels.update(log_ns="probe-log", log_id="1")
            injected.append({"labels": alert_labels, "startsAt": timestamp(-1), "endsAt": timestamp(180),
                             "annotations": {"summary": "Isolated inhibition verification"}})
            if inhibited is not None:
                expected[alert_labels["alertname"]] = inhibited
        request(AM, "/api/v2/alerts", injected)
        def inhibition_matches():
            actual = {a["labels"]["alertname"]: bool(a["status"]["inhibitedBy"]) for a in am_alerts()}
            return all(key in actual and actual[key] == value for key, value in expected.items())
        wait_for("Inhibition chains match and unrelated nodes remain visible", inhibition_matches)
        report["inhibition_probe"] = expected
    finally:
        if injected:
            for alert in injected:
                alert["endsAt"] = timestamp(-1)
            request(AM, "/api/v2/alerts", injected)
        subprocess.run(cmd + ["delete", "prometheusrule", name, "--ignore-not-found"], check=True, timeout=30)
        # If a preceding assertion failed while the probe was firing, stop refreshing it.
        active = am_alerts()
        if active:
            request(AM, "/api/v2/alerts", [{"labels": a["labels"], "startsAt": a["startsAt"],
                                            "endsAt": timestamp(-1)} for a in active])
        wait_for("Synthetic alerts cleaned up", lambda: not am_alerts())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--context", default=os.getenv("CONTEXT", ""))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = {"namespace": NS, "checked_at": timestamp(), "failures": []}
    try:
        report["notifications"] = notification_mode()
        if args.smoke and report["notifications"] == "external":
            raise RuntimeError("Use the local-only or SMTP capture profile for inhibition probes")
        discovery = request(PROM, "/api/v1/alertmanagers")["data"]["activeAlertmanagers"]
        if not discovery:
            raise RuntimeError("Prometheus has not discovered Alertmanager")
        report["alertmanagers"] = discovery
        groups = request(PROM, "/api/v1/rules")["data"]["groups"]
        rules = [r for g in groups if g["name"] == "woodpecker-full-stack" for r in g["rules"]]
        alerts = [r for r in rules if r["type"] == "alerting"]
        expected = {f"{prefix}{i}" for prefix, end in [("R",7),("F",9),("S",6),("C",9)] for i in range(1,end+1)}
        if {r["labels"].get("rule_id") for r in alerts} != expected:
            raise RuntimeError("Not all 31 reference alerts are loaded")
        bad = [r["name"] for r in rules if r["health"] != "ok" or r.get("lastError")]
        if bad:
            raise RuntimeError("Rule evaluation errors: " + str(bad))
        matcher = '{namespace="' + NS + '",cluster="local"}'
        names = set(request(PROM, "/api/v1/label/__name__/values?" + urllib.parse.urlencode({"match[]":matcher}))["data"])
        report["rules"] = []
        for rule in alerts:
            metrics = set(re.findall(r"\b((?:woodpecker_|grpc_|container_|kube_|process_|wp:)[a-zA-Z_0-9:]+)\s*(?=[{\[])", rule["query"]))
            row = {"rule_id": rule["labels"]["rule_id"], "name": rule["name"], "state": rule["state"],
                   "health": rule["health"], "missing_metric_names": sorted(metrics - names)}
            if row["rule_id"] == "R2":
                cadvisor = {"container_network_receive_bytes_total", "container_network_transmit_bytes_total"}
                process = {"process_network_receive_bytes_total", "process_network_transmit_bytes_total"}
                optional = (process if cadvisor <= names else cadvisor if process <= names else set())
                row["optional_missing_metric_names"] = sorted((metrics - names) & optional)
                row["missing_metric_names"] = sorted((metrics - names) - optional)
                row["network_source"] = "cAdvisor" if cadvisor <= names else "WP process counters" if process <= names else "missing"
            if row["rule_id"] == "C2":
                row["seven_day_history_available"] = bool(query('count(woodpecker_client_append_latency_count'+matcher+' offset 7d)'))
            report["rules"].append(row)
        report["active_alerts"] = [a for a in request(AM, "/api/v2/alerts") if a["labels"].get("namespace") == NS]
        gaps = [r["rule_id"] for r in report["rules"] if r["missing_metric_names"] or r.get("seven_day_history_available") is False]
        print(f"PASS: 31 alert rules and {len(rules)-len(alerts)} recording rules healthy; Prometheus discovers Alertmanager", flush=True)
        print("Notification mode: " + report["notifications"], flush=True)
        print("Rules with absent metrics/history (may include unexercised lazy counters): " + ", ".join(gaps), flush=True)
        if args.smoke:
            smoke(args.context, report)
    except Exception as error:
        report["failures"].append(str(error))
        raise
    finally:
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
            print("Alerting evidence:", args.output, flush=True)


if __name__ == "__main__":
    main()
