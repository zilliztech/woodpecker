#!/usr/bin/env python3
"""Verify Prometheus -> Alertmanager -> SMTP -> captured firing/resolved messages."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import uuid

import importlib.util

spec = importlib.util.spec_from_file_location("alert_checks", Path(__file__).with_name("check-alerts.py"))
alerts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(alerts)
MAIL = os.getenv("MAILPIT_URL", "http://127.0.0.1:" + os.getenv("MAILPIT_PORT", "8025"))


def run(context, recipient, report):
    # Helm --wait covers Pod readiness, not asynchronous Secret/config reloads.
    alerts.wait_for("SMTP capture receiver is active (deploy with --mail-capture)",
                    lambda: alerts.notification_mode() == "smtp-capture")
    alerts.request(MAIL, "/api/v1/messages?limit=1")
    run_id = uuid.uuid4().hex[:12]
    name = "full-stack-email-probe-" + run_id
    report.update(test_run=run_id, recipient=recipient, delivery="local SMTP capture; no external inbox delivery")
    labels = dict(notification_test="email", test_run=run_id, cluster="probe-" + run_id,
                  namespace=alerts.NS, woodpecker_id="milvus", severity="info", priority="P2")
    resource = {"apiVersion": "monitoring.coreos.com/v1", "kind": "PrometheusRule",
                "metadata": {"name": name, "namespace": alerts.NS, "labels": {"full-stack": "milvus"}},
                "spec": {"groups": [{"name": name, "interval": "5s", "rules": [{
                    "alert": "FullStackEmailProbe", "expr": "vector(1)", "for": "10s",
                    "labels": labels, "annotations": {
                        "summary": "Controlled local email acceptance test " + run_id,
                        "description": "Verify firing and resolution through the SMTP receiver.",
                        "dashboard_url": "http://127.0.0.1:3000/d/woodpecker-server-k8s"}}]}]}}
    cmd = ["kubectl", "--context", context, "-n", alerts.NS]

    def apply():
        subprocess.run(cmd + ["apply", "-f", "-"], input=json.dumps(resource), text=True, check=True, timeout=30)

    def active():
        return [a for a in alerts.request(alerts.AM, "/api/v2/alerts") if a["labels"].get("test_run") == run_id]

    def mail(status):
        messages = alerts.request(MAIL, "/api/v1/messages?limit=1000").get("messages") or []
        for item in messages:
            if run_id not in item["Subject"] or "[" + status not in item["Subject"]:
                continue
            message = alerts.request(MAIL, "/api/v1/message/" + item["ID"])
            if recipient not in {address["Address"] for address in message["To"]}:
                raise RuntimeError("Captured email has unexpected recipient")
            body = message.get("Text", "") + message.get("HTML", "")
            if run_id not in body or "FullStackEmailProbe" not in body:
                raise RuntimeError("Captured email is missing test alert details")
            report[status.lower()] = {"message_id": item["ID"], "subject": item["Subject"],
                                      "recipient": recipient, "body_verified": True}
            return True
        return False

    try:
        apply()
        alerts.wait_for("Email probe reaches Alertmanager", lambda: bool(active()))
        alerts.wait_for("FIRING email captured over SMTP with correct recipient and body", lambda: mail("FIRING"))
        resource["spec"]["groups"][0]["rules"][0]["expr"] = "vector(0) == 1"
        apply()
        alerts.wait_for("Email alert resolves in Alertmanager", lambda: not active())
        alerts.wait_for("RESOLVED email captured over SMTP with correct recipient and body", lambda: mail("RESOLVED"))
    finally:
        subprocess.run(cmd + ["delete", "prometheusrule", name, "--ignore-not-found"], check=True, timeout=30)
        remaining = active()
        if remaining:
            alerts.request(alerts.AM, "/api/v2/alerts", [
                {"labels": a["labels"], "startsAt": a["startsAt"], "endsAt": alerts.timestamp(-1)} for a in remaining])
        alerts.wait_for("Email probe cleaned up", lambda: not active())
        report["probe_cleaned_up"] = True
        # Keep the two captured messages for inspection. Never delete other runs' mail.


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", default=os.getenv("CONTEXT", os.getenv("PROFILE", "wp-ops-v0146")))
    parser.add_argument("--recipient", default=os.getenv("EMAIL_TO", "zhenyuan.wei@zilliz.com"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = {"checked_at": alerts.timestamp(), "failures": []}
    try:
        run(args.context, args.recipient, report)
    except Exception as error:
        report["failures"].append(str(error))
        raise
    finally:
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
            print("Email evidence:", args.output, flush=True)


if __name__ == "__main__":
    main()
