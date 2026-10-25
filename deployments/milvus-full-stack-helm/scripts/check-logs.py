#!/usr/bin/env python3
"""Verify real namespace Pod logs reach Loki through the configured collector."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
LOKI = os.getenv("LOKI_URL", "http://127.0.0.1:" + os.getenv("LOKI_PORT", "3100"))
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def query(namespace, marker=None, pod=None):
    selector = "{namespace=" + json.dumps(namespace)
    if pod:
        selector += ",pod=" + json.dumps(pod)
    selector += "}"
    return selector + (" |= " + json.dumps(marker) if marker else "")


def fetch(expression, start):
    params = urllib.parse.urlencode({"query": expression, "start": str(int(start * 1e9)),
                                   "end": str(time.time_ns()), "limit": 20, "direction": "backward"})
    with OPENER.open(LOKI + "/loki/api/v1/query_range?" + params, timeout=15) as response:
        result = json.load(response)
    if result.get("status") != "success":
        raise RuntimeError(str(result))
    return result["data"]["result"]


def smoke(context, namespace):
    name = "log_smoke_" + str(time.time_ns())
    result = subprocess.run(["python3", str(ROOT / "scripts/workload.py")],
                  env=dict(os.environ, COLLECTION=name, BATCHES="1", BATCH_SIZE="10", INTERVAL="0"),
                  capture_output=True, text=True, timeout=180)
    if result.returncode:
        raise RuntimeError("Log smoke workload failed: " + result.stdout[-2000:] + result.stderr[-1000:])
    k = ["kubectl", "--context", context, "-n", namespace]
    pods = subprocess.run(k + ["get", "pods", "-l", "app.kubernetes.io/instance=milvus,component=proxy", "-o", "json"],
                          check=True, capture_output=True, text=True, timeout=15)
    for pod in json.loads(pods.stdout)["items"]:
        pod_name = pod["metadata"]["name"]
        logs = subprocess.run(k + ["logs", pod_name, "--since=3m", "--tail=500"],
                              check=True, capture_output=True, text=True, timeout=15)
        matching = [line for line in logs.stdout.splitlines() if name in line]
        if matching:
            return {"collection": name, "rows": 10, "query_and_search": "passed", "pod": pod_name,
                    "marker": name, "source_log_line": matching[-1]}
    raise RuntimeError("Workload passed but its unique collection name was not found in bounded Proxy logs")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default=os.getenv("NAMESPACE", "milvus-stack"))
    parser.add_argument("--context", default=os.getenv("CONTEXT", ""))
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = {"loki": LOKI, "namespace": args.namespace, "failures": []}
    start = time.time() - (120 if args.smoke else 600)
    try:
        with OPENER.open(LOKI + "/ready", timeout=10) as response:
            report["ready"] = response.status == 200
        source = smoke(args.context, args.namespace) if args.smoke else {}
        report["source"] = source
        expression = query(args.namespace, source.get("marker"), source.get("pod"))
        report["query"] = expression
        deadline = time.monotonic() + (60 if args.smoke else 0)
        while True:
            streams = fetch(expression, start)
            if streams or time.monotonic() >= deadline:
                break
            time.sleep(2)
        report["streams"] = streams
        report["matched_lines"] = sum(len(s["values"]) for s in streams)
        if not report["matched_lines"]:
            report["failures"].append("No matching collected Pod logs; check Alloy discovery/RBAC, labels and Loki ingestion")
        trace_ids = {m.group(1) for stream in streams for _, line in stream["values"]
                     for m in re.finditer(r"\[traceID=([a-fA-F0-9]{32})\]", line)}
        report["trace_ids_in_logs"] = sorted(trace_ids)
    except Exception as error:
        report["failures"].append(str(error))
    report["passed"] = not report["failures"]
    report["scope"] = "Collected container logs, not synthetic Loki push data; does not prove no loss across rotation/outages."
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return int(not report["passed"])


if __name__ == "__main__":
    raise SystemExit(main())
