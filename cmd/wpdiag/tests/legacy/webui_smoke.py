#!/usr/bin/env python3
"""Run the real WebUI/runner against bounded Prometheus/Jaeger/Loki HTTP API fixtures.

Requires Node 22+ and Chrome (CHROME_BIN may override its path). Real Prometheus
query evaluation and native integration remain covered by integrated_smoke.py.
This fixture targets browser selection, execution, evidence and export behavior.
"""
import argparse
import http.server
import json
from pathlib import Path
import queue
import re
import signal
import subprocess
import tempfile
import threading
import time
import urllib.parse


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wpdiag", default="bin/wpdiag")
    parser.add_argument("--output", default="/tmp/wpdiag-webui-evidence")
    args = parser.parse_args()
    binary = str(Path(args.wpdiag).resolve())
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    begin = int(time.time()) - 3600
    end = begin + 60
    catalog = []
    for stage in ("append", "persistence", "consume", "query"):
        catalog += json.loads(subprocess.check_output([binary, "milvus", stage, "--list", "-o", "json"], text=True))["signals"]
    catalog.sort(key=lambda spec: len(spec["metric"]), reverse=True)
    state = {"requests": 0}

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            state["requests"] += 1
            if self.path.startswith("/slow/"):
                time.sleep(0.2)
            if self.path.startswith("/denied/"):
                self.send_response(401)
                self.end_headers()
                self.wfile.write(b"sentinel-secret")
                return
            trace_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            if self.path.startswith("/jaeger/"):
                processes = {"p1": {"serviceName": "proxy"}, "p2": {"serviceName": "streamingnode"},
                             "p3": {"serviceName": "wp-client", "tags": [{"key": "otel.library.name", "type": "string", "value": "LogWriter"}]},
                             "p4": {"serviceName": "wp-server"}}
                spans = []
                for i, operation in enumerate(("Proxy-Insert", "AppendMessages", "Write", "AddEntry")):
                    spans.append({"traceID": trace_id, "spanID": f"{i+1:016x}", "processID": f"p{i+1}",
                                  "operationName": operation, "startTime": (begin + 10) * 1000000 + i * 1000,
                                  "duration": 2000000 - i * 10000, "tags": [],
                                  "references": [] if i == 0 else [{"refType": "CHILD_OF", "traceID": trace_id, "spanID": f"{i:016x}"}]})
                payload = {"data": [{"traceID": trace_id, "processes": processes, "spans": spans}]}
                self.send_response(200); self.end_headers(); self.wfile.write(json.dumps(payload).encode()); return
            if self.path.startswith("/logs/"):
                payload = {"status": "success", "data": {"resultType": "streams", "result": [{
                    "stream": {"namespace": "fixture", "cluster": "fixture", "component": "woodpecker", "pod": "wp-server-1"},
                    "values": [[str((begin + 11) * 1000000000), json.dumps({"msg": "add entry failed", "traceID": trace_id, "error": "sentinel-secret"})]]}]}}
                self.send_response(200); self.end_headers(); self.wfile.write(json.dumps(payload).encode()); return
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).get("query", [""])[0]
            selected = next((s for s in catalog if s["metric"] in query), None)
            result = []
            if selected:
                metric = selected["metric"]
                component = "querynode" if selected["component"] == "consumer" else selected["component"]
                labels = {"cluster": "fixture", "component": component, "instance": component + ":9091", "node_id": "node-1"}
                if selected["id"].startswith("milvus.query") and selected["id"] != "milvus.query.loading":
                    labels["query_type"] = "Search"
                labels.update(selected.get("match_labels", {}))
                labels.update({k: v.split("|")[0] for k, v in selected.get("regex_labels", {}).items()})
                if selected["id"].startswith("wp."):
                    labels.update(log_ns="fixture-tenant", log_id="9007199254740993")
                # The same metric family can serve append/read profiles. Match
                # requested fixture selectors instead of guessing the operation.
                for key, regex, escaped in re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)=(~?)"((?:[^"\\]|\\.)*)"', query):
                    if key != "__name__":
                        value = json.loads('"' + escaped + '"')
                        labels[key] = value.split("|")[0] if regex else value
                if "channel" in query:
                    labels["channel_name"] = "fixture-channel"
                if "interceptor" in metric:
                    labels["interceptor_name"] = "timetick"
                if selected["kind"] == "histogram":
                    count = [i * 20 for i in range(6)]
                    latency = 5000 if selected["id"] in ("milvus.append.quota", "milvus.query") else 10
                    factor = selected.get("metric_to_milliseconds", 1)
                    rows = [(metric + "_count", {}, count), (metric + "_sum", {}, [n * latency / factor for n in count])]
                    for bound in (10, 100, 1000, 10000):
                        rows.append((metric + "_bucket", {"le": str(bound / factor)}, count if bound >= latency else [0] * 6))
                    rows.append((metric + "_bucket", {"le": "+Inf"}, count))
                else:
                    rows = [(metric, {}, [0] * 6)]
                for name, extra, values in rows:
                    result.append({"metric": dict(labels, __name__=name, **extra), "values": [[begin + 5 + i * 10, str(value)] for i, value in enumerate(values)]})
            self.send_response(200)
            self.end_headers()
            try:
                self.wfile.write(json.dumps({"status": "success", "data": {"resultType": "matrix", "result": result}}).encode())
            except (BrokenPipeError, ConnectionResetError):
                pass

    backend = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=backend.serve_forever, daemon=True).start()
    endpoint = "http://127.0.0.1:" + str(backend.server_port)
    proc = None
    try:
        with tempfile.TemporaryDirectory(prefix="wpdiag-webui-") as folder:
            root = Path(folder)
            config = {"schema_version": "wpdiag.config/v1", "sources": {
                "prometheus": {"connections": {
                    "fixture": {"settings": {"endpoint": endpoint, "deployment": "fixture", "match_labels": {"cluster": "fixture"}}},
                    "slow": {"settings": {"endpoint": endpoint + "/slow", "deployment": "fixture", "match_labels": {"cluster": "fixture"}}},
                    "denied": {"settings": {"endpoint": endpoint + "/denied", "deployment": "fixture", "match_labels": {"cluster": "fixture"}}}}},
                "trace": {"connections": {
                    "unavailable": {"enabled": False, "settings": {}},
                    "fixture": {"settings": {"endpoint": endpoint + "/jaeger", "deployment": "fixture",
                        "services": {"proxy": "proxy", "streamingnode": "streamingnode", "wp-client": "wp_client", "wp-server": "wp_server"},
                        "search": {"service": "proxy"}}}}},
                "loki": {"connections": {"fixture": {"settings": {"endpoint": endpoint + "/logs", "deployment": "fixture", "match_labels": {"namespace": "fixture"}}}}}}}
            path = root / "wpdiag.yaml"
            path.write_text(json.dumps(config))
            proc = subprocess.Popen([binary, "serve", "--config", str(path), "--listen", "127.0.0.1:0"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            lines = queue.Queue()
            threading.Thread(target=lambda: [lines.put(line) for line in proc.stdout], daemon=True).start()
            deadline = time.monotonic() + 10
            url = None
            while time.monotonic() < deadline:
                try:
                    line = lines.get(timeout=0.2)
                    match = re.search(r"WebUI: (http://127\.0\.0\.1:\d+)", line)
                    if match:
                        url = match[1]
                        break
                except queue.Empty:
                    if proc.poll() is not None:
                        raise RuntimeError(proc.stderr.read())
            assert url, "No local WebUI startup URL"
            driver = Path(__file__).with_name("webui_browser.mjs")
            result = subprocess.run(["node", str(driver), url, str(begin), str(end), str(output)], capture_output=True, text=True, timeout=180)
            (output / "browser.stdout").write_text(result.stdout)
            (output / "browser.stderr").write_text(result.stderr)
            assert result.returncode == 0, result.stderr + result.stdout
            checks = json.loads(result.stdout)
            assert state["requests"] > 0, "No source collector was invoked"
            print(json.dumps({"passed": True, "browser": checks, "backend_requests": state["requests"]}, indent=2))
    finally:
        if proc:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
        backend.shutdown()
        backend.server_close()


if __name__ == "__main__":
    main()
