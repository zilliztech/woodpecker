"""Small notebook adapter for the existing stack CLI; owns only its own forwards."""
import atexit
from datetime import datetime
import hashlib
import html
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.request


class StackLab:
    def __init__(self, root, profile, context, namespace, ports, local_arm64=True):
        self.root = Path(root).resolve()
        self.state = Path(os.getenv("STATE", str(self.root / ".local"))).expanduser().resolve()
        self.profile, self.context, self.namespace = profile, context, namespace
        self.ports = ports.copy()
        self.flags = ["--local-arm64"] if local_arm64 else []
        self.env = dict(os.environ, PROFILE=profile, CONTEXT=context, NAMESPACE=namespace,
                        STATE=str(self.state), **{key + "_PORT": str(value) for key, value in ports.items()})
        # Port settings are authoritative even if the launching shell had URLs.
        self.env.update({key + "_URL": "http://127.0.0.1:" + str(value) for key, value in ports.items()})
        self.forward_process = None
        self.forward_log = None
        atexit.register(self.close_forwards)

    def snapshot(self):
        source = (self.root / "stack.sh").read_bytes()
        path = self.root / (".stack-notebook-" + hashlib.sha256(source).hexdigest()[:16] + ".sh")
        # Bash reads a running script incrementally. Never overwrite a snapshot.
        if not path.exists():
            try:
                with path.open("xb") as stream:
                    stream.write(source)
                path.chmod(0o755)
            except FileExistsError:
                pass
        if path.read_bytes() != source:
            raise RuntimeError("Incomplete or modified CLI snapshot; retry after the other writer finishes")
        return path

    @staticmethod
    def terminate(process):
        if process is None or process.poll() is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)

    def run(self, action, env=None, check=True):
        args = ["bash", str(self.snapshot()), action]
        if action in ("render", "images", "deploy"):
            args += self.flags
        print("./stack.sh " + " ".join(args[2:]), flush=True)
        log_dir = self.state / "notebook"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / (str(time.time_ns()) + "-" + action + ".log")
        process = subprocess.Popen(args, cwd=self.root, env=dict(self.env, **(env or {})),
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                   start_new_session=True)
        try:
            with log_path.open("w") as log:
                for line in process.stdout:
                    print(line, end="", flush=True)
                    log.write(line)
                    log.flush()
            code = process.wait()
        except BaseException:
            self.terminate(process)
            raise
        finally:
            process.stdout.close()
        if check and code:
            print("Command log:", log_path, flush=True)
            raise subprocess.CalledProcessError(code, args)
        return code

    @staticmethod
    def occupied(port):
        with socket.socket() as sock:
            sock.settimeout(.3)
            return sock.connect_ex(("127.0.0.1", port)) == 0

    def ready(self):
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        checks = (("GRAFANA", "/api/health", None), ("PROMETHEUS", "/-/ready", None),
                  ("ALERTMANAGER", "/-/ready", None), ("MILVUS", "/v2/vectordb/collections/list", b"{}"))
        try:
            for service, path, data in checks:
                req = urllib.request.Request(self.env[service + "_URL"] + path, data=data,
                                             headers={"Content-Type": "application/json"})
                with opener.open(req, timeout=2) as response:
                    body = response.read()
                    if service == "MILVUS" and json.loads(body).get("code") != 0:
                        return False
            return True
        except Exception:
            return False

    def open_forwards(self, timeout=180):
        if self.forward_process and self.forward_process.poll() is None:
            if self.ready():
                print("Reusing this notebook's healthy forwards.")
                return
            self.close_forwards()
        used = [port for port in self.ports.values() if self.occupied(port)]
        if used:
            if len(used) == len(self.ports) and self.ready():
                print("Reusing existing healthy endpoints. Their forward processes remain externally owned.")
                return
            raise RuntimeError(f"Ports already occupied: {used}. Use four free ports or restore your existing forward process.")
        folder = self.state / "notebook"
        folder.mkdir(parents=True, exist_ok=True)
        self.forward_log = (folder / "forward.log").open("a")
        self.forward_process = subprocess.Popen(["bash", str(self.snapshot()), "forward"], cwd=self.root,
                                                env=self.env, stdout=self.forward_log, stderr=subprocess.STDOUT,
                                                start_new_session=True)
        try:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if self.forward_process.poll() is not None:
                    raise RuntimeError("Forward process exited; inspect .local/notebook/forward.log")
                if self.ready():
                    print("All four endpoints ready; this notebook owns these forwards.")
                    return
                time.sleep(1)
            raise TimeoutError("Endpoint readiness timed out; inspect .local/notebook/forward.log")
        except BaseException:
            self.close_forwards()
            raise

    def close_forwards(self):
        self.terminate(self.forward_process)
        self.forward_process = None
        if self.forward_log:
            self.forward_log.close()
            self.forward_log = None

    def get_json(self, service, path):
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(self.env[service + "_URL"] + path, timeout=5) as response:
            return json.load(response)

    def wait_monitoring(self, timeout=180):
        """Wait through rollout/scrape transitions; persistent errors still fail."""
        deadline = time.monotonic() + timeout
        consecutive = 0
        last = None
        expected_ids = {f"{prefix}{i}" for prefix, count in (("R", 7), ("F", 9), ("S", 6), ("C", 9)) for i in range(1, count + 1)}
        while time.monotonic() < deadline:
            try:
                targets = self.get_json("PROMETHEUS", "/api/v1/targets")["data"]["activeTargets"]
                targets = [t for t in targets if t["labels"].get("namespace") == self.namespace
                           and t["labels"].get("woodpecker_id") == "milvus"]
                servers = [t for t in targets if t["labels"].get("component") == "woodpecker"]
                groups = self.get_json("PROMETHEUS", "/api/v1/rules")["data"]["groups"]
                rules = [r for group in groups if group["name"] == "woodpecker-full-stack" for r in group["rules"]]
                ids = {r.get("labels", {}).get("rule_id") for r in rules if r["type"] == "alerting"}
                errors = [{"name": r["name"], "error": r.get("lastError", r.get("health"))}
                          for r in rules if r.get("health") != "ok" or r.get("lastError")]
                healthy = (len(targets) == 8 and len(servers) == 3 and
                           all(t["health"] == "up" for t in targets) and ids == expected_ids and not errors)
                consecutive = consecutive + 1 if healthy else 0
                last = {"targets_up": sum(t["health"] == "up" for t in targets), "targets": len(targets),
                        "reference_rules": len(ids), "rule_errors": errors}
                if consecutive >= 2:
                    print("Monitoring ready: 8 targets UP and all reference rules healthy on two consecutive checks.")
                    return
            except Exception as error:
                consecutive = 0
                last = {"error": str(error)}
            print("Waiting for monitoring convergence:", last, flush=True)
            time.sleep(5)
        raise TimeoutError("Monitoring did not become ready: " + str(last))

    def latest_perf(self):
        files = list((self.state / "perf").glob("*/report.json"))
        if not files:
            raise FileNotFoundError("Run the perf cell first")
        return max(files, key=lambda path: path.stat().st_mtime)


def show_report(path):
    """Render a readable summary and bounded latency plot without extra dependencies."""
    from IPython.display import HTML, Markdown, SVG, display
    path = Path(path)
    report = json.loads(path.read_text())
    display(Markdown(f"**Run:** `{report['run_id']}` · **Passed:** `{report['passed']}` · **Collection:** `{report['collection']}`"))
    measured = report.get("phases", {}).get("measure", {})
    table = [(key, measured.get(key)) for key in ("acknowledged_rows", "acknowledged_rows_per_second", "failed_requests")]
    table += [("insert_" + key + "_ms", value) for key, value in (measured.get("success_latency_ms") or {}).items() if key in ("p50", "p95", "p99")]
    display(HTML("<table>" + "".join("<tr><th>" + html.escape(key) + "</th><td>" + html.escape(str(value)) + "</td></tr>" for key, value in table) + "</table>"))
    for name, url in report["dashboards"].items():
        display(HTML('<a target="_blank" href="' + html.escape(url, quote=True) + '">' + html.escape(name) + ' — test time window</a>'))
    records = [json.loads(line) for line in (path.parent / "requests.jsonl").read_text().splitlines()]
    records = [r for r in records if r["phase"] == "measure"]
    if records:
        first = min(datetime.fromisoformat(r["at"]).timestamp() for r in records)
        points = [(datetime.fromisoformat(r["at"]).timestamp() - first, r["latency_ms"], r["ok"]) for r in records]
        duration = max(1, max(p[0] for p in points))
        upper = max(1, max(p[1] for p in points))
        circles = "".join(f'<circle cx="{60+x/duration*690:.1f}" cy="{170-y/upper*130:.1f}" r="2" fill="{"#2b8a5a" if ok else "#cf3b35"}"/>' for x, y, ok in points[::max(1, len(points)//1000)])
        display(SVG(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 210"><rect width="800" height="210" fill="white"/><text x="20" y="20" fill="black">Insert latency (ms), measurement phase; red = failed requests</text><path d="M60 35 V170 H760" fill="none" stroke="#777"/><text x="4" y="48" fill="black">{upper:.1f}</text><text x="40" y="185" fill="black">0</text><text x="670" y="195" fill="black">{duration:.1f} seconds</text>{circles}</svg>'))
    display(Markdown("**Failures:** " + str(report["failures"]) + "\n\n**Coverage gaps:** " + str(report["coverage_gaps"])))
    return report
