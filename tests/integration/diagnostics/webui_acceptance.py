#!/usr/bin/env python3
"""Deployed browser acceptance (#492); offline plan unless --execute is supplied."""
import argparse
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import signal
import subprocess
import threading
import time
import uuid

import cli_acceptance as shared
import source_acceptance as sources
import source_fixture
from webui_probe import Probe

CASES = {"metrics-write", "metrics-read", "append-missing-producer", "multi-source",
         "e2e-partial", "read-trace", "selected-unreachable", "selected-disabled",
         "unselected-unreachable", "transport-denied", "output-limit", "input-limit",
         "cancel-live", "cancel-inflight", "deadline-inflight"}
DRIVER = Path(__file__).with_name("webui_browser.mjs")


def checked_artifact(folder, name):
    path = folder / name
    if not name or path.resolve().parent != folder.resolve() or path.stat().st_size > 16 << 20:
        raise RuntimeError("Browser artifact is outside its owned folder or byte budget")
    return path


def verify_request(request, actual):
    for key in ("scope", "flow", "profile", "stage", "target"):
        if request["selection"].get(key, {}) != actual["selection"].get(key, {}):
            raise RuntimeError("Executed selection differs from browser: " + key)
    before, after = request["selection"]["window"], actual["selection"]["window"]
    if before["mode"] != after["mode"]:
        raise RuntimeError("Executed window mode differs")
    for key in ("from", "to"):
        if key in before and datetime.fromisoformat(before[key].replace("Z", "+00:00")) != datetime.fromisoformat(after[key].replace("Z", "+00:00")):
            raise RuntimeError("Executed historical interval differs")
    for key in ("duration_ms", "interval_ms"):
        if key in before and before[key] != after.get(key):
            raise RuntimeError("Executed live interval differs")
    for key in ("modules", "connections", "source_targets", "limits"):
        if request.get(key, {}) != actual.get(key, {}):
            raise RuntimeError("Executed request differs from browser: " + key)
    sources = request["selection"]["sources"]
    if sources and set(sources) != set(actual["selection"]["sources"]):
        raise RuntimeError("Executed sources differ")


def verify_stored_run(folder, row):
    raw = checked_artifact(folder, row["report"]).read_bytes()
    if hashlib.sha256(raw).hexdigest() != row["sha256"]:
        raise RuntimeError("Browser report bytes changed after collection")
    report = json.loads(raw)
    if row["status"] != report["status"]:
        raise RuntimeError("Browser and original report status differ")
    original = {f["id"] for f in report.get("summary", {}).get("findings", [])}
    rendered = set(row["rendered_ids"])
    if not rendered.issubset(original) or len(original) <= 25 and rendered != original:
        raise RuntimeError("Browser fabricated or omitted a finding identity")
    for child in report.get("runs", []):
        shared.lineage(child["report"])
    for evidence in row.get("evidence", []):
        child = next(c["report"] for c in report["runs"] if c["module_id"] == evidence["module"])
        original_observation = next(o for r in child["results"] for o in r.get("observations", []) if o["id"] == evidence["id"])
        actual = json.loads(checked_artifact(folder, evidence["file"]).read_bytes())
        if actual != original_observation:
            raise RuntimeError("Evidence viewer changed an original observation or integer")
    request = json.loads(checked_artifact(folder, row["request"]).read_bytes())
    plan = json.loads(checked_artifact(folder, row["plan"]).read_bytes())
    verify_request(request, plan["request"])
    compact = any(p["code"] == "report_output_limit" for p in report["plan"]["problems"])
    if not compact:
        verify_request(request, report["plan"]["request"])
    return report


def verify_browser(folder, result):
    """Validate original artifacts, not only the browser's success boolean."""
    if not result.get("passed") or result.get("uncaught_exceptions") != []:
        raise RuntimeError("Browser acceptance did not complete cleanly")
    rows = result["runs"]
    if len(rows) != len(CASES) or {r["case"] for r in rows} != CASES:
        raise RuntimeError("Browser case coverage is incomplete or duplicated")
    reports = {row["case"]: verify_stored_run(folder, row) for row in rows}
    metrics = reports["metrics-write"]
    observations = [o for c in metrics["runs"] for r in c["report"]["results"] for o in r.get("observations", [])]
    if metrics["status"] != "completed" or not any(o["kind"] == "prometheus_matrix" and o["status"] == "observed" and o.get("query", {}).get("executed") for o in observations):
        raise RuntimeError("No actual metrics collection in browser report")
    if not any(series.get("samples") for o in observations for series in o.get("data", {}).get("series", [])):
        raise RuntimeError("Metrics report contains no actual retained samples")
    if not any(f["severity"] == "unknown" for c in metrics["runs"] for f in sources.findings(c["report"])):
        raise RuntimeError("Small/absent metric populations lost their unknown state")
    multi = reports["multi-source"]
    if multi["status"] != "completed" or not multi["summary"]["associations"]:
        raise RuntimeError("No genuine multi-source browser association")
    error_id = result["associated_trace_id"]
    sources.positive_gate(multi, error_id)
    if reports["e2e-partial"]["summary"]["associations"] == [] or "partial_observed_path" not in json.dumps(reports["e2e-partial"]):
        raise RuntimeError("E2E partial producer coverage lost")
    expected = {"selected-unreachable": "partial", "selected-disabled": "partial", "unselected-unreachable": "completed",
                "transport-denied": "failed", "output-limit": "budget_exceeded", "input-limit": "budget_exceeded",
                "cancel-live": "cancelled", "cancel-inflight": "cancelled", "deadline-inflight": "budget_exceeded"}
    for name, status in expected.items():
        if reports[name]["status"] != status:
            raise RuntimeError("Wrong browser control status: " + name)
    live = next(r for r in rows if r["case"] == "cancel-live")
    if not any(e.get("module_id") == "prometheus" and e["status"] == "started" for e in live["progress_events"]):
        raise RuntimeError("No actual module progress before cancellation")
    metrics_raw = checked_artifact(folder, next(r["report"] for r in rows if r["case"] == "metrics-write")).read_bytes()
    if checked_artifact(folder, result["download"]).read_bytes() != metrics_raw:
        raise RuntimeError("Downloaded JSON is not the exact stored report")
    return reports


def cli_arguments(request, config, binary):
    s, limits = request["selection"], request["limits"]
    argv = [binary, "run", "--config", str(config), "--scope", s["scope"], "--flow", s["flow"],
            "--mode", s["window"]["mode"], "--from", s["window"]["from"], "--to", s["window"]["to"],
            "--budget", str(limits["budget_ms"]) + "ms", "--max-input-bytes", str(limits["max_input_bytes"]),
            "--max-output-bytes", str(limits["max_output_bytes"]), "--max-targets", str(limits["max_targets"]), "-o", "json"]
    if request.get("modules"):
        argv += ["--modules", ",".join(request["modules"])]
    else:
        argv += ["--sources", ",".join(s["sources"])]
    for source, name in request["connections"].items():
        argv += ["--connections", source + "=" + name]
    for source, target in request.get("source_targets", {}).items():
        for field, value in target.items():
            argv += ["--target", source + "." + field + "=" + value]
    if s.get("profile"):
        argv += ["--profile", s["profile"]]
    return argv


class BrowserServer:
    def __init__(self, binary, config, folder, env):
        self.binary, self.config, self.folder, self.env = binary, config, folder, env

    def __enter__(self):
        self.log = (self.folder / "server.log").open("w")
        self.process = None
        try:
            self.process = subprocess.Popen([self.binary, "serve", "--config", str(self.config), "--listen", "127.0.0.1:0"],
                stdout=subprocess.PIPE, stderr=self.log, text=True, env=self.env)
            lines = queue.Queue(maxsize=16)
            def read():
                for line in self.process.stdout:
                    try:
                        lines.put_nowait(line)
                    except queue.Full:
                        pass
            threading.Thread(target=read, daemon=True).start()
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                try:
                    line = lines.get(timeout=.1)
                except queue.Empty:
                    if self.process.poll() is not None:
                        break
                    continue
                match = re.search(r"WebUI: (http://127\.0\.0\.1:\d+)", line)
                if match:
                    self.url = match[1]
                    return self
            raise RuntimeError("WebUI startup failed; inspect owned server.log")
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *_):
        if self.process and self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)
        if self.process:
            self.process.stdout.close()
        self.log.close()


def run_browser(argv, env, timeout):
    """Own the driver process group, including Chrome, on timeout/interruption."""
    process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, env=env, start_new_session=True)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
        return subprocess.CompletedProcess(argv, process.returncode, stdout, stderr)
    finally:
        # Kill only this newly created group. Chrome's own normal shutdown is
        # handled by the driver; this also resolves early driver/timeout failures.
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=3)
        process.stdout.close()
        process.stderr.close()


class Run(sources.Run):
    def __init__(self, args):
        super().__init__(args)
        self.receipt["schema_version"] = "wpdiag.webui-acceptance/v1"
        self.receipt["browser_acceptance"] = {"passed": False, "issue": 492}
        self.checkpoint()

    def inspect_sources(self, endpoints):
        self.remaining(240)
        folder = self.out / "browser"
        folder.mkdir(mode=0o700)
        token, marker = "wp492-token-" + uuid.uuid4().hex, "wp492-error-" + uuid.uuid4().hex
        env = dict(os.environ, WPDIAG_WEBUI_ACCEPTANCE_TOKEN=token)
        if self.a.chrome:
            env["CHROME_BIN"] = self.a.chrome
        config = json.loads(self.config.read_text())
        browser_receipt = self.receipt["browser_acceptance"]
        browser_receipt["driver_sha256"] = hashlib.sha256(DRIVER.read_bytes()).hexdigest()
        with Probe(endpoints, token, marker) as probe:
            for source, value in config["sources"].items():
                settings = value["connections"]["acceptance"]["settings"]
                settings["endpoint"] = probe.url + "/normal/" + source
                settings["bearer_token_env"] = "WPDIAG_WEBUI_ACCEPTANCE_TOKEN"
                value["connections"]["unreachable"] = {"settings": dict(settings, endpoint="http://127.0.0.1:1")}
                value["connections"]["disabled"] = {"enabled": False, "settings": {}}
            settings = config["sources"]["prometheus"]["connections"]["acceptance"]["settings"]
            for mode in ("hold", "deny"):
                config["sources"]["prometheus"]["connections"][mode] = {"settings": dict(settings, endpoint=probe.url + "/" + mode + "/prometheus")}
            path = self.out / "browser-private-config.json"
            sources.save(path, config)
            # Reject remote binding using the shipped command, before startup.
            rejected = subprocess.run([self.a.wpdiag, "serve", "--config", str(path), "--listen", "0.0.0.0:0"],
                                      capture_output=True, text=True, timeout=5, env=env)
            if rejected.returncode == 0 or "loopback" not in rejected.stderr:
                raise RuntimeError("Non-loopback server binding was not rejected")
            manifest = {"from": self.start, "to": self.end, "deployment": self.owner,
                        "associated_trace_id": self.receipt["genuine_association"]["trace_id"],
                        "read_trace_id": next(r["trace_id"] for r in self.receipt["requests"] if r["case"] == "read"),
                        "probe": probe.url}
            manifest_path = self.out / "browser-input.json"
            sources.save(manifest_path, manifest)
            with BrowserServer(self.a.wpdiag, path, folder, env) as server:
                try:
                    result = run_browser(["node", str(DRIVER), server.url, str(manifest_path), str(folder)],
                                         env, timeout=min(210, self.remaining(20)))
                    for name, value in (("browser.stdout", result.stdout), ("browser.stderr", result.stderr)):
                        if len(value.encode()) > 2 << 20:
                            raise RuntimeError("Browser output budget exceeded")
                        (folder / name).write_text(value)
                    if result.returncode:
                        raise RuntimeError("Browser acceptance failed; inspect owned browser artifacts")
                    browser = json.loads(result.stdout)
                    reports = verify_browser(folder, browser)
                    browser_receipt["result"] = browser
                    # Compare separate CLI collections by owner semantics and
                    # actual retained spans/events, never by fresh UUIDs.
                    original_env = os.environ.get("WPDIAG_WEBUI_ACCEPTANCE_TOKEN")
                    os.environ["WPDIAG_WEBUI_ACCEPTANCE_TOKEN"] = token
                    try:
                        for name in ("metrics-write", "metrics-read", "multi-source", "e2e-partial", "read-trace"):
                            row = next(r for r in browser["runs"] if r["case"] == name)
                            request = json.loads((folder / row["request"]).read_text())
                            _, cli = self.invoke(cli_arguments(request, path, self.a.wpdiag))
                            sources.save(folder / (name + ".cli.json"), cli)
                            if cli["status"] != reports[name]["status"]:
                                raise RuntimeError("CLI/UI execution differs: " + name)
                            children = {c["module_id"]: c["report"] for c in cli["runs"]}
                            for child in reports[name]["runs"]:
                                sources.equivalent(child["report"], children[child["module_id"]])
                    finally:
                        if original_env is None:
                            os.environ.pop("WPDIAG_WEBUI_ACCEPTANCE_TOKEN", None)
                        else:
                            os.environ["WPDIAG_WEBUI_ACCEPTANCE_TOKEN"] = original_env
                    browser_receipt["cli_semantic_parity"] = True
                finally:
                    sources.save(folder / "transport.json", probe.snapshot())
            records = probe.snapshot()
            if not records or not all(r["authenticated"] for r in records):
                raise RuntimeError("Private environment credential was not used by actual collectors")
            if any(r.get("relay_failed") or r.get("hold_expired") for r in records):
                raise RuntimeError("Transport acceptance failed or left a held request")
            # Public browser/export artifacts must not expose config endpoints,
            # credentials or the synthetic raw backend error marker.
            private = [token.encode(), marker.encode(), probe.url.encode()] + [v.encode() for v in endpoints.values()]
            for file in folder.iterdir():
                if file.is_file() and file.name not in ("transport.json", "server.log", "chrome.stderr"):
                    raw = file.read_bytes()
                    if any(value in raw for value in private):
                        raise RuntimeError("Private value leaked into browser/export artifact: " + file.name)
            size = sum(f.stat().st_size for f in self.out.rglob("*") if f.is_file())
            if size > 96 << 20:
                raise RuntimeError("Whole source/browser artifact budget exceeded")
            browser_receipt.update(passed=True, transport_requests=len(records), artifact_bytes=size,
                                   remote_bind_rejected=True, credentials_and_error_body_withheld=True)
            self.checkpoint()
            print(json.dumps({"stage": "deployed-browser", "passed": True, "runs": len(browser["runs"]),
                              "checks": len(browser["checks"]), "transport_requests": len(records)}), flush=True)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--context", required=True)
    p.add_argument("--namespace", required=True)
    p.add_argument("--output", type=Path, default=Path("/tmp/wp-webui-492"))
    p.add_argument("--wpdiag", default=str(shared.ROOT / "bin/wpdiag"))
    p.add_argument("--chrome", help="Chrome executable; otherwise CHROME_BIN or standard locations")
    p.add_argument("--budget", type=int, default=600)
    p.add_argument("--execute", action="store_true")
    a = p.parse_args(argv)
    if not 360 <= a.budget <= 900:
        p.error("budget must be 360..900 seconds")
    if not a.execute:
        print(json.dumps({"execute": False, "issue": 492, "context": a.context, "namespace": a.namespace,
            "requires_namespace_label": source_fixture.LABEL + "=" + source_fixture.PROFILE,
            "reuses": "source_acceptance.Run and its exclusive fixture/lock/cleanup/recovery journal",
            "images": source_fixture.IMAGES, "browser": "Node 22+ and Chrome; shipped wpdiag serve",
            "cases": sorted(CASES), "cluster_lifecycle": "external; no current-context changes",
            "limits": {"work_seconds": a.budget, "browser_seconds": 210, "artifact_bytes": 96 << 20},
            "controls": "normal backend bytes unchanged; explicit held-return and synthetic 401 privacy controls"}, indent=2))
        return 0
    def interrupted(*_):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, interrupted)
    return Run(a).run()


if __name__ == "__main__":
    raise SystemExit(main())
