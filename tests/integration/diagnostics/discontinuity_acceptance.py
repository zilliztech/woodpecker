#!/usr/bin/env python3
"""Actual owned-process restart/reset and scrape-gap acceptance (#493). Plan by default."""
import argparse
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import time
import urllib.parse

import cli_acceptance as shared
import source_acceptance as sources
import source_fixture as fixture

CLIENT_IMAGE = "wp-source-client-493:local"
GAP_SECONDS = 40
MAX_GAP_MS = 30000  # Existing owner default, never overridden by this harness.


def one_restart(before, after):
    if before["uid"] != after["uid"] or before["spec_sha256"] != after["spec_sha256"]:
        raise RuntimeError("Owned Pod identity/configuration changed")
    old = {c["name"]: c for c in before["containers"]}
    new = {c["name"]: c for c in after["containers"]}
    if "woodpecker" not in old or set(old) != set(new):
        raise RuntimeError("Fixture container set changed")
    for name, prior in old.items():
        current = new[name]
        if name != "woodpecker":
            if current != prior:
                raise RuntimeError("Unselected fixture process changed: " + name)
        elif (current["image_id"] != prior["image_id"] or current["image"] != prior["image"] or
              current["restarts"] != prior["restarts"] + 1 or not current["container_id"] or
              current["container_id"] == prior["container_id"]):
            raise RuntimeError("Expected exactly one actual WP container restart")


def read_samples(report):
    points = []
    for result in report.get("results", []):
        for observation in result.get("observations", []):
            if observation.get("kind") != "prometheus_matrix" or observation.get("status") != "observed":
                continue
            for series in observation.get("data", {}).get("series", []):
                labels = series["labels"]
                if labels.get("__name__", "").endswith("_count") and labels.get("operation") == "get_batch_entries" and labels.get("status") == "success":
                    points.append({"series_id": series["series_id"], "labels": labels, "samples": series["samples"]})
    if len(points) != 1 or len(points[0]["samples"]) < 2:
        raise RuntimeError("Actual single-population read counter samples are absent")
    return points[0]


def discontinuity(report, code):
    rows = [f for f in sources.findings(report) if f["rule_id"] == "wp.wal_read.latency"]
    if not rows or not any(code in f["unknown"] for f in rows):
        raise RuntimeError("Required discontinuity not recognized: " + code)
    if any(f["severity"] != "unknown" or f["outcome"] != "inconclusive" for f in rows):
        raise RuntimeError("Discontinuous evidence became a healthy or latency verdict")
    series = read_samples(report)
    pairs = list(zip(series["samples"], series["samples"][1:]))
    resets = [(a, b) for a, b in pairs if b["value"] < a["value"]]
    gaps = [(a, b) for a, b in pairs if b["time_ms"] - a["time_ms"] > MAX_GAP_MS]
    if code == "counter_reset_or_restart" and (not resets or gaps):
        raise RuntimeError("Reset proof requires actual counter decrease without a masking scrape gap")
    if code == "scrape_gap" and (not gaps or resets):
        raise RuntimeError("Gap proof requires actual missing samples without another counter reset")
    shared.lineage(report)
    return {"code": code, "series_id": series["series_id"], "samples": len(series["samples"]),
            "reset_pairs": resets, "gap_pairs": gaps, "original_finding_ids": [f["id"] for f in rows]}


class OwnedFixture(fixture.Fixture):
    def setup(self, images=None):
        super().setup(dict(fixture.IMAGES, client=CLIENT_IMAGE))

    def objects(self, images=None):
        objects = super().objects(images)
        # Only this disposable Pod shares its process namespace. WP is not the
        # namespace init, whose STOP/KILL signals can otherwise be ignored.
        objects[-1]["spec"]["shareProcessNamespace"] = True
        return objects


class Run(sources.Run):
    def __init__(self, args):
        super().__init__(args)
        self.receipt["schema_version"] = "wpdiag.discontinuity-acceptance/v1"
        self.receipt["actions"] = []
        self.fixture = OwnedFixture(args.context, args.namespace, self.out, self.receipt)
        self.fixture.deadline = self.deadline

    def checked_pod(self):
        self.fixture.namespace_check()
        lock = self.fixture.command(["get", "configmap", fixture.LOCK, "-o", "json"])
        self.fixture.original("configmap/" + fixture.LOCK, lock)
        pod = self.fixture.command(["get", "pod", self.owner, "-o", "json"])
        self.fixture.original("pod/" + self.owner, pod)
        return pod

    @staticmethod
    def process(pod):
        row = next(c for c in pod["status"]["containerStatuses"] if c["name"] == "woodpecker")
        return {"pod_uid": pod["metadata"]["uid"], "container_id": row["containerID"],
                "restarts": row["restartCount"], "state": row["state"], "last_state": row.get("lastState", {})}

    def runtime_process(self, pod, restore=False):
        target = self.process(pod)
        script = 'for f in /proc/[0-9]*/exe; do if [ "$(readlink "$f")" = /woodpecker/bin/woodpecker ]; then p="${f%/exe}"; cat "$p/stat"; cat "$p/cmdline"; fi; done'
        p = subprocess.run(self.fixture.kube + ["exec", self.owner, "-c", "woodpecker", "--", "/bin/sh", "-c", script],
                           capture_output=True, timeout=10 if restore else min(10, self.remaining()))
        if p.returncode or p.stdout.count(b"\n") != 1:
            raise RuntimeError("Expected exactly one identifiable owned WP process")
        stat, raw_argv = p.stdout.split(b"\n", 1)
        fields = stat.decode().split()
        argv = raw_argv.rstrip(b"\0").decode().split("\0")
        if (len(fields) < 22 or not fields[0].isdigit() or int(fields[0]) <= 1 or not fields[21].isdigit() or
            argv[:2] != ["/woodpecker/bin/woodpecker", "server"] or
            "--node-name" not in argv or argv[argv.index("--node-name") + 1] != self.owner or
            "--cluster" not in argv or argv[argv.index("--cluster") + 1] != self.owner):
            raise RuntimeError("PID/argv does not identify the owned non-init WP process")
        target.update(pid=int(fields[0]), proc_start_ticks=fields[21], argv_sha256=hashlib.sha256(raw_argv).hexdigest())
        return target

    def send_signal(self, name, expected=None, restore=False):
        if name not in ("KILL", "STOP", "CONT") or restore and name != "CONT":
            raise ValueError("Unregistered process action")
        previous = self.fixture.deadline
        if restore:
            self.fixture.deadline = None  # Separate bounded restoration calls.
        try:
            target = self.runtime_process(self.checked_pod(), restore=restore)
            if expected and any(target.get(k) != expected.get(k) for k in ("container_id", "pid", "proc_start_ticks")):
                raise RuntimeError("Process replaced; refusing to signal a different instance")
            action = {"signal": name, "intent_at": shared.utc(), "target": target, "outcome": "pending"}
            self.receipt["actions"].append(action)
            self.checkpoint()
            self.fixture.checkpoint()  # Same journal is recoverable after ambiguous exec.
            timeout = 10 if restore else min(10, self.remaining())
            try:
                pid, ticks = target["pid"], target["proc_start_ticks"]
                script = ('set -- $(cat /proc/' + str(pid) + '/stat); [ "${22}" = ' + ticks + ' ] || exit 75; '
                          '[ "$(readlink /proc/' + str(pid) + '/exe)" = /woodpecker/bin/woodpecker ] || exit 76; '
                          'kill -' + name + ' ' + str(pid))
                p = subprocess.run(self.fixture.kube + ["exec", self.owner, "-c", "woodpecker", "--",
                    "/bin/sh", "-c", script], capture_output=True, text=True, timeout=timeout)
                action.update(outcome="returned", exit_code=p.returncode)
            except subprocess.TimeoutExpired:
                action["outcome"] = "ambiguous_timeout"
            action["returned_at"] = shared.utc()
            self.checkpoint()
            return target
        finally:
            self.fixture.deadline = previous

    def verify_values(self, name):
        argv = self.fixture.kube + ["exec", self.owner, "-c", "client", "--", "/source-client",
            "--endpoint", "127.0.0.1:18080", "--otlp", "127.0.0.1:4318", "--owner", self.owner, "--execute", "--verify-only"]
        p = subprocess.run(argv, capture_output=True, text=True, timeout=min(60, self.remaining()))
        records = [json.loads(line) for line in p.stdout.splitlines()]
        sources.save(self.out / (name + ".requests.json"), {"exit_code": p.returncode, "records": records})
        if p.returncode or [r["case"] for r in records] != ["read", "eof"] or not all(r["verified"] for r in records):
            raise RuntimeError("Original acknowledged values/EOF could not be verified: " + name)
        return records

    def wait_ready(self, restart=False):
        deadline = min(time.monotonic() + 90, self.deadline)
        while time.monotonic() < deadline:
            pod = self.checked_pod()
            rows = pod.get("status", {}).get("containerStatuses", [])
            if len(rows) == 7 and all(c.get("ready") for c in rows):
                if not restart or self.process(pod)["container_id"] != self.receipt["restart_before"]["container_id"]:
                    return pod
            self.pause(.5)
        raise RuntimeError("Owned process readiness/restart deadline exceeded")

    def metric_report(self, name, start, end, code=None):
        self.start, self.end = start, end
        result = self.diagnose(name, ["prometheus"], "read")
        report = result["runs"][0]["report"]
        proof = discontinuity(report, code) if code else read_samples(report)
        self.receipt.setdefault("discontinuities", {})[name] = proof
        self.checkpoint()
        return report

    def inspect_sources(self, endpoints):
        self.remaining(210)
        self.receipt["original_source_window"] = {"from": self.start, "to": self.end}
        start = shared.utc()
        for i in range(3):
            self.verify_values("warm-" + str(i))
            self.pause(2)
        self.pause(4)
        self.receipt["restart_before"] = self.runtime_process(self.checked_pod())
        self.send_signal("KILL", self.receipt["restart_before"])
        after = self.wait_ready(restart=True)
        self.receipt["restart_after"] = self.runtime_process(after)
        one_restart(self.receipt["pod_before"], fixture.pod_identity(after))
        self.verify_values("restart-recovery")
        self.pause(6)
        reset_end = shared.utc()
        self.metric_report("actual-reset", start, reset_end, "counter_reset_or_restart")

        # The next window begins after restart; its process stays unchanged.
        gap_start = shared.utc()
        for i in range(2):
            self.verify_values("gap-warm-" + str(i))
            self.pause(2)
        self.pause(4)
        target = self.runtime_process(self.checked_pod())
        self.receipt["pause"] = {"intent_at": shared.utc(), "target": target, "restored": False,
                                 "duration_seconds": GAP_SECONDS}
        self.checkpoint()
        try:
            self.send_signal("STOP", target)
            began = time.monotonic()
            self.pause(8)
            query = 'up{woodpecker_id=' + json.dumps(self.owner) + '}'
            down = shared.http(endpoints["prometheus"], "/api/v1/query?" + urllib.parse.urlencode({"query": query}))
            sources.save(self.out / "paused-exporter-up.json", down)
            if len(down["data"]["result"]) != 1 or down["data"]["result"][0]["value"][1] != "0":
                raise RuntimeError("Actual stopped exporter was not observed as up=0")
            self.pause(max(0, GAP_SECONDS - (time.monotonic() - began)))
        finally:
            with shared.cleanup_signals():
                self.send_signal("CONT", target, restore=True)
        ready = self.wait_ready()
        if self.process(ready)["container_id"] != target["container_id"]:
            raise RuntimeError("Gap unexpectedly restarted the selected process")
        self.verify_values("gap-recovery")
        self.pause(6)
        query = 'up{woodpecker_id=' + json.dumps(self.owner) + '}'
        up = shared.http(endpoints["prometheus"], "/api/v1/query?" + urllib.parse.urlencode({"query": query}))
        sources.save(self.out / "recovered-exporter-up.json", up)
        if len(up["data"]["result"]) != 1 or up["data"]["result"][0]["value"][1] != "1":
            raise RuntimeError("Recovered exporter did not return to up=1")
        self.receipt["pause"].update(restored=True, restored_at=shared.utc(), process_after=self.runtime_process(ready))
        gap_end = shared.utc()
        self.metric_report("actual-gap", gap_start, gap_end, "scrape_gap")
        # Recovery cannot retroactively clear either retained incident.
        historical = self.metric_report("reset-after-recovery", start, reset_end, "counter_reset_or_restart")
        old = json.loads((self.out / "actual-reset.prometheus.json").read_text())
        sources.equivalent(old, historical)
        now_start = shared.utc()
        self.verify_values("current-recovery")
        self.pause(6)
        current = self.metric_report("current-after-recovery", now_start, shared.utc())
        if any(code in f["unknown"] for f in sources.findings(current) for code in ("scrape_gap", "counter_reset_or_restart")):
            raise RuntimeError("Current recovery unexpectedly shares the old discontinuity")
        self.receipt["current_process"] = self.runtime_process(self.checked_pod())
        self.receipt["checks"].append({"case": "owned-process-discontinuities", "reset": True, "gap": True,
                                       "current_cannot_clear_history": True, "acknowledged_values_verified": True})
        self.checkpoint()

    def verify_fixture_identity(self):
        one_restart(self.receipt["pod_before"], self.receipt["pod_after"])
        if not self.receipt.get("pause", {}).get("restored"):
            raise RuntimeError("Owned pause restoration not verified")


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--context", required=True)
    p.add_argument("--namespace", required=True)
    p.add_argument("--output", type=Path, default=Path("/tmp/wp-discontinuity-493"))
    p.add_argument("--wpdiag", default=str(shared.ROOT / "bin/wpdiag"))
    p.add_argument("--budget", type=int, default=600)
    p.add_argument("--execute", action="store_true")
    a = p.parse_args(argv)
    if not 360 <= a.budget <= 900:
        p.error("budget must be 360..900 seconds")
    if not a.execute:
        print(json.dumps({"execute": False, "issue": 493, "context": a.context, "namespace": a.namespace,
            "requires_namespace_label": fixture.LABEL + "=" + fixture.PROFILE,
            "images": dict(fixture.IMAGES, client=CLIENT_IMAGE), "reuses": "#491 source fixture/lock/cleanup/recovery",
            "actions": ["one KILL/restart of owned WP only", "40-second STOP/CONT of same recovered WP only"],
            "process_scope": "shared PID namespace inside owned Pod only; never hostPID; verify PID/start ticks and argv",
            "restoration": "journal original Pod/container identities before signals; verify CONT and exact payloads; UID-safe cleanup",
            "cluster_lifecycle": "external; no current-context change", "work_budget_seconds": a.budget}, indent=2))
        return 0
    def interrupted(*_):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, interrupted)
    return Run(a).run()


if __name__ == "__main__":
    raise SystemExit(main())
