"""Journaled in-place MinIO CPU quota for an explicitly disposable namespace."""
import copy
import hashlib
import json
import re
import time

import cli_acceptance as shared

PROFILE = "cli-persistence-v1"
CPU = "25m"


def quota_resources(resources):
    value = copy.deepcopy(resources)
    value.setdefault("limits", {})["cpu"] = CPU
    value.setdefault("requests", {})["cpu"] = CPU
    return value


def resources_applied(row, wanted):
    actual = row.get("applied_resources") or {}
    # Status.resources can retain the entire earlier CPU allocation while the
    # kernel enforces a new quota. Verify cpu.max, not a desired-state assertion.
    memory_requests = lambda value: {k: v for k, v in value.get("requests", {}).items() if k != "cpu"}
    memory_limits = lambda value: {k: v for k, v in value.get("limits", {}).items() if k != "cpu"}
    cpu = wanted.get("limits", {}).get("cpu")
    target = float(cpu[:-1]) if cpu and cpu.endswith("m") else float(cpu) * 1000 if cpu else None
    return row["resources"] == wanted and row.get("kernel_cpu_available") is True and row.get("kernel_cpu_millicores") == target \
        and memory_limits(actual) == memory_limits(wanted) and memory_requests(actual) == memory_requests(wanted)


def fingerprint(value):
    spec = dict(value["spec"])
    spec.pop("replicas", None)
    return hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()


class StorageFault:
    def __init__(self, kube, name, output, remaining=None):
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", name):
            raise RuntimeError("Explicit bounded StatefulSet name required")
        self.kube, self.name, self.output = kube, name, output
        self.remaining = remaining or (lambda cap=30: cap)
        self.before, self.changed, self.restored = None, False, False

    def command(self, args):
        return shared.command(self.kube + args, timeout=self.remaining(15))

    def read(self):
        value = json.loads(self.command(["get", "statefulset", self.name, "-o", "json"]))
        if value["metadata"].get("deletionTimestamp") or not value["spec"].get("volumeClaimTemplates"):
            raise RuntimeError("Storage target must be an existing PVC-backed StatefulSet")
        if value["spec"].get("persistentVolumeClaimRetentionPolicy", {}).get("whenScaled", "Retain") != "Retain":
            raise RuntimeError("Scale-down must retain persistent claims")
        replicas = value["spec"].get("replicas", 1)
        if type(replicas) is not int:
            raise RuntimeError("Invalid replica count")
        claims = []
        for template in value["spec"]["volumeClaimTemplates"]:
            name = template["metadata"]["name"] + "-" + self.name + "-0"
            pvc = json.loads(self.command(["get", "pvc", name, "-o", "json"]))
            if pvc["metadata"].get("deletionTimestamp") or pvc.get("status", {}).get("phase") != "Bound":
                raise RuntimeError("Original storage claims must remain Bound")
            claims.append({"name": name, "uid": pvc["metadata"]["uid"], "volume": pvc["spec"]["volumeName"]})
        pods = json.loads(self.command(["get", "pods", "-l", "statefulset.kubernetes.io/pod-name=" + self.name + "-0", "-o", "json"]))["items"]
        for pod in pods:
            if not any(o.get("uid") == value["metadata"]["uid"] and o.get("controller") for o in pod["metadata"].get("ownerReferences", [])):
                raise RuntimeError("Storage Pod ownership differs from selected controller")
        if replicas != 1 or len(pods) != 1 or len(pods[0]["spec"].get("containers", [])) != 1:
            raise RuntimeError("One original storage Pod/container required; replicas must not change")
        pod = pods[0]
        statuses = pod.get("status", {}).get("containerStatuses", [])
        if len(statuses) != 1:
            raise RuntimeError("Actual storage container resource status required")
        kernel = {"kernel_cpu_available": False}
        try:
            raw = self.command(["exec", pod["metadata"]["name"], "--", "cat", "/sys/fs/cgroup/cpu.max"]).strip()
            quota, period = raw.split()
            period = int(period)
            if period <= 0 or quota != "max" and int(quota) <= 0:
                raise ValueError("Invalid CPU quota")
            kernel = {"kernel_cpu_available": True, "kernel_cpu_max": raw, "kernel_cpu_millicores": None if quota == "max" else int(quota) * 1000 / period,
                      "kernel_cpu_observed_at": shared.utc()}
        except (RuntimeError, ValueError):
            pass  # Restoration may proceed before an affected container can exec.
        return {"name": self.name, "uid": value["metadata"]["uid"], "replicas": replicas,
            **kernel,
            "spec_sha256": fingerprint(value), "claims": sorted(claims, key=lambda c: c["name"]),
            "resources": pod["spec"]["containers"][0].get("resources", {}), "applied_resources": statuses[0].get("resources"),
            "pods": [{"name": p["metadata"]["name"], "uid": p["metadata"]["uid"],
                      "terminating": bool(p["metadata"].get("deletionTimestamp")),
                      "containers": [{k: c.get(k) for k in ("name", "image", "imageID", "restartCount")} for c in p.get("status", {}).get("containerStatuses", [])],
                      "ready": any(c["type"] == "Ready" and c["status"] == "True" for c in p.get("status", {}).get("conditions", []))} for p in pods]}

    def journal(self, **extra):
        shared.save(self.output / "storage-restoration.json", {"schema_version": "wpdiag.storage-resize/v1", "profile": PROFILE,
            "before": self.before, "fault_resources": self.desired() if self.before else None,
            "mutation_started": self.changed, "restored": self.restored, **extra})

    def desired(self):
        return quota_resources(self.before["resources"])

    def prepare(self):
        before = self.read()
        if before["replicas"] != 1 or len(before["pods"]) != 1 or not before["pods"][0]["ready"] or before["pods"][0]["terminating"]:
            raise RuntimeError("Profile requires one Ready storage replica")
        discovery = json.loads(shared.command(self.kube[:3] + ["get", "--raw", "/api/v1"]))
        if not any(r["name"] == "pods/resize" and "patch" in r["verbs"] for r in discovery["resources"]):
            raise RuntimeError("This profile requires existing in-place Pod CPU resize support")
        if not resources_applied(before, before["resources"]) or before["resources"] == quota_resources(before["resources"]):
            raise RuntimeError("Storage baseline has pending resize or is already at the fault quota")
        if not before["resources"].get("limits", {}).get("cpu"):
            raise RuntimeError("Explicit baseline CPU limit required: Kubernetes forbids removing a limit added by resize")
        self.before = before
        self.restored = True
        self.journal()  # A journal exists before workload creation or injection.

    def check(self, row):
        if not self.before or any(row[k] != self.before[k] for k in ("name", "uid", "spec_sha256", "claims")):
            raise RuntimeError("Storage controller/spec/claim ownership changed; refusing mutation")
        original, current = self.before["pods"][0], row["pods"][0]
        identities = lambda containers: [{k: c.get(k) for k in ("name", "image", "imageID")} for c in containers]
        if row["replicas"] != 1 or current["uid"] != original["uid"] or identities(current["containers"]) != identities(original["containers"]):
            raise RuntimeError("Storage Pod/image/restart or replica identity changed; refusing mutation")
        if row["resources"] != self.before["resources"] and row["resources"] != self.desired():
            raise RuntimeError("External storage resource change; refusing mutation")

    def wait(self, predicate, seconds=90):
        end = time.monotonic() + seconds
        while True:
            row = self.read()
            self.check(row)
            if predicate(row):
                return row
            if time.monotonic() >= end:
                raise RuntimeError("Storage state did not converge before its deadline")
            time.sleep(.5)

    def resize(self, current, desired):
        operations = [{"op": "test", "path": "/metadata/uid", "value": self.before["pods"][0]["uid"]},
                      {"op": "test", "path": "/spec/containers/0/resources", "value": current},
                      {"op": "replace", "path": "/spec/containers/0/resources", "value": desired}]
        self.command(["patch", "pod", self.before["pods"][0]["name"], "--subresource=resize", "--type=json", "-p", json.dumps(operations)])

    def apply(self):
        row = self.read()
        self.check(row)
        if row["pods"] != self.before["pods"] or row["resources"] != self.before["resources"]:
            raise RuntimeError("Storage baseline changed before injection")
        self.changed, self.restored = True, False
        self.journal()  # Even a failed scale call may have committed.
        self.resize(self.before["resources"], self.desired())
        row = self.wait(lambda r: resources_applied(r, self.desired()) and r["pods"][0]["ready"])
        self.journal(applied=row, applied_at=shared.utc())
        return row

    def restore(self):
        row = self.read()
        self.check(row)
        if row["resources"] == self.desired():
            if not self.changed:
                raise RuntimeError("Storage changed before this run injected a fault")
            self.resize(self.desired(), self.before["resources"])
        row = self.wait(lambda r: resources_applied(r, self.before["resources"]) and r["pods"][0]["ready"] and not r["pods"][0]["terminating"])
        self.restored = True
        self.journal(after=row, restored_at=shared.utc())
        return row
