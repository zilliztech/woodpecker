#!/usr/bin/env python3
"""Owned, ephemeral Kubernetes fixture and UID-guarded restoration for #489."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time

import cli_acceptance as shared

LABEL = "diagnostics.woodpecker.io/native-acceptance"
PROFILE = "cli-native-v1"
RUN_LABEL = "diagnostics.woodpecker.io/native-run"
LOCK = "wpdiag-cli-acceptance-lock"
OWNER = re.compile(r"native-[0-9]{8}t[0-9]{6}z-[a-f0-9]{8}\Z")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def manifests(owner, namespace, gate_image, wp_image, minio_image, prometheus_image):
    if not OWNER.fullmatch(owner):
        raise ValueError("Invalid fixture owner")
    # This representative service uses the supported MinIO backend. It tests
    # real native operations, not staged WAL disk fsync or production replication.
    config = {"woodpecker": {"storage": {"type": "minio", "rootPath": "/data/wp"},
                "logstore": {"segmentSyncPolicy": {"maxInterval": 10},
                             "processorCleanupPolicy": {"maxIdleTime": 600}}},
              "minio": {"address": "127.0.0.1", "port": 19000, "bucketName": "native-acceptance",
                        "rootPath": owner, "accessKeyID": "nativefixture", "secretAccessKey": "nativefixture",
                        "useSSL": False, "useIAM": False, "cloudProvider": "aws", "createBucket": True},
              "log": {"level": "warn"}, "trace": {"exporter": "noop"}}
    scrape = {"global": {"scrape_interval": "2s"}, "scrape_configs": [{"job_name": "native-acceptance",
        "static_configs": [{"targets": ["127.0.0.1:9091"], "labels": {
            "component": "woodpecker", "namespace": namespace, "cluster": "native-acceptance", "woodpecker_id": owner}}]}]}
    meta = {"name": owner, "namespace": namespace, "labels": {RUN_LABEL: owner}}
    cm = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": meta,
          "data": {"wp.yaml": json.dumps(config), "empty.yaml": "{}", "prometheus.yaml": json.dumps(scrape)}}
    def container(name, image, command, args, memory):
        return {"name": name, "image": image, "imagePullPolicy": "IfNotPresent", "command": command, "args": args,
                "resources": {"requests": {"cpu": "25m", "memory": "32Mi"}, "limits": {"memory": memory}}}
    wp = container("woodpecker", wp_image, ["/woodpecker/bin/woodpecker"], ["server", "--node-name", owner,
        "--cluster", owner, "--seeds", "127.0.0.1:17946", "--advertise-gossip-addr", "127.0.0.1:17946",
        "--advertise-service-addr", "127.0.0.1:18080", "--config", "/fixture/wp.yaml", "--external-default-config", "/fixture/empty.yaml",
        "--external-user-config", "/fixture/empty.yaml", "--data-dir", "/data/wp"], "512Mi")
    wp["volumeMounts"] = [{"name": "config", "mountPath": "/fixture", "readOnly": True}, {"name": "wp", "mountPath": "/data"}]
    wp["env"] = [{"name": "NODE_NAME", "value": owner}, {"name": "METRICS_PORT", "value": "9091"}]
    wp["readinessProbe"] = {"httpGet": {"path": "/admin/memberlist", "port": 9091}, "initialDelaySeconds": 2, "periodSeconds": 2}
    gate = container("gate", gate_image, ["/native-fixture"], ["gate", "--owner", owner], "128Mi")
    gate["readinessProbe"] = {"httpGet": {"path": "/", "port": 19001}, "periodSeconds": 2}
    minio = container("minio", minio_image, ["minio"], ["server", "/data", "--address", ":9000"], "384Mi")
    minio["env"] = [{"name": "MINIO_ROOT_USER", "value": "nativefixture"}, {"name": "MINIO_ROOT_PASSWORD", "value": "nativefixture"}]
    minio["volumeMounts"] = [{"name": "objects", "mountPath": "/data"}]
    prom = container("prometheus", prometheus_image, ["/bin/prometheus"], ["--config.file=/fixture/prometheus.yaml",
        "--storage.tsdb.path=/prometheus", "--storage.tsdb.retention.time=1h", "--web.listen-address=:19090"], "256Mi")
    prom["volumeMounts"] = [{"name": "config", "mountPath": "/fixture", "readOnly": True}, {"name": "metrics", "mountPath": "/prometheus"}]
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": meta, "spec": {"restartPolicy": "OnFailure",
        "automountServiceAccountToken": False, "terminationGracePeriodSeconds": 10,
        "containers": [wp, gate, minio, prom], "volumes": [{"name": "config", "configMap": {"name": owner}},
            *[{"name": name, "emptyDir": {"sizeLimit": "256Mi"}} for name in ("wp", "objects", "metrics")]]}}
    return cm, pod


class Fixture:
    def __init__(self, context, namespace, folder, receipt):
        self.kube = ["kubectl", "--context", context, "-n", namespace]
        self.namespace, self.folder, self.receipt = namespace, Path(folder), receipt

    def command(self, args, payload=None, timeout=30):
        p = subprocess.run(self.kube + args, input=None if payload is None else json.dumps(payload),
                           capture_output=True, text=True, timeout=timeout)
        if p.returncode:
            raise RuntimeError("Kubernetes fixture operation failed; private API output withheld")
        return json.loads(p.stdout) if p.stdout.strip() else None

    def checkpoint(self):
        shared.save(self.folder / "fixture.json", self.receipt)

    def namespace_check(self):
        ns = self.command(["get", "namespace", self.namespace, "-o", "json"])
        if ns["metadata"].get("labels", {}).get(LABEL) != PROFILE:
            raise RuntimeError("Require explicit disposable namespace label " + LABEL + "=" + PROFILE)
        uid = ns["metadata"]["uid"]
        if self.receipt.get("namespace_uid", uid) != uid:
            raise RuntimeError("Namespace identity changed")
        self.receipt["namespace_uid"] = uid

    def create(self, value):
        # Intent precedes API submission, including an ambiguous create timeout.
        kind, name = value["kind"].lower(), value["metadata"]["name"]
        key = kind + "/" + name
        self.receipt["resources"][key] = {"intent": value, "created": False}
        self.checkpoint()
        row = self.command(["create", "-f", "-", "-o", "json"], value)
        self.receipt["resources"][key].update(created=True, uid=row["metadata"]["uid"])
        self.checkpoint()
        return row

    def setup(self, images):
        self.namespace_check()
        if self.command(["get", "configmap", LOCK, "--ignore-not-found", "-o", "json"]) is not None:
            raise RuntimeError("Namespace acceptance lock already exists")
        owner = self.receipt["run_id"]
        lock = self.create({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": LOCK, "labels": {RUN_LABEL: owner}},
                            "data": {"run-id": owner}})
        self.receipt["lock_uid"] = lock["metadata"]["uid"]
        self.checkpoint()
        for obj in manifests(owner, self.namespace, **images):
            obj["metadata"]["ownerReferences"] = [{"apiVersion": "v1", "kind": "ConfigMap", "name": LOCK, "uid": self.receipt["lock_uid"]}]
            self.create(obj)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            pod = self.command(["get", "pod", owner, "-o", "json"])
            states = pod.get("status", {}).get("containerStatuses", [])
            # Dependency startup may need a WP retry. The Ready baseline records
            # actual restart counts; any subsequent change invalidates the run.
            if len(states) == 4 and all(c.get("ready") for c in states):
                self.receipt["pod_before"] = pod_identity(pod)
                self.checkpoint()
                return
            time.sleep(1)
        raise RuntimeError("Fixture readiness deadline exceeded")

    def lock_check(self):
        self.namespace_check()
        lock = self.command(["get", "configmap", LOCK, "-o", "json"])
        if lock["metadata"]["uid"] != self.receipt.get("lock_uid") or lock.get("data") != {"run-id": self.receipt["run_id"]}:
            raise RuntimeError("Original namespace lock ownership changed; refusing cleanup")

    def verify_original(self, key, current):
        saved = self.receipt["resources"][key]
        intent = saved["intent"]
        if current["metadata"].get("labels", {}).get(RUN_LABEL) != self.receipt["run_id"] or (
                saved.get("uid") and current["metadata"]["uid"] != saved["uid"]):
            raise RuntimeError("Foreign fixture resource identity; refusing cleanup")
        if current["metadata"].get("ownerReferences", []) != intent["metadata"].get("ownerReferences", []):
            raise RuntimeError("Fixture resource owner changed")
        if key.startswith("configmap/"):
            if current.get("data") != intent.get("data"):
                raise RuntimeError("Fixture ConfigMap data changed")
        else:
            # Server defaulting is allowed, external edits of desired fields are not.
            def contains(actual, expected):
                if isinstance(expected, dict):
                    return isinstance(actual, dict) and all(k in actual and contains(actual[k], v) for k, v in expected.items())
                if isinstance(expected, list):
                    return isinstance(actual, list) and len(actual) == len(expected) and all(contains(a, b) for a, b in zip(actual, expected))
                return actual == expected
            if not contains(current["spec"], intent["spec"]):
                raise RuntimeError("Fixture Pod settings changed")
        # Resolve an ambiguous create only through exact run labels/settings.
        saved["uid"] = current["metadata"]["uid"]
        return saved["uid"]

    def cleanup(self):
        if not self.receipt.get("lock_uid"):
            # Resolve only our original create intent, never someone else's lock.
            key = "configmap/" + LOCK
            if key not in self.receipt["resources"]:
                return
            lock = self.command(["get", key, "--ignore-not-found", "-o", "json"])
            if lock is None:
                return
            self.receipt["lock_uid"] = self.verify_original(key, lock)
        self.lock_check()
        keys = sorted(self.receipt["resources"], key=lambda key: (key == "configmap/" + LOCK, not key.startswith("pod/")))
        # Verify every resource first; a foreign mutation preserves the lock.
        current = {}
        for key in keys:
            row = self.command(["get", key, "--ignore-not-found", "-o", "json"])
            if row is not None:
                current[key] = self.verify_original(key, row)
        for key, uid in current.items():
            kind, name = key.split("/", 1)
            plural = "pods" if kind == "pod" else "configmaps"
            options = self.folder / "delete-options.json"
            shared.save(options, {"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": uid}, "gracePeriodSeconds": 10})
            self.command(["delete", "--raw", "/api/v1/namespaces/" + self.namespace + "/" + plural + "/" + name, "-f", str(options)])
            end = time.monotonic() + 45
            while self.command(["get", key, "--ignore-not-found", "-o", "json"]) is not None:
                if time.monotonic() >= end:
                    raise RuntimeError("Owned resource deletion did not finish; lock retained")
                time.sleep(.5)
            self.receipt["resources"][key]["deleted"] = True
            self.checkpoint()
        self.receipt["cleaned"] = True
        self.checkpoint()


def pod_identity(pod):
    return {"uid": pod["metadata"]["uid"], "spec_sha256": digest(pod["spec"]),
            "containers": [{"name": c["name"], "image": c["image"], "image_id": c.get("imageID", ""),
                            "container_id": c.get("containerID", ""), "restarts": c.get("restartCount", 0)}
                           for c in pod.get("status", {}).get("containerStatuses", [])]}
