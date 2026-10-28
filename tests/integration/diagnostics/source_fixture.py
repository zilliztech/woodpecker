"""Ephemeral real WP/MinIO + observability fixture; never manages a cluster."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time

import cli_acceptance as shared

PROFILE = "cli-source-v1"
LABEL = "diagnostics.woodpecker.io/source-acceptance"
RUN_LABEL = "diagnostics.woodpecker.io/source-run"
LOCK = "wpdiag-cli-acceptance-lock"
OWNER = re.compile(r"source-[0-9]{8}t[0-9]{6}z-[a-f0-9]{8}\Z")
IMAGES = {"woodpecker": "milvusdb/woodpecker:v0.1.46", "minio": "milvusdb/minio:RELEASE.2024-12-18T13-15-44Z",
          "jaeger": "jaegertracing/all-in-one:1.53.0", "loki": "grafana/loki:3.5.7",
          "alloy": "grafana/alloy:v1.10.2", "prometheus": "quay.io/prometheus/prometheus:v3.0.1",
          "client": "wp-source-client-491:local"}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def manifests(owner, namespace, images=None):
    if not OWNER.fullmatch(owner):
        raise ValueError("Invalid source fixture owner")
    images = dict(IMAGES if images is None else images)
    wp = {"woodpecker": {"storage": {"type": "minio", "rootPath": "/data/wp"},
          "logstore": {"segmentSyncPolicy": {"maxInterval": 10}, "processorCleanupPolicy": {"maxIdleTime": 600}}},
          "minio": {"address": "127.0.0.1", "port": 9000, "bucketName": "source-acceptance", "rootPath": owner,
                    "accessKeyID": "sourcefixture", "secretAccessKey": "sourcefixture", "useSSL": False,
                    "useIAM": False, "cloudProvider": "aws", "createBucket": True},
          "log": {"level": "info", "format": "json"},
          "trace": {"exporter": "otlp", "sampleFraction": 1, "otlp": {"endpoint": "127.0.0.1:4317", "method": "grpc", "secure": False}}}
    # Same API-based collection and five labels as the stack, narrowed to the
    # owned WP container. No log push or content/identity rewriting exists here.
    alloy = '''discovery.kubernetes "pods" {
  role = "pod"
  namespaces { names = ["NAMESPACE"] }
  selectors { role = "pod"
    label = "RUN_LABEL=OWNER"
  }
}
discovery.relabel "logs" {
  targets = discovery.kubernetes.pods.targets
  rule { source_labels = ["__meta_kubernetes_pod_container_name"]
    regex = "woodpecker"
    action = "keep"
  }
  rule { source_labels = ["__meta_kubernetes_namespace"]
    target_label = "namespace"
  }
  rule { source_labels = ["__meta_kubernetes_pod_name"]
    target_label = "pod"
  }
  rule { source_labels = ["__meta_kubernetes_pod_container_name"]
    target_label = "container"
  }
  rule { source_labels = ["__meta_kubernetes_pod_label_component"]
    target_label = "component"
  }
  rule { target_label = "cluster"
    replacement = "source-acceptance"
  }
}
loki.source.kubernetes "pods" {
  targets = discovery.relabel.logs.output
  forward_to = [loki.write.local.receiver]
}
loki.write "local" {
  endpoint { url = "http://127.0.0.1:3100/loki/api/v1/push" }
}
'''.replace("NAMESPACE", namespace).replace("RUN_LABEL", RUN_LABEL).replace("OWNER", owner)
    loki = {"auth_enabled": False, "server": {"http_listen_port": 3100},
        "common": {"path_prefix": "/loki", "replication_factor": 1, "ring": {"kvstore": {"store": "inmemory"}},
                   "storage": {"filesystem": {"chunks_directory": "/loki/chunks", "rules_directory": "/loki/rules"}}},
        "schema_config": {"configs": [{"from": "2024-01-01", "store": "tsdb", "object_store": "filesystem", "schema": "v13",
                                       "index": {"prefix": "loki_index_", "period": "24h"}}]},
        "limits_config": {"retention_period": "24h"}, "analytics": {"reporting_enabled": False}}
    prom = {"global": {"scrape_interval": "2s"}, "scrape_configs": [{"job_name": "source-acceptance",
        "static_configs": [{"targets": ["127.0.0.1:9091"], "labels": {"component": "woodpecker", "cluster": "source-acceptance",
                                "namespace": namespace, "woodpecker_id": owner}}]}]}
    meta = {"name": owner, "namespace": namespace, "labels": {RUN_LABEL: owner}}
    cm = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": meta,
          "data": {"wp.yaml": json.dumps(wp), "empty.yaml": "{}", "loki.yaml": json.dumps(loki),
                   "prometheus.yaml": json.dumps(prom), "config.alloy": alloy}}
    sa = {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": meta}
    role = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": meta,
            "rules": [{"apiGroups": [""], "resources": ["pods", "pods/log"], "verbs": ["get", "list", "watch"]}]}
    binding = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": meta,
               "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": owner},
               "subjects": [{"kind": "ServiceAccount", "name": owner, "namespace": namespace}]}
    def container(name, command, args, memory, data=None):
        c = {"name": name, "image": images[name], "imagePullPolicy": "IfNotPresent", "command": command, "args": args,
             "resources": {"requests": {"cpu": "20m", "memory": "32Mi"}, "limits": {"memory": memory}},
             "volumeMounts": [{"name": "config", "mountPath": "/fixture", "readOnly": True}]}
        if data:
            c["volumeMounts"].append({"name": name, "mountPath": data})
        return c
    w = container("woodpecker", ["/woodpecker/bin/woodpecker"], ["server", "--node-name", owner, "--cluster", owner,
        "--seeds", "127.0.0.1:17946", "--advertise-gossip-addr", "127.0.0.1:17946", "--advertise-service-addr", "127.0.0.1:18080",
        "--config", "/fixture/wp.yaml", "--external-default-config", "/fixture/empty.yaml", "--external-user-config", "/fixture/empty.yaml",
        "--data-dir", "/data/wp"], "512Mi", "/data")
    w["env"] = [{"name": "NODE_NAME", "value": owner}, {"name": "METRICS_PORT", "value": "9091"}]
    m = container("minio", ["minio"], ["server", "/data", "--address", ":9000"], "384Mi", "/data")
    m["env"] = [{"name": "MINIO_ROOT_USER", "value": "sourcefixture"}, {"name": "MINIO_ROOT_PASSWORD", "value": "sourcefixture"}]
    j = container("jaeger", None, ["--query.http-server.host-port=:16686"], "512Mi")
    del j["command"]
    j["env"] = [{"name": "COLLECTOR_OTLP_ENABLED", "value": "true"}, {"name": "SPAN_STORAGE_TYPE", "value": "memory"},
                {"name": "MEMORY_MAX_TRACES", "value": "10000"}, {"name": "GOMAXPROCS", "value": "2"}]
    l = container("loki", ["/usr/bin/loki"], ["-config.file=/fixture/loki.yaml"], "384Mi", "/loki")
    a = container("alloy", ["/bin/alloy"], ["run", "/fixture/config.alloy", "--storage.path=/alloy", "--server.http.listen-addr=0.0.0.0:12345"], "192Mi", "/alloy")
    a["volumeMounts"].append({"name": "api", "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount", "readOnly": True})
    p = container("prometheus", ["/bin/prometheus"], ["--config.file=/fixture/prometheus.yaml", "--storage.tsdb.path=/prometheus",
        "--storage.tsdb.retention.time=1h", "--web.listen-address=:19090"], "192Mi", "/prometheus")
    client = container("client", ["/source-client"], ["--idle"], "128Mi")
    for c, port, path in ((w, 9091, "/admin/memberlist"), (m, 9000, "/minio/health/ready"), (j, 14269, "/"),
                           (l, 3100, "/ready"), (a, 12345, "/-/ready"), (p, 19090, "/-/ready")):
        c["readinessProbe"] = {"httpGet": {"path": path, "port": port}, "periodSeconds": 2}
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {**meta, "labels": {RUN_LABEL: owner, "component": "woodpecker"}},
        "spec": {"serviceAccountName": owner, "automountServiceAccountToken": False, "restartPolicy": "OnFailure", "terminationGracePeriodSeconds": 10,
                 "containers": [w, m, j, l, a, p, client], "volumes": [{"name": "config", "configMap": {"name": owner}},
                    {"name": "api", "projected": {"sources": [
                        {"serviceAccountToken": {"path": "token", "expirationSeconds": 3600}},
                        {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}},
                        {"downwardAPI": {"items": [{"path": "namespace", "fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.namespace"}}]}}]}},
                    *[{"name": name, "emptyDir": {"sizeLimit": "256Mi"}} for name in ("woodpecker", "minio", "loki", "alloy", "prometheus")]]}}
    return [cm, sa, role, binding, pod]


class Fixture:
    def __init__(self, context, namespace, folder, receipt):
        self.kube = ["kubectl", "--context", context, "-n", namespace]
        self.namespace, self.folder, self.receipt = namespace, Path(folder), receipt
        self.deadline = None

    def command(self, args, value=None, timeout=30):
        if self.deadline is not None:
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("Source fixture work budget exhausted")
            timeout = min(timeout, remaining)
        p = subprocess.run(self.kube + args, input=None if value is None else json.dumps(value), capture_output=True, text=True, timeout=timeout)
        if p.returncode:
            raise RuntimeError("Fixture Kubernetes command failed; private output withheld")
        return json.loads(p.stdout) if p.stdout.strip() else None

    def checkpoint(self):
        shared.save(self.folder / "fixture.json", self.receipt)

    def existing_inventory(self):
        rows = self.command(["get", "pods,deployments,statefulsets,configmaps", "-o", "json"])["items"]
        out = {}
        for row in rows:
            meta = row["metadata"]
            if meta.get("labels", {}).get(RUN_LABEL) == self.receipt["run_id"]:
                continue
            key = row["kind"].lower() + "/" + meta["name"]
            fact = {"uid": meta["uid"]}
            if row["kind"] == "Pod":
                fact.update(pod_identity(row))
            elif row["kind"] == "ConfigMap":
                fact["data_sha256"] = digest({"data": row.get("data"), "binaryData": row.get("binaryData")})
            else:
                fact["spec_sha256"] = digest(row["spec"])
            out[key] = fact
        return out

    def namespace_check(self):
        ns = self.command(["get", "namespace", self.namespace, "-o", "json"])
        if ns["metadata"].get("labels", {}).get(LABEL) != PROFILE:
            raise RuntimeError("Require disposable namespace opt-in " + LABEL + "=" + PROFILE)
        uid = ns["metadata"]["uid"]
        if self.receipt.get("namespace_uid", uid) != uid:
            raise RuntimeError("Namespace identity changed")
        self.receipt["namespace_uid"] = uid

    def create(self, obj):
        key = obj["kind"].lower() + "/" + obj["metadata"]["name"]
        self.receipt["resources"][key] = {"intent": obj}
        self.checkpoint()  # Includes ambiguous create outcomes.
        row = self.command(["create", "-f", "-", "-o", "json"], obj)
        self.receipt["resources"][key]["uid"] = row["metadata"]["uid"]
        self.checkpoint()
        return row

    def setup(self, images=None):
        self.namespace_check()
        owner = self.receipt["run_id"]
        lock = self.create({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": LOCK, "labels": {RUN_LABEL: owner}},
                            "data": {"run-id": owner, "profile": PROFILE}})
        self.receipt["lock_uid"] = lock["metadata"]["uid"]
        self.checkpoint()
        for obj in manifests(owner, self.namespace, images):
            obj["metadata"]["ownerReferences"] = [{"apiVersion": "v1", "kind": "ConfigMap", "name": LOCK, "uid": self.receipt["lock_uid"]}]
            self.create(obj)
        deadline = min(time.monotonic() + 180, self.deadline or float("inf"))
        while time.monotonic() < deadline:
            pod = self.command(["get", "pod", owner, "-o", "json"])
            states = pod.get("status", {}).get("containerStatuses", [])
            if len(states) == 7 and all(c.get("ready") for c in states):
                self.receipt["pod_before"] = pod_identity(pod)
                self.checkpoint()
                return
            time.sleep(1)
        raise RuntimeError("Real source fixture readiness deadline exceeded")

    def original(self, key, current):
        saved = self.receipt["resources"][key]
        wanted = saved["intent"]
        if current["metadata"].get("labels", {}).get(RUN_LABEL) != self.receipt["run_id"] or (
                saved.get("uid") and current["metadata"]["uid"] != saved["uid"]):
            raise RuntimeError("Foreign resource identity; refusing cleanup")
        if current["metadata"].get("ownerReferences", []) != wanted["metadata"].get("ownerReferences", []):
            raise RuntimeError("Fixture resource ownership changed; refusing cleanup")
        if key.startswith("configmap/") and current.get("data") != wanted.get("data"):
            raise RuntimeError("Fixture configuration changed; refusing cleanup")
        def contains(actual, expected):
            if isinstance(expected, dict):
                return isinstance(actual, dict) and all(k in actual and contains(actual[k], v) for k, v in expected.items())
            if isinstance(expected, list):
                return isinstance(actual, list) and len(actual) == len(expected) and all(contains(a, b) for a, b in zip(actual, expected))
            return actual == expected
        if not contains(current, wanted):
            raise RuntimeError("Fixture settings changed; refusing cleanup")
        saved["uid"] = current["metadata"]["uid"]
        return saved["uid"]

    def cleanup(self):
        # Restoration has its own bounded calls even after work-budget expiry.
        self.deadline = None
        if "configmap/" + LOCK not in self.receipt["resources"]:
            return
        self.namespace_check()
        lock = self.command(["get", "configmap", LOCK, "--ignore-not-found", "-o", "json"])
        if lock is None:
            # Resolve an uncommitted create or interrupted final checkpoint only
            # through verified absence. Never delete any remaining object here.
            if any(self.command(["get", key, "--ignore-not-found", "-o", "json"]) is not None for key in self.receipt["resources"]):
                raise RuntimeError("Original lock absent with remaining resources; refusing unowned cleanup")
            for saved in self.receipt["resources"].values():
                saved["deleted"] = True
            self.receipt["cleaned"] = True
            self.checkpoint()
            return
        self.receipt["lock_uid"] = self.original("configmap/" + LOCK, lock)
        # Validate all identities before deleting anything; keep lock on conflict.
        keys = sorted(self.receipt["resources"], key=lambda k: (k == "configmap/" + LOCK, not k.startswith("pod/")))
        rows = {k: self.command(["get", k, "--ignore-not-found", "-o", "json"]) for k in keys}
        for k, row in rows.items():
            if row is not None:
                self.original(k, row)
        plurals = {"pod": "pods", "configmap": "configmaps", "serviceaccount": "serviceaccounts", "role": "roles", "rolebinding": "rolebindings"}
        for k, row in rows.items():
            if row is not None:
                kind, name = k.split("/", 1)
                prefix = "/apis/rbac.authorization.k8s.io/v1" if kind in ("role", "rolebinding") else "/api/v1"
                options = self.folder / "delete-options.json"
                shared.save(options, {"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": row["metadata"]["uid"],
                    "resourceVersion": row["metadata"]["resourceVersion"]}, "gracePeriodSeconds": 10})
                self.command(["delete", "--raw", prefix + "/namespaces/" + self.namespace + "/" + plurals[kind] + "/" + name, "-f", str(options)])
                end = time.monotonic() + 45
                while self.command(["get", k, "--ignore-not-found", "-o", "json"]) is not None:
                    if time.monotonic() >= end:
                        raise RuntimeError("Owned deletion deadline exceeded; lock retained")
                    time.sleep(.5)
            self.receipt["resources"][k]["deleted"] = True
            self.checkpoint()
        self.receipt["cleaned"] = True
        self.checkpoint()


def pod_identity(pod):
    return {"uid": pod["metadata"]["uid"], "spec_sha256": digest(pod["spec"]),
            "containers": [{"name": c["name"], "image": c["image"], "image_id": c.get("imageID", ""),
                            "container_id": c.get("containerID", ""), "restarts": c.get("restartCount", 0)}
                           for c in pod.get("status", {}).get("containerStatuses", [])]}
