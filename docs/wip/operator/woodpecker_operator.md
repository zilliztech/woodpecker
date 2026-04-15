# Woodpecker Kubernetes Operator

## Overview

The Woodpecker Operator provides automated deployment and lifecycle management for Woodpecker **service mode** clusters on Kubernetes. It manages Woodpecker server pods through a `WoodpeckerCluster` Custom Resource Definition (CRD).

### Capabilities

| Capability | Description |
|---|---|
| Declarative deployment | Define a `WoodpeckerCluster` CR, operator creates StatefulSet, Services, PDB, etc. |
| Rolling upgrades | Change `spec.image`, operator performs rolling update one pod at a time |
| Config change detection | Modify the referenced ConfigMap, operator detects the change and triggers rolling restart |
| Graceful scale-down | Operator calls Woodpecker decommission API before removing pods |
| Self-healing | Reconciliation loop detects and recovers from drift |
| Node topology awareness | Automatically injects AZ/Region info from K8s node labels |

### Scope

The operator manages **only** Woodpecker server pods and their associated K8s resources. It does not install or manage any external services (etcd, object storage, Prometheus, etc.). Woodpecker process configuration is provided by the user via a ConfigMap.

## Architecture

```
  kubectl apply -k config/default/
  ┌────────────────┐  ┌─────────────┐  ┌──────┐
  │ Operator Pod   │  │ CRD         │  │ RBAC │
  └───────┬────────┘  └─────────────┘  └──────┘
          │ watches
  ┌───────▼──────────────────────────┐
  │     WoodpeckerCluster CR         │
  │  spec.replicas, image, ...       │
  │  spec.configRef → ConfigMap      │
  └───────┬──────────────────────────┘
          │ creates/manages
  ┌───────────┐ ┌──────────┐ ┌──────────┐
  │StatefulSet│ │ Services │ │ PDB, SA, │
  │ Pod-0..N  │ │ Headless │ │ ConfigMap│
  │           │ │ Client   │ │          │
  │           │ │ Metrics  │ │          │
  └───────────┘ └──────────┘ └──────────┘
```

### Managed Resources

| Resource | Name Pattern | Purpose |
|---|---|---|
| StatefulSet | `{name}-server` | Woodpecker server pods with stable network identity |
| Headless Service | `{name}-server-headless` | Pod DNS for gossip discovery |
| Client Service | `{name}-server-client` | ClusterIP for gRPC client access (port 18080) |
| Metrics Service | `{name}-server-metrics` | Metrics/health endpoint (port 9091) |
| ConfigMap | `{name}-config` | Mounts user-provided `woodpecker.yaml` (via `configRef`) |
| PodDisruptionBudget | `{name}-server-pdb` | Availability during voluntary disruptions |
| ServiceAccount | `{name}-server` | Pod identity |

## CRD Reference

### API

```
Group:   woodpecker.zilliz.io
Version: v1alpha1
Kind:    WoodpeckerCluster
```

### Spec

| Field | Type | Default | Description |
|---|---|---|---|
| `image` | string | `zilliztech/woodpecker:v0.1.26` | Server container image |
| `imagePullPolicy` | string | `IfNotPresent` | Image pull policy |
| `imagePullSecrets` | []LocalObjectReference | - | Secrets for private registries |
| `replicas` | int32 | `3` | Number of server nodes |
| `resources` | ResourceRequirements | - | CPU/memory requests and limits |
| `storageClassName` | string | - | StorageClass for data PVC |
| `storageSize` | Quantity | `10Gi` | Data PVC size |
| `servicePort` | int32 | `18080` | gRPC service port |
| `gossipPort` | int32 | `17946` | Gossip protocol port |
| `metricsPort` | int32 | `9091` | HTTP metrics/health port |
| `configRef` | ConfigMapReference | - | ConfigMap containing `woodpecker.yaml` |
| `affinity` | Affinity | - | Pod scheduling affinity |
| `tolerations` | []Toleration | - | Pod scheduling tolerations |
| `nodeSelector` | map[string]string | - | Pod node selector |
| `topologySpreadConstraints` | []TopologySpreadConstraint | - | Pod topology spread |
| `podAnnotations` | map[string]string | - | Additional pod annotations |
| `podLabels` | map[string]string | - | Additional pod labels |
| `paused` | bool | `false` | Stop reconciliation for maintenance |

### Status

| Field | Type | Description |
|---|---|---|
| `phase` | string | `Pending`, `Creating`, `Running`, `Upgrading`, `ScalingUp`, `ScalingDown`, `Failed` |
| `readyReplicas` | int32 | Number of ready pods |
| `endpoint` | string | Client service endpoint |
| `conditions` | []Condition | `Ready` (all replicas), `Available` (at least one) |
| `observedGeneration` | int64 | Last reconciled spec generation |
| `currentImage` | string | Currently running image |
| `server` | ComponentStatus | StatefulSet replica counts |

### ConfigMap Format

The ConfigMap referenced by `configRef` must have a key `woodpecker.yaml` containing the full Woodpecker server configuration. If `configRef` is not set, the operator creates a minimal default ConfigMap.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-wp-config
data:
  woodpecker.yaml: |
    woodpecker:
      meta:
        type: etcd
      client:
        quorum:
          replicaCount: 3
      storage:
        type: service
        rootPath: /woodpecker/data
    log:
      level: info
      stdout: true
    etcd:
      endpoints:
        - etcd.default.svc:2379
      rootPath: by-dev
    minio:
      address: minio.default.svc
      port: 9000
      bucketName: woodpecker
      rootPath: files
```

### Example CR

```yaml
apiVersion: woodpecker.zilliz.io/v1alpha1
kind: WoodpeckerCluster
metadata:
  name: my-woodpecker
spec:
  image: zilliztech/woodpecker:v0.1.26
  replicas: 3
  resources:
    requests: { cpu: "500m", memory: "512Mi" }
    limits:   { cpu: "1", memory: "1Gi" }
  storageSize: 10Gi
  configRef:
    name: my-wp-config
```

## Reconciliation Logic

### Main Flow

1. Fetch CR (skip if not found)
2. Handle deletion via finalizer (decommission all pods, then remove finalizer)
3. Ensure finalizer is present
4. Skip if `paused`
5. Check for scale-down (decommission pods before reducing replicas)
6. Reconcile sub-resources: ServiceAccount, ConfigMap, Services, PDB, StatefulSet
7. Clean up orphaned PVCs after scale-down
8. Update status

### Gossip Seed Discovery

The operator uses a headless Service for stable pod DNS. An init container computes gossip seeds based on the pod's ordinal:

- Pod-0: no seeds (bootstrap node)
- Pod-N: uses Pod-0 through Pod-min(N-1, 2) as seeds

### ConfigMap Mounting

The operator mounts the user-provided ConfigMap at `/woodpecker/configs/woodpecker.yaml`. When the ConfigMap content changes, the operator computes a SHA-256 hash and writes it to a pod annotation, triggering a rolling restart via StatefulSet update.

### Graceful Scale-Down

When `spec.replicas` decreases:

1. Operator calls `POST /admin/node/decommission` on pods being removed (highest ordinal first)
2. Polls `GET /admin/node/decommission/progress` until `safe_to_terminate: true`
3. Only then reduces StatefulSet replicas
4. Cleans up orphaned PVCs

### Rolling Update

Uses `RollingUpdate` strategy with `maxUnavailable=1`. For each pod: SIGTERM, graceful shutdown (300s grace period), new pod starts, joins cluster via gossip, readiness probe passes, next pod updates.

### Node Topology

The operator injects node AZ/Region information via an init container, enabling Woodpecker's quorum selection to distribute segments across availability zones.

## Operations

### Scale

```bash
kubectl patch woodpeckercluster my-cluster --type merge -p '{"spec":{"replicas":5}}'
```

### Upgrade

```bash
kubectl patch woodpeckercluster my-cluster --type merge -p '{"spec":{"image":"zilliztech/woodpecker:v0.2.0"}}'
```

### Config Change

Edit the ConfigMap referenced by `configRef`. The operator detects the change and triggers a rolling restart.

### Pause / Resume

```bash
kubectl patch woodpeckercluster my-cluster --type merge -p '{"spec":{"paused":true}}'
kubectl patch woodpeckercluster my-cluster --type merge -p '{"spec":{"paused":false}}'
```

### Delete

```bash
kubectl delete woodpeckercluster my-cluster
```

The finalizer ensures all pods are gracefully decommissioned before resources are deleted.

## Project Structure

```
deployments/operator/
├── api/v1alpha1/              CRD types (Spec, Status, Webhooks)
├── cmd/main.go                Operator entry point
├── internal/controller/       Reconciler and sub-reconcilers
├── config/                    Kustomize manifests (CRD, RBAC, Manager, Samples)
├── test/                      E2E test script
├── Makefile                   Build, test, deploy targets
└── Dockerfile                 Operator container image
```

## Labels

All managed resources use standard Kubernetes labels:

```yaml
app.kubernetes.io/name: woodpecker
app.kubernetes.io/instance: {cr-name}
app.kubernetes.io/component: server
app.kubernetes.io/part-of: woodpecker
app.kubernetes.io/managed-by: woodpecker-operator
```

## Health Probes

| Probe | Path | Port | Timing |
|---|---|---|---|
| Startup | `/healthz` | 9091 | 10s initial, 5s period, 30 failures (150s max) |
| Liveness | `/healthz` | 9091 | 30s initial, 10s period, 3 failures |
| Readiness | `/healthz` | 9091 | 5s initial, 5s period, 3 failures |

`terminationGracePeriodSeconds: 300`

## References

- [Kubernetes Operator Pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/)
- [Kubebuilder Book](https://book.kubebuilder.io/)
- [Milvus Operator](https://github.com/zilliztech/milvus-operator)
- Woodpecker Server Lifecycle: `server/service.go`, `server/lifecycle.go`
- Woodpecker Decommission API: `common/http/management/`
- Woodpecker Configuration: `config/woodpecker.yaml`
