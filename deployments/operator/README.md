# Woodpecker Operator

Kubernetes operator for deploying and managing [Woodpecker](https://github.com/zilliztech/woodpecker) service mode clusters.

The operator manages Woodpecker server pods and their full lifecycle:
- Declarative deployment via `WoodpeckerCluster` CRD
- Rolling upgrades on image or config changes
- Graceful scale-down with segment decommission
- Health checks and self-healing via reconciliation loop
- Node topology awareness (AZ/Region auto-injection)

The operator does **not** install or manage any external dependencies (etcd, object storage, etc.). Woodpecker process configuration is provided by the user via a ConfigMap.

## Prerequisites

- Kubernetes v1.28+
- kubectl configured to access the cluster
- (For development) Go 1.24+, Docker

## Quick Start

### 1. Install the CRD

```sh
make install
```

### 2. Run the operator locally (for development)

```sh
make run
```

### 3. Create a WoodpeckerCluster

```sh
kubectl apply -f config/samples/woodpecker_v1alpha1_woodpeckercluster.yaml
```

This creates:
- A ConfigMap with Woodpecker configuration (etcd, MinIO endpoints, etc.)
- A `WoodpeckerCluster` CR that the operator reconciles into a StatefulSet, Services, PDB, etc.

### 4. Check status

```sh
kubectl get woodpeckerclusters
```

Output:
```
NAME                PHASE     READY   REPLICAS   AGE
woodpecker-sample   Running   3       3          5m
```

## Deploy to a Cluster

### Build and push the operator image

```sh
make docker-build docker-push IMG=<registry>/woodpecker-operator:v0.1.26
```

### Deploy the operator

```sh
make deploy IMG=<registry>/woodpecker-operator:v0.1.26
```

### Apply a WoodpeckerCluster CR

```sh
kubectl apply -f config/samples/woodpecker_v1alpha1_woodpeckercluster.yaml
```

## CRD Reference

### WoodpeckerCluster Spec

| Field | Type | Default | Description |
|---|---|---|---|
| `image` | string | `zilliztech/woodpecker:v0.1.26` | Server container image |
| `replicas` | int32 | `3` | Number of server nodes |
| `resources` | ResourceRequirements | - | CPU/memory requests and limits |
| `storageClassName` | string | - | StorageClass for data PVC |
| `storageSize` | Quantity | `10Gi` | Data PVC size |
| `servicePort` | int32 | `18080` | gRPC service port |
| `gossipPort` | int32 | `17946` | Gossip protocol port |
| `metricsPort` | int32 | `9091` | HTTP metrics/health port |
| `configRef` | ConfigMapReference | - | Reference to ConfigMap containing `woodpecker.yaml` |
| `affinity` | Affinity | - | Pod scheduling affinity |
| `tolerations` | []Toleration | - | Pod scheduling tolerations |
| `nodeSelector` | map[string]string | - | Pod node selector |
| `topologySpreadConstraints` | []TopologySpreadConstraint | - | Pod topology spread |
| `paused` | bool | `false` | Stop reconciliation for maintenance |

### WoodpeckerCluster Status

| Field | Type | Description |
|---|---|---|
| `phase` | string | `Pending`, `Creating`, `Running`, `Upgrading`, `ScalingUp`, `ScalingDown`, `Failed` |
| `readyReplicas` | int32 | Number of ready pods |
| `endpoint` | string | Client service endpoint (`{name}-server-client.{ns}.svc:{port}`) |
| `conditions` | []Condition | `Ready`, `Available` conditions |

### ConfigMap Format

The ConfigMap referenced by `configRef` must contain a `woodpecker.yaml` key with the full Woodpecker server configuration. See `config/samples/` for an example.

If `configRef` is not set, the operator creates a minimal default ConfigMap.

## Managed Resources

For each `WoodpeckerCluster` CR, the operator creates:

| Resource | Name | Purpose |
|---|---|---|
| StatefulSet | `{name}-server` | Woodpecker server pods |
| Headless Service | `{name}-server-headless` | Pod DNS for gossip discovery |
| Client Service | `{name}-server-client` | gRPC client access |
| Metrics Service | `{name}-server-metrics` | Metrics/health endpoint |
| ServiceAccount | `{name}-server` | Pod identity |
| PDB | `{name}-server-pdb` | Availability during disruptions |

## Operations

### Scale

```sh
kubectl patch woodpeckercluster my-cluster --type merge -p '{"spec":{"replicas":5}}'
```

Scale-down triggers graceful decommission: the operator calls the Woodpecker decommission API on pods being removed and waits until all segment data is flushed to object storage before reducing replicas.

### Upgrade

```sh
kubectl patch woodpeckercluster my-cluster --type merge -p '{"spec":{"image":"zilliztech/woodpecker:v0.2.0"}}'
```

The operator performs a rolling update (one pod at a time).

### Config Change

Modify the ConfigMap referenced by `configRef`. The operator detects the content change via SHA-256 hash and triggers a rolling restart.

### Pause

```sh
kubectl patch woodpeckercluster my-cluster --type merge -p '{"spec":{"paused":true}}'
```

### Uninstall

```sh
kubectl delete woodpeckercluster my-cluster
make undeploy
make uninstall
```

## Development

```sh
# Run tests
make test

# Run operator locally
make run

# Generate CRD manifests after type changes
make manifests

# Regenerate deepcopy after type changes
make generate
```

## License

Apache License 2.0
