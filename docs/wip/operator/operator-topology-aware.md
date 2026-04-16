# Operator Topology-Aware Deployment — Design

- Status: Draft
- Date: 2026-04-15
- Tracking issue: [zilliztech/woodpecker#134](https://github.com/zilliztech/woodpecker/issues/134)
- Milestone: v0.1.26
- Branch: `enhance_operator_topology_aware`

## 1. Motivation

The Woodpecker operator today schedules `WoodpeckerCluster` pods without any
zone/region awareness and hard-codes `AVAILABILITY_ZONE=default` and
`RESOURCE_GROUP=default` in the `init-topology` init container. Two problems
follow:

1. **No HA across AZ failures.** Multiple pods can be placed on nodes in the
   same availability zone. If that AZ goes down, the whole cluster can lose
   quorum.
2. **No location awareness in the Woodpecker process.** Woodpecker's quorum
   selection and data-placement logic wants to know its physical zone and
   region, but the operator does not expose those to the process.

This spec adds topology-aware scheduling and real node-label-driven env-var
injection to the operator.

## 2. Goals and Non-Goals

### Goals

- Woodpecker pods spread across availability zones by default, enforced at
  schedule time.
- Each pod's Woodpecker process receives the real node's zone and region as
  `AVAILABILITY_ZONE` / `CLUSTER_NAME` env vars, with safe fallbacks when the
  node has no such labels.
- User-provided `spec.topologySpreadConstraints` continue to work and can
  override the operator default on the same topology key.

### Non-Goals

- Making the zone/region label keys configurable in the CRD. The K8s
  well-known labels `topology.kubernetes.io/zone` and
  `topology.kubernetes.io/region` are used directly.
- Changing `RESOURCE_GROUP`. It remains hard-coded to `default`; it will be
  wired to software-level resource groups in a future change outside this
  scope.
- Auto-detecting whether the cluster is single-AZ; K8s' native skew
  computation handles that correctly (see §5.3).

## 3. Requirements

| # | Requirement |
|---|-------------|
| R1 | Operator injects a default `TopologySpreadConstraint` on `topology.kubernetes.io/zone` with `maxSkew=1`, `whenUnsatisfiable=DoNotSchedule`, and a label selector matching the cluster's own pods. |
| R2 | If the user specifies a constraint on the same `topologyKey`, the operator does NOT add its default; the user's takes precedence. User constraints on other keys (e.g. hostname) coexist with the default. |
| R3 | Each pod's main container gets `AVAILABILITY_ZONE` = the node's `topology.kubernetes.io/zone` label, falling back to `default-az` if the label is absent. |
| R4 | Each pod's main container gets `CLUSTER_NAME` = the node's `topology.kubernetes.io/region` label, falling back to `default-cluster` if the label is absent. |
| R5 | Node labels are resolved at pod startup by an init container, not statically at reconcile time. |
| R6 | `RESOURCE_GROUP` continues to be set (hard-coded `default`) for forward compatibility, but is NOT driven from node labels. |
| R7 | Deleting a `WoodpeckerCluster` CR cleans up any cluster-scoped RBAC objects (ClusterRole, ClusterRoleBinding) the operator created for it. |

## 4. Overall Architecture

Three independent but coordinated changes:

1. **RBAC extension.** The operator creates a `ClusterRole` + `ClusterRoleBinding`
   per WoodpeckerCluster so the pod's ServiceAccount can `get` node objects at
   runtime.
2. **Init container enhancement.** The existing `init-topology` init container
   is retained but its script is replaced to call the K8s API using the
   pod's ServiceAccount token, resolve the node's zone/region labels, and
   write the real values into `/etc/woodpecker/topology.env`.
3. **Default `TopologySpreadConstraint` injection.** In `buildPodSpec`, the
   operator merges the user's `spec.topologySpreadConstraints` with a default
   zone-spread constraint, deduplicating on `topologyKey`.

### Data flow

```
Operator reconcile
  ├── ConfigMap
  ├── ServiceAccount       (existing)
  ├── ClusterRole + Binding (NEW, via reconcileRBAC)
  ├── Service / PDB / etc.
  └── StatefulSet
        └── Pod startup:
              InitContainer init-topology:
                1. Read HOST_NODE_NAME from Downward API (spec.nodeName)
                2. GET /api/v1/nodes/$HOST_NODE_NAME via service-account token
                3. Parse .metadata.labels["topology.kubernetes.io/zone"]   → AZ
                4. Parse .metadata.labels["topology.kubernetes.io/region"] → REGION
                5. Fallback to default-az / default-cluster on any failure
                6. Write /etc/woodpecker/topology.env:
                     SEEDS=...
                     AVAILABILITY_ZONE=<az>
                     CLUSTER_NAME=<region>
                     RESOURCE_GROUP=default
              MainContainer woodpecker:
                . /etc/woodpecker/topology.env
                export SEEDS AVAILABILITY_ZONE CLUSTER_NAME RESOURCE_GROUP
                exec /tini -- /woodpecker/bin/start-woodpecker.sh
```

## 5. Detailed Design

### 5.1 RBAC

New file: `deployments/operator/internal/controller/reconcile_rbac.go`, with a
`reconcileRBAC` function invoked from the main reconcile pipeline after
`reconcileServiceAccount` and before `reconcileStatefulSet`.

- **ClusterRole**
  - Name: `woodpecker-node-reader-<namespace>-<cluster-name>` (namespace is
    encoded because ClusterRole is cluster-scoped and must be unique across
    clusters).
  - Rules: `apiGroups: [""], resources: ["nodes"], verbs: ["get"]`.
- **ClusterRoleBinding**
  - Name: same pattern as ClusterRole.
  - Subjects: `<namespace>/<serverName(cluster)>` ServiceAccount.
  - Labels: the standard `commonLabels(cluster)`; plus
    `woodpecker.zilliz.io/owned-by: <namespace>/<name>` to aid debugging and
    external cleanup.

**Garbage collection.** Native owner references cannot point from a namespaced
CR to a cluster-scoped object, so K8s GC will not cascade. We extend the
existing finalizer to explicitly delete the ClusterRole and ClusterRoleBinding
when the WoodpeckerCluster CR is deleted. See §5.4.

**Operator's own manager permissions.** The controller-gen kubebuilder marker
on the reconciler is extended to request
`clusterroles` and `clusterrolebindings` in the `rbac.authorization.k8s.io`
API group with the usual CRUD verbs. `make manifests` regenerates
`config/rbac/role.yaml`.

### 5.2 Init container

The existing `init-topology` container is kept (same name, same mount path,
same volume). The script and image are updated.

**Image.** Change from `busybox:1.36` to `curlimages/curl:8.x`. The new script
uses `curl` for the K8s API call. JSON parsing uses plain `grep -o` / `cut`
(no `jq` dependency) to keep the image minimal.

**Env vars added:**
- `HOST_NODE_NAME` — from Downward API `spec.nodeName`. This is intentionally
  a new name to avoid conflict with the existing `NODE_NAME` env (which is
  the pod name, not the K8s node name).

**Script outline:**

```sh
#!/bin/sh
set -e

SEEDS="<built from cluster.Name, replicas, HEADLESS_SVC, GOSSIP_PORT>"

APISERVER=https://kubernetes.default.svc
TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
CACERT=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt

NODE_JSON=$(curl -sf --cacert "$CACERT" \
  -H "Authorization: Bearer $TOKEN" \
  "$APISERVER/api/v1/nodes/$HOST_NODE_NAME" || true)

AZ=$(printf '%s' "$NODE_JSON" \
  | grep -o '"topology.kubernetes.io/zone":"[^"]*"' \
  | head -n1 | cut -d'"' -f4)
REGION=$(printf '%s' "$NODE_JSON" \
  | grep -o '"topology.kubernetes.io/region":"[^"]*"' \
  | head -n1 | cut -d'"' -f4)

: "${AZ:=default-az}"
: "${REGION:=default-cluster}"

cat > /etc/woodpecker/topology.env <<EOF
SEEDS=${SEEDS}
AVAILABILITY_ZONE=${AZ}
CLUSTER_NAME=${REGION}
RESOURCE_GROUP=default
EOF

echo "Init complete: pod=$POD_NAME az=$AZ cluster=$REGION"
```

The init container never fails due to a node-lookup error; on failure it just
writes fallback values. This keeps startup resilient to transient API-server
issues.

**Main container.** The `Args` field is updated so that the `export` list
includes `CLUSTER_NAME`:

```
. /etc/woodpecker/topology.env && \
  export SEEDS AVAILABILITY_ZONE CLUSTER_NAME RESOURCE_GROUP && \
  exec /tini -- /woodpecker/bin/start-woodpecker.sh
```

### 5.3 Default `TopologySpreadConstraint`

New helpers in `reconcile_statefulset.go`:

```go
func defaultZoneTopologySpreadConstraint(cluster *woodpeckerv1alpha1.WoodpeckerCluster) corev1.TopologySpreadConstraint {
    return corev1.TopologySpreadConstraint{
        MaxSkew:           1,
        TopologyKey:       "topology.kubernetes.io/zone",
        WhenUnsatisfiable: corev1.DoNotSchedule,
        LabelSelector: &metav1.LabelSelector{
            MatchLabels: commonLabels(cluster),
        },
    }
}

func mergeTopologySpreadConstraints(
    cluster *woodpeckerv1alpha1.WoodpeckerCluster,
    user []corev1.TopologySpreadConstraint,
) []corev1.TopologySpreadConstraint {
    result := append([]corev1.TopologySpreadConstraint{}, user...)
    for _, c := range user {
        if c.TopologyKey == "topology.kubernetes.io/zone" {
            return result // user overrides the default
        }
    }
    return append(result, defaultZoneTopologySpreadConstraint(cluster))
}
```

`buildPodSpec` replaces the direct assignment of `cluster.Spec.TopologySpreadConstraints`
with a call to `mergeTopologySpreadConstraints(cluster, cluster.Spec.TopologySpreadConstraints)`.

**`minDomains` is intentionally not set.** Without `minDomains`, K8s computes
skew only across zones that actually exist. In a single-AZ cluster, there is
only one eligible domain → skew is always 0 → the constraint does not block
scheduling. This makes the default safe for single-AZ and multi-AZ clusters
alike, with no operator-side cluster inspection required.

### 5.4 Finalizer

Extend `finalizer.go` so that the finalizer callback additionally deletes the
ClusterRole and ClusterRoleBinding by name (ignoring NotFound). Order: delete
RoleBinding first, then Role. Failure to find an object is not an error.

### 5.5 Tests

**Unit tests (envtest) — new or extended files:**

- `reconcile_rbac_test.go` (new):
  - First reconcile creates both objects with expected name, labels, rules,
    subjects.
  - Second reconcile is idempotent (no spurious updates).
- `reconcile_statefulset_test.go` (extend):
  - `Args` of main container contains `CLUSTER_NAME` in its export list.
  - Init container image is a curl image.
  - Init container env contains `HOST_NODE_NAME` sourced from `spec.nodeName`.
- `topology_spread_test.go` or an addition to the existing controller test:
  - Empty user constraints → result contains exactly the default zone
    constraint with the right selector.
  - User has a constraint on `kubernetes.io/hostname` only → result is
    `[user_hostname, default_zone]`.
  - User has a constraint on `topology.kubernetes.io/zone` → result is the
    user's only; the default is NOT added.
- `finalizer_test.go` (extend):
  - CR deletion path removes ClusterRole and ClusterRoleBinding.
  - Missing RBAC objects do not cause the finalizer to error.

**Integration / e2e (`test/operator-integration`):**

- Deploy a WoodpeckerCluster; assert that ClusterRole and ClusterRoleBinding
  exist with the expected label selectors.
- Assert that the rendered StatefulSet's `PodSpec.TopologySpreadConstraints`
  contains the default zone constraint.
- Assert the pod env surfaces `AVAILABILITY_ZONE` and `CLUSTER_NAME` (values
  may be fallbacks under kind).

**Out of scope for this spec:** A full shell test of the init-container script
with mocked curl responses. If time allows, add a focused shell script test;
otherwise defer.

**Manual verification (minikube multi-node):**

A lightweight end-to-end check to run once before merge. This is human-driven,
not CI-automated.

1. **Start a multi-node minikube cluster** (3 nodes):
   ```sh
   minikube start --nodes=3 --driver=docker -p wp-topo
   ```
2. **Label nodes with different zones and a shared region.** For example:
   ```sh
   kubectl label node wp-topo       topology.kubernetes.io/zone=zone-a topology.kubernetes.io/region=region-x --overwrite
   kubectl label node wp-topo-m02   topology.kubernetes.io/zone=zone-b topology.kubernetes.io/region=region-x --overwrite
   kubectl label node wp-topo-m03   topology.kubernetes.io/zone=zone-c topology.kubernetes.io/region=region-x --overwrite
   ```
3. **Install the operator** (from this branch) following `deployments/operator/README.md`.
4. **Create a WoodpeckerCluster with `replicas: 3`** and no user-supplied
   `topologySpreadConstraints`.
5. **Verify pod distribution:**
   ```sh
   kubectl get pods -l app.kubernetes.io/name=woodpecker -o wide
   ```
   Expected: one pod per node, i.e. one pod per zone.
6. **Verify env vars inside a pod:**
   ```sh
   kubectl exec <pod> -- sh -c 'echo $AVAILABILITY_ZONE $CLUSTER_NAME'
   ```
   Expected: the zone of the node the pod is running on, and `region-x`.
7. **Scale up to `replicas: 5`** via `kubectl edit` or `kubectl patch` on the CR.
   Expected: 5 pods distributed across the 3 zones as evenly as maxSkew=1
   allows (2 + 2 + 1).
8. **Scale down to `replicas: 2`**.
   Expected: 2 pods remain, each in a distinct zone.
9. **Verify RBAC cleanup:** delete the CR, confirm the per-cluster
   ClusterRole and ClusterRoleBinding are gone:
   ```sh
   kubectl get clusterrole,clusterrolebinding | grep woodpecker-node-reader
   ```

If all nine steps pass, the feature is considered manually validated in
addition to the automated suites above.

## 6. File Change Summary

**New:**
- `deployments/operator/internal/controller/reconcile_rbac.go`
- `deployments/operator/internal/controller/reconcile_rbac_test.go`

**Modified:**
- `deployments/operator/internal/controller/woodpeckercluster_controller.go`
  (pipeline wiring + kubebuilder RBAC markers)
- `deployments/operator/internal/controller/reconcile_statefulset.go`
  (init container script/image/env, main container `Args`,
  `mergeTopologySpreadConstraints`, `defaultZoneTopologySpreadConstraint`)
- `deployments/operator/internal/controller/finalizer.go`
- `deployments/operator/internal/controller/finalizer_test.go`
- `deployments/operator/internal/controller/woodpeckercluster_controller_test.go`
  (default topology constraint, env var surface, RBAC presence)
- `deployments/operator/internal/controller/suite_test.go`
  (only if new fixtures are needed; otherwise leave as-is)
- `deployments/operator/config/rbac/role.yaml`
  (auto-generated via `make manifests`)

**Unchanged:**
- `deployments/operator/api/v1alpha1/woodpeckercluster_types.go` — no new CRD
  fields are introduced.
- ConfigMap, Service, PDB, webhook reconcilers.

**Documentation:**
- `deployments/operator/README.md` — new section "Topology-aware scheduling"
  describing the default behavior, how to override, the semantics of
  `AVAILABILITY_ZONE` / `CLUSTER_NAME`, and fallback values.

## 7. Rollout and Compatibility

- **Behavior change for existing users:**
  - Upgrading operator with existing WoodpeckerClusters will cause a rolling
    restart of server pods because the PodTemplate changes (new init image,
    new Args, new constraint). This is expected and aligned with normal
    operator upgrades.
  - Pods that previously got `AVAILABILITY_ZONE=default` will now get their
    real zone, or `default-az` fallback. Any downstream that relied on the
    literal string `default` should be reviewed.
- **RBAC:** The operator's own manifests must be regenerated and redeployed
  because new cluster-scoped permissions are required.
- **Rollback:** Reverting the operator binary + CRs to a pre-feature version
  will orphan the per-cluster ClusterRole/Binding objects. Operators should
  clean those up manually or re-run the finalizer via a fresh deletion cycle.

## 8. Open Questions

None at the time of writing. All four brainstorming questions are resolved:
label keys are fixed, `RESOURCE_GROUP` stays as `default`, the default
constraint merges with user input by `topologyKey`, and the init container
path (option A) is the chosen resolution mechanism.
