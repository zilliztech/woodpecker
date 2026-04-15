# Operator Topology-Aware Deployment Implementation Plan

> **Progress (2026-04-15):** Tasks 1-10 complete on branch `enhance_operator_topology_aware`. Task 11 (manual minikube verification) is the only remaining step — **this is where you come in**. See the Task 11 section below for the procedure you need to run.
>
> **Commit map:**
>
> | Task    | Commit      | Description                                                |
> | ------- | ----------- | ---------------------------------------------------------- |
> | T1      | `487757a` | topology spread merge helpers                              |
> | T2      | `9c7444f` | inject default zone TSC in pod spec                        |
> | T3      | `ca8742a` | reconcileRBAC for per-cluster node reader                  |
> | T4      | `04fbc71` | wire reconcileRBAC + envtest (+ fix label-value `/` bug) |
> | T5      | `f633382` | init container reads real node topology labels             |
> | T6      | `31bb800` | export CLUSTER_NAME from main container                    |
> | T7      | `63124ca` | finalizer cleans up cluster-scoped RBAC                    |
> | T8      | `289c8c6` | regenerate RBAC manifests                                  |
> | (gofmt) | `b672968` | gofmt doc comment                                          |
> | T10     | `f469f03` | README topology-aware scheduling section                   |
> | hotfix  | `af8e69e` | grant manager `get nodes` (RBAC escalation block found during T11) |
> | hotfix  | `ba4c6c3` | init script regex tolerates pretty-printed JSON whitespace (T11) |
>
> **Test status at HEAD (`ba4c6c3`):** `make test` green — controller 77.8% coverage (was 46.7%), webhook 92.9%. Total: 20 unit tests + 16 Ginkgo envtest specs pass. Manual T11: Steps 5/6/7/9 confirmed pass on a first run; Step 8 (scale-down) was skipped that round because pod main containers crashed without an object-storage backend. The revised T11 below adds a single-pod minio + a real `woodpecker.yaml` so the pods actually run and Step 8 becomes verifiable.
>
> **Known deviation from spec** (documented in commit `04fbc71` and below): the `rbacLabels()` helper was changed from a single `owned-by: "<ns>/<name>"` label to two labels `owned-by-namespace` + `owned-by-name` because K8s label values cannot contain `/`. Finalizer cleanup (Task 7) still works because deletion is by name, not label selector.
>
> **Non-blocking follow-ups identified by final review** (track separately if desired):
>
> - Add `Watches(&rbacv1.ClusterRoleBinding{})` so operator recreates the CRB if someone deletes it out-of-band (otherwise pods silently fall back to `default-az` until next CR edit / operator restart).
> - Anchor `grep` patterns in the init script to prevent collision with hypothetical labels whose keys END with `topology.kubernetes.io/zone` or `/region`.
> - Add `curl --max-filesize 1M` for defence in depth against pathological node objects.
> - Extract the embedded init script via `go:embed` into a separate `.sh` file for shellcheck coverage and easier editing.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Make the Woodpecker operator schedule pods across availability zones by default and inject each node's real `topology.kubernetes.io/zone` / `topology.kubernetes.io/region` labels as `AVAILABILITY_ZONE` / `CLUSTER_NAME` env vars in the Woodpecker process, with fallbacks `default-az` / `default-cluster`.

**Architecture:** Three coordinated changes on branch `enhance_operator_topology_aware`: (1) a new `reconcileRBAC` sub-reconciler that creates a per-cluster `ClusterRole` + `ClusterRoleBinding` granting the pod's ServiceAccount `get` on nodes; (2) the existing `init-topology` init container switched to `curlimages/curl` with a script that queries the K8s API for the node's labels and writes real values into `/etc/woodpecker/topology.env`; (3) a merge helper in `buildPodSpec` that injects a default zone `TopologySpreadConstraint` alongside any user-supplied constraints (user constraint on the same `topologyKey` wins).

**Tech Stack:** Go, controller-runtime, kubebuilder, Ginkgo v2 + envtest (integration), plain `testing` (unit), fake client from `sigs.k8s.io/controller-runtime/pkg/client/fake`.

**Spec:** [docs/superpowers/specs/2026-04-15-operator-topology-aware-design.md](../specs/2026-04-15-operator-topology-aware-design.md)

---

## File Structure

| File                                                                                | Role                                                                  | Action                             |
| ----------------------------------------------------------------------------------- | --------------------------------------------------------------------- | ---------------------------------- |
| `deployments/operator/internal/controller/reconcile_statefulset.go`               | Existing StatefulSet builder; holds init container + pod spec helpers | Modify                             |
| `deployments/operator/internal/controller/reconcile_statefulset_topology_test.go` | Unit tests for merge helpers (pure functions)                         | Create                             |
| `deployments/operator/internal/controller/reconcile_rbac.go`                      | New sub-reconciler for per-cluster ClusterRole + ClusterRoleBinding   | Create                             |
| `deployments/operator/internal/controller/reconcile_rbac_test.go`                 | Unit tests using fake client                                          | Create                             |
| `deployments/operator/internal/controller/woodpeckercluster_controller.go`        | Controller pipeline + kubebuilder RBAC markers                        | Modify                             |
| `deployments/operator/internal/controller/woodpeckercluster_controller_test.go`   | Ginkgo end-to-end assertions for RBAC + constraint                    | Modify                             |
| `deployments/operator/internal/controller/finalizer.go`                           | CR-deletion cleanup, extended to delete cluster-scoped RBAC           | Modify                             |
| `deployments/operator/internal/controller/finalizer_test.go`                      | Cover RBAC cleanup path                                               | Modify                             |
| `deployments/operator/config/rbac/role.yaml`                                      | Auto-generated manager RBAC                                           | Regenerated via `make manifests` |
| `deployments/operator/README.md`                                                  | Operator user docs                                                    | Modify (new section)               |

---

## Task 1: Unit-test and implement topology spread merge helpers

**Files:**

- Create: `deployments/operator/internal/controller/reconcile_statefulset_topology_test.go`
- Modify: `deployments/operator/internal/controller/reconcile_statefulset.go` (add two helpers at end of file)

- [X] **Step 1: Write the failing unit tests**

Create `deployments/operator/internal/controller/reconcile_statefulset_topology_test.go`:

```go
/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

func newTestCluster(name string) *woodpeckerv1alpha1.WoodpeckerCluster {
	return &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
	}
}

func TestDefaultZoneTopologySpreadConstraint(t *testing.T) {
	c := defaultZoneTopologySpreadConstraint(newTestCluster("wp"))

	assert.Equal(t, int32(1), c.MaxSkew)
	assert.Equal(t, "topology.kubernetes.io/zone", c.TopologyKey)
	assert.Equal(t, corev1.DoNotSchedule, c.WhenUnsatisfiable)
	require.NotNil(t, c.LabelSelector)
	assert.Equal(t, "wp", c.LabelSelector.MatchLabels["app.kubernetes.io/instance"])
	assert.Equal(t, "woodpecker", c.LabelSelector.MatchLabels["app.kubernetes.io/name"])
}

func TestMergeTopologySpreadConstraints_EmptyUser_AddsDefault(t *testing.T) {
	cluster := newTestCluster("wp")

	result := mergeTopologySpreadConstraints(cluster, nil)

	require.Len(t, result, 1)
	assert.Equal(t, "topology.kubernetes.io/zone", result[0].TopologyKey)
}

func TestMergeTopologySpreadConstraints_UserOnlyHostname_AddsDefault(t *testing.T) {
	cluster := newTestCluster("wp")
	userC := []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       "kubernetes.io/hostname",
		WhenUnsatisfiable: corev1.ScheduleAnyway,
	}}

	result := mergeTopologySpreadConstraints(cluster, userC)

	require.Len(t, result, 2)
	assert.Equal(t, "kubernetes.io/hostname", result[0].TopologyKey)
	assert.Equal(t, "topology.kubernetes.io/zone", result[1].TopologyKey)
}

func TestMergeTopologySpreadConstraints_UserHasZone_DefaultNotAdded(t *testing.T) {
	cluster := newTestCluster("wp")
	userC := []corev1.TopologySpreadConstraint{{
		MaxSkew:           3,
		TopologyKey:       "topology.kubernetes.io/zone",
		WhenUnsatisfiable: corev1.ScheduleAnyway,
	}}

	result := mergeTopologySpreadConstraints(cluster, userC)

	require.Len(t, result, 1)
	assert.Equal(t, int32(3), result[0].MaxSkew)
	assert.Equal(t, corev1.ScheduleAnyway, result[0].WhenUnsatisfiable)
}
```

- [X] **Step 2: Run tests to verify they fail**

```
cd deployments/operator
go test ./internal/controller/ -run 'TestDefaultZoneTopologySpreadConstraint|TestMergeTopologySpreadConstraints' -v
```

Expected: compilation errors — `defaultZoneTopologySpreadConstraint` and `mergeTopologySpreadConstraints` undefined.

- [X] **Step 3: Add the helpers to `reconcile_statefulset.go`**

Append to the end of `deployments/operator/internal/controller/reconcile_statefulset.go` (inside the existing `package controller`, no new imports needed — `corev1` and `metav1` are already imported):

```go
// defaultZoneTopologySpreadConstraint returns the operator's default
// TopologySpreadConstraint on the well-known zone label. It enforces
// at-most-1 skew across zones and blocks scheduling if the constraint
// cannot be satisfied.
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

// mergeTopologySpreadConstraints returns the user-supplied constraints plus
// the operator's default zone constraint, unless the user already specified
// a constraint on the zone topologyKey — in which case the user's wins.
func mergeTopologySpreadConstraints(
	cluster *woodpeckerv1alpha1.WoodpeckerCluster,
	user []corev1.TopologySpreadConstraint,
) []corev1.TopologySpreadConstraint {
	result := append([]corev1.TopologySpreadConstraint{}, user...)
	for _, c := range user {
		if c.TopologyKey == "topology.kubernetes.io/zone" {
			return result
		}
	}
	return append(result, defaultZoneTopologySpreadConstraint(cluster))
}
```

- [X] **Step 4: Run tests to verify they pass**

```
go test ./internal/controller/ -run 'TestDefaultZoneTopologySpreadConstraint|TestMergeTopologySpreadConstraints' -v
```

Expected: all four cases PASS.

- [X] **Step 5: Commit**

```
git add deployments/operator/internal/controller/reconcile_statefulset.go \
        deployments/operator/internal/controller/reconcile_statefulset_topology_test.go
git commit -m "feat(operator): add topology spread merge helpers"
```

---

## Task 2: Wire the merge helper into buildPodSpec

**Files:**

- Modify: `deployments/operator/internal/controller/reconcile_statefulset.go:102-114` (`buildPodSpec`)

- [X] **Step 1: Modify `buildPodSpec`**

In `deployments/operator/internal/controller/reconcile_statefulset.go`, replace the `buildPodSpec` function body (around lines 102-114). Find:

```go
func (r *WoodpeckerClusterReconciler) buildPodSpec(cluster *woodpeckerv1alpha1.WoodpeckerCluster) corev1.PodSpec {
	return corev1.PodSpec{
		ServiceAccountName:            serverName(cluster),
		TerminationGracePeriodSeconds: ptr.To(int64(300)),
		InitContainers:                r.buildInitContainers(cluster),
		Containers:                    r.buildContainers(cluster),
		Volumes:                       r.buildVolumes(cluster),
		Affinity:                      cluster.Spec.Affinity,
		Tolerations:                   cluster.Spec.Tolerations,
		NodeSelector:                  cluster.Spec.NodeSelector,
		TopologySpreadConstraints:     cluster.Spec.TopologySpreadConstraints,
	}
}
```

Replace with:

```go
func (r *WoodpeckerClusterReconciler) buildPodSpec(cluster *woodpeckerv1alpha1.WoodpeckerCluster) corev1.PodSpec {
	return corev1.PodSpec{
		ServiceAccountName:            serverName(cluster),
		TerminationGracePeriodSeconds: ptr.To(int64(300)),
		InitContainers:                r.buildInitContainers(cluster),
		Containers:                    r.buildContainers(cluster),
		Volumes:                       r.buildVolumes(cluster),
		Affinity:                      cluster.Spec.Affinity,
		Tolerations:                   cluster.Spec.Tolerations,
		NodeSelector:                  cluster.Spec.NodeSelector,
		TopologySpreadConstraints:     mergeTopologySpreadConstraints(cluster, cluster.Spec.TopologySpreadConstraints),
	}
}
```

- [X] **Step 2: Add a unit test for the wiring**

Append to `deployments/operator/internal/controller/reconcile_statefulset_topology_test.go`:

```go
func TestBuildPodSpec_InjectsDefaultZoneConstraint(t *testing.T) {
	r := &WoodpeckerClusterReconciler{}
	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "default"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			ServicePort: 18080,
			GossipPort:  17946,
		},
	}

	spec := r.buildPodSpec(cluster)

	require.Len(t, spec.TopologySpreadConstraints, 1)
	assert.Equal(t, "topology.kubernetes.io/zone", spec.TopologySpreadConstraints[0].TopologyKey)
}
```

- [X] **Step 3: Run the tests**

```
cd deployments/operator
go test ./internal/controller/ -run 'TestBuildPodSpec_InjectsDefaultZoneConstraint' -v
```

Expected: PASS.

- [X] **Step 4: Commit**

```
git add deployments/operator/internal/controller/reconcile_statefulset.go \
        deployments/operator/internal/controller/reconcile_statefulset_topology_test.go
git commit -m "feat(operator): inject default zone topology spread in pod spec"
```

---

## Task 3: Implement `reconcileRBAC` sub-reconciler

**Files:**

- Create: `deployments/operator/internal/controller/reconcile_rbac.go`
- Create: `deployments/operator/internal/controller/reconcile_rbac_test.go`

- [X] **Step 1: Write the failing tests**

Create `deployments/operator/internal/controller/reconcile_rbac_test.go`:

```go
/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

func newRBACTestScheme(t *testing.T) *runtime.Scheme {
	s := runtime.NewScheme()
	require.NoError(t, woodpeckerv1alpha1.AddToScheme(s))
	require.NoError(t, rbacv1.AddToScheme(s))
	return s
}

func newRBACTestCluster() *woodpeckerv1alpha1.WoodpeckerCluster {
	return &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "ns1"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			Replicas: ptr.To(int32(1)),
		},
	}
}

func TestClusterRoleNames(t *testing.T) {
	c := newRBACTestCluster()
	assert.Equal(t, "woodpecker-node-reader-ns1-wp", clusterRoleName(c))
	assert.Equal(t, "woodpecker-node-reader-ns1-wp", clusterRoleBindingName(c))
}

func TestReconcileRBAC_CreatesRoleAndBinding(t *testing.T) {
	s := newRBACTestScheme(t)
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	r := &WoodpeckerClusterReconciler{Client: cl, Scheme: s}
	cluster := newRBACTestCluster()

	err := r.reconcileRBAC(context.Background(), cluster)
	require.NoError(t, err)

	cr := &rbacv1.ClusterRole{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: clusterRoleName(cluster)}, cr))
	require.Len(t, cr.Rules, 1)
	assert.Equal(t, []string{""}, cr.Rules[0].APIGroups)
	assert.Equal(t, []string{"nodes"}, cr.Rules[0].Resources)
	assert.Equal(t, []string{"get"}, cr.Rules[0].Verbs)

	crb := &rbacv1.ClusterRoleBinding{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: clusterRoleBindingName(cluster)}, crb))
	assert.Equal(t, "ClusterRole", crb.RoleRef.Kind)
	assert.Equal(t, clusterRoleName(cluster), crb.RoleRef.Name)
	require.Len(t, crb.Subjects, 1)
	assert.Equal(t, "ServiceAccount", crb.Subjects[0].Kind)
	assert.Equal(t, serverName(cluster), crb.Subjects[0].Name)
	assert.Equal(t, cluster.Namespace, crb.Subjects[0].Namespace)
	assert.Equal(t, "ns1/wp", crb.Labels["woodpecker.zilliz.io/owned-by"])
}

func TestReconcileRBAC_Idempotent(t *testing.T) {
	s := newRBACTestScheme(t)
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	r := &WoodpeckerClusterReconciler{Client: cl, Scheme: s}
	cluster := newRBACTestCluster()

	require.NoError(t, r.reconcileRBAC(context.Background(), cluster))
	require.NoError(t, r.reconcileRBAC(context.Background(), cluster))

	list := &rbacv1.ClusterRoleList{}
	require.NoError(t, cl.List(context.Background(), list, client.MatchingLabels{"woodpecker.zilliz.io/owned-by": "ns1/wp"}))
	assert.Len(t, list.Items, 1)
}

func TestReconcileRBAC_DoesNotErrorWhenAlreadyExists(t *testing.T) {
	s := newRBACTestScheme(t)
	cluster := newRBACTestCluster()
	existing := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: clusterRoleName(cluster)},
	}
	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(existing).Build()
	r := &WoodpeckerClusterReconciler{Client: cl, Scheme: s}

	err := r.reconcileRBAC(context.Background(), cluster)
	require.NoError(t, err)

	cr := &rbacv1.ClusterRole{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: clusterRoleName(cluster)}, cr))
	assert.Len(t, cr.Rules, 1, "rules should be updated on existing ClusterRole")
}
```

- [X] **Step 2: Run tests to verify compilation failure**

```
cd deployments/operator
go test ./internal/controller/ -run 'TestReconcileRBAC|TestClusterRoleNames' -v
```

Expected: compilation failure — `reconcileRBAC`, `clusterRoleName`, `clusterRoleBindingName` undefined.

- [X] **Step 3: Create `reconcile_rbac.go`**

Create `deployments/operator/internal/controller/reconcile_rbac.go`:

```go
/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package controller

import (
	"context"
	"fmt"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

const ownedByLabel = "woodpecker.zilliz.io/owned-by"

func clusterRoleName(cluster *woodpeckerv1alpha1.WoodpeckerCluster) string {
	return fmt.Sprintf("woodpecker-node-reader-%s-%s", cluster.Namespace, cluster.Name)
}

func clusterRoleBindingName(cluster *woodpeckerv1alpha1.WoodpeckerCluster) string {
	return clusterRoleName(cluster)
}

func rbacLabels(cluster *woodpeckerv1alpha1.WoodpeckerCluster) map[string]string {
	l := commonLabels(cluster)
	l[ownedByLabel] = fmt.Sprintf("%s/%s", cluster.Namespace, cluster.Name)
	return l
}

// reconcileRBAC ensures a per-cluster ClusterRole and ClusterRoleBinding exist
// so the pod's ServiceAccount can GET node objects to read topology labels.
// These cluster-scoped objects cannot have namespaced owner references; they
// are cleaned up by the finalizer on CR deletion.
func (r *WoodpeckerClusterReconciler) reconcileRBAC(ctx context.Context, cluster *woodpeckerv1alpha1.WoodpeckerCluster) error {
	logger := log.FromContext(ctx)

	cr := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: clusterRoleName(cluster)}}
	crOp, err := controllerutil.CreateOrUpdate(ctx, r.Client, cr, func() error {
		cr.Labels = rbacLabels(cluster)
		cr.Rules = []rbacv1.PolicyRule{{
			APIGroups: []string{""},
			Resources: []string{"nodes"},
			Verbs:     []string{"get"},
		}}
		return nil
	})
	if err != nil {
		return fmt.Errorf("reconciling ClusterRole: %w", err)
	}
	logger.Info("ClusterRole reconciled", "name", cr.Name, "operation", crOp)

	crb := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: clusterRoleBindingName(cluster)}}
	crbOp, err := controllerutil.CreateOrUpdate(ctx, r.Client, crb, func() error {
		crb.Labels = rbacLabels(cluster)
		crb.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     clusterRoleName(cluster),
		}
		crb.Subjects = []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      serverName(cluster),
			Namespace: cluster.Namespace,
		}}
		return nil
	})
	if err != nil {
		return fmt.Errorf("reconciling ClusterRoleBinding: %w", err)
	}
	logger.Info("ClusterRoleBinding reconciled", "name", crb.Name, "operation", crbOp)

	return nil
}
```

- [X] **Step 4: Run tests to verify they pass**

```
cd deployments/operator
go test ./internal/controller/ -run 'TestReconcileRBAC|TestClusterRoleNames' -v
```

Expected: all four cases PASS.

- [X] **Step 5: Commit**

```
git add deployments/operator/internal/controller/reconcile_rbac.go \
        deployments/operator/internal/controller/reconcile_rbac_test.go
git commit -m "feat(operator): add reconcileRBAC for per-cluster node reader"
```

---

## Task 4: Wire `reconcileRBAC` into the reconcile pipeline

**Files:**

- Modify: `deployments/operator/internal/controller/woodpeckercluster_controller.go`

- [X] **Step 1: Add kubebuilder RBAC marker**

Open `deployments/operator/internal/controller/woodpeckercluster_controller.go`. After the existing kubebuilder markers (around line 49, after the `coordination.k8s.io/leases` marker), add one more line:

```go
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles;clusterrolebindings,verbs=get;list;watch;create;update;patch;delete
```

- [X] **Step 2: Insert `reconcileRBAC` in the sub-reconcilers slice**

In the same file, find the `reconcilers := []func(...)` slice around lines 98-106. It currently begins with `r.reconcileServiceAccount,`. Insert `r.reconcileRBAC,` directly after it:

```go
reconcilers := []func(context.Context, *woodpeckerv1alpha1.WoodpeckerCluster) error{
    r.reconcileServiceAccount,
    r.reconcileRBAC,
    r.reconcileConfigMap,
    r.reconcileHeadlessService,
    r.reconcileClientService,
    r.reconcileMetricsService,
    r.reconcilePDB,
    r.reconcileStatefulSet,
}
```

- [X] **Step 3: Extend the existing Ginkgo test to assert RBAC creation**

Open `deployments/operator/internal/controller/woodpeckercluster_controller_test.go`. Locate the `"When reconciling a resource"` Context's main `It("should reconcile the resource")` block (if the exact `It` name differs, locate the first `It` within that Context). Inside the `It` body, after the reconcile call, add assertions. First extend the imports — at the top of the file add the `rbacv1` import:

```go
import (
    // ... existing imports ...
    rbacv1 "k8s.io/api/rbac/v1"
)
```

Inside the `It` body where other resources (StatefulSet, ConfigMap) are verified, append:

```go
By("creating a per-cluster ClusterRole for node reads")
cr := &rbacv1.ClusterRole{}
Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "woodpecker-node-reader-default-" + resourceName}, cr)).To(Succeed())
Expect(cr.Rules).To(HaveLen(1))
Expect(cr.Rules[0].Resources).To(ConsistOf("nodes"))
Expect(cr.Rules[0].Verbs).To(ConsistOf("get"))

By("creating a matching ClusterRoleBinding bound to the server ServiceAccount")
crb := &rbacv1.ClusterRoleBinding{}
Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "woodpecker-node-reader-default-" + resourceName}, crb)).To(Succeed())
Expect(crb.Subjects).To(HaveLen(1))
Expect(crb.Subjects[0].Name).To(Equal(resourceName + "-server"))

By("injecting the default zone TopologySpreadConstraint in the StatefulSet")
sts := &appsv1.StatefulSet{}
Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-server", Namespace: "default"}, sts)).To(Succeed())
Expect(sts.Spec.Template.Spec.TopologySpreadConstraints).ToNot(BeEmpty())
foundZone := false
for _, c := range sts.Spec.Template.Spec.TopologySpreadConstraints {
    if c.TopologyKey == "topology.kubernetes.io/zone" {
        foundZone = true
        Expect(c.MaxSkew).To(Equal(int32(1)))
        Expect(c.WhenUnsatisfiable).To(Equal(corev1.DoNotSchedule))
    }
}
Expect(foundZone).To(BeTrue(), "expected zone topology spread constraint")
```

- [X] **Step 4: Run the controller Ginkgo suite**

```
cd deployments/operator
make test
```

Expected: the controller suite passes, including the new RBAC + constraint assertions.

- [X] **Step 5: Commit**

```
git add deployments/operator/internal/controller/woodpeckercluster_controller.go \
        deployments/operator/internal/controller/woodpeckercluster_controller_test.go
git commit -m "feat(operator): wire reconcileRBAC and verify topology constraint"
```

---

## Task 5: Change init container image, env, and script to read real node labels

**Files:**

- Modify: `deployments/operator/internal/controller/reconcile_statefulset.go:191-243` (`buildInitContainers`)

- [X] **Step 1: Replace `buildInitContainers` in `reconcile_statefulset.go`**

Find the existing `buildInitContainers` function (starts ~line 191). Replace the whole function with:

```go
// buildInitContainers creates an init container that:
// 1. Computes gossip seeds from pod ordinal and headless service DNS
// 2. Queries the K8s API using the pod's ServiceAccount token to read the
//    node's topology labels (zone, region) and writes them to a shared env file.
// On any lookup failure the container still writes fallback values and exits 0,
// so a transient API-server hiccup will not block pod startup.
func (r *WoodpeckerClusterReconciler) buildInitContainers(cluster *woodpeckerv1alpha1.WoodpeckerCluster) []corev1.Container {
	maxSeeds := int32(3)
	if cluster.Spec.Replicas != nil && *cluster.Spec.Replicas < maxSeeds {
		maxSeeds = *cluster.Spec.Replicas
	}
	var seedPatterns []string
	for i := int32(0); i < maxSeeds; i++ {
		seedPatterns = append(seedPatterns, fmt.Sprintf(
			"%s-server-%d.${HEADLESS_SVC}:${GOSSIP_PORT}", cluster.Name, i,
		))
	}
	seedsExpr := strings.Join(seedPatterns, ",")

	script := fmt.Sprintf(`#!/bin/sh
set -e

# All nodes get the same seeds — gossip ignores the seed pointing to self
SEEDS="%s"

# Query the node's topology labels via K8s API. Fail-soft: any error → fallback.
APISERVER=https://kubernetes.default.svc
TOKEN_FILE=/var/run/secrets/kubernetes.io/serviceaccount/token
CA_FILE=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt
NODE_JSON=""
if [ -r "$TOKEN_FILE" ] && [ -r "$CA_FILE" ] && [ -n "$HOST_NODE_NAME" ]; then
    NODE_JSON=$(curl -sf --max-time 10 --cacert "$CA_FILE" \
        -H "Authorization: Bearer $(cat $TOKEN_FILE)" \
        "$APISERVER/api/v1/nodes/$HOST_NODE_NAME" || true)
fi

AZ=$(printf '%%s' "$NODE_JSON" \
    | grep -o '"topology.kubernetes.io/zone":"[^"]*"' \
    | head -n1 | cut -d'"' -f4)
REGION=$(printf '%%s' "$NODE_JSON" \
    | grep -o '"topology.kubernetes.io/region":"[^"]*"' \
    | head -n1 | cut -d'"' -f4)

: "${AZ:=default-az}"
: "${REGION:=default-cluster}"

cat > /etc/woodpecker/topology.env << EOF
SEEDS=${SEEDS}
AVAILABILITY_ZONE=${AZ}
CLUSTER_NAME=${REGION}
RESOURCE_GROUP=default
EOF

echo "Init complete: pod=$POD_NAME node=$HOST_NODE_NAME az=$AZ cluster=$REGION"
`, seedsExpr)

	return []corev1.Container{
		{
			Name:    "init-topology",
			Image:   "curlimages/curl:8.7.1",
			Command: []string{"/bin/sh", "-c", script},
			Env: []corev1.EnvVar{
				{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
				{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
				// HOST_NODE_NAME is the K8s node this pod landed on (spec.nodeName),
				// distinct from POD_NAME which is the pod's own name.
				{Name: "HOST_NODE_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}}},
				{Name: "HEADLESS_SVC", Value: fmt.Sprintf("%s.$(POD_NAMESPACE).svc.cluster.local", headlessServiceName(cluster))},
				{Name: "GOSSIP_PORT", Value: fmt.Sprintf("%d", cluster.Spec.GossipPort)},
			},
			VolumeMounts: []corev1.VolumeMount{
				{Name: "topology", MountPath: "/etc/woodpecker"},
			},
		},
	}
}
```

Note the `%%s` in the `fmt.Sprintf` template — it escapes a literal `%s` that is passed to shell `printf`. Only the first `%s` (for `seedsExpr`) is a Go format verb.

- [X] **Step 2: Add unit tests for the init container shape**

Append to `deployments/operator/internal/controller/reconcile_statefulset_topology_test.go`:

```go
func TestBuildInitContainers_UsesCurlImage(t *testing.T) {
	r := &WoodpeckerClusterReconciler{}
	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "default"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			GossipPort: 17946,
			Replicas:   ptr.To(int32(3)),
		},
	}

	initCs := r.buildInitContainers(cluster)
	require.Len(t, initCs, 1)
	assert.Equal(t, "init-topology", initCs[0].Name)
	assert.Contains(t, initCs[0].Image, "curlimages/curl")
}

func TestBuildInitContainers_HasHostNodeNameFromSpecNodeName(t *testing.T) {
	r := &WoodpeckerClusterReconciler{}
	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "default"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			GossipPort: 17946,
			Replicas:   ptr.To(int32(1)),
		},
	}
	initCs := r.buildInitContainers(cluster)
	require.Len(t, initCs, 1)

	var hostNodeName *corev1.EnvVar
	for i := range initCs[0].Env {
		if initCs[0].Env[i].Name == "HOST_NODE_NAME" {
			hostNodeName = &initCs[0].Env[i]
			break
		}
	}
	require.NotNil(t, hostNodeName, "expected HOST_NODE_NAME env var")
	require.NotNil(t, hostNodeName.ValueFrom)
	require.NotNil(t, hostNodeName.ValueFrom.FieldRef)
	assert.Equal(t, "spec.nodeName", hostNodeName.ValueFrom.FieldRef.FieldPath)
}

func TestBuildInitContainers_ScriptCallsK8sAPI(t *testing.T) {
	r := &WoodpeckerClusterReconciler{}
	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "default"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			GossipPort: 17946,
			Replicas:   ptr.To(int32(1)),
		},
	}
	initCs := r.buildInitContainers(cluster)
	require.Len(t, initCs, 1)
	require.Len(t, initCs[0].Command, 3)
	script := initCs[0].Command[2]
	assert.Contains(t, script, "kubernetes.default.svc")
	assert.Contains(t, script, "topology.kubernetes.io/zone")
	assert.Contains(t, script, "topology.kubernetes.io/region")
	assert.Contains(t, script, "default-az")
	assert.Contains(t, script, "default-cluster")
	assert.Contains(t, script, "CLUSTER_NAME=")
}
```

Add `"k8s.io/utils/ptr"` to the test file's imports if not already present.

- [X] **Step 3: Run the tests**

```
cd deployments/operator
go test ./internal/controller/ -run 'TestBuildInitContainers' -v
```

Expected: all three tests PASS.

- [X] **Step 4: Commit**

```
git add deployments/operator/internal/controller/reconcile_statefulset.go \
        deployments/operator/internal/controller/reconcile_statefulset_topology_test.go
git commit -m "feat(operator): init container reads real node topology labels"
```

---

## Task 6: Export `CLUSTER_NAME` from the main container

**Files:**

- Modify: `deployments/operator/internal/controller/reconcile_statefulset.go:124` (`buildContainers`)

- [X] **Step 1: Write the failing test**

Append to `deployments/operator/internal/controller/reconcile_statefulset_topology_test.go`:

```go
func TestBuildContainers_ExportsClusterName(t *testing.T) {
	r := &WoodpeckerClusterReconciler{}
	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "default"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			Image:       "zilliztech/woodpecker:v0.1.26",
			ServicePort: 18080,
			GossipPort:  17946,
			MetricsPort: 9091,
		},
	}
	cs := r.buildContainers(cluster)
	require.Len(t, cs, 1)
	require.Len(t, cs[0].Args, 1)
	assert.Contains(t, cs[0].Args[0], "export SEEDS AVAILABILITY_ZONE CLUSTER_NAME RESOURCE_GROUP")
}
```

- [X] **Step 2: Run test to verify it fails**

```
cd deployments/operator
go test ./internal/controller/ -run 'TestBuildContainers_ExportsClusterName' -v
```

Expected: FAIL — the current `Args` still exports only `SEEDS AVAILABILITY_ZONE RESOURCE_GROUP`.

- [X] **Step 3: Update `buildContainers` Args**

In `deployments/operator/internal/controller/reconcile_statefulset.go`, find the main container's `Args` line (around line 124):

```go
Args:    []string{". /etc/woodpecker/topology.env && export SEEDS AVAILABILITY_ZONE RESOURCE_GROUP && exec /tini -- /woodpecker/bin/start-woodpecker.sh"},
```

Change to:

```go
Args:    []string{". /etc/woodpecker/topology.env && export SEEDS AVAILABILITY_ZONE CLUSTER_NAME RESOURCE_GROUP && exec /tini -- /woodpecker/bin/start-woodpecker.sh"},
```

- [X] **Step 4: Run test to verify it passes**

```
go test ./internal/controller/ -run 'TestBuildContainers_ExportsClusterName' -v
```

Expected: PASS.

- [X] **Step 5: Commit**

```
git add deployments/operator/internal/controller/reconcile_statefulset.go \
        deployments/operator/internal/controller/reconcile_statefulset_topology_test.go
git commit -m "feat(operator): export CLUSTER_NAME from main container"
```

---

## Task 7: Clean up ClusterRole and ClusterRoleBinding in the finalizer

**Files:**

- Modify: `deployments/operator/internal/controller/finalizer.go`
- Modify: `deployments/operator/internal/controller/finalizer_test.go`

- [X] **Step 1: Write a failing unit test**

Append to `deployments/operator/internal/controller/finalizer_test.go`:

```go
func TestReconcileDelete_RemovesClusterScopedRBAC(t *testing.T) {
	// Imports needed: rbacv1, runtime, fake client, woodpeckerv1alpha1 — add to the
	// existing import block if not already present.
	s := runtime.NewScheme()
	require.NoError(t, woodpeckerv1alpha1.AddToScheme(s))
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, rbacv1.AddToScheme(s))

	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "wp",
			Namespace:  "default",
			Finalizers: []string{finalizerName},
			DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
		},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{MetricsPort: 9091},
	}
	cr := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "woodpecker-node-reader-default-wp"}}
	crb := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "woodpecker-node-reader-default-wp"}}

	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster, cr, crb).Build()
	r := &WoodpeckerClusterReconciler{Client: cl, Scheme: s}

	_, err := r.reconcileDelete(context.Background(), cluster)
	require.NoError(t, err)

	gotCR := &rbacv1.ClusterRole{}
	err = cl.Get(context.Background(), types.NamespacedName{Name: "woodpecker-node-reader-default-wp"}, gotCR)
	assert.True(t, apierrors.IsNotFound(err), "ClusterRole should be deleted, got err=%v", err)

	gotCRB := &rbacv1.ClusterRoleBinding{}
	err = cl.Get(context.Background(), types.NamespacedName{Name: "woodpecker-node-reader-default-wp"}, gotCRB)
	assert.True(t, apierrors.IsNotFound(err), "ClusterRoleBinding should be deleted, got err=%v", err)
}

func TestReconcileDelete_RBACCleanupIgnoresNotFound(t *testing.T) {
	s := runtime.NewScheme()
	require.NoError(t, woodpeckerv1alpha1.AddToScheme(s))
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, rbacv1.AddToScheme(s))

	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wp",
			Namespace:         "default",
			Finalizers:        []string{finalizerName},
			DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
		},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{MetricsPort: 9091},
	}
	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build()
	r := &WoodpeckerClusterReconciler{Client: cl, Scheme: s}

	_, err := r.reconcileDelete(context.Background(), cluster)
	require.NoError(t, err, "missing RBAC objects must not cause finalizer error")
}
```

Add to the existing `finalizer_test.go` imports:

```go
import (
    // ... existing ...
    rbacv1 "k8s.io/api/rbac/v1"
    apierrors "k8s.io/apimachinery/pkg/api/errors"
    "k8s.io/apimachinery/pkg/runtime"
    "k8s.io/apimachinery/pkg/types"
    "sigs.k8s.io/controller-runtime/pkg/client/fake"
    woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)
```

- [X] **Step 2: Run to verify it fails**

```
cd deployments/operator
go test ./internal/controller/ -run 'TestReconcileDelete_RemovesClusterScopedRBAC|TestReconcileDelete_RBACCleanupIgnoresNotFound' -v
```

Expected: tests compile, but the first test FAILS (ClusterRole/Binding still exist).

- [X] **Step 3: Extend `reconcileDelete` in `finalizer.go`**

Open `deployments/operator/internal/controller/finalizer.go`. Add to the imports:

```go
rbacv1 "k8s.io/api/rbac/v1"
apierrors "k8s.io/apimachinery/pkg/api/errors"
metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
```

(`metav1` is not currently imported in `finalizer.go` — add all three lines.)

In `reconcileDelete`, locate the block right before `controllerutil.RemoveFinalizer(...)`:

```go
// All pods safe — remove finalizer
logger.Info("All pods decommissioned, removing finalizer")
```

Insert **before** that block:

```go
// Clean up cluster-scoped RBAC before removing the finalizer. These objects
// do not have namespace-scoped owner refs, so K8s GC will not cascade.
if err := r.deleteClusterScopedRBAC(ctx, cluster); err != nil {
    return ctrl.Result{}, err
}
```

Then add a new helper method to the same file:

```go
func (r *WoodpeckerClusterReconciler) deleteClusterScopedRBAC(ctx context.Context, cluster *woodpeckerv1alpha1.WoodpeckerCluster) error {
	logger := log.FromContext(ctx)

	crb := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: clusterRoleBindingName(cluster)}}
	if err := r.Delete(ctx, crb); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("deleting ClusterRoleBinding: %w", err)
	}
	logger.Info("ClusterRoleBinding deleted", "name", crb.Name)

	cr := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: clusterRoleName(cluster)}}
	if err := r.Delete(ctx, cr); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("deleting ClusterRole: %w", err)
	}
	logger.Info("ClusterRole deleted", "name", cr.Name)

	return nil
}
```

- [X] **Step 4: Run tests to verify they pass**

```
go test ./internal/controller/ -run 'TestReconcileDelete_RemovesClusterScopedRBAC|TestReconcileDelete_RBACCleanupIgnoresNotFound' -v
```

Expected: both PASS.

- [X] **Step 5: Commit**

```
git add deployments/operator/internal/controller/finalizer.go \
        deployments/operator/internal/controller/finalizer_test.go
git commit -m "feat(operator): finalizer cleans up cluster-scoped RBAC"
```

---

## Task 8: Regenerate operator manager RBAC manifests

**Files:**

- Regenerated: `deployments/operator/config/rbac/role.yaml`

- [X] **Step 1: Run `make manifests`**

```
cd deployments/operator
make manifests
```

Expected: the command prints `controller-gen ...` and returns 0. No source-code changes.

- [X] **Step 2: Inspect the regenerated role**

```
git diff config/rbac/role.yaml
```

Expected: the diff adds rules under `apiGroups: ["rbac.authorization.k8s.io"]` for `clusterroles` and `clusterrolebindings` with `get;list;watch;create;update;patch;delete`.

If the diff is empty, the kubebuilder marker in Task 4 Step 1 was not added correctly — revisit and re-run.

- [X] **Step 3: Commit**

```
git add config/rbac/role.yaml
git commit -m "chore(operator): regenerate RBAC manifests for cluster-scoped perms"
```

---

## Task 9: Run the full operator test suite

**Files:** none (verification only)

- [X] **Step 1: Run all operator tests**

```
cd deployments/operator
make test
```

Expected: `ok` for every package; total failures = 0. The coverage file `cover.out` is regenerated.

- [X] **Step 2: If any test fails, stop and fix before continuing**

Do not proceed to manual verification until `make test` is green.

---

## Task 10: Document topology-aware scheduling in the operator README

**Files:**

- Modify: `deployments/operator/README.md`

- [X] **Step 1: Add a new section**

Open `deployments/operator/README.md`. After the existing configuration or CR-spec section (pick a location consistent with the doc's structure — near where `topologySpreadConstraints` or `nodeSelector` are mentioned, or at the end of the "Usage" / "Configuration" area), add:

````markdown
## Topology-aware scheduling

The operator schedules Woodpecker pods across availability zones by default and
exposes each pod's physical location to the Woodpecker process.

### Spread across zones by default

Every `WoodpeckerCluster` gets a default `TopologySpreadConstraint` on
`topology.kubernetes.io/zone` with `maxSkew: 1` and
`whenUnsatisfiable: DoNotSchedule`. In single-AZ clusters this is a no-op
(Kubernetes computes skew only over existing zones). In multi-AZ clusters pods
are pinned to distinct zones.

To customize zone behavior, set your own constraint on the same `topologyKey`:

```yaml
spec:
  topologySpreadConstraints:
    - maxSkew: 2
      topologyKey: topology.kubernetes.io/zone
      whenUnsatisfiable: ScheduleAnyway
      labelSelector:
        matchLabels:
          app.kubernetes.io/instance: my-cluster
```

The operator detects your constraint on the zone key and does not add its default.

### Node-label environment variables

Each pod's Woodpecker process receives two env vars sourced from the node it is
running on:

| Env var | Source node label | Fallback |
|---|---|---|
| `AVAILABILITY_ZONE` | `topology.kubernetes.io/zone` | `default-az` |
| `CLUSTER_NAME` | `topology.kubernetes.io/region` | `default-cluster` |

An init container reads the node via the K8s API at pod startup using the
pod's ServiceAccount token. The operator creates a per-cluster `ClusterRole`
and `ClusterRoleBinding` granting `get` on `nodes`. These are cleaned up when
the `WoodpeckerCluster` CR is deleted.
````

- [X] **Step 2: Commit**

```
git add deployments/operator/README.md
git commit -m "docs(operator): document topology-aware scheduling"
```

---

## Task 11: Manual verification on minikube

**Files:** none (human-in-the-loop validation)

This is the manual verification section. Automation can't exercise real
multi-AZ scheduling or the init container's K8s-API path, so we run a small
multi-node minikube cluster end-to-end. **This must pass before opening the
PR for review.**

The flow brings up a single-pod **minio** (object storage) so the woodpecker
servers can actually start; we do **not** stand up etcd in this test. The
woodpecker config provided below uses minio for storage and lists the three
gossip seeds explicitly so the server's config validation passes.

### Prerequisites

Install locally:

- `minikube` (tested with v1.32+)
- `docker` (or another minikube driver)
- `kubectl`
- A working local Go ≥ 1.25 (for the fast operator build path, see Step 3)

### Network notes (China users)

If you're on a network that's slow or blocked from Docker Hub, you'll hit two
choke points:

1. **Operator image base `golang:1.25`** — Step 3 below uses a *host build* +
   a thin `Dockerfile.local` so we never have to pull `golang:1.25` at all.
2. **`zilliztech/woodpecker:vX.Y.Z` and `minio/minio:...`** — Step 6 has notes
   on pulling via mirrors (`docker.m.daocloud.io`, `dockerproxy.com`, etc.) and
   then `minikube image load`-ing.

### What success looks like (cheat sheet)

| Step | What it proves                                           | Pass signal                                                                     |
| ---- | -------------------------------------------------------- | ------------------------------------------------------------------------------- |
| 9    | Pods reach Running                                       | All `topo-server-*` pods `READY 1/1 Running`                                    |
| 10   | Default zone TSC works                                   | 3 pods on 3 distinct nodes (one per zone)                                       |
| 11   | Init script reads real node labels                       | `AZ=zone-X CLUSTER_NAME=region-x` matching the host node's labels               |
| 12   | Scheduling tolerates `replicas > zones` with `maxSkew=1` | 5 pods split 2+2+1 across 3 zones, none stuck Pending                           |
| 13   | Scale-down preserves zone spread                         | 2 pods survive, each on a distinct zone                                         |
| 14   | Finalizer cleans cluster-scoped RBAC                     | `kubectl get clusterrole \| grep woodpecker-node-reader-default-topo` → empty |

If any pass signal is wrong, **stop and report** before continuing — don't
tear down the cluster (you'll want the state for debugging).

### Execution

Run each step in order, ticking the boxes as you go. All commands can be
copy-pasted directly.

- [X] **Step 1: Start a 3-node minikube cluster**

```
minikube start --nodes=3 --driver=docker -p wp-topo
kubectl config use-context wp-topo
```

- [X] **Step 2: Label nodes with zone + region**

```
kubectl label node wp-topo     topology.kubernetes.io/zone=zone-a topology.kubernetes.io/region=region-x --overwrite
kubectl label node wp-topo-m02 topology.kubernetes.io/zone=zone-b topology.kubernetes.io/region=region-x --overwrite
kubectl label node wp-topo-m03 topology.kubernetes.io/zone=zone-c topology.kubernetes.io/region=region-x --overwrite
```

- [X] **Step 3: Build the operator image (host-compile, fast path)**

We deliberately avoid `make docker-build` because it pulls `golang:1.25`
(~1.3 GB from Docker Hub). The host-compile path uses your local Go and
ships only the `manager` binary into a thin `distroless` image — finishes in
under a minute even on slow networks.

```
cd /Users/zilliz/Downloads/zilliz/Woodpecker_PRs/zilliztechMainRepo/woodpecker/deployments/operator

# Confirm minikube node arch (decides GOARCH below)
minikube -p wp-topo ssh "uname -m"
# aarch64 → use GOARCH=arm64 ; x86_64 → use GOARCH=amd64

# Cross-compile the manager binary (uses your local Go module cache)
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o bin/manager cmd/main.go

# Whitelist bin/manager in .dockerignore (one-time; idempotent)
grep -qxF '!bin/manager' .dockerignore || echo '!bin/manager' >> .dockerignore

# Thin Dockerfile (one-time; safe to recreate)
cat > Dockerfile.local <<'EOF'
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY bin/manager /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
EOF

# Build (only distroless base needs to be pulled; ~2 MB)
docker build -f Dockerfile.local -t woodpecker-operator:topo-test .

# Load into the minikube cluster
minikube -p wp-topo image load woodpecker-operator:topo-test
```

- [X] **Step 4: Deploy the operator**

`make deploy` applies `config/default` which includes the CRD, the manager
RBAC, and the operator Deployment. CRDs and webhooks are NOT separate steps.

```
make deploy IMG=woodpecker-operator:topo-test

kubectl -n woodpecker-operator-system rollout status deploy/woodpecker-operator-controller-manager --timeout=120s
kubectl get crd | grep woodpeckerclusters
```

Pass: rollout reports successful, CRD `woodpeckerclusters.woodpecker.zilliz.io`
is listed. Tail the operator logs in another terminal during the next steps:

```
kubectl -n woodpecker-operator-system logs deploy/woodpecker-operator-controller-manager -f
```

- [X] **Step 5: Deploy a single-pod minio for object storage**

Apply this manifest (Deployment + Service + a one-shot Job that creates the
`woodpecker` bucket on first start):

````yaml
cat > /tmp/minio.yaml <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: default
  labels: { app: minio }
spec:
  replicas: 1
  selector: { matchLabels: { app: minio } }
  template:
    metadata:
      labels: { app: minio }
    spec:
      containers:
      - name: minio
        image: minio/minio:RELEASE.2024-12-18T13-15-44Z
        imagePullPolicy: IfNotPresent
        args: ["server", "/data"]
        env:
        - { name: MINIO_ROOT_USER,     value: minioadmin }
        - { name: MINIO_ROOT_PASSWORD, value: minioadmin }
        ports:
        - containerPort: 9000
---
apiVersion: v1
kind: Service
metadata: { name: minio, namespace: default }
spec:
  selector: { app: minio }
  ports:
  - { name: s3, port: 9000, targetPort: 9000 }
---
apiVersion: batch/v1
kind: Job
metadata: { name: minio-bucket-init, namespace: default }
spec:
  backoffLimit: 6
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: mc
        image: minio/mc:RELEASE.2024-11-21T17-21-54Z
        command: ["/bin/sh","-c"]
        args:
        - |
          set -e
          until mc alias set local http://minio:9000 minioadmin minioadmin; do
            echo "waiting for minio..."; sleep 3
          done
          mc mb -p local/woodpecker || true
          mc ls local/
EOF
minikube -p wp-topo image load minio/mc:RELEASE.2024-11-21T17-21-54Z
minikube -p wp-topo image load minio/minio:RELEASE.2024-12-18T13-15-44Z
kubectl apply -f /tmp/minio.yaml

# Wait until minio is ready and the bucket exists
kubectl rollout status deploy/minio --timeout=120s
kubectl wait --for=condition=complete job/minio-bucket-init --timeout=120s
kubectl logs job/minio-bucket-init | tail -5
````

Pass signal: the bucket-init job log shows `[2025-...]  0B woodpecker/`.

- [X] **Step 6: Pre-pull the woodpecker server image (handles slow Docker Hub)**

Adjust the tag to whatever release exists at the time of testing — `v0.1.25`
is the most recent published tag as of writing. If `v0.1.26` (the operator's
default) hasn't been published yet, use `v0.1.25` and override the CR's
`spec.image` in Step 8.

```
# Try direct Docker Hub first
docker pull zilliztech/woodpecker:latest || \
  docker pull docker.m.daocloud.io/zilliztech/woodpecker:latest && \
  docker tag docker.m.daocloud.io/zilliztech/woodpecker:latest zilliztech/woodpecker:latest

# Same for the curl init image (small, usually fast)
docker pull curlimages/curl:8.7.1 || \
  docker pull docker.m.daocloud.io/curlimages/curl:8.7.1 && \
  docker tag docker.m.daocloud.io/curlimages/curl:8.7.1 curlimages/curl:8.7.1

# Load both into minikube
minikube -p wp-topo image load woodpecker:latest
minikube -p wp-topo image load curlimages/curl:8.7.1
```

If the daocloud mirror is also down, try `docker.mirrors.ustc.edu.cn`,
`dockerproxy.com`, or `docker.1ms.run`.

- [X] **Step 7: Create a `woodpecker.yaml` ConfigMap pointing at minio**

The operator's auto-generated default ConfigMap is intentionally minimal and
won't pass the woodpecker server's config validation (no buffer-pool seeds,
no minio endpoint). Provide a real one and reference it from the CR.

```yaml
cat > /tmp/wp-config.yaml <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: wp-topo-config
  namespace: default
data:
  woodpecker.yaml: |
    woodpecker:
      client:
        quorum:
          quorumBufferPools:
            - name: default-pool
              seeds:
                - topo-server-0.topo-server-headless.default.svc.cluster.local:18080
                - topo-server-1.topo-server-headless.default.svc.cluster.local:18080
                - topo-server-2.topo-server-headless.default.svc.cluster.local:18080
          quorumSelectStrategy:
            affinityMode: soft
            replicas: 3
            strategy: random
      storage:
        type: minio
        rootPath: /woodpecker/data
    minio:
      address: minio.default.svc.cluster.local
      port: 9000
      accessKeyID: minioadmin
      secretAccessKey: minioadmin
      useSSL: false
      bucketName: woodpecker
      region: ""
      requestTimeoutMs: 10000
    log:
      level: info
      format: json
      stdout: true
EOF
kubectl apply -f /tmp/wp-config.yaml
```

> **Note:** if the woodpecker server's config validator complains about other
> required fields when pods start, copy more fields from `config/woodpecker.yaml`
> in the repo root into this ConfigMap.

- [ ] **Step 8: Apply the WoodpeckerCluster CR (with `configRef` and image override)**

```yaml
cat > /tmp/wp-topo.yaml <<'EOF'
apiVersion: woodpecker.zilliz.io/v1alpha1
kind: WoodpeckerCluster
metadata:
  name: topo
  namespace: default
spec:
  replicas: 3
  image: woodpecker:latest      # match the tag you pulled in Step 6
  storageSize: 1Gi
  configRef:
    name: wp-topo-config
EOF
kubectl apply -f /tmp/wp-topo.yaml
```

- [X] **Step 9: Wait for all 3 pods to reach Running**

```
kubectl wait --for=condition=ready pod -l app.kubernetes.io/instance=topo --timeout=180s
kubectl get pods -l app.kubernetes.io/instance=topo -o wide
```

Pass: `READY 1/1` and `STATUS Running` for all three pods. If any pod is in
`CrashLoopBackOff`, check its logs:

```
kubectl logs topo-server-0 -c woodpecker --tail=40
```

Common cause: missing field in the ConfigMap from Step 7. Add the field and
re-apply.

- [X] **Step 10: Verify pod-to-zone distribution**

```
kubectl get pods -l app.kubernetes.io/instance=topo \
  -o custom-columns=POD:.metadata.name,NODE:.spec.nodeName,STATUS:.status.phase
```

Pass: the three pods land on the three different nodes (`wp-topo`, `wp-topo-m02`,
`wp-topo-m03`).

- [ ] **Step 11: Verify env vars are injected into the woodpecker process**

> **Why NOT `kubectl exec ... -- sh -c 'echo $AVAILABILITY_ZONE'`?** That
> command spawns a **new** shell inside the container; that shell inherits
> only what's in the PodSpec's `env` field, not what the entrypoint script
> `export`-ed before running `exec /tini -- .../woodpecker`. The vars ARE in
> the woodpecker process's environment — you just have to read them from the
> right place.

Two equivalent ways to check. Pick one (or both):

**A. Read the `topology.env` file the init container wrote:**

```
for i in 0 1 2; do
  echo "=== topo-server-$i ==="
  kubectl exec topo-server-$i -c woodpecker -- cat /etc/woodpecker/topology.env \
    | grep -E "AVAILABILITY_ZONE|CLUSTER_NAME"
done
```

**B. Read PID 1's environ directly (the woodpecker / tini process):**

```
for i in 0 1 2; do
  echo "=== topo-server-$i ==="
  kubectl exec topo-server-$i -c woodpecker -- sh -c \
    'tr "\0" "\n" < /proc/1/environ | grep -E "AVAILABILITY_ZONE|CLUSTER_NAME|RESOURCE_GROUP"'
done
```

Pass: each pod's `AVAILABILITY_ZONE` matches the zone of its host node
(one of `zone-a`, `zone-b`, `zone-c`). All three `CLUSTER_NAME` values equal
`region-x`. `RESOURCE_GROUP` is `default` (hardcoded, reserved for future use).

**Backup check (doesn't need the main container running):**

```
kubectl logs topo-server-0 -c init-topology
```

Should end with something like
`Init complete: pod=topo-server-0 node=wp-topo az=zone-a cluster=region-x`.

- [X] **Step 12: Scale up to 5**

```
kubectl patch woodpeckercluster topo --type=merge -p '{"spec":{"replicas":5}}'
sleep 30
kubectl get pods -l app.kubernetes.io/instance=topo \
  -o custom-columns=POD:.metadata.name,NODE:.spec.nodeName,STATUS:.status.phase
```

Pass: 5 pods, distributed across the 3 zones in some 2+2+1 pattern. None stuck
`Pending`. Skew across zones is at most 1.

- [X] **Step 13: Scale down to 2**

```
kubectl patch woodpeckercluster topo --type=merge -p '{"spec":{"replicas":2}}'
# operator decommissions pods via HTTP; allow some time
sleep 60
kubectl get pods -l app.kubernetes.io/instance=topo \
  -o custom-columns=POD:.metadata.name,NODE:.spec.nodeName,STATUS:.status.phase
```

Pass: exactly 2 pods remain, on 2 distinct zones. (If pods are stuck because
decommission isn't completing, check operator logs — but with healthy pods this
should succeed within ~30 seconds.)

- [ ] **Step 14: Delete the CR and verify cluster-scoped RBAC cleanup**

```
echo "=== BEFORE delete ==="
kubectl get clusterrole,clusterrolebinding | grep woodpecker-node-reader-default-topo

kubectl delete woodpeckercluster topo --timeout=180s

echo "=== AFTER delete (should be empty) ==="
kubectl get clusterrole,clusterrolebinding 2>/dev/null | grep woodpecker-node-reader-default-topo || echo "clean ✓"
```

Pass: BEFORE shows the per-cluster ClusterRole + ClusterRoleBinding; AFTER
prints `clean ✓` (i.e. no matching objects).

- [ ] **Step 15: Clean up minio + wp-config ConfigMap**

```
kubectl delete -f /tmp/wp-config.yaml
kubectl delete -f /tmp/minio.yaml
```

- [ ] **Step 16: Tear down minikube**

```
minikube delete -p wp-topo
```

- [ ] **Step 17: Record the result**

If Steps 9–14 all pass, note it in the PR description, e.g.:

> Manual verification on minikube v1.xx (3 nodes, 3 zones, single-pod minio):
> Steps 9–14 pass. Operator image built via host-compile + thin Dockerfile;
> woodpecker server image `v0.1.25` from Docker Hub (mirror).

If anything fails, capture the failing pod's logs + `kubectl describe` output
and open an issue (or fix in this branch before merge).

---

## Summary of commits

See the commit map at the top of this document for the final set of 10 commits
(Tasks 1-10 plus a gofmt cleanup). Task 11 produces no commits — it is
human-run validation only.
