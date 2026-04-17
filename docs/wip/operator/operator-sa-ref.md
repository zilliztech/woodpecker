# Operator External ServiceAccount Reference Support

> **Version:** v0.1 (2026-04-17)
> **Branch:** `feat/service-account-customization`
> **Status:** Implemented

## Background

The Woodpecker Operator currently creates and manages its own ServiceAccount for each
WoodpeckerCluster. The SA is overwritten on every reconcile cycle (labels only, no
annotations), making it impossible to use cloud-provider identity mechanisms such as:

- **AWS EKS IRSA** (`eks.amazonaws.com/role-arn`)
- **GCP GKE Workload Identity** (`iam.gke.io/gcp-service-account`)
- **Azure AKS Workload Identity** (`azure.workload.identity/client-id`)
- **Alibaba Cloud ACK RRSA** (`pod-identity.alibabacloud.com/role-name`)

Woodpecker pods need to access object storage (S3/GCS/Azure Blob/OSS) in cloud
environments. The industry-standard approach is to associate an IAM role with a
Kubernetes ServiceAccount, allowing pods to obtain temporary credentials without
static AK/SK.

## Design Decision

Follow the **Milvus Operator pattern**: Operator does NOT create or manage the SA
when an external SA is specified. The user creates the SA themselves (with appropriate
annotations), and the CRD references it by name.

Key difference from Milvus Operator: Woodpecker's init container requires `get nodes`
permission to read topology labels. The Operator will still create a ClusterRole +
ClusterRoleBinding for the external SA to grant this permission.

### Behavior Summary

| `serviceAccountName` set? | SA Creation | RBAC (get nodes) | StatefulSet SA ref |
|---------------------------|-------------|-------------------|--------------------|
| No (empty)                | Operator creates `{name}-server` | Binds to `{name}-server` | `{name}-server` |
| Yes (e.g. `woodpecker-admin`) | **Skipped** | Binds to `woodpecker-admin` | `woodpecker-admin` |

---

## Part 1: Code Changes

### - [x] Task 1 — CRD: Add `serviceAccountName` field

**File:** `api/v1alpha1/woodpeckercluster_types.go`

Add to `WoodpeckerClusterSpec`:

```go
// serviceAccountName references a pre-existing ServiceAccount for server pods.
// If set, operator skips SA creation and uses this one directly.
// The SA must exist in the same namespace as the WoodpeckerCluster.
// Typically used for cloud IAM integration (IRSA, GKE Workload Identity, etc.).
// +optional
ServiceAccountName string `json:"serviceAccountName,omitempty"`
```

### - [x] Task 2 — Helper: Add `resolveServiceAccountName()` function

**File:** `internal/controller/labels.go`

```go
func resolveServiceAccountName(cluster *woodpeckerv1alpha1.WoodpeckerCluster) string {
    if cluster.Spec.ServiceAccountName != "" {
        return cluster.Spec.ServiceAccountName
    }
    return serverName(cluster)
}
```

### - [x] Task 3 — reconcileServiceAccount: Skip when external SA specified

**File:** `internal/controller/reconcile_serviceaccount.go`

Add early return at the top of `reconcileServiceAccount()`:

```go
if cluster.Spec.ServiceAccountName != "" {
    logger.Info("Using external ServiceAccount, skipping creation",
        "name", cluster.Spec.ServiceAccountName)
    return nil
}
```

### - [x] Task 4 — reconcileStatefulSet: Use resolved SA name

**File:** `internal/controller/reconcile_statefulset.go`

Change `buildPodSpec()`:

```go
// Before:
ServiceAccountName: serverName(cluster),
// After:
ServiceAccountName: resolveServiceAccountName(cluster),
```

### - [x] Task 5 — reconcileRBAC: Bind to resolved SA name

**File:** `internal/controller/reconcile_rbac.go`

Change ClusterRoleBinding Subject:

```go
// Before:
Name: serverName(cluster),
// After:
Name: resolveServiceAccountName(cluster),
```

### - [x] Task 6 — Regenerate manifests

```bash
cd deployments/operator
make manifests generate
```

Verify `config/crd/bases/woodpecker.zilliz.io_woodpeckerclusters.yaml` contains the
new `serviceAccountName` field.

### - [x] Task 7 — Unit tests

Add test cases for:
1. Default behavior (no `serviceAccountName`): SA created, StatefulSet and RBAC use
   `{name}-server`
2. External SA behavior: SA creation skipped, StatefulSet and RBAC reference the
   specified SA name

---

## Part 2: Open-Source Deployment Flow

### - [x] Task 8 — Sample YAML: Default mode

**File:** `config/samples/woodpecker_v1alpha1_woodpeckercluster.yaml` (already exists)

Contains:
- `ConfigMap` with `woodpecker.yaml` (etcd + minio config)
- `WoodpeckerCluster` (no `serviceAccountName`, operator auto-creates SA)

No changes needed — existing sample already covers the default mode.

### - [x] Task 9 — Sample YAML: External SA mode

**File:** `config/samples/woodpeckercluster_external_sa.yaml`

Contains (3 resources in one file, `---` separated):
1. `ConfigMap` with `woodpecker.yaml`
2. `ServiceAccount` (user-managed, placeholder for annotations)
3. `WoodpeckerCluster` with `serviceAccountName` referencing the SA

### - [x] Task 10 — e2e test on kind cluster

Verify on kind:
1. Default mode: operator creates SA, pods run, RBAC binding correct
2. External SA mode: operator skips SA creation, pods reference correct SA, RBAC
   binding targets external SA
3. Topology init container works with external SA (get nodes permission)

---

## Part 3: Cloud Deployment (IAM Object Storage)

### - [x] Task 11 — AWS S3 IAM sample YAML

**File:** `config/samples/cloud/woodpeckercluster_aws_s3_iam.yaml`

```yaml
# Resource 1: ConfigMap (S3 config, useIAM: true)
# Resource 2: ServiceAccount (with eks.amazonaws.com/role-arn annotation)
# Resource 3: WoodpeckerCluster (references the SA)
```

For users with an existing SA (e.g. `woodpecker-admin` already annotated with IRSA):
skip Resource 2, set `serviceAccountName: woodpecker-admin` in Resource 3 directly.

### - [x] Task 12 — GCP/Azure IAM sample YAML

**Files:**
- `config/samples/cloud/woodpeckercluster_gcp_gcs_iam.yaml`
  - SA annotation: `iam.gke.io/gcp-service-account`
- `config/samples/cloud/woodpeckercluster_azure_blob_iam.yaml`
  - SA annotation: `azure.workload.identity/client-id`
  - Pod label: `azure.workload.identity/use: "true"` (via `spec.podLabels`)

### - [x] Task 13 — Cloud deployment documentation

**File:** `docs/wip/operator/operator-cloud-deploy.md`

Sections:
1. **Prerequisites**: OIDC provider, IAM role, SA annotation
2. **New SA deployment flow**: Create SA → Annotate → Apply ConfigMap → Apply CR
3. **Existing SA deployment flow**: Verify annotation → Apply ConfigMap → Apply CR
   (skip SA creation)
4. **Verification steps**: Check Pod SA, check IRSA env injection, check S3 access
5. **Provider-specific notes**: AWS / GCP / Azure / Alibaba Cloud differences

### - [x] Task 14 — EKS UAT environment verification

> Refer to [operator-cloud-deploy.md](operator-cloud-deploy.md) for the general cloud
> deployment guide. This section covers the **specific steps** for our EKS UAT environment
> using the existing `woodpecker-admin` SA.

#### Environment Info

- **Cluster**: EKS UAT (us-west-2)
- **kubectl**: `kubectl --kubeconfig kubeconfig_cloud`
- **Existing SA**: `woodpecker-admin` in namespace `woodpecker`
- **Existing SA token**: `woodpecker-admin-token` (secret-based)

#### Step 1: Add IRSA annotation to existing SA

The existing SA currently has no annotations. Add the IRSA role ARN:

```bash
kubectl --kubeconfig kubeconfig_cloud annotate sa woodpecker-admin -n woodpecker \
  eks.amazonaws.com/role-arn=arn:aws:iam::<ACCOUNT_ID>:role/<ROLE_NAME>
```

> **Important**: The IAM role must already exist with:
> - S3 permissions (read/write on the target bucket)
> - Trust policy allowing `system:serviceaccount:woodpecker:woodpecker-admin` to
>   assume it via OIDC
>
> See [operator-cloud-deploy.md](operator-cloud-deploy.md) Step 1.2 "AWS EKS" for
> the trust-policy.json template.

Verify:

```bash
kubectl --kubeconfig kubeconfig_cloud describe sa woodpecker-admin -n woodpecker
# Should show:
# Annotations: eks.amazonaws.com/role-arn: arn:aws:iam::<ACCOUNT_ID>:role/<ROLE_NAME>
```

#### Step 2: Install CRD

```bash
cd deployments/operator
make install
```

Verify:

```bash
kubectl --kubeconfig kubeconfig_cloud get crd woodpeckerclusters.woodpecker.zilliz.io
```

#### Step 3: Deploy Operator

```bash
export IMG=<your-registry>/woodpecker-operator:v0.1.0
make docker-build IMG=$IMG
make docker-push IMG=$IMG
make deploy IMG=$IMG
# Or to a custom namespace:
# make deploy IMG=$IMG OPERATOR_NAMESPACE=woodpecker
```

Verify:

```bash
kubectl --kubeconfig kubeconfig_cloud -n woodpecker-operator-system get pods
# Should see operator-controller-manager pod Running
```

#### Step 4: Prepare ConfigMap

Create a ConfigMap with S3 config (no AK/SK, using IAM):

```bash
cat <<'EOF' | kubectl --kubeconfig kubeconfig_cloud apply -n woodpecker -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: woodpecker-config
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
      logstore:
        segmentSyncPolicy:
          syncInterval: 1000
          syncMaxBytes: 4194304
    log:
      level: info
      format: json
      stdout: true
    etcd:
      endpoints:
        - <etcd-endpoint>:2379
      rootPath: by-dev
    minio:
      address: s3.<REGION>.amazonaws.com
      port: 443
      useSSL: true
      useIAM: true
      bucketName: <BUCKET_NAME>
      rootPath: woodpecker
EOF
```

> **Note**: Replace `<etcd-endpoint>`, `<REGION>`, `<BUCKET_NAME>` with actual values.

#### Step 5: Apply WoodpeckerCluster CR (using existing SA)

Since we're using the existing `woodpecker-admin` SA, we skip SA creation and apply
only the CR:

```bash
cat <<'EOF' | kubectl --kubeconfig kubeconfig_cloud apply -n woodpecker -f -
apiVersion: woodpecker.zilliz.io/v1alpha1
kind: WoodpeckerCluster
metadata:
  name: woodpecker-uat
spec:
  image: zilliztech/woodpecker:v0.1.26
  replicas: 3
  serviceAccountName: woodpecker-admin
  resources:
    requests:
      cpu: "500m"
      memory: "512Mi"
    limits:
      cpu: "1"
      memory: "1Gi"
  storageSize: 10Gi
  servicePort: 18080
  gossipPort: 17946
  metricsPort: 9091
  configRef:
    name: woodpecker-config
EOF
```

#### Step 6: Verification Checklist

Run each verification and confirm the expected output:

**6.1 Cluster status**

```bash
kubectl --kubeconfig kubeconfig_cloud get woodpeckerclusters -n woodpecker
# Expected: PHASE=Running, READY=3, REPLICAS=3
```

**6.2 Pod uses the correct SA**

```bash
kubectl --kubeconfig kubeconfig_cloud get pod woodpecker-uat-server-0 -n woodpecker \
  -o jsonpath='{.spec.serviceAccountName}'
# Expected: woodpecker-admin
```

**6.3 IRSA environment variables are injected by EKS**

```bash
kubectl --kubeconfig kubeconfig_cloud describe pod woodpecker-uat-server-0 -n woodpecker \
  | grep -E "AWS_ROLE_ARN|AWS_WEB_IDENTITY_TOKEN_FILE"
# Expected:
#   AWS_ROLE_ARN:                 arn:aws:iam::<ACCOUNT_ID>:role/<ROLE_NAME>
#   AWS_WEB_IDENTITY_TOKEN_FILE:  /var/run/secrets/eks.amazonaws.com/serviceaccount/token
```

**6.4 IRSA token volume is mounted**

```bash
kubectl --kubeconfig kubeconfig_cloud describe pod woodpecker-uat-server-0 -n woodpecker \
  | grep "aws-iam-token"
# Expected: a Projected volume with TokenExpirationSeconds
```

**6.5 ClusterRoleBinding targets the existing SA**

```bash
kubectl --kubeconfig kubeconfig_cloud get clusterrolebinding \
  woodpecker-node-reader-woodpecker-woodpecker-uat \
  -o jsonpath='{.subjects[0].name}'
# Expected: woodpecker-admin
```

**6.6 Operator did NOT create a separate SA**

```bash
kubectl --kubeconfig kubeconfig_cloud get sa woodpecker-uat-server -n woodpecker
# Expected: Error / NotFound (operator should NOT create this)
```

**6.7 Init container topology detection works**

```bash
kubectl --kubeconfig kubeconfig_cloud logs woodpecker-uat-server-0 -n woodpecker \
  -c init-topology
# Expected: "Init complete: pod=... node=... az=us-west-2x cluster=us-west-2"
# (not default-az / default-cluster)
```

**6.8 Woodpecker can access S3**

```bash
kubectl --kubeconfig kubeconfig_cloud logs woodpecker-uat-server-0 -n woodpecker \
  | grep -i "storage\|s3\|minio\|bucket"
# Expected: no credential errors, successful initialization
```

#### Troubleshooting

| Symptom | Check | Fix |
|---------|-------|-----|
| Pod has no `AWS_ROLE_ARN` env | `describe sa woodpecker-admin` → missing annotation | Add annotation per Step 1 |
| Pod stuck in `Pending` | `describe pod` → SA not found | Verify SA exists in `woodpecker` namespace |
| Init container shows `default-az` | `get clusterrolebinding` → wrong SA name or missing | Check Task 5 code change is deployed |
| S3 `AccessDenied` | IAM role policy | Check S3 bucket policy and IAM role permissions |
| S3 `WebIdentityErr` | OIDC trust policy mismatch | Verify SA name and namespace in trust policy |

#### Cleanup

```bash
kubectl --kubeconfig kubeconfig_cloud delete woodpeckercluster woodpecker-uat -n woodpecker
kubectl --kubeconfig kubeconfig_cloud delete configmap woodpecker-config -n woodpecker
# Do NOT delete woodpecker-admin SA — it's shared / pre-existing
```

---

## Revision History

| Version | Date       | Changes                        |
|---------|------------|--------------------------------|
| v0.1    | 2026-04-17 | Initial plan and Task 1-13     |
| v0.2    | 2026-04-17 | Task 14 EKS UAT verification steps added |
