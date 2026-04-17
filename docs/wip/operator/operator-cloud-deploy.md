# Woodpecker Operator Cloud Deployment Guide (IAM Object Storage)

> **Version:** v0.1 (2026-04-17)

This guide covers deploying Woodpecker on Kubernetes with cloud-native IAM-based object
storage access. This avoids static AK/SK credentials by leveraging each cloud's identity
federation mechanism.

## Supported Cloud Providers

| Cloud | Mechanism | SA Annotation | Extra Config |
|-------|-----------|---------------|--------------|
| AWS EKS | IRSA | `eks.amazonaws.com/role-arn` | None |
| GCP GKE | Workload Identity | `iam.gke.io/gcp-service-account` | None |
| Azure AKS | Workload Identity | `azure.workload.identity/client-id` | Pod label: `azure.workload.identity/use: "true"` |
| Alibaba Cloud ACK | RRSA | `pod-identity.alibabacloud.com/role-name` | None |

For clouds without SA-based IAM (Tencent TKE, Huawei CCE), use static credentials in
the ConfigMap's `woodpecker.yaml` or inject via Kubernetes Secrets.

---

## Namespace Architecture

The operator and Woodpecker clusters run in **separate namespaces**:

```
Operator namespace (e.g. woodpecker-operator-system)
├── Deployment: operator-controller-manager
├── SA, RBAC, Webhook (operator's own resources)
└── Watches all namespaces for WoodpeckerCluster CRs

Workload namespace (e.g. woodpecker)    ← you choose this via kubectl apply -n
├── ConfigMap: woodpecker-config
├── ServiceAccount: woodpecker-sa       ← with cloud IAM annotation
├── WoodpeckerCluster CR
│   └── Operator creates in the same namespace:
│       ├── StatefulSet, Pods
│       ├── Services (headless, client, metrics)
│       ├── PDB, PVCs
│       └── (operator-managed SA, if no external SA specified)
└── Cluster-scoped (not namespaced):
    ├── ClusterRole: woodpecker-node-reader-<ns>-<name>
    └── ClusterRoleBinding (binds SA in workload namespace)
```

**Key rule:** ConfigMap, ServiceAccount, and WoodpeckerCluster CR must all be in the
same namespace. Use `kubectl apply -n <namespace>` to target the workload namespace.

---

## Step 1: Prerequisites

### 1.1 Install CRD and Operator

```bash
cd deployments/operator

# Install CRD (cluster-scoped, no namespace needed)
make install

# Build and deploy operator (replace with your registry)
export IMG=<your-registry>/woodpecker-operator:v0.1.0
make docker-build IMG=$IMG
make docker-push IMG=$IMG

# Deploy operator to default namespace (woodpecker-operator-system)
make deploy IMG=$IMG

# Or deploy to a custom namespace
make deploy IMG=$IMG OPERATOR_NAMESPACE=my-operator-ns

# Verify operator is running
kubectl -n woodpecker-operator-system get pods
# (or -n my-operator-ns if you used a custom namespace)
```

### 1.2 Cloud-Specific IAM Setup

#### AWS EKS

```bash
# Verify OIDC provider is enabled
aws eks describe-cluster --name <CLUSTER> \
  --query "cluster.identity.oidc.issuer" --output text

# Create IAM role with S3 permissions
aws iam create-role --role-name woodpecker-s3-access \
  --assume-role-policy-document file://trust-policy.json

aws iam attach-role-policy --role-name woodpecker-s3-access \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

# trust-policy.json should contain:
# {
#   "Version": "2012-10-17",
#   "Statement": [{
#     "Effect": "Allow",
#     "Principal": {
#       "Federated": "arn:aws:iam::<ACCOUNT_ID>:oidc-provider/oidc.eks.<REGION>.amazonaws.com/id/<OIDC_ID>"
#     },
#     "Action": "sts:AssumeRoleWithWebIdentity",
#     "Condition": {
#       "StringEquals": {
#         "oidc.eks.<REGION>.amazonaws.com/id/<OIDC_ID>:sub":
#           "system:serviceaccount:<NAMESPACE>:woodpecker-sa"
#       }
#     }
#   }]
# }
```

#### GCP GKE

```bash
# Create GCP service account
gcloud iam service-accounts create woodpecker-sa \
  --display-name="Woodpecker Storage SA"

# Grant Storage Object Admin on the bucket
gsutil iam ch \
  serviceAccount:woodpecker-sa@<PROJECT>.iam.gserviceaccount.com:objectAdmin \
  gs://<BUCKET>

# Bind K8s SA to GCP SA
gcloud iam service-accounts add-iam-policy-binding \
  woodpecker-sa@<PROJECT>.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:<PROJECT>.svc.id.goog[<NAMESPACE>/woodpecker-sa]"
```

#### Azure AKS

```bash
# Create managed identity
az identity create --name woodpecker-identity \
  --resource-group <RESOURCE_GROUP>

# Assign Storage Blob Data Contributor role
az role assignment create \
  --assignee <MANAGED_IDENTITY_CLIENT_ID> \
  --role "Storage Blob Data Contributor" \
  --scope /subscriptions/<SUB>/resourceGroups/<RG>/providers/Microsoft.Storage/storageAccounts/<ACCOUNT>

# Create federated credential
az identity federated-credential create \
  --name woodpecker-fed-cred \
  --identity-name woodpecker-identity \
  --resource-group <RESOURCE_GROUP> \
  --issuer <AKS_OIDC_ISSUER_URL> \
  --subject system:serviceaccount:<NAMESPACE>:woodpecker-sa
```

---

## Step 2: Deploy Woodpecker Cluster

### Option A: Create a new ServiceAccount

Apply the full cloud sample YAML (3 resources: ConfigMap + SA + CR):

```bash
# AWS
kubectl apply -f config/samples/cloud/woodpeckercluster_aws_s3_iam.yaml -n <NAMESPACE>

# GCP
kubectl apply -f config/samples/cloud/woodpeckercluster_gcp_gcs_iam.yaml -n <NAMESPACE>

# Azure
kubectl apply -f config/samples/cloud/woodpeckercluster_azure_blob_iam.yaml -n <NAMESPACE>
```

### Option B: Use an existing ServiceAccount

If you already have a SA with the IAM annotation (e.g. `woodpecker-admin`):

1. Verify the SA has the correct annotation:

```bash
kubectl describe sa woodpecker-admin -n <NAMESPACE>
# Should show:
# Annotations: eks.amazonaws.com/role-arn: arn:aws:iam::...
```

2. If missing, add the annotation:

```bash
kubectl annotate sa woodpecker-admin -n <NAMESPACE> \
  eks.amazonaws.com/role-arn=arn:aws:iam::<ACCOUNT_ID>:role/<ROLE_NAME>
```

3. Apply only ConfigMap + CR (skip SA creation):

```yaml
# configmap-and-cr.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: woodpecker-config
data:
  woodpecker.yaml: |
    # ... your woodpecker config with useIAM: true ...
---
apiVersion: woodpecker.zilliz.io/v1alpha1
kind: WoodpeckerCluster
metadata:
  name: woodpecker-sample
spec:
  replicas: 3
  serviceAccountName: woodpecker-admin    # your existing SA
  configRef:
    name: woodpecker-config
```

```bash
kubectl apply -f configmap-and-cr.yaml -n <NAMESPACE>
```

---

## Step 3: Verification

### 3.1 Check cluster status

```bash
kubectl get woodpeckerclusters -n <NAMESPACE>
# NAME                PHASE     READY   REPLICAS   AGE
# woodpecker-sample   Running   3       3          5m
```

### 3.2 Verify Pod references the correct SA

```bash
kubectl get pod woodpecker-sample-server-0 -n <NAMESPACE> \
  -o jsonpath='{.spec.serviceAccountName}'
# woodpecker-sa  (or woodpecker-admin if using existing SA)
```

### 3.3 Verify IAM credentials are injected (AWS example)

```bash
kubectl exec woodpecker-sample-server-0 -n <NAMESPACE> -- env | grep AWS
# AWS_ROLE_ARN=arn:aws:iam::<ACCOUNT_ID>:role/<ROLE_NAME>
# AWS_WEB_IDENTITY_TOKEN_FILE=/var/run/secrets/eks.amazonaws.com/serviceaccount/token
```

For GCP, check for `GOOGLE_APPLICATION_CREDENTIALS` or the metadata endpoint.
For Azure, check for `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE`.

### 3.4 Verify RBAC (topology init container)

```bash
kubectl get clusterrolebinding \
  woodpecker-node-reader-<NAMESPACE>-woodpecker-sample \
  -o jsonpath='{.subjects[0].name}'
# woodpecker-sa  (should match the SA used by pods)
```

### 3.5 Verify object storage access

Check Woodpecker pod logs for successful storage initialization:

```bash
kubectl logs woodpecker-sample-server-0 -n <NAMESPACE> | grep -i "storage\|s3\|minio"
```

No credential errors = IAM access is working.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Pod has no `AWS_ROLE_ARN` env var | SA missing IRSA annotation | `kubectl annotate sa ...` |
| `AccessDenied` in logs | IAM role lacks S3 permissions | Check IAM policy |
| `WebIdentityErr` in logs | OIDC trust policy mismatch | Verify SA namespace/name in trust policy |
| Init container `403 Forbidden` on node query | ClusterRoleBinding missing | Check `kubectl get clusterrolebinding woodpecker-node-reader-...` |
| Pods stuck in `Pending` | SA doesn't exist | Verify SA exists in the same namespace |
