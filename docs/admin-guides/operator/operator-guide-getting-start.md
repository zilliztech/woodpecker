# Woodpecker Operator Getting Started Guide

This guide walks through deploying a Woodpecker cluster on Kubernetes using the Woodpecker Operator with pre-built images.

## Prerequisites

| Tool | Minimum Version | Install |
|---|---|---|
| Docker | 20.10+ | https://docs.docker.com/get-docker/ |
| minikube | 1.30+ | `brew install minikube` |
| kubectl | 1.28+ | `brew install kubectl` |

## Step 1: Start minikube

```bash
minikube start --cpus=4 --memory=4096 --driver=docker
kubectl cluster-info
```

## Step 2: Deploy the operator

```bash
# Load the operator image into minikube
minikube image load woodpecker-operator:v0.1.26

# Install CRD + RBAC + operator Deployment
kubectl apply -k deployments/operator/config/default/

# Verify the operator is running
kubectl get pods -n woodpecker-operator-system
kubectl logs -n woodpecker-operator-system -l control-plane=controller-manager -f
```

## Step 3: Deploy etcd and MinIO

Woodpecker requires etcd (metadata) and MinIO (object storage). The operator does not manage these -- deploy them separately.

```bash
# etcd
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: etcd
  labels: { app: etcd }
spec:
  containers:
    - name: etcd
      image: quay.io/coreos/etcd:v3.5.18
      command: ["etcd", "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://etcd.default.svc:2379"]
      ports: [{ containerPort: 2379 }]
---
apiVersion: v1
kind: Service
metadata:
  name: etcd
spec:
  selector: { app: etcd }
  ports: [{ port: 2379, targetPort: 2379 }]
EOF

# MinIO
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: minio
  labels: { app: minio }
spec:
  containers:
    - name: minio
      image: minio/minio:RELEASE.2024-06-13T22-53-53Z
      command: ["minio", "server", "/data"]
      env:
        - { name: MINIO_ROOT_USER, value: minioadmin }
        - { name: MINIO_ROOT_PASSWORD, value: minioadmin }
      ports: [{ containerPort: 9000 }]
---
apiVersion: v1
kind: Service
metadata:
  name: minio
spec:
  selector: { app: minio }
  ports: [{ port: 9000, targetPort: 9000 }]
EOF

# Wait for both to be ready
kubectl wait --for=condition=Ready pod/etcd pod/minio --timeout=120s
```

## Step 4: Create a WoodpeckerCluster

### 4.1 Create the ConfigMap

```bash
kubectl apply -f - <<'EOF'
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
          quorumBufferPools:
            - name: default-region-pool
              seeds:
                - my-woodpecker-server-0.my-woodpecker-server-headless.default.svc:18080
                - my-woodpecker-server-1.my-woodpecker-server-headless.default.svc:18080
                - my-woodpecker-server-2.my-woodpecker-server-headless.default.svc:18080
      storage:
        type: service
        rootPath: /woodpecker/data
    log:
      level: info
      format: json
      stdout: true
    etcd:
      endpoints:
        - etcd.default.svc:2379
      rootPath: by-dev
    minio:
      address: minio.default.svc
      port: 9000
      accessKeyID: minioadmin
      secretAccessKey: minioadmin
      bucketName: woodpecker
      rootPath: files
      createBucket: true
EOF
```

### 4.2 Create the CR

```bash
kubectl apply -f - <<'EOF'
apiVersion: woodpecker.zilliz.io/v1alpha1
kind: WoodpeckerCluster
metadata:
  name: my-woodpecker
spec:
  image: zilliztech/woodpecker:v0.1.26
  imagePullPolicy: IfNotPresent
  replicas: 3
  resources:
    requests: { cpu: "250m", memory: "256Mi" }
    limits:   { cpu: "500m", memory: "512Mi" }
  storageSize: 2Gi
  configRef:
    name: my-wp-config
EOF
```

### 4.3 Verify

```bash
kubectl get woodpeckerclusters
# NAME            PHASE     READY   REPLICAS   AGE
# my-woodpecker   Running   3       3          2m

kubectl get statefulset,svc,pdb | grep woodpecker
kubectl get pods -l app.kubernetes.io/instance=my-woodpecker
```

## Step 5: Operations

### Scale

```bash
kubectl patch woodpeckercluster my-woodpecker --type merge -p '{"spec":{"replicas":5}}'
kubectl get pods -l app.kubernetes.io/instance=my-woodpecker -w
```

### Upgrade image

```bash
kubectl patch woodpeckercluster my-woodpecker --type merge -p '{"spec":{"image":"zilliztech/woodpecker:latest"}}'
kubectl rollout status statefulset/my-woodpecker-server
```

### Change config

```bash
kubectl edit configmap my-wp-config
# The operator detects the hash change and triggers a rolling restart
```

### Pause / Resume

```bash
kubectl patch woodpeckercluster my-woodpecker --type merge -p '{"spec":{"paused":true}}'
kubectl patch woodpeckercluster my-woodpecker --type merge -p '{"spec":{"paused":false}}'
```

## Step 6: Uninstall

```bash
# Delete the WoodpeckerCluster (triggers graceful decommission)
kubectl delete woodpeckercluster my-woodpecker

# Delete ConfigMap
kubectl delete configmap my-wp-config

# Undeploy operator
kubectl delete -k deployments/operator/config/default/

# Clean up etcd and MinIO
kubectl delete pod/etcd svc/etcd pod/minio svc/minio

# Delete minikube cluster
minikube delete
```

## Client Connectivity

Woodpecker clients must run **inside the same K8s cluster** as the server pods. The operator creates ClusterIP services and pod DNS names that are only resolvable within the cluster.

- Client Service: `my-woodpecker-server-client.default.svc:18080`
- Pod DNS: `my-woodpecker-server-N.my-woodpecker-server-headless.default.svc`

For development/debugging from outside the cluster, use:

```bash
kubectl port-forward svc/my-woodpecker-server-client 18080:18080
```

Note: `port-forward` only exposes a single endpoint. Gossip-discovered node addresses are still unreachable from outside the cluster.

## Troubleshooting

### Pods stuck in Pending

```bash
kubectl describe pod <pod-name>
```

Common causes: insufficient CPU/memory (reduce `resources` in CR or increase minikube resources), missing StorageClass.

### Pods in CrashLoopBackOff

```bash
kubectl logs <pod-name>
```

Common causes: etcd/MinIO not reachable (check ConfigMap endpoints), invalid `woodpecker.yaml` syntax.

### Operator not reconciling

- Check operator logs: `kubectl logs -n woodpecker-operator-system -l control-plane=controller-manager`
- Verify CRD: `kubectl get crd woodpeckerclusters.woodpecker.zilliz.io`
- Check finalizer: `kubectl get woodpeckercluster my-woodpecker -o yaml | grep finalizers`
