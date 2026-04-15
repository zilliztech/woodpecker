# Woodpecker Operator E2E Test -- Build from Source

This guide covers building the operator and Woodpecker from source, deploying to minikube, and running end-to-end tests including scale-up/down verification.

## Prerequisites

| Tool | Minimum Version | Install |
|---|---|---|
| Docker | 20.10+ | https://docs.docker.com/get-docker/ |
| minikube | 1.30+ | `brew install minikube` |
| kubectl | 1.28+ | `brew install kubectl` |
| Go | 1.24+ | `brew install go` |

## Automated Smoke Test

A fully automated script is provided at `deployments/operator/test/smoke-test.sh`. Each step can be run independently or all at once.

### Run all steps

```bash
cd deployments/operator
./test/smoke-test.sh all
```

This runs step1 through step9 sequentially: creates a minikube cluster, builds images from source, deploys operator + Woodpecker cluster, verifies gossip, runs E2E client tests, tests scale-up/down, and cleans up. Total runtime is approximately 5 minutes.

### Run individual steps

```bash
cd deployments/operator

./test/smoke-test.sh step1    # Start minikube
./test/smoke-test.sh step2    # Build and deploy operator
./test/smoke-test.sh step3    # Build Woodpecker server image
./test/smoke-test.sh step4    # Deploy etcd and MinIO
./test/smoke-test.sh step5    # Create WoodpeckerCluster (3 replicas)
./test/smoke-test.sh step6    # Verify gossip cluster health
./test/smoke-test.sh step7    # Launch test pod and run E2E tests
./test/smoke-test.sh step8    # Scale up to 5 and re-test
./test/smoke-test.sh step9    # Scale down to 2 and re-test
```

Each step is idempotent and can be re-run independently. This is useful for:
- Debugging a specific step without re-running everything
- Re-running E2E tests after fixing code (`step7`)
- Testing scale operations repeatedly (`step8`, `step9`)

### Manual inspection

After running steps, you can inspect the environment:

```bash
kubectl get woodpeckerclusters
kubectl get pods
kubectl exec -it wp-client-test -- bash

# Inside the test pod, run tests manually:
cd /root/woodpecker/tests/e2e_operator
go test -v -count=1 -timeout 10m -config-file /tmp/test-config.yaml .
```

### Clean up

```bash
./test/smoke-test.sh clean
```

This is idempotent — safe to run multiple times or even if some resources don't exist.

### Show help

```bash
./test/smoke-test.sh --help
```

## What Each Step Does

### Step 1: Start minikube

Creates a minikube cluster named `wp-operator-smoke` with 4 CPUs and 4GB memory. Skips if already running.

### Step 2: Build and deploy operator

Builds the operator Docker image, loads it into minikube, and deploys via `make deploy` (CRD + RBAC + controller pod). Verifies the operator pod is Running.

### Step 3: Build Woodpecker server image

Builds the Woodpecker server image from source (`make docker`), tags it as `zilliztech/woodpecker:v0.1.26`, and loads into minikube.

### Step 4: Deploy etcd and MinIO

Deploys single-pod etcd and MinIO instances with Services. Skips if already running. These are Woodpecker's runtime dependencies (not managed by the operator).

### Step 5: Create WoodpeckerCluster

Creates a ConfigMap with Woodpecker configuration and a WoodpeckerCluster CR with 3 replicas. Waits for all pods to be Ready.

### Step 6: Verify gossip cluster health

Polls each Woodpecker node's `/admin/node/status` API until all nodes report `state: active`. Then displays the memberlist from node-0 to confirm all nodes have joined the gossip cluster. Fails if gossip doesn't form within 120 seconds.

### Step 7: Run E2E tests

Creates a `golang:1.24` test pod, clones the Woodpecker repo, and runs 3 E2E tests:

| Test | Description |
|---|---|
| `TestOperatorE2E_HealthCheck` | HTTP health check on all server nodes |
| `TestOperatorE2E_WriteAndRead` | Write 100 entries, read back, verify payload |
| `TestOperatorE2E_MultipleLogsParallel` | 3 logs x 50 entries, parallel read/write |

### Step 8: Scale up

Patches the CR to 5 replicas, waits for new pods, then re-runs WriteAndRead test.

### Step 9: Scale down

Patches the CR to 2 replicas. The operator calls the decommission API on pods being removed and waits for `safe_to_terminate` before reducing replicas. Then re-runs WriteAndRead test.

## Manual Step-by-Step Guide

If you prefer not to use the script, see below for the equivalent manual commands.

### Start minikube

```bash
minikube start --cpus=4 --memory=4096 --driver=docker
```

### Build and deploy operator

```bash
cd deployments/operator
make docker-build IMG=woodpecker-operator:smoke
minikube image load woodpecker-operator:smoke
make deploy IMG=woodpecker-operator:smoke
kubectl get pods -n woodpecker-operator-system
```

### Build Woodpecker image

```bash
cd ../..
make docker
docker tag woodpecker:latest zilliztech/woodpecker:v0.1.26
minikube image load zilliztech/woodpecker:v0.1.26
```

### Deploy dependencies and create cluster

See the ConfigMap and CR YAML in `deployments/operator/config/samples/` or refer to Step 4-5 in `smoke-test.sh`.

### Verify gossip

```bash
for i in 0 1 2; do
  echo "--- server-$i ---"
  kubectl exec my-woodpecker-server-$i -- curl -s http://localhost:9091/admin/node/status
  echo ""
done
```

All nodes should show `"state":"active"` and `"member_count":3`.

### Run E2E tests from test pod

```bash
kubectl exec -it wp-client-test -- bash
cd /root/woodpecker/tests/e2e_operator
go test -v -count=1 -timeout 10m -config-file /tmp/test-config.yaml .
```

### Clean up

```bash
kubectl delete woodpeckercluster my-woodpecker
kubectl delete configmap my-wp-config
kubectl delete pod wp-client-test --force
kubectl delete pod/etcd svc/etcd pod/minio svc/minio
cd deployments/operator && make undeploy
minikube delete
```
