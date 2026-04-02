#!/bin/bash
# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Woodpecker Operator Smoke Test (minikube)
#
# Each step can be run independently. Steps are idempotent.
#
# Prerequisites: minikube, docker, kubectl, go
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPERATOR_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$OPERATOR_DIR/../.." && pwd)"

CLUSTER_NAME="wp-operator-smoke"
OPERATOR_IMG="woodpecker-operator:smoke"
WP_IMG="zilliztech/woodpecker:v0.1.26"
NAMESPACE="default"
CR_NAME="my-woodpecker"
REPLICAS=3
GIT_BRANCH="introduce_wp_operator"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[$(date +'%H:%M:%S')]${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date +'%H:%M:%S')] WARN:${NC} $*"; }
fail() { echo -e "${RED}[$(date +'%H:%M:%S')] FAIL:${NC} $*"; exit 1; }

usage() {
    cat <<EOF
Woodpecker Operator Smoke Test

Usage: $(basename "$0") <command> [options]

Commands (run in order, or individually):
  step1    Start minikube cluster
  step2    Build and deploy operator
  step3    Build Woodpecker server image
  step4    Deploy etcd and MinIO
  step5    Create WoodpeckerCluster (3 replicas)
  step6    Wait for gossip cluster to form and verify
  step7    Launch client test pod and run E2E tests
  step8    Scale up to 5 replicas and re-test
  step9    Scale down to 2 replicas and re-test
  all      Run step1 through step9 sequentially
  clean    Clean up all resources (idempotent)

Options:
  --help   Show this help message

Examples:
  $(basename "$0") all               # Full test run
  $(basename "$0") step1             # Only start minikube
  $(basename "$0") step7             # Only run E2E tests (requires step1-6 done)
  $(basename "$0") clean             # Clean everything up
  $(basename "$0") step5 && $(basename "$0") step6   # Recreate WP cluster and verify
EOF
}

# ============================================================
# Clean (idempotent)
# ============================================================
do_clean() {
    log "Cleaning up all resources..."
    kubectl config use-context "$CLUSTER_NAME" 2>/dev/null || true

    kubectl patch woodpeckercluster "$CR_NAME" --type merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
    kubectl delete woodpeckercluster "$CR_NAME" --timeout=60s 2>/dev/null || true
    kubectl delete statefulset "${CR_NAME}-server" 2>/dev/null || true
    kubectl delete configmap my-wp-config 2>/dev/null || true
    kubectl delete pvc -l app.kubernetes.io/instance="$CR_NAME" 2>/dev/null || true
    kubectl delete pod wp-client-test --force --grace-period=0 2>/dev/null || true
    kubectl delete pod/etcd svc/etcd pod/minio svc/minio 2>/dev/null || true

    cd "$OPERATOR_DIR"
    make undeploy 2>/dev/null || true

    minikube delete -p "$CLUSTER_NAME" 2>/dev/null || true
    log "Clean done"
}

# ============================================================
# Step 1: Start minikube
# ============================================================
do_step1() {
    log "=== Step 1: Starting minikube cluster '$CLUSTER_NAME' ==="
    if minikube status -p "$CLUSTER_NAME" &>/dev/null; then
        log "Cluster '$CLUSTER_NAME' already running, skipping"
        return 0
    fi
    minikube start -p "$CLUSTER_NAME" --cpus=4 --memory=4096 --driver=docker
    kubectl cluster-info
    log "Step 1 done: minikube running"
}

# ============================================================
# Step 2: Build and deploy operator
# ============================================================
do_step2() {
    log "=== Step 2: Building and deploying operator ==="
    cd "$OPERATOR_DIR"
    make docker-build IMG="$OPERATOR_IMG"
    minikube -p "$CLUSTER_NAME" image load "$OPERATOR_IMG"
    make deploy IMG="$OPERATOR_IMG"

    log "Waiting for operator pod..."
    kubectl wait --for=condition=Available deployment -l control-plane=controller-manager \
        -n woodpecker-operator-system --timeout=120s
    log "Step 2 done: operator running"
    kubectl get pods -n woodpecker-operator-system
}

# ============================================================
# Step 3: Build Woodpecker server image
# ============================================================
do_step3() {
    log "=== Step 3: Building Woodpecker server image ==="
    cd "$PROJECT_ROOT"
    make docker 2>/dev/null || docker build -t woodpecker:latest -f build/docker/ubuntu22.04/Dockerfile .
    docker tag woodpecker:latest "$WP_IMG" 2>/dev/null || true
    minikube -p "$CLUSTER_NAME" image load "$WP_IMG"
    # Pre-load busybox for init container
    docker pull busybox:1.36 2>/dev/null || true
    minikube -p "$CLUSTER_NAME" image load busybox:1.36
    log "Step 3 done: Woodpecker + busybox images loaded"
}

# ============================================================
# Step 4: Deploy etcd and MinIO
# ============================================================
do_step4() {
    log "=== Step 4: Deploying etcd and MinIO ==="

    # Skip if already running
    if kubectl get pod/etcd pod/minio &>/dev/null; then
        log "etcd and MinIO already running, skipping"
        return 0
    fi

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
---
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

    kubectl wait --for=condition=Ready pod/etcd pod/minio --timeout=120s
    log "Step 4 done: etcd and MinIO ready"
}

# ============================================================
# Step 5: Create WoodpeckerCluster
# ============================================================
do_step5() {
    log "=== Step 5: Creating WoodpeckerCluster ($REPLICAS replicas) ==="

    # Generate seeds
    SEEDS_YAML=""
    for i in $(seq 0 $((REPLICAS - 1))); do
        SEEDS_YAML="$SEEDS_YAML
                - ${CR_NAME}-server-${i}.${CR_NAME}-server-headless.${NAMESPACE}.svc:18080"
    done

    kubectl apply -f - <<EOF
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
          replicaCount: $REPLICAS
          quorumBufferPools:
            - name: default-region-pool
              seeds:${SEEDS_YAML}
      storage:
        type: service
        rootPath: /woodpecker/data
      logstore:
        segmentSyncPolicy:
          syncInterval: 1000
          syncMaxBytes: 4194304
        retentionPolicy:
          ttl: 10
    log:
      level: info
      format: json
      stdout: true
    etcd:
      endpoints: ["etcd.${NAMESPACE}.svc:2379"]
      rootPath: by-dev
    minio:
      address: minio.${NAMESPACE}.svc
      port: 9000
      accessKeyID: minioadmin
      secretAccessKey: minioadmin
      bucketName: woodpecker
      rootPath: files
      createBucket: true
---
apiVersion: woodpecker.zilliz.io/v1alpha1
kind: WoodpeckerCluster
metadata:
  name: ${CR_NAME}
spec:
  image: ${WP_IMG}
  imagePullPolicy: Never
  replicas: ${REPLICAS}
  resources:
    requests:
      cpu: "250m"
      memory: "256Mi"
    limits:
      cpu: "500m"
      memory: "512Mi"
  storageSize: 2Gi
  configRef:
    name: my-wp-config
EOF

    log "Waiting for StatefulSet to appear..."
    TIMEOUT=60
    while [ $TIMEOUT -gt 0 ]; do
        if kubectl get statefulset "${CR_NAME}-server" &>/dev/null; then break; fi
        sleep 3; TIMEOUT=$((TIMEOUT - 3))
    done

    kubectl wait --for=condition=Ready pod -l app.kubernetes.io/instance="$CR_NAME" --timeout=300s
    log "Step 5 done: WoodpeckerCluster running ($REPLICAS replicas)"
    kubectl get woodpeckerclusters
    kubectl get pods -l app.kubernetes.io/instance="$CR_NAME"
}

# ============================================================
# Step 6: Wait for gossip and verify cluster health
# ============================================================
do_step6() {
    log "=== Step 6: Verifying gossip cluster health ==="

    # Check each node's memberlist via admin API
    log "Checking gossip memberlist on each node..."
    TIMEOUT=120
    ALL_READY=false

    while [ $TIMEOUT -gt 0 ] && [ "$ALL_READY" = false ]; do
        ALL_READY=true
        for i in $(seq 0 $((REPLICAS - 1))); do
            POD="${CR_NAME}-server-${i}"
            HOST="${POD}.${CR_NAME}-server-headless.${NAMESPACE}.svc"

            # Check healthz
            HEALTH=$(kubectl exec "$POD" -- curl -s -o /dev/null -w "%{http_code}" "http://localhost:9091/healthz" 2>/dev/null || echo "000")
            if [ "$HEALTH" != "200" ]; then
                ALL_READY=false
                continue
            fi

            # Check node status - state should be "active"
            NODE_STATUS=$(kubectl exec "$POD" -- curl -s "http://localhost:9091/admin/node/status" 2>/dev/null || echo "{}")
            IS_ACTIVE=$(echo "$NODE_STATUS" | tr -d '\n' | grep -c '"state":"active"' 2>/dev/null || echo "0")

            if [ "$IS_ACTIVE" -lt 1 ]; then
                ALL_READY=false
            fi
        done

        if [ "$ALL_READY" = false ]; then
            log "  Gossip not fully formed yet (waiting...)  timeout=$TIMEOUT"
            sleep 5
            TIMEOUT=$((TIMEOUT - 5))
        fi
    done

    if [ "$ALL_READY" = false ]; then
        warn "Gossip cluster may not be fully formed after timeout. Dumping status..."
        for i in $(seq 0 $((REPLICAS - 1))); do
            POD="${CR_NAME}-server-${i}"
            echo "--- $POD ---"
            kubectl exec "$POD" -- curl -s "http://localhost:9091/admin/memberlist" 2>/dev/null || echo "  (unreachable)"
            echo ""
        done
        fail "Gossip cluster not ready"
    fi

    log "All $REPLICAS nodes have joined the gossip cluster"

    # Show memberlist from node-0 for verification
    log "Memberlist from ${CR_NAME}-server-0:"
    kubectl exec "${CR_NAME}-server-0" -- curl -s "http://localhost:9091/admin/memberlist" 2>/dev/null || true
    echo ""

    # Check node status
    for i in $(seq 0 $((REPLICAS - 1))); do
        POD="${CR_NAME}-server-${i}"
        STATUS=$(kubectl exec "$POD" -- curl -s "http://localhost:9091/admin/node/status" 2>/dev/null || echo "{}")
        log "  $POD: $STATUS"
    done

    log "Step 6 done: gossip cluster verified"
}

# ============================================================
# Step 7: Launch test pod and run E2E tests
# ============================================================
do_step7() {
    log "=== Step 7: Running E2E tests ==="

    # Create test pod if not exists
    if ! kubectl get pod wp-client-test &>/dev/null; then
        docker pull golang:1.24 2>/dev/null || true
        minikube -p "$CLUSTER_NAME" image load golang:1.24

        kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: wp-client-test
spec:
  containers:
    - name: test
      image: golang:1.24
      imagePullPolicy: IfNotPresent
      command: ["/bin/bash", "-c", "sleep infinity"]
      resources:
        requests: { cpu: "500m", memory: "1Gi" }
  restartPolicy: Never
EOF
        kubectl wait --for=condition=Ready pod/wp-client-test --timeout=300s
    fi

    # Generate test config
    SEEDS_YAML=""
    for i in $(seq 0 $((REPLICAS - 1))); do
        SEEDS_YAML="$SEEDS_YAML
            - ${CR_NAME}-server-${i}.${CR_NAME}-server-headless.${NAMESPACE}.svc:18080"
    done

    kubectl exec wp-client-test -- bash -c "cat > /tmp/test-config.yaml << 'CFGEOF'
woodpecker:
  meta:
    type: etcd
  client:
    segmentRollingPolicy:
      maxSize: 1000
    quorum:
      replicaCount: $REPLICAS
      quorumBufferPools:
        - name: default-region-pool
          seeds:${SEEDS_YAML}
  logstore:
    retentionPolicy:
      ttl: 10
  storage:
    type: service
    rootPath: /tmp/wp-test-data
log:
  level: info
  format: json
  stdout: true
etcd:
  endpoints:
    - etcd.${NAMESPACE}.svc:2379
  rootPath: by-dev
minio:
  address: minio.${NAMESPACE}.svc
  port: 9000
  accessKeyID: minioadmin
  secretAccessKey: minioadmin
  bucketName: woodpecker
  rootPath: files
  createBucket: true
CFGEOF"

    # Clone code if not already done
    kubectl exec -i wp-client-test -- /bin/bash -c "
        if [ ! -d /root/woodpecker ]; then
            git clone -b $GIT_BRANCH --depth 1 https://github.com/zilliztech/woodpecker.git /root/woodpecker
        fi
    "

    # Run tests
    log "Running E2E tests..."
    kubectl exec -i wp-client-test -- /bin/bash -c '
set -e
cd /root/woodpecker/tests/e2e_operator
go test -v -count=1 -timeout 10m -config-file /tmp/test-config.yaml .
'
    log "Step 7 done: E2E tests PASSED"
}

# ============================================================
# Step 8: Scale up and re-test
# ============================================================
do_step8() {
    NEW_REPLICAS=4
    log "=== Step 8: Scaling up $REPLICAS → $NEW_REPLICAS ==="
    # Clean any orphaned PVCs from previous runs to ensure new nodes start clean
    for i in $(seq $REPLICAS $((NEW_REPLICAS - 1))); do
        kubectl delete pvc "data-${CR_NAME}-server-${i}" 2>/dev/null || true
    done
    kubectl patch woodpeckercluster "$CR_NAME" --type merge -p "{\"spec\":{\"replicas\":$NEW_REPLICAS}}"
    sleep 5
    kubectl wait --for=condition=Ready pod -l app.kubernetes.io/instance="$CR_NAME" --timeout=300s
    kubectl get woodpeckerclusters

    # Regenerate test config with all seeds (including new node)
    SEEDS_YAML=""
    for i in $(seq 0 $((NEW_REPLICAS - 1))); do
        SEEDS_YAML="$SEEDS_YAML
            - ${CR_NAME}-server-${i}.${CR_NAME}-server-headless.${NAMESPACE}.svc:18080"
    done

    kubectl exec wp-client-test -- bash -c "cat > /tmp/test-config.yaml << 'CFGEOF'
woodpecker:
  meta:
    type: etcd
  client:
    segmentRollingPolicy:
      maxSize: 1000
    quorum:
      replicaCount: $REPLICAS
      quorumBufferPools:
        - name: default-region-pool
          seeds:${SEEDS_YAML}
  logstore:
    retentionPolicy:
      ttl: 10
  storage:
    type: service
    rootPath: /tmp/wp-test-data
log:
  level: info
  format: json
  stdout: true
etcd:
  endpoints:
    - etcd.${NAMESPACE}.svc:2379
  rootPath: by-dev
minio:
  address: minio.${NAMESPACE}.svc
  port: 9000
  accessKeyID: minioadmin
  secretAccessKey: minioadmin
  bucketName: woodpecker
  rootPath: files
  createBucket: true
CFGEOF"
    log "Test config updated with $NEW_REPLICAS seeds"

    log "Running E2E tests after scale-up..."
    kubectl exec -i wp-client-test -- /bin/bash -c '
set -e
cd /root/woodpecker/tests/e2e_operator
go test -v -count=1 -timeout 10m -run TestOperatorE2E_WriteAndRead -config-file /tmp/test-config.yaml .
'
    log "Step 8 done: scale-up test PASSED ($NEW_REPLICAS replicas)"
}

# ============================================================
# Step 9: Scale down and re-test
# ============================================================
do_step9() {
    CURRENT_REPLICAS=$(kubectl get pods -l app.kubernetes.io/instance="$CR_NAME" --no-headers 2>/dev/null | wc -l | tr -d ' ')
    [ "$CURRENT_REPLICAS" -eq 0 ] && CURRENT_REPLICAS=4
    FINAL_REPLICAS=$((CURRENT_REPLICAS - 1))
    DECOMMISSION_NODE="${CR_NAME}-server-$((CURRENT_REPLICAS - 1)).${CR_NAME}-server-headless.${NAMESPACE}.svc:9091"

    log "=== Step 9: Scale down $CURRENT_REPLICAS → $FINAL_REPLICAS (decommission ${DECOMMISSION_NODE}) ==="

    # Sync latest test code to the pod (in case local changes haven't been pushed)
    kubectl cp "$PROJECT_ROOT/tests/e2e_operator/main_test.go" wp-client-test:/root/woodpecker/tests/e2e_operator/main_test.go 2>/dev/null || true

    # Phase 1: Run decommission test inside the test pod
    # Test will: decommission the node → write data → truncate → wait safe_to_terminate
    log "Running decommission test inside test pod..."
    kubectl exec -i wp-client-test -- /bin/bash -c "
set -e
cd /root/woodpecker/tests/e2e_operator
go test -v -count=1 -timeout 5m \
    -run TestOperatorE2E_ScaleDownWithDecommission \
    -config-file /tmp/test-config.yaml \
    -decommission-nodes '${DECOMMISSION_NODE}' .
"
    log "Decommission test PASSED — node is safe to terminate"

    # Phase 2: Now patch replicas to let operator remove the pod
    log "Patching replicas $CURRENT_REPLICAS → $FINAL_REPLICAS..."
    kubectl patch woodpeckercluster "$CR_NAME" --type merge -p "{\"spec\":{\"replicas\":$FINAL_REPLICAS}}"

    log "Waiting for pod to be removed..."
    TIMEOUT=120
    while [ $TIMEOUT -gt 0 ]; do
        CURRENT=$(kubectl get pods -l app.kubernetes.io/instance="$CR_NAME" --no-headers 2>/dev/null | wc -l | tr -d ' ')
        if [ "$CURRENT" -le "$FINAL_REPLICAS" ]; then break; fi
        sleep 5; TIMEOUT=$((TIMEOUT - 5))
    done
    kubectl get woodpeckerclusters
    kubectl get pods -l app.kubernetes.io/instance="$CR_NAME"

    # Phase 3: Final write/read test on reduced cluster
    SEEDS_YAML=""
    for i in $(seq 0 $((FINAL_REPLICAS - 1))); do
        SEEDS_YAML="$SEEDS_YAML
            - ${CR_NAME}-server-${i}.${CR_NAME}-server-headless.${NAMESPACE}.svc:18080"
    done

    kubectl exec wp-client-test -- bash -c "cat > /tmp/test-config.yaml << 'CFGEOF'
woodpecker:
  meta:
    type: etcd
  client:
    segmentRollingPolicy:
      maxSize: 1000
    quorum:
      replicaCount: $FINAL_REPLICAS
      quorumBufferPools:
        - name: default-region-pool
          seeds:${SEEDS_YAML}
  logstore:
    retentionPolicy:
      ttl: 10
  storage:
    type: service
    rootPath: /tmp/wp-test-data
log:
  level: info
  format: json
  stdout: true
etcd:
  endpoints:
    - etcd.${NAMESPACE}.svc:2379
  rootPath: by-dev
minio:
  address: minio.${NAMESPACE}.svc
  port: 9000
  accessKeyID: minioadmin
  secretAccessKey: minioadmin
  bucketName: woodpecker
  rootPath: files
  createBucket: true
CFGEOF"

    log "Running final write/read test on $FINAL_REPLICAS-node cluster..."
    kubectl exec -i wp-client-test -- /bin/bash -c '
set -e
cd /root/woodpecker/tests/e2e_operator
go test -v -count=1 -timeout 10m -run TestOperatorE2E_WriteAndRead -config-file /tmp/test-config.yaml .
'
    log "Step 9 done: scale-down PASSED ($FINAL_REPLICAS replicas)"
}

# ============================================================
# Run all steps
# ============================================================
do_all() {
    do_step1
    do_step2
    do_step3
    do_step4
    do_step5
    do_step6
    do_step7
    do_step8
    do_step9
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN} ALL SMOKE TESTS PASSED${NC}"
    echo -e "${GREEN}========================================${NC}"
}

# ============================================================
# Main
# ============================================================
CMD="${1:-}"
case "$CMD" in
    step1)  do_step1 ;;
    step2)  do_step2 ;;
    step3)  do_step3 ;;
    step4)  do_step4 ;;
    step5)  do_step5 ;;
    step6)  do_step6 ;;
    step7)  do_step7 ;;
    step8)  do_step8 ;;
    step9)  do_step9 ;;
    all)    do_all ;;
    clean)  do_clean ;;
    --help|-h) usage ;;
    "")     usage; exit 1 ;;
    *)      echo "Unknown command: $CMD"; usage; exit 1 ;;
esac
