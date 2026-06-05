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

# Shared Woodpecker operator bring-up library. Source me; do not execute.
: "${CLUSTER_NAME:=wp-operator-smoke}"
: "${CR_NAME:=my-woodpecker}"
: "${NAMESPACE:=default}"
: "${REPLICAS:=3}"
: "${OPERATOR_IMG:=woodpecker-operator:smoke}"
: "${WP_IMG:=zilliztech/woodpecker:v0.1.26}"
: "${MK_CPUS:=4}"            # chaos overrides -> 6
: "${MK_MEMORY:=4096}"      # chaos overrides -> 8192
: "${MK_RUNTIME:=}"         # chaos overrides -> containerd ; empty = minikube default
: "${CLIENT_POD:=wp-client-test}"

_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPERATOR_DIR="$(cd "$_LIB_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$OPERATOR_DIR/../.." && pwd)"

log()  { echo -e "\033[0;32m[$(date +'%H:%M:%S')]\033[0m $*"; }
warn() { echo -e "\033[1;33m[$(date +'%H:%M:%S')] WARN:\033[0m $*"; }
fail() { echo -e "\033[0;31m[$(date +'%H:%M:%S')] FAIL:\033[0m $*"; exit 1; }

wp_minikube_start() {
    if minikube status -p "$CLUSTER_NAME" &>/dev/null; then log "cluster $CLUSTER_NAME up"; return 0; fi
    local rt=""; [ -n "$MK_RUNTIME" ] && rt="--container-runtime=$MK_RUNTIME"
    minikube start -p "$CLUSTER_NAME" --cpus="$MK_CPUS" --memory="$MK_MEMORY" --driver=docker $rt
    kubectl config use-context "$CLUSTER_NAME"
}

wp_deploy_operator() {
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

wp_build_wp_image() {
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

wp_deploy_deps() {
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

wp_create_cr() {
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

wp_wait_healthy() {
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

wp_launch_client_pod() {
    kubectl get pod "$CLIENT_POD" &>/dev/null && return 0
    docker pull golang:1.24 2>/dev/null || true; minikube -p "$CLUSTER_NAME" image load golang:1.24
    kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${CLIENT_POD}
  labels: { role: wp-client }
spec:
  containers:
    - name: test
      image: golang:1.24
      imagePullPolicy: IfNotPresent
      command: ["/bin/bash","-c","sleep infinity"]
      resources: { requests: { cpu: "500m", memory: "1Gi" } }
  restartPolicy: Never
EOF
    kubectl wait --for=condition=Ready pod/"$CLIENT_POD" --timeout=300s
}

wp_write_client_config() {
    local replicas="${1:-$REPLICAS}" seeds=""
    for i in $(seq 0 $((replicas-1))); do
        seeds="$seeds
            - ${CR_NAME}-server-${i}.${CR_NAME}-server-headless.${NAMESPACE}.svc:18080"
    done
    kubectl exec "$CLIENT_POD" -- bash -c "cat > /tmp/test-config.yaml <<'CFGEOF'
woodpecker:
  meta: { type: etcd }
  client:
    segmentRollingPolicy: { maxSize: 1000 }
    quorum:
      replicaCount: ${replicas}
      quorumBufferPools:
        - name: default-region-pool
          seeds:${seeds}
  logstore: { retentionPolicy: { ttl: 10 } }
  storage: { type: service, rootPath: /tmp/wp-test-data }
log: { level: info, format: json, stdout: true }
etcd: { endpoints: [ etcd.${NAMESPACE}.svc:2379 ], rootPath: by-dev }
minio:
  address: minio.${NAMESPACE}.svc
  port: 9000
  accessKeyID: minioadmin
  secretAccessKey: minioadmin
  bucketName: woodpecker
  rootPath: files
  createBucket: true
CFGEOF"
}
