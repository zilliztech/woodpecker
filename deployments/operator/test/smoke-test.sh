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

# Source shared bring-up library (defines log/warn/fail, OPERATOR_DIR, PROJECT_ROOT, wp_* functions)
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

# smoke-test.sh keeps its own default knobs; intentionally does NOT set
# MK_RUNTIME/MK_CPUS/MK_MEMORY so lib.sh defaults (4 CPU / 4096 MB / docker) apply.
GIT_BRANCH="introduce_wp_operator"

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
    kubectl delete pod "$CLIENT_POD" --force --grace-period=0 2>/dev/null || true
    kubectl delete pod/etcd svc/etcd pod/minio svc/minio 2>/dev/null || true

    cd "$OPERATOR_DIR"
    make undeploy 2>/dev/null || true

    minikube delete -p "$CLUSTER_NAME" 2>/dev/null || true
    log "Clean done"
}

# ============================================================
# Step 1: Start minikube
# ============================================================
do_step1() { wp_minikube_start; }

# ============================================================
# Step 2: Build and deploy operator
# ============================================================
do_step2() { wp_deploy_operator; }

# ============================================================
# Step 3: Build Woodpecker server image
# ============================================================
do_step3() { wp_build_wp_image; }

# ============================================================
# Step 4: Deploy etcd and MinIO
# ============================================================
do_step4() { wp_deploy_deps; }

# ============================================================
# Step 5: Create WoodpeckerCluster
# ============================================================
do_step5() { wp_create_cr; }

# ============================================================
# Step 6: Wait for gossip and verify cluster health
# ============================================================
do_step6() { wp_wait_healthy; }

# ============================================================
# Step 7: Launch test pod and run E2E tests
# ============================================================
do_step7() {
    log "=== Step 7: Running E2E tests ==="

    wp_launch_client_pod
    wp_write_client_config "$REPLICAS"

    # Clone code if not already done
    kubectl exec -i "$CLIENT_POD" -- /bin/bash -c "
        if [ ! -d /root/woodpecker ]; then
            git clone -b $GIT_BRANCH --depth 1 https://github.com/zilliztech/woodpecker.git /root/woodpecker
        fi
    "

    # Run tests
    log "Running E2E tests..."
    kubectl exec -i "$CLIENT_POD" -- /bin/bash -c '
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

    wp_write_client_config "$NEW_REPLICAS"
    log "Test config updated with $NEW_REPLICAS seeds"

    log "Running E2E tests after scale-up..."
    kubectl exec -i "$CLIENT_POD" -- /bin/bash -c '
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
    kubectl cp "$PROJECT_ROOT/tests/e2e_operator/main_test.go" "$CLIENT_POD":/root/woodpecker/tests/e2e_operator/main_test.go 2>/dev/null || true

    # Phase 1: Run decommission test inside the test pod
    # Test will: decommission the node → write data → truncate → wait safe_to_terminate
    log "Running decommission test inside test pod..."
    kubectl exec -i "$CLIENT_POD" -- /bin/bash -c "
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
    wp_write_client_config "$FINAL_REPLICAS"

    log "Running final write/read test on $FINAL_REPLICAS-node cluster..."
    kubectl exec -i "$CLIENT_POD" -- /bin/bash -c '
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
    echo -e "\033[0;32m========================================\033[0m"
    echo -e "\033[0;32m ALL SMOKE TESTS PASSED\033[0m"
    echo -e "\033[0;32m========================================\033[0m"
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
