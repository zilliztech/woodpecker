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

# One-click chaos test runner for Woodpecker.
# This script builds the Docker image (if needed), starts the cluster,
# runs chaos tests, collects logs on failure, and cleans up.
#
# Usage:
#   ./run_chaos_tests.sh                    # Run all chaos tests
#   ./run_chaos_tests.sh -run TestChaos_BasicReadWrite  # Run specific test
#   ./run_chaos_tests.sh --no-cleanup       # Keep cluster after tests
#   ./run_chaos_tests.sh --skip-build       # Skip Docker image build
#   ./run_chaos_tests.sh --cleanup          # Only clean up resources (no tests)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
DEPLOYMENTS_DIR="$PROJECT_DIR/deployments"
COMPOSE_FILE="$DEPLOYMENTS_DIR/docker-compose.yaml"
OVERRIDE_FILE="$SCRIPT_DIR/docker-compose.chaos.yaml"
PROJECT_NAME="woodpecker-chaos"
LOG_DIR="$SCRIPT_DIR/logs"

# Default settings
RUN_FILTER=""
NO_CLEANUP=false
SKIP_BUILD=false
CLEANUP_ONLY=false
TEST_TIMEOUT="600s"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -run|--run)
            RUN_FILTER="$2"
            shift 2
            ;;
        --no-cleanup)
            NO_CLEANUP=true
            shift
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --cleanup)
            CLEANUP_ONLY=true
            shift
            ;;
        --timeout)
            TEST_TIMEOUT="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -run PATTERN        Run tests matching pattern (e.g., TestChaos_BasicReadWrite)"
            echo "  --no-cleanup        Keep cluster running after tests"
            echo "  --cleanup           Only clean up resources (stop cluster, remove volumes)"
            echo "  --skip-build        Skip Docker image build check"
            echo "  --timeout DURATION  Test timeout (default: 600s)"
            echo "  -h, --help          Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

compose_cmd() {
    docker compose -f "$COMPOSE_FILE" -f "$OVERRIDE_FILE" -p "$PROJECT_NAME" "$@"
}

echo "========================================"
echo " Woodpecker Chaos Test Runner"
echo "========================================"
echo ""

# Cleanup-only mode: stop cluster, remove volumes, and clean up logs
if [ "$CLEANUP_ONLY" = true ]; then
    echo "Cleaning up chaos test resources..."
    compose_cmd down -v 2>/dev/null || true
    rm -rf "$LOG_DIR"
    rm -f "$SCRIPT_DIR/test-output.log"
    echo "Cluster stopped, volumes and logs removed."
    exit 0
fi

# Step 1: Build Docker image if needed
if [ "$SKIP_BUILD" = false ]; then
    echo "[1/5] Checking Docker image..."
    if ! docker images -q woodpecker:latest | grep -q .; then
        echo "       Building Docker image (this may take a while)..."
        cd "$PROJECT_DIR"
        ./build/build_image.sh ubuntu22.04 auto -t woodpecker:latest
        cd "$SCRIPT_DIR"
        echo "       Docker image built successfully"
    else
        echo "       Docker image woodpecker:latest already exists"
    fi
else
    echo "[1/5] Skipping Docker image build (--skip-build)"
fi

# Step 2: Start the cluster
echo ""
echo "[2/5] Starting chaos test cluster..."
compose_cmd up -d
echo "       Cluster containers started"

# Step 3: Wait for cluster to be ready
echo ""
echo "[3/5] Waiting for cluster to be ready..."

wait_for_container() {
    local container=$1
    local max_retries=30
    local retry=0
    while [ $retry -lt $max_retries ]; do
        if docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null | grep -q "true"; then
            return 0
        fi
        sleep 2
        retry=$((retry + 1))
    done
    echo "ERROR: Container $container did not become ready"
    return 1
}

for container in etcd minio woodpecker-node1 woodpecker-node2 woodpecker-node3 woodpecker-node4; do
    echo -n "       Waiting for $container... "
    if wait_for_container "$container"; then
        echo "ready"
    else
        echo "FAILED"
        echo ""
        echo "Cluster failed to start. Collecting logs..."
        mkdir -p "$LOG_DIR"
        compose_cmd logs > "$LOG_DIR/startup-failure.log" 2>&1 || true
        echo "Logs saved to $LOG_DIR/startup-failure.log"
        compose_cmd down -v 2>/dev/null || true
        exit 1
    fi
done

# Wait for gossip convergence
echo "       Waiting for gossip convergence..."
sleep 15
echo "       Cluster is ready"

# Step 4: Run chaos tests
echo ""
echo "[4/5] Running chaos tests..."
echo ""

TEST_ARGS="-v -timeout $TEST_TIMEOUT -count=1"
if [ -n "$RUN_FILTER" ]; then
    TEST_ARGS="$TEST_ARGS -run $RUN_FILTER"
fi

TEST_EXIT_CODE=0
cd "$SCRIPT_DIR"
go test $TEST_ARGS ./... 2>&1 | tee "$SCRIPT_DIR/test-output.log" || TEST_EXIT_CODE=$?

# Step 5: Collect logs and clean up
echo ""
if [ $TEST_EXIT_CODE -ne 0 ]; then
    echo "[5/5] Tests FAILED (exit code: $TEST_EXIT_CODE). Collecting logs..."
    mkdir -p "$LOG_DIR"
    for container in etcd minio woodpecker-node1 woodpecker-node2 woodpecker-node3 woodpecker-node4; do
        docker logs "$container" > "$LOG_DIR/${container}.log" 2>&1 || true
    done
    echo "       Logs saved to $LOG_DIR/"
else
    echo "[5/5] All tests PASSED"
fi

if [ "$NO_CLEANUP" = true ]; then
    echo ""
    echo "       Cluster kept running (--no-cleanup). To stop:"
    echo "       docker compose -f $COMPOSE_FILE -f $OVERRIDE_FILE -p $PROJECT_NAME down -v"
else
    echo "       Cleaning up cluster..."
    compose_cmd down -v 2>/dev/null || true
    echo "       Cluster cleaned up"
fi

echo ""
echo "========================================"
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo " Result: PASSED"
else
    echo " Result: FAILED"
fi
echo "========================================"

exit $TEST_EXIT_CODE
