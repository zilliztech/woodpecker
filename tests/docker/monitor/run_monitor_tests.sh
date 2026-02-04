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

# One-click monitor test runner for Woodpecker.
# This script builds the Docker image (if needed), starts the cluster
# with Prometheus and Grafana, runs monitor tests, collects logs on
# failure, and cleans up.
#
# Usage:
#   ./run_monitor_tests.sh                    # Run all monitor tests
#   ./run_monitor_tests.sh -run TestMonitor_BasicMetricsVerification  # Run specific test
#   ./run_monitor_tests.sh --no-cleanup       # Keep cluster after tests
#   ./run_monitor_tests.sh --skip-build       # Skip Docker image build
#   ./run_monitor_tests.sh --cleanup          # Only clean up resources (no tests)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OVERRIDE_FILE="$SCRIPT_DIR/docker-compose.monitor.yaml"
PROJECT_NAME="woodpecker-monitor"
LOG_DIR="$SCRIPT_DIR/logs"
CONTAINERS="etcd minio woodpecker-node1 woodpecker-node2 woodpecker-node3 woodpecker-node4 prometheus grafana"
SUITE_NAME="Monitor"

# Source shared functions
source "$SCRIPT_DIR/../common.sh"

parse_args "$@"

echo "========================================"
echo " Woodpecker Monitor Test Runner"
echo "========================================"
echo ""

# Cleanup-only mode
if [ "$CLEANUP_ONLY" = true ]; then
    cleanup_only
fi

# Step 1: Build Docker image if needed
build_image_if_needed

# Step 2: Start the cluster with Prometheus and Grafana
echo ""
echo "[2/5] Starting monitor test cluster (with Prometheus + Grafana)..."
compose_cmd up -d
echo "       Cluster containers started"

# Step 3: Wait for cluster to be ready
echo ""
echo "[3/5] Waiting for cluster to be ready..."
wait_for_containers

# Wait for gossip convergence
echo "       Waiting for gossip convergence..."
sleep 15

# Wait for Prometheus readiness
echo -n "       Waiting for Prometheus API readiness... "
PROM_RETRIES=0
PROM_MAX_RETRIES=15
while [ $PROM_RETRIES -lt $PROM_MAX_RETRIES ]; do
    if curl -sf http://localhost:9090/-/ready > /dev/null 2>&1; then
        echo "ready"
        break
    fi
    sleep 2
    PROM_RETRIES=$((PROM_RETRIES + 1))
done
if [ $PROM_RETRIES -eq $PROM_MAX_RETRIES ]; then
    echo "FAILED (Prometheus API not ready after retries)"
    mkdir -p "$LOG_DIR"
    docker logs prometheus > "$LOG_DIR/prometheus.log" 2>&1 || true
    echo "Prometheus logs saved to $LOG_DIR/prometheus.log"
fi

# Set up Grafana dashboard
echo -n "       Setting up Grafana dashboard... "
if go run ./grafana/cmd/ 2>/dev/null; then
    echo "done"
else
    echo "skipped (non-fatal)"
fi

echo "       Cluster is ready"

# Step 4: Run monitor tests
echo ""
echo "[4/5] Running monitor tests..."
echo ""
run_tests

# Step 5: Collect logs and clean up
collect_logs_on_failure

if [ "$NO_CLEANUP" = true ]; then
    echo ""
    echo "       Cluster kept running (--no-cleanup). To stop:"
    echo "       docker compose -f $COMPOSE_FILE -f $OVERRIDE_FILE -p $PROJECT_NAME down -v"
    echo ""
    echo "       Access Prometheus: http://localhost:9090"
    echo "       Access Grafana:    http://localhost:3000/d/woodpecker-overview/woodpecker"
else
    echo "       Cleaning up cluster..."
    compose_cmd down -v 2>/dev/null || true
    echo "       Cluster cleaned up"
fi

print_result
