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
OVERRIDE_FILE="$SCRIPT_DIR/docker-compose.chaos.yaml"
PROJECT_NAME="woodpecker-chaos"
LOG_DIR="$SCRIPT_DIR/logs"
CONTAINERS="etcd minio woodpecker-node1 woodpecker-node2 woodpecker-node3 woodpecker-node4"
SUITE_NAME="Chaos"

# Source shared functions
source "$SCRIPT_DIR/../common.sh"

parse_args "$@"

echo "========================================"
echo " Woodpecker Chaos Test Runner"
echo "========================================"
echo ""

# Cleanup-only mode
if [ "$CLEANUP_ONLY" = true ]; then
    cleanup_only
fi

# Step 1: Build Docker image if needed
build_image_if_needed

# Step 2: Start the cluster
echo ""
echo "[2/5] Starting chaos test cluster..."
compose_cmd up -d
echo "       Cluster containers started"

# Step 3: Wait for cluster to be ready
echo ""
echo "[3/5] Waiting for cluster to be ready..."
wait_for_containers

# Wait for gossip convergence
echo "       Waiting for gossip convergence..."
sleep 15
echo "       Cluster is ready"

# Step 4: Run chaos tests
echo ""
echo "[4/5] Running chaos tests..."
echo ""
run_tests

# Step 5: Collect logs and clean up
collect_logs_on_failure
final_cleanup
print_result
