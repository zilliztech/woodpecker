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

# One-click runner for the service stability suite, Docker tier.
# Builds the image (if needed), starts a 4-node cluster with etcd and MinIO,
# runs TestServiceStabilityDocker_*, collects logs on failure, and cleans up.
#
# The tests assert latency, so they run without -race.
#
# Usage:
#   ./run_service_stability.sh                                   # all cases
#   ./run_service_stability.sh -run TestServiceStabilityDocker_KillQuorumPod
#   ./run_service_stability.sh --no-cleanup | --skip-build | --cleanup

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OVERRIDE_FILE="$SCRIPT_DIR/docker-compose.stability.yaml"
PROJECT_NAME="woodpecker-stability"
LOG_DIR="$SCRIPT_DIR/logs"
CONTAINERS="etcd minio woodpecker-node1 woodpecker-node2 woodpecker-node3 woodpecker-node4"
SUITE_NAME="Service Stability (Docker)"

source "$SCRIPT_DIR/../common.sh"

parse_args "$@"

echo "========================================"
echo " Woodpecker $SUITE_NAME"
echo "========================================"

if [ "$CLEANUP_ONLY" = true ]; then
    cleanup_only
fi

build_image_if_needed

echo ""
echo "[2/5] Starting cluster..."
compose_cmd up -d

echo ""
echo "[3/5] Waiting for cluster to be ready..."
wait_for_containers
echo "       Waiting for gossip convergence..."
sleep 15

TEST_TIMEOUT="${TEST_TIMEOUT_OVERRIDE:-1200s}"

echo ""
echo "[4/5] Running tests..."
run_tests

collect_logs_on_failure
final_cleanup
print_result
exit $TEST_EXIT_CODE
