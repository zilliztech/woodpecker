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

# One-click upgrade compatibility test runner for Woodpecker (service mode).
#
# Unlike the chaos/monitor runners, this script does NOT start the cluster
# itself: TestUpgradeCompat_ServiceMode orchestrates the full sequence
# (up v0.1.30 -> write -> swap image -> verify) via tests/docker/framework and
# tears the cluster down in t.Cleanup. This runner only ensures the two images
# are available and then runs the Go test.
#
# Usage:
#   ./run_upgrade_tests.sh                    # Run upgrade tests
#   ./run_upgrade_tests.sh -run TestUpgradeCompat_ServiceMode  # Specific test
#   ./run_upgrade_tests.sh --no-cleanup       # Keep cluster after tests
#   ./run_upgrade_tests.sh --skip-build       # Skip target image build
#   ./run_upgrade_tests.sh --cleanup          # Only clean up resources
#
# Environment:
#   WOODPECKER_IMAGE_BASELINE  baseline (old) server image
#                              (default: ghcr.io/zilliztech/woodpecker/upgrade-base:v0.1.30)
#   WOODPECKER_IMAGE_TARGET    target (HEAD) server image
#                              (default: woodpecker:current)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OVERRIDE_FILE="$SCRIPT_DIR/docker-compose.upgrade.yaml"
PROJECT_NAME="woodpecker-upgrade"
LOG_DIR="$SCRIPT_DIR/logs"
CONTAINERS="etcd minio woodpecker-node1 woodpecker-node2 woodpecker-node3 woodpecker-node4"
SUITE_NAME="Upgrade"

# Image selection (exported so the Go test reads the same values).
export WOODPECKER_IMAGE_BASELINE="${WOODPECKER_IMAGE_BASELINE:-ghcr.io/zilliztech/woodpecker/upgrade-base:v0.1.30}"
export WOODPECKER_IMAGE_TARGET="${WOODPECKER_IMAGE_TARGET:-woodpecker:current}"

# Source shared functions (parse_args, run_tests, collect_logs_on_failure,
# final_cleanup, print_result, cleanup_only, PROJECT_DIR, ...).
# shellcheck source=tests/docker/common.sh
source "$SCRIPT_DIR/../common.sh"

parse_args "$@"

echo "========================================"
echo " Woodpecker Upgrade Test Runner"
echo "========================================"
echo ""
echo "Baseline image: $WOODPECKER_IMAGE_BASELINE"
echo "Target image:   $WOODPECKER_IMAGE_TARGET"
echo ""

# Cleanup-only mode.
if [ "$CLEANUP_ONLY" = true ]; then
    cleanup_only
fi

# Step 1: Build the target (HEAD) image to an explicit tag. We deliberately do
# NOT reuse woodpecker:latest (see upgrade design correction #4); build the
# distinct target tag so the baseline/target images never collide.
echo "[1/4] Ensuring target image $WOODPECKER_IMAGE_TARGET ..."
if [ "$SKIP_BUILD" = true ]; then
    echo "       Skipping target image build (--skip-build)"
elif docker images -q "$WOODPECKER_IMAGE_TARGET" | grep -q .; then
    echo "       Target image $WOODPECKER_IMAGE_TARGET already exists"
else
    echo "       Building target image (this may take a while)..."
    ( cd "$PROJECT_DIR" && ./build/build_image.sh ubuntu22.04 auto -t "$WOODPECKER_IMAGE_TARGET" )
    echo "       Target image built successfully"
fi

# Step 2: Ensure the baseline image is available (already-local or pull).
echo ""
echo "[2/4] Ensuring baseline image $WOODPECKER_IMAGE_BASELINE ..."
if docker image inspect "$WOODPECKER_IMAGE_BASELINE" >/dev/null 2>&1; then
    echo "       Baseline image found locally"
elif docker pull "$WOODPECKER_IMAGE_BASELINE"; then
    echo "       Baseline image pulled successfully"
else
    echo "ERROR: baseline image $WOODPECKER_IMAGE_BASELINE is not local and could not be pulled."
    echo "       Run a workflow_dispatch on .github/workflows/prime-upgrade-baselines.yaml"
    echo "       or pre-pull the image, then retry."
    exit 1
fi

# Upgrade phases take longer than the default; give the test a generous budget.
TEST_TIMEOUT="1500s"

# Step 3: Run the upgrade test. The test brings the cluster up/down itself.
echo ""
echo "[3/4] Running upgrade compatibility tests..."
echo ""
run_tests

# Step 4: Collect logs on failure and clean up any leftover cluster.
echo ""
echo "[4/4] Finalizing..."
collect_logs_on_failure
final_cleanup
print_result
