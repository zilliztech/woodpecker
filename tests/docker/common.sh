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

# Shared functions for Docker integration test runners.
#
# Callers must set:
#   SCRIPT_DIR      - Absolute path to the caller's directory
#   OVERRIDE_FILE   - Path to the docker-compose override file
#   PROJECT_NAME    - Docker Compose project name
#   LOG_DIR         - Directory for log output
#   CONTAINERS      - Space-separated list of containers to monitor
#   SUITE_NAME      - Display name for the test suite (e.g., "Chaos", "Monitor")

# Derived paths (3 levels up from tests/docker/<suite>/)
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
DEPLOYMENTS_DIR="$PROJECT_DIR/deployments"
COMPOSE_FILE="$DEPLOYMENTS_DIR/docker-compose.yaml"

# Default settings
RUN_FILTER=""
NO_CLEANUP=false
SKIP_BUILD=false
CLEANUP_ONLY=false
TEST_TIMEOUT="600s"

# parse_args parses standard command-line arguments.
parse_args() {
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
                echo "  -run PATTERN        Run tests matching pattern"
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
}

# compose_cmd runs docker compose with the correct files and project name.
compose_cmd() {
    docker compose -f "$COMPOSE_FILE" -f "$OVERRIDE_FILE" -p "$PROJECT_NAME" "$@"
}

# wait_for_container polls until a container is in running state.
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

# build_image_if_needed builds the Docker image if it doesn't exist.
build_image_if_needed() {
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
}

# cleanup_only stops the cluster and removes volumes and logs.
cleanup_only() {
    echo "Cleaning up $SUITE_NAME test resources..."
    compose_cmd down -v 2>/dev/null || true
    rm -rf "$LOG_DIR"
    rm -f "$SCRIPT_DIR/test-output.log"
    echo "Cluster stopped, volumes and logs removed."
    exit 0
}

# wait_for_containers waits for all containers in CONTAINERS to be running.
wait_for_containers() {
    for container in $CONTAINERS; do
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
}

# run_tests runs Go tests and captures the exit code.
run_tests() {
    local test_args="-v -timeout $TEST_TIMEOUT -count=1"
    if [ -n "$RUN_FILTER" ]; then
        test_args="$test_args -run $RUN_FILTER"
    fi

    TEST_EXIT_CODE=0
    cd "$SCRIPT_DIR"
    go test $test_args ./... 2>&1 | tee "$SCRIPT_DIR/test-output.log" || TEST_EXIT_CODE=$?
}

# collect_logs_on_failure collects container logs when tests fail.
collect_logs_on_failure() {
    echo ""
    if [ $TEST_EXIT_CODE -ne 0 ]; then
        echo "[5/5] Tests FAILED (exit code: $TEST_EXIT_CODE). Collecting logs..."
        mkdir -p "$LOG_DIR"
        for container in $CONTAINERS; do
            docker logs "$container" > "$LOG_DIR/${container}.log" 2>&1 || true
        done
        echo "       Logs saved to $LOG_DIR/"
    else
        echo "[5/5] All tests PASSED"
    fi
}

# final_cleanup stops the cluster unless --no-cleanup was specified.
final_cleanup() {
    if [ "$NO_CLEANUP" = true ]; then
        echo ""
        echo "       Cluster kept running (--no-cleanup). To stop:"
        echo "       docker compose -f $COMPOSE_FILE -f $OVERRIDE_FILE -p $PROJECT_NAME down -v"
    else
        echo "       Cleaning up cluster..."
        compose_cmd down -v 2>/dev/null || true
        echo "       Cluster cleaned up"
    fi
}

# print_result prints the final pass/fail banner.
print_result() {
    echo ""
    echo "========================================"
    if [ $TEST_EXIT_CODE -eq 0 ]; then
        echo " Result: PASSED"
    else
        echo " Result: FAILED"
    fi
    echo "========================================"

    exit $TEST_EXIT_CODE
}
