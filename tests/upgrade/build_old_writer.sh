#!/usr/bin/env bash
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
# Builds the branch-v0.1.13 embed writer image:
#   1. Add a git worktree pinned to origin/branch-v0.1.13.
#   2. Inject the generic writer (tests/upgrade/writer/main.go) and the shared
#      manifest (tests/upgrade/manifest.go) from THIS checkout into the worktree.
#   3. go build the writer inside the v0.1.13 module (CGO disabled, linux/amd64).
#   4. docker build a minimal image wrapping the binary, tagged as requested.
#   5. Optionally docker push.
#
# Usage: build_old_writer.sh <image-tag> [--push]

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <image-tag> [--push]" >&2
    echo "Example: $0 ghcr.io/zilliztech/woodpecker/upgrade-writer:v0.1.13-abc123" >&2
    exit 1
fi

IMAGE_TAG="$1"
PUSH_IMAGE="${2:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

BASELINE_REF="${BASELINE_REF:-origin/branch-v0.1.13}"
# Cross-compile target for the Alpine runtime image.
GOOS_TARGET="${GOOS_TARGET:-linux}"
GOARCH_TARGET="${GOARCH_TARGET:-amd64}"

WORKTREE_DIR="$(mktemp -d)"
BUILD_CONTEXT_DIR="$(mktemp -d)"

cleanup() {
    # Remove the worktree registration first, then the temp dirs.
    git -C "$REPO_ROOT" worktree remove --force "$WORKTREE_DIR" >/dev/null 2>&1 || true
    rm -rf "$WORKTREE_DIR" "$BUILD_CONTEXT_DIR"
}
trap cleanup EXIT

echo "[build_old_writer] Fetching baseline ref ${BASELINE_REF}..."
git -C "$REPO_ROOT" fetch --quiet origin "branch-v0.1.13" 2>/dev/null || true

echo "[build_old_writer] Creating git worktree at ${WORKTREE_DIR} (${BASELINE_REF})..."
# mktemp created the dir; worktree add needs it absent, so remove then add.
rm -rf "$WORKTREE_DIR"
git -C "$REPO_ROOT" worktree add --detach "$WORKTREE_DIR" "$BASELINE_REF"

echo "[build_old_writer] Injecting writer + manifest sources..."
mkdir -p "$WORKTREE_DIR/tests/upgrade/writer"
cp "$SCRIPT_DIR/manifest.go" "$WORKTREE_DIR/tests/upgrade/manifest.go"
cp "$SCRIPT_DIR/writer/main.go" "$WORKTREE_DIR/tests/upgrade/writer/main.go"

echo "[build_old_writer] Building writer binary in v0.1.13 tree (${GOOS_TARGET}/${GOARCH_TARGET})..."
(
    cd "$WORKTREE_DIR"
    CGO_ENABLED=0 GOOS="$GOOS_TARGET" GOARCH="$GOARCH_TARGET" \
        go build -o "$BUILD_CONTEXT_DIR/upgrade-writer" ./tests/upgrade/writer
)

echo "[build_old_writer] Binary size: $(du -h "$BUILD_CONTEXT_DIR/upgrade-writer" | cut -f1)"

echo "[build_old_writer] Building Docker image ${IMAGE_TAG}..."
cp "$SCRIPT_DIR/Dockerfile.writer" "$BUILD_CONTEXT_DIR/Dockerfile.writer"
docker build \
    -t "$IMAGE_TAG" \
    -f "$BUILD_CONTEXT_DIR/Dockerfile.writer" \
    "$BUILD_CONTEXT_DIR"

echo "[build_old_writer] Image built: ${IMAGE_TAG}"

if [[ "$PUSH_IMAGE" == "--push" ]]; then
    echo "[build_old_writer] Pushing image..."
    docker push "$IMAGE_TAG"
    echo "[build_old_writer] Image pushed: ${IMAGE_TAG}"
fi

echo "[build_old_writer] Done"
