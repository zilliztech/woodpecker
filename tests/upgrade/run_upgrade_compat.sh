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
# Orchestrates the embed upgrade-compatibility flow for one storage mode:
#   1. Resolve the v0.1.13 writer image (pull fast-path; build fallback).
#   2. Run the writer container (--network host; host-mounted data dir for
#      local storage) to lay down a deterministic pre-upgrade dataset + manifest.
#   3. Build the HEAD verifier and run it against the same etcd + storage.
# Exits non-zero if any step fails.

set -euo pipefail

usage() {
    cat >&2 <<'EOF'
Usage: run_upgrade_compat.sh [options]
  --storage {local|minio}   Storage mode (default: local)
  --data-dir DIR            Host data dir for local storage (default: /tmp/woodpecker_upgrade_data)
  --etcd ENDPOINT           etcd endpoint host:port (default: localhost:2379)
  --minio-endpoint ENDPOINT MinIO endpoint host:port (default: localhost:9000)
  --bucket NAME             MinIO bucket name (default: a-bucket)
  --logs N                  Number of logs to write (default: 3)
  --entries N               Entries per log (default: 100)
  --payload-prefix PREFIX   Payload prefix (default: upgrade-payload-)
  --roll-at-bytes BYTES     Force segment roll size (default: 50000)
  --max-blocks N            Force segment roll block count (default: 4)
  --writer-image IMAGE      Override writer image reference
  -h, --help                Show this help
EOF
}

STORAGE="local"
DATA_DIR="/tmp/woodpecker_upgrade_data"
ETCD_ENDPOINT="localhost:2379"
MINIO_ENDPOINT="localhost:9000"
MINIO_BUCKET="a-bucket"
# Keep the dataset small: object storage flushes ~1 block/sec, and with MAX_BLOCKS=4
# a dozen entries already rolls 3 completed segments (which the auditor compacts into
# merged m_*.blk) while the writer's separate 'fresh' log stays uncompacted.
LOGS_COUNT=1
ENTRIES_PER_LOG=12
PAYLOAD_PREFIX="upgrade-payload-"
ROLL_AT_BYTES=50000
MAX_BLOCKS=4
WRITER_IMAGE=""
GHCR_REPO="${GHCR_REPO:-ghcr.io/zilliztech/woodpecker}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --storage) STORAGE="$2"; shift 2 ;;
        --data-dir) DATA_DIR="$2"; shift 2 ;;
        --etcd) ETCD_ENDPOINT="$2"; shift 2 ;;
        --minio-endpoint) MINIO_ENDPOINT="$2"; shift 2 ;;
        --bucket) MINIO_BUCKET="$2"; shift 2 ;;
        --logs) LOGS_COUNT="$2"; shift 2 ;;
        --entries) ENTRIES_PER_LOG="$2"; shift 2 ;;
        --payload-prefix) PAYLOAD_PREFIX="$2"; shift 2 ;;
        --roll-at-bytes) ROLL_AT_BYTES="$2"; shift 2 ;;
        --max-blocks) MAX_BLOCKS="$2"; shift 2 ;;
        --writer-image) WRITER_IMAGE="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; usage; exit 1 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# content_hash prints a stable hash of the writer + manifest sources, portable
# across macOS (shasum) and Linux (sha256sum). Used to address the writer image
# so a dataset/source change yields a new tag.
content_hash() {
    local hasher
    if command -v sha256sum >/dev/null 2>&1; then
        hasher="sha256sum"
    else
        hasher="shasum -a 256"
    fi
    # Hash the concatenation of the source files in a deterministic order.
    {
        cat "$SCRIPT_DIR/manifest.go"
        find "$SCRIPT_DIR/writer" -type f -name '*.go' | sort | while read -r f; do
            cat "$f"
        done
    } | $hasher | awk '{print substr($1, 1, 16)}'
}

if [[ -z "$WRITER_IMAGE" ]]; then
    WRITER_IMAGE="${GHCR_REPO}/upgrade-writer:v0.1.13-$(content_hash)"
fi

echo "[ORCHESTRATOR] Writer image: ${WRITER_IMAGE}"
echo "[ORCHESTRATOR] Storage: ${STORAGE}"
echo "[ORCHESTRATOR] Data dir: ${DATA_DIR}"
echo "[ORCHESTRATOR] etcd: ${ETCD_ENDPOINT}"

TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT
MANIFEST_FILE="$TEMP_DIR/manifest.json"

# --- Phase 1: resolve the writer image (pull fast-path, build fallback) -------
echo "[ORCHESTRATOR] Phase 1: resolving writer image..."
if docker manifest inspect "$WRITER_IMAGE" >/dev/null 2>&1; then
    echo "[ORCHESTRATOR] Writer image present in registry, pulling..."
    if ! docker pull "$WRITER_IMAGE"; then
        echo "[ORCHESTRATOR] Pull failed; building locally..."
        bash "$SCRIPT_DIR/build_old_writer.sh" "$WRITER_IMAGE"
    fi
else
    echo "[ORCHESTRATOR] Writer image absent; building locally..."
    bash "$SCRIPT_DIR/build_old_writer.sh" "$WRITER_IMAGE"
fi

# --- Phase 2: run the writer container ----------------------------------------
echo "[ORCHESTRATOR] Phase 2: running writer..."

# The manifest must be written somewhere the host can read. For local storage we
# mount the data dir into the container and place the manifest inside it. For
# minio we mount a dedicated dir just for the manifest.
# Run as the host user so files written into the mounted data/manifest dir are
# owned by the runner (not root) and remain readable by the host-side verifier
# and the manifest copy below.
DOCKER_RUN_ARGS=(--network=host --rm --user "$(id -u):$(id -g)")
WRITER_ARGS=(
    "--storage=$STORAGE"
    "--etcd=$ETCD_ENDPOINT"
    "--logs=$LOGS_COUNT"
    "--entries=$ENTRIES_PER_LOG"
    "--payload-prefix=$PAYLOAD_PREFIX"
    "--roll-at-bytes=$ROLL_AT_BYTES"
    "--max-blocks=$MAX_BLOCKS"
)

if [[ "$STORAGE" == "local" ]]; then
    mkdir -p "$DATA_DIR"
    DOCKER_RUN_ARGS+=(-v "$DATA_DIR:/data")
    WRITER_ARGS+=("--data-dir=/data" "--manifest-out=/data/manifest.json")
    HOST_MANIFEST="$DATA_DIR/manifest.json"
else
    DOCKER_RUN_ARGS+=(-v "$TEMP_DIR:/out")
    WRITER_ARGS+=(
        "--minio-endpoint=$MINIO_ENDPOINT"
        "--bucket=$MINIO_BUCKET"
        "--manifest-out=/out/manifest.json"
    )
    HOST_MANIFEST="$TEMP_DIR/manifest.json"
fi

echo "[ORCHESTRATOR] docker run ${WRITER_IMAGE} ${WRITER_ARGS[*]}"
docker run "${DOCKER_RUN_ARGS[@]}" "$WRITER_IMAGE" "${WRITER_ARGS[@]}"

if [[ ! -f "$HOST_MANIFEST" ]]; then
    echo "[ORCHESTRATOR] ERROR: manifest not found at $HOST_MANIFEST" >&2
    exit 1
fi
# For minio the writer wrote the manifest directly into $TEMP_DIR (mounted at
# /out), so HOST_MANIFEST and MANIFEST_FILE are the same path; copy only if they
# differ (the local-storage case mounts the data dir instead).
if [[ "$HOST_MANIFEST" != "$MANIFEST_FILE" ]]; then
    cp "$HOST_MANIFEST" "$MANIFEST_FILE"
fi
echo "[ORCHESTRATOR] Writer completed; manifest at ${MANIFEST_FILE}"

# --- Phase 2.5: assert the dataset exercises the right block layouts -----------
# Fail loudly here rather than silently under-testing if the dataset stops
# producing the expected on-disk shapes.
if [[ "$STORAGE" == "local" ]]; then
    # The disk backend stores one data.log per segment (compaction happens in
    # place; there is no separate merged object). Require multiple segments so the
    # test still reads across several sealed segments plus the fresh active one.
    echo "[ORCHESTRATOR] Phase 2.5: asserting the local dataset spans multiple segments..."
    SEG_COUNT="$(find "$DATA_DIR" -type f -name 'data.log' 2>/dev/null | wc -l | tr -d ' ')"
    if [[ "${SEG_COUNT:-0}" -lt 2 ]]; then
        echo "[ORCHESTRATOR] ERROR: expected >=2 segment data.log files for local storage, found ${SEG_COUNT}." >&2
        echo "[ORCHESTRATOR]        Raise --entries / lower --max-blocks so segments roll." >&2
        exit 1
    fi
    echo "[ORCHESTRATOR] OK: local dataset spans ${SEG_COUNT} segment data.log files (compacted-in-place + fresh)."
else
    # Object storage: a complete upgrade test must read BOTH a COMPACTED (merged
    # m_*.blk) v5 block and an UNCOMPACTED (plain N.blk) v5 block.
    echo "[ORCHESTRATOR] Phase 2.5: asserting BOTH compacted (m_*.blk) and uncompacted (.blk) blocks exist..."
    LISTING="$(docker run --rm --network host -e MC_CONFIG_DIR=/tmp/mc --entrypoint sh minio/mc:latest -c \
        "mc alias set L http://${MINIO_ENDPOINT} minioadmin minioadmin >/dev/null 2>&1 && mc ls -r L/${MINIO_BUCKET}/ 2>/dev/null" 2>/dev/null || true)"
    MERGED="$(printf '%s\n' "$LISTING" | grep -E 'm_[0-9]+\.blk' | head -1 || true)"
    PLAIN="$(printf '%s\n' "$LISTING" | grep -E '/[0-9]+\.blk' | grep -v 'footer.blk' | head -1 || true)"
    if [[ -z "$MERGED" ]]; then
        echo "[ORCHESTRATOR] ERROR: no compacted (m_*.blk) block found — the test did not exercise reading MERGED v0.1.13 blocks." >&2
        echo "[ORCHESTRATOR]        Raise --entries / --compaction-wait-seconds so the auditor compacts completed segments." >&2
        exit 1
    fi
    if [[ -z "$PLAIN" ]]; then
        echo "[ORCHESTRATOR] ERROR: no uncompacted (plain .blk) block found — the test did not exercise reading PLAIN v0.1.13 blocks." >&2
        exit 1
    fi
    echo "[ORCHESTRATOR] OK: dataset has a compacted block [${MERGED##* }] AND an uncompacted block [${PLAIN##* }]"
fi

# --- Phase 3: build the HEAD verifier -----------------------------------------
echo "[ORCHESTRATOR] Phase 3: building HEAD verifier..."
VERIFIER_BINARY="$TEMP_DIR/verifier"
(
    cd "$REPO_ROOT"
    go build -o "$VERIFIER_BINARY" ./tests/upgrade/verifier
)
echo "[ORCHESTRATOR] Verifier built at ${VERIFIER_BINARY}"

# --- Phase 4: run the verifier ------------------------------------------------
echo "[ORCHESTRATOR] Phase 4: running verifier..."
VERIFIER_ARGS=(
    "--storage=$STORAGE"
    "--etcd=$ETCD_ENDPOINT"
    "--manifest-in=$MANIFEST_FILE"
)
if [[ "$STORAGE" == "local" ]]; then
    VERIFIER_ARGS+=("--data-dir=$DATA_DIR")
else
    VERIFIER_ARGS+=("--minio-endpoint=$MINIO_ENDPOINT" "--bucket=$MINIO_BUCKET")
fi

"$VERIFIER_BINARY" "${VERIFIER_ARGS[@]}"

echo "[ORCHESTRATOR] ===== ALL UPGRADE COMPATIBILITY TESTS PASSED ====="
