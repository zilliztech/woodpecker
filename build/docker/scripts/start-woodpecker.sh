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


set -e
# Default values
# Support both old and new naming conventions
SERVICE_PORT=${SERVICE_PORT:-${SERVICE_PORT:-18080}}   # Service port (for client connections)
GOSSIP_PORT=${GOSSIP_PORT:-${GOSSIP_PORT:-17946}}       # Gossip port (for cluster communication)
NODE_NAME=${NODE_NAME:-$(hostname)}
DATA_DIR=${DATA_DIR:-/woodpecker/data}
CONFIG_FILE=${CONFIG_FILE:-/woodpecker/configs/woodpecker.yaml}
CLUSTER=${CLUSTER:-woodpecker-cluster}
STORAGE_TYPE=${STORAGE_TYPE:-service}
LOG_LEVEL=${LOG_LEVEL:-info}
EXTERNAL_USER_CONFIG_FILE=${EXTERNAL_USER_CONFIG_FILE:-/woodpecker/configs/user.yaml}

# Node metadata configuration
RESOURCE_GROUP=${RESOURCE_GROUP:-default}
AVAILABILITY_ZONE=${AVAILABILITY_ZONE:-default}

# Advertise address configuration for Docker bridge networking
# Gossip advertise configuration (for internal cluster communication)
ADVERTISE_GOSSIP_ADDR=${ADVERTISE_GOSSIP_ADDR:-""}

# Service advertise configuration (for client connections)
ADVERTISE_SERVICE_ADDR=${ADVERTISE_SERVICE_ADDR:-""}

# Note: IP auto-detection is now primarily handled by the Go application
# using the common/net package, but we keep this for backward compatibility
# and explicit configuration in Docker environments

# Auto-detect advertise addresses if set to "auto"
if [ "$ADVERTISE_GOSSIP_ADDR" = "auto" ]; then
    # Try to get Docker host gateway IP address
    HOST_IP=$(ip route show default 2>/dev/null | awk '/default/ {print $3}' | head -1)
    if [ -z "$HOST_IP" ]; then
        # Fallback to common Docker gateway IP
        HOST_IP="172.17.0.1"
    fi
    ADVERTISE_GOSSIP_ADDR="$HOST_IP:$GOSSIP_PORT"
    log "Auto-detected gossip advertise address:port: $ADVERTISE_GOSSIP_ADDR"
fi

if [ "$ADVERTISE_SERVICE_ADDR" = "auto" ]; then
    # Use the same logic as gossip address for Docker environments
    if [ -n "$HOST_IP" ]; then
        ADVERTISE_SERVICE_ADDR="$HOST_IP:$SERVICE_PORT"
    else
        HOST_IP=$(ip route show default 2>/dev/null | awk '/default/ {print $3}' | head -1)
        ADVERTISE_SERVICE_ADDR="${HOST_IP:-172.17.0.1}:$SERVICE_PORT"
    fi
    log "Auto-detected service advertise address:port: $ADVERTISE_SERVICE_ADDR"
fi

# MinIO configuration
MINIO_ADDRESS=${MINIO_ADDRESS:-minio}
MINIO_PORT=${MINIO_PORT:-9000}
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-minioadmin}
MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-minioadmin}
MINIO_BUCKET=${MINIO_BUCKET:-${CLUSTER}-data}

# Dependency check configuration
WAIT_FOR_DEPS=${WAIT_FOR_DEPS:-true}

# Seeds configuration
SEEDS=${SEEDS:-""}
SERVICE_SEEDS=${SERVICE_SEEDS:-""}

# Log function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

# Validate configuration
if [ -z "$NODE_NAME" ]; then
    log "ERROR NODE_NAME cannot be empty"
    exit 1
fi

if [ "$SERVICE_PORT" -eq "$GOSSIP_PORT" ]; then
    log "ERROR service port and Gossip port cannot be the same: $SERVICE_PORT"
    exit 1
fi

# Wait for dependencies
if [ "$WAIT_FOR_DEPS" = "true" ]; then
    log "Waiting for dependencies..."
    
    # Wait for MinIO
    log "Waiting for MinIO at $MINIO_ADDRESS:$MINIO_PORT (timeout: 120s)..."
    timeout=120
    while [ $timeout -gt 0 ]; do
        if nc -z $MINIO_ADDRESS $MINIO_PORT 2>/dev/null; then
            log "✓ MinIO is available at $MINIO_ADDRESS:$MINIO_PORT"
            break
        fi
        sleep 1
        timeout=$((timeout-1))
    done

    if [ $timeout -eq 0 ]; then
        log "ERROR MinIO is not available at $MINIO_ADDRESS:$MINIO_PORT after 120s"
        exit 1
    fi
else
    log "WARN Skipping dependency checks (WAIT_FOR_DEPS=false)"
fi

# Create configuration if it doesn't exist
if [ ! -f "$CONFIG_FILE" ]; then
    log "Creating configuration file: $CONFIG_FILE"
    mkdir -p "$(dirname "$CONFIG_FILE")"
    cat > "$CONFIG_FILE" << EOC
woodpecker:
  storage:
    type: $STORAGE_TYPE
    rootPath: $DATA_DIR
  client:
    quorum:
      quorumBufferPools:
        - name: default-region-pool
          seeds: [$SERVICE_SEEDS]
  logstore:
    segmentReadPolicy:
      maxBatchSize: 2000000
log:
  level: $LOG_LEVEL
  stdout: true
etcd:
  endpoints: [$ETCD_ENDPOINTS]
minio:
  address: $MINIO_ADDRESS
  port: $MINIO_PORT
  accessKeyID: $MINIO_ACCESS_KEY
  secretAccessKey: $MINIO_SECRET_KEY
  bucketName: $MINIO_BUCKET
  createBucket: true
EOC
else
    log "Using existing configuration: $CONFIG_FILE"
fi

log "Starting Woodpecker Server:"
log "  Node Name: $NODE_NAME"
log "  Service Port: $SERVICE_PORT"
log "  Gossip Port: $GOSSIP_PORT"
log "  Resource Group: $RESOURCE_GROUP"
log "  Availability Zone: $AVAILABILITY_ZONE"

# Log gossip advertise configuration
if [ -n "$ADVERTISE_GOSSIP_ADDR" ]; then
    log "  Gossip Advertise Address:Port: $ADVERTISE_GOSSIP_ADDR"
fi

# Log service advertise configuration
if [ -n "$ADVERTISE_SERVICE_ADDR" ]; then
    log "  Service Advertise Address:Port: $ADVERTISE_SERVICE_ADDR"
fi
log "  Seeds: $SEEDS"
log "  Service Seeds: $SERVICE_SEEDS"
log "  Storage Type: $STORAGE_TYPE"
log "  Cluster: $CLUSTER"
log "  etcd: $ETCD_ENDPOINTS"
log "  MinIO: $MINIO_ADDRESS:$MINIO_PORT"

# Ensure data directory exists
mkdir -p "$DATA_DIR"

# Build command array
CMD_ARGS=(
    "/woodpecker/bin/woodpecker"
    "server"
    "--service-port" "$SERVICE_PORT"
    "--gossip-port" "$GOSSIP_PORT"
    "--node-name" "$NODE_NAME"
    "--data-dir" "$DATA_DIR"
    "--config" "$CONFIG_FILE"
    "--resource-group" "$RESOURCE_GROUP"
    "--availability-zone" "$AVAILABILITY_ZONE"
    "--external-user-config" "$EXTERNAL_USER_CONFIG_FILE"
)

# Add gossip advertise configuration if provided
if [ -n "$ADVERTISE_GOSSIP_ADDR" ]; then
    CMD_ARGS+=("--advertise-gossip-addr" "$ADVERTISE_GOSSIP_ADDR")
fi

# Add service advertise configuration if provided
if [ -n "$ADVERTISE_SERVICE_ADDR" ]; then
    CMD_ARGS+=("--advertise-service-addr" "$ADVERTISE_SERVICE_ADDR")
fi

# Add seeds if provided and not empty
if [ -n "$SEEDS" ] && [ "$SEEDS" != "" ]; then
    CMD_ARGS+=("--seeds" "$SEEDS")
fi

log "Starting: ${CMD_ARGS[*]}"

# Execute the command
exec "${CMD_ARGS[@]}"
