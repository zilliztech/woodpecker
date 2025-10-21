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


# Woodpecker Cluster Deployment Script
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Woodpecker Cluster Deployment"
echo "=================================="

# Function to show usage
usage() {
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  build       Build Woodpecker Docker image"
    echo "  up          Start the complete cluster"
    echo "  down        Stop the cluster"
    echo "  clean       Stop cluster and remove volumes"
    echo "  logs        Show all service logs"
    echo "  status      Show cluster status"
    echo "  test        Run cluster connectivity tests"
    echo "  help        Show this help message"
    echo ""
    echo "The cluster includes:"
    echo "  - 3 Woodpecker nodes (ports 18080-18082)"
    echo "  - etcd (port 2379)"
    echo "  - MinIO (ports 9000-9001)"
    echo "  - Jaeger (port 16686)"
}

# Build Woodpecker binary and Docker image
build() {
    echo "Building Woodpecker..."
    cd "$PROJECT_DIR"
    
    # Use the new build system
    echo "Building binary..."
    ./build/build_bin.sh
    echo "Binary built successfully"
    
    echo "Building Docker image..."
    ./build/build_image.sh ubuntu22.04 auto -t woodpecker:latest
    echo "Docker image built successfully"
}

# Start the cluster
up() {
    echo "Starting Woodpecker cluster..."
    cd "$SCRIPT_DIR"
    
    # Check if woodpecker:latest image exists
    if ! docker images | grep -q "woodpecker.*latest"; then
        echo "📦 Woodpecker image not found, building first..."
        cd "$PROJECT_DIR"
        ./build/build_image.sh ubuntu22.04 auto -t woodpecker:latest
        cd "$SCRIPT_DIR"
    fi
    
    docker-compose up -d
    
    echo ""
    echo "Waiting for services to be ready..."
    sleep 10
    
    echo ""
    echo "Cluster Services:"
    echo "==================="
    echo "• Woodpecker Node 1: http://localhost:18080"
    echo "• Woodpecker Node 2: http://localhost:18081"
    echo "• Woodpecker Node 3: http://localhost:18082"
    echo "• MinIO Console:     http://localhost:9001 (minioadmin/minioadmin)"
    echo "• Jaeger UI:         http://localhost:16686"
    echo "• etcd:              http://localhost:2379"
    echo ""
    echo "Success: Cluster is starting up!"
    echo "Tips: Use '$0 status' to check service health"
    echo "Tips: Use '$0 logs' to view logs"
}

# Stop the cluster
down() {
    echo "Stopping Woodpecker cluster..."
    cd "$SCRIPT_DIR"
    docker-compose down
    echo "Success: Cluster stopped"
}

# Clean cluster and volumes
clean() {
    echo "Cleaning Woodpecker cluster..."
    cd "$SCRIPT_DIR"
    docker-compose down -v --remove-orphans
    echo "Success: Cluster cleaned (all data removed)"
}

# Show logs
logs() {
    echo "Cluster Logs:"
    echo "================"
    cd "$SCRIPT_DIR"
    docker-compose logs --tail=50 -f
}

# Show cluster status
status() {
    echo "Cluster Status:"
    echo "=================="
    cd "$SCRIPT_DIR"
    
    echo ""
    echo "Docker containers:"
    docker-compose ps
    
    echo ""
    echo "Service health checks:"
    
    # Check Woodpecker nodes (gRPC services)
    for container in woodpecker-node1 woodpecker-node2 woodpecker-node3; do
        port=""
        case $container in
            woodpecker-node1) port=18080 ;;
            woodpecker-node2) port=18081 ;;
            woodpecker-node3) port=18082 ;;
        esac
        
        # Check container health status and internal health check
        if docker inspect --format='{{.State.Health.Status}}' $container 2>/dev/null | grep -q "healthy"; then
            echo "Success: Woodpecker $container (port $port): Healthy"
        elif nc -z localhost $port 2>/dev/null; then
            echo "Warn: Woodpecker $container (port $port): gRPC port open, checking container health..."
            if docker exec $container /woodpecker/bin/health-check.sh >/dev/null 2>&1; then
                echo "Success: Woodpecker $container (port $port): Internal health check passed"
            else
                echo "Error: Woodpecker $container (port $port): Internal health check failed"
            fi
        else
            echo "Error: Woodpecker $container (port $port): Not accessible"
        fi
    done
    
    # Check MinIO
    if curl -s --connect-timeout 2 http://localhost:9000/minio/health/live >/dev/null 2>&1; then
        echo "Success: MinIO Healthy"
    else
        echo "Error: MinIO Not accessible"
    fi
    
    # Check etcd
    if curl -s --connect-timeout 2 http://localhost:2379/health >/dev/null 2>&1; then
        echo "Success: etcd Healthy"
    elif nc -z localhost 2379 2>/dev/null; then
        echo "Warn: etcd Port open but no health endpoint"
    else
        echo "Error: etcd Not accessible"
    fi
    
    # Check Jaeger
    if curl -s --connect-timeout 2 http://localhost:16686 >/dev/null 2>&1; then
        echo "Success: Jaeger Healthy"
    else
        echo "Error: Jaeger Not accessible"
    fi
}

# Test cluster connectivity
test() {
    echo "Testing Cluster Connectivity:"
    echo "================================="
    echo ""
    
    # Test gRPC ports
    echo "Testing gRPC ports..."
    for port in 18080 18081 18082; do
        if nc -z localhost $port 2>/dev/null; then
            echo "Success: Port $port Available"
        else
            echo "Error: Port $port Not available"
        fi
    done
    
    # Test gossip ports
    echo ""
    echo "Testing gossip ports..."
    for port in 17946 17947 17948; do
        if nc -z localhost $port 2>/dev/null; then
            echo "Success: Port $port Available"
        else
            echo "Error: Port $port Not available"
        fi
    done
    
    echo ""
    echo "Tips: If tests fail, check '$0 status' for more details"
}

# Main script logic
case "${1:-}" in
    build)
        build
        ;;
    up|start)
        up
        ;;
    down|stop)
        down
        ;;
    clean)
        clean
        ;;
    logs)
        logs
        ;;
    status)
        status
        ;;
    test)
        test
        ;;
    help|--help|-h)
        usage
        ;;
    "")
        echo "Error: No command specified"
        echo ""
        usage
        exit 1
        ;;
    *)
        echo "Error: Unknown command: $1"
        echo ""
        usage
        exit 1
        ;;
esac
