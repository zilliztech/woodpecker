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


# Build Docker image script for different architectures and OS
# Usage: ./build_image.sh [OS] [ARCH] [OPTIONS]
set -e

# Default values
DEFAULT_OS="ubuntu22.04"
DEFAULT_ARCH="auto"
DEFAULT_TAG="woodpecker:latest"

# Function to display usage
usage() {
    echo "Usage: $0 [OS] [ARCH] [OPTIONS]"
    echo ""
    echo "Arguments:"
    echo "  OS: Operating system (default: ubuntu22.04)"
    echo "      Supported: ubuntu20.04, ubuntu22.04, amazonlinux2023, rockylinux8"
    echo "  ARCH: Architecture (default: auto)"
    echo "        Supported: amd64, arm64, auto"
    echo ""
    echo "Options:"
    echo "  -t, --tag TAG        Image tag (default: woodpecker:latest)"
    echo "  --no-cache          Build without using cache"
    echo "  -h, --help          Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Build ubuntu22.04 for current arch"
    echo "  $0 ubuntu20.04                        # Build ubuntu20.04 for current arch"
    echo "  $0 ubuntu22.04 arm64                  # Build ubuntu22.04 for ARM64"
    echo "  $0 amazonlinux2023 amd64 --no-cache  # Build without cache"
    exit 1
}

# Function to detect current architecture
detect_arch() {
    local machine=$(uname -m)
    case $machine in
        x86_64|amd64)
            echo "amd64"
            ;;
        aarch64|arm64)
            echo "arm64"
            ;;
        *)
            echo "Unsupported architecture: $machine" >&2
            exit 1
            ;;
    esac
}

# Function to validate OS
validate_os() {
    local os=$1
    case $os in
        ubuntu20.04|ubuntu22.04|amazonlinux2023|rockylinux8)
            return 0
            ;;
        *)
            echo "❌ Error: Unsupported OS '$os'"
            echo "Supported OS: ubuntu20.04, ubuntu22.04, amazonlinux2023, rockylinux8"
            exit 1
            ;;
    esac
}

# Function to check if Dockerfile exists
check_dockerfile() {
    local os=$1
    local dockerfile="build/docker/$os/Dockerfile"
    
    if [ ! -f "$dockerfile" ]; then
        echo "❌ Error: Dockerfile not found: $dockerfile"
        exit 1
    fi
    echo "$dockerfile"
}

# Parse arguments
OS=${1:-$DEFAULT_OS}
ARCH=${2:-$DEFAULT_ARCH}
shift 2 2>/dev/null || true

# Parse options
TAG=$DEFAULT_TAG
NO_CACHE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--tag)
            TAG="$2"
            shift 2
            ;;
        --no-cache)
            NO_CACHE="--no-cache"
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate inputs
validate_os "$OS"

# Auto-detect architecture if needed
if [ "$ARCH" = "auto" ]; then
    ARCH=$(detect_arch)
    echo "🔍 Auto-detected architecture: $ARCH"
fi

# Validate architecture
case $ARCH in
    amd64|arm64)
        ;;
    *)
        echo "❌ Error: Unsupported architecture '$ARCH'"
        echo "Supported architectures: amd64, arm64, auto"
        exit 1
        ;;
esac

# Check Dockerfile
DOCKERFILE=$(check_dockerfile "$OS")

# Change to project root
cd "$(dirname "$0")/.."

# Build binary first
echo "📦 Building binary for architecture: $ARCH"
./build/build_bin.sh "$ARCH"

echo "🚀 Building Woodpecker Docker image..."
echo "📋 Configuration:"
echo "   OS: $OS"
echo "   Architecture: $ARCH"
echo "   Dockerfile: $DOCKERFILE"
echo "   Tag: $TAG"
echo "   Cache: $([ -n "$NO_CACHE" ] && echo "disabled" || echo "enabled")"

# Build single-architecture image
echo "🏗️  Building single-architecture image for $ARCH..."
BUILD_CMD="docker build --platform linux/$ARCH"

# Add common options
BUILD_CMD="$BUILD_CMD -f $DOCKERFILE -t $TAG $NO_CACHE ."

echo "🔨 Executing: $BUILD_CMD"
eval $BUILD_CMD

# Verify the build
# Extract image name without tag for grep
IMAGE_NAME=$(echo "$TAG" | cut -d':' -f1)
if docker images | grep -q "$IMAGE_NAME"; then
    echo "✅ Image built successfully: $TAG"
    docker images "$TAG"
else
    echo "❌ Build failed: Image not found"
    exit 1
fi

echo "🎉 Build completed successfully!"

# Show next steps
echo ""
echo "💡 Next steps:"
echo "   To run the image: docker run -it $TAG"
echo "   To test deployment: cd deployments && ./deploy.sh up"