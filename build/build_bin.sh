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


# Build binary script for different CPU architectures
# Usage: ./build_bin.sh [ARCH]
# ARCH: amd64, arm64, or auto (default: auto)
set -e

# Function to display usage
usage() {
    echo "Usage: $0 [ARCH]"
    echo "  ARCH: amd64, arm64, or auto (default: auto)"
    echo "  auto: automatically detect architecture"
    echo ""
    echo "Examples:"
    echo "  $0           # Auto-detect and build for current architecture"
    echo "  $0 amd64     # Build for AMD64/x86-64"
    echo "  $0 arm64     # Build for ARM64"
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

# Parse arguments
ARCH=${1:-auto}

if [ "$ARCH" = "auto" ]; then
    ARCH=$(detect_arch)
    echo "🔍 Auto-detected architecture: $ARCH"
elif [ "$ARCH" = "--help" ] || [ "$ARCH" = "-h" ]; then
    usage
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

echo "🚀 Building Woodpecker binary for $ARCH architecture..."

# Change to project root if not already there
cd "$(dirname "$0")/.."

# Build based on architecture
case $ARCH in
    amd64)
        echo "📦 Building for AMD64/x86-64..."
        make build-linux
        ;;
    arm64)
        echo "📦 Building for ARM64..."
        make build-linux-arm64
        ;;
esac

# Verify the build
if [ -f "bin/woodpecker" ]; then
    echo "✅ Binary built successfully: bin/woodpecker"
    file bin/woodpecker
    ls -lh bin/woodpecker
else
    echo "❌ Build failed: bin/woodpecker not found"
    exit 1
fi

echo "🎉 Build completed successfully!"
