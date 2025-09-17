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

FROM rockylinux/rockylinux:8


# Build arguments for multi-platform support
ARG TARGETPLATFORM

# Metadata
LABEL maintainer="zilliz-team@zilliz.com"
LABEL version="1.0.0"
LABEL description="Woodpecker distributed log store server"

# Install dependencies including user management tools
RUN dnf install -y wget libgomp libaio libatomic shadow-utils

RUN dnf -y install dnf-plugins-core && \
    dnf config-manager --set-enabled powertools && \
    dnf -y install openblas-devel

# Install tini for proper signal handling (multi-platform support)
RUN case ${TARGETPLATFORM} in \
    "linux/amd64") TINI_ARCH=amd64 ;; \
    "linux/arm64") TINI_ARCH=arm64 ;; \
    *) echo "Unsupported platform: ${TARGETPLATFORM}" && exit 1 ;; \
    esac && \
    wget -O /tini https://github.com/krallin/tini/releases/download/v0.19.0/tini-${TINI_ARCH} && \
    chmod +x /tini

# Create woodpecker user and directories
RUN useradd -r -u 1000 -g root -s /bin/bash -d /woodpecker woodpecker && \
    mkdir -p /woodpecker/bin && \
    mkdir -p /woodpecker/configs && \
    mkdir -p /woodpecker/data && \
    mkdir -p /woodpecker/logs && \
    chown -R woodpecker:root /woodpecker && \
    chmod -R g=u /woodpecker

# Copy Woodpecker binary
COPY --chown=woodpecker:root --chmod=755 ./bin/woodpecker /woodpecker/bin/woodpecker

# Copy startup and health check scripts
COPY --chown=woodpecker:root --chmod=755 ./build/docker/scripts/start-woodpecker.sh /woodpecker/bin/start-woodpecker.sh
COPY --chown=woodpecker:root --chmod=755 ./build/docker/scripts/health-check.sh /woodpecker/bin/health-check.sh

# Set working directory
WORKDIR /woodpecker

# Switch to woodpecker user
USER woodpecker

# Environment variables with defaults (removed sensitive defaults)
ENV PORT=17946 \
    SERVICE_PORT=18080 \
    NODE_NAME="" \
    DATA_DIR="/woodpecker/data" \
    CONFIG_FILE="/woodpecker/configs/woodpecker.yaml" \
    CLUSTER="woodpecker-cluster" \
    STORAGE_TYPE="service" \
    LOG_LEVEL="info" \
    ETCD_ENDPOINTS="etcd:2379" \
    MINIO_ADDRESS="minio" \
    MINIO_PORT="9000" \
    MINIO_ACCESS_KEY="" \
    MINIO_SECRET_KEY="" \
    MINIO_BUCKET="" \
    SEEDS=""

# Expose ports (use fixed ports for better documentation)
EXPOSE 18080 17946

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD /woodpecker/bin/health-check.sh

# Entry point
ENTRYPOINT ["/tini", "--"]
CMD ["/woodpecker/bin/start-woodpecker.sh"]