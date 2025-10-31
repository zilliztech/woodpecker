# Integration Tests

## Overview

The integration test suite provides comprehensive end-to-end testing for Woodpecker, validating functionality across different storage backends, deployment modes, and component interactions. These tests ensure that all parts of the system work correctly together in realistic scenarios.

## Prerequisites

### Required External Services
- **Etcd**: Metadata storage and distributed coordination
- **MinIO/S3**: Object storage for segment persistence

### Optional Monitoring Stack
- **Prometheus**: Metrics collection
- **Grafana**: Metrics visualization
- **Jaeger**: Distributed tracing

### Docker Compose Setup Example

For local testing, use the following `docker-compose.yaml` to set up external dependencies:

```yaml
version: '3.5'

services:
  jaeger:
    image: ${REGISTRY:-}jaegertracing/jaeger:${JAEGER_VERSION:-latest}
    command:
      - --set=receivers.otlp.protocols.grpc.endpoint="0.0.0.0:4317"
      - --set=receivers.otlp.protocols.http.endpoint="0.0.0.0:4318"
    ports:
      - "16686:16686"
      - "4317:4317"
      - "4318:4318"
    environment:
      - LOG_LEVEL=debug

  etcd:
    container_name: milvus-etcd
    image: bitnami/etcd:3.5.16
    environment:
      - ETCD_AUTO_COMPACTION_MODE=revision
      - ETCD_AUTO_COMPACTION_RETENTION=1000
      - ETCD_QUOTA_BACKEND_BYTES=4294967296
      - ETCD_SNAPSHOT_COUNT=50000
      - ETCD_EXPERIMENTAL_ENABLE_DISTRIBUTED_TRACING=true
      - ETCD_EXPERIMENTAL_DISTRIBUTED_TRACING_SAMPLING_RATE=1000000
      - ETCD_EXPERIMENTAL_DISTRIBUTED_TRACING_ADDRESS=jaeger:4317
      - ETCD_EXPERIMENTAL_DISTRIBUTED_TRACING_SERVICE_NAME=etcd
      - ETCD_EXPERIMENTAL_DISTRIBUTED_TRACING_INSTANCE_ID=etcd1
    ports:
      - "2379:2379"
    volumes:
      - ${DOCKER_VOLUME_DIRECTORY:-.}/volumes/etcd2:/etcd
    command: etcd 
    healthcheck:
      test: ["CMD", "etcdctl", "endpoint", "health"]
      interval: 30s
      timeout: 20s
      retries: 3
    depends_on:
      - "jaeger"

  minio:
    container_name: milvus-minio
    image: minio/minio:RELEASE.2024-12-18T13-15-44Z 
    environment:
      MINIO_ACCESS_KEY: minioadmin
      MINIO_SECRET_KEY: minioadmin
      MINIO_TRACING_ENABLED: 1
      MINIO_TRACING_ENDPOINT: http://jaeger:4318
      MINIO_TRACING_COLLECTOR_TYPE: jaeger
      MINIO_PROFILING: "cpu,mem,block,goroutine"
      MINIO_PPROF_ADDR: ":6060"
      MINIO_CI_CD: true
      MINIO_SUBNET_LICENSE: ""
    ports:
      - "9001:9001"
      - "9000:9000"
      - "6060:6060"
    volumes:
      - ${DOCKER_VOLUME_DIRECTORY:-.}/volumes/minio:/minio_data
    command: minio server /minio_data --console-address ":9001"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 20s
      retries: 3
    depends_on:
      - "jaeger"

networks:
  default:
    name: milvus
```

## Test Categories

### 1. End-to-End Integration Tests

These tests validate complete workflows across the entire Woodpecker stack:

#### `client_read_write_test.go`
**Purpose:** Comprehensive concurrent read/write testing across different storage backends.

**Test Scenarios:**
- Sequential reads of written data
- Concurrent multi-client reads and writes
- Storage backend compatibility (local filesystem, object storage, service mode)
- Data consistency verification
- Reader lag and catch-up behavior

**Key Features:**
- Tests all three storage modes: local, object storage, and distributed service
- Validates data integrity across replicas
- Measures throughput and latency under load

#### `client_test.go`
**Purpose:** Additional concurrent read/write patterns and edge cases.

**Test Coverage:**
- High-concurrency scenarios
- Mixed workload patterns
- Error handling and recovery

#### `service_failover_test.go`
**Purpose:** Tests failover and recovery in distributed service mode.

**Test Scenarios:**
- Node failure and automatic failover
- Writer lock loss and recovery
- Multi-replica consistency during failures
- Graceful degradation under partial failures

**Deployment:** Requires multi-node cluster (typically 3+ nodes)

#### `truncate_test.go`
**Purpose:** Validates log truncation and cleanup operations.

**Test Scenarios:**
- Basic truncate operation
- Truncate with concurrent readers
- Truncate with active writers
- Storage space reclamation
- Metadata consistency after truncation

---

### 2. Client-Side Component Tests

Tests focusing on client library components and their interactions:

#### `internal_log_writer_test.go`
**Purpose:** Tests the lock-free internal log writer implementation.

**Test Coverage:**
- Basic open-write-close-reopen flow
- Concurrent writes without mutual exclusion
- End-to-end write path validation
- Performance characteristics
- Error handling and recovery

**Key Insight:** Validates the internal writer used within the client library for high-throughput scenarios.

#### `log_writer_test.go`
**Purpose:** Tests the distributed lock-based log writer for multi-client exclusivity.

**Test Coverage:**
- Writer lock acquisition and release
- Session expiry and lock loss detection
- Manual lock release and recovery
- Mutual exclusion across multiple clients
- Automatic failover on lock loss

**Key Insight:** Ensures only one writer can hold the lock at a time, critical for multi-replica consistency.

#### `metadata_test.go`
**Purpose:** Tests metadata operations and etcd integration.

**Test Coverage:**
- Log registration and discovery
- Metadata CRUD operations
- Concurrent metadata access
- Consistency guarantees

#### `quorum_discovery_test.go`
**Purpose:** Tests service discovery and node selection in distributed mode.

**Test Coverage:**
- Node discovery via gossip protocol
- Quorum selection strategies (random, zone-aware, affinity-based)
- Performance of discovery operations
- Dynamic node addition/removal
- Health check integration

**Key Metrics:**
- Node discovery latency
- Selection strategy effectiveness
- Failover time

---

### 3. Server-Side Component Tests

Tests focusing on server-side storage layer and segment management:

#### Local Filesystem Storage

##### `local_file_rw_test.go`
Basic read/write operations on local filesystem storage.

##### `local_file_adv_r_test.go`
Advanced read patterns including:
- Random access
- Range queries
- Large batch reads
- Cache effectiveness

#### MinIO/Object Storage

##### `minio_file_rw_test.go`
Basic read/write operations on MinIO/S3-compatible storage.

##### `minio_file_adv_r_test.go`
Advanced MinIO read patterns including:
- Multi-part reads
- Concurrent access
- Large object handling

##### `minio_condition_write_test.go`
Conditional write operations:
- Put-if-not-exists semantics
- Optimistic concurrency control
- Conflict detection and resolution

#### Generic Object Storage

##### `objectstorage_condition_write_test.go`
Tests conditional writes across different object storage backends (MinIO, Azure, GCP).

#### Staged Storage (Hybrid)

##### `staged_file_rw_test.go`
Tests the staged storage layer that combines local cache with object storage:
- Write-through caching
- Cache eviction policies
- Consistency between layers

##### `staged_file_adv_r_test.go`
Advanced staged storage reads:
- Cache hit/miss patterns
- Read-ahead optimization
- Multi-tier access

#### Cluster and Remote Access

##### `mini_cluster_test.go`
Tests in-process mini-cluster for development and testing:
- Multi-node coordination
- Inter-node communication
- Cluster lifecycle management

##### `logstore_client_remote_test.go`
Tests remote gRPC client access to logstore server:
- Client-server communication
- Network failure handling
- Request retries and timeouts

---

## Running Integration Tests

### Setup External Services

```bash
# Start external dependencies
docker-compose up -d

# Verify services are healthy
docker-compose ps
```

### Run All Integration Tests

```bash
# Run all tests
go test -v ./tests/integration/

# Run with timeout for long-running tests
go test -timeout=30m -v ./tests/integration/
```