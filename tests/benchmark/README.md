# Benchmark Tests

## Overview

The benchmark test suite provides comprehensive performance testing for Woodpecker, including end-to-end client operations and low-level I/O primitives. These benchmarks help identify performance bottlenecks, validate optimization strategies, and establish performance baselines for regression testing.

## Prerequisites

### Required External Services
- **Etcd**: Metadata storage backend (standalone or embedded)
- **MinIO/S3**: Object storage for segment persistence

### Optional Monitoring Stack
- **Prometheus**: Metrics collection and querying
- **Grafana**: Metrics visualization and dashboards
- **Jaeger**: Distributed tracing for request flow analysis

## Test Categories

### 1. End-to-End Client Performance (`client_test.go`)

Comprehensive benchmarks testing the full Woodpecker client API under various workload patterns:

**Test Scenarios:**
- **Single Writer, Single Reader**: Basic read/write performance baseline
- **Single Writer, Multiple Readers**: Reader scalability under write load
- **Multiple Writers, Single Reader**: Write throughput with concurrent writers
- **Multiple Writers, Multiple Readers**: Full system stress testing
- **Lock Lost Recovery**: Writer failover and recovery performance
- **Large Entry Handling**: Performance with varying message sizes

**Key Metrics:**
- Throughput (entries/sec, MB/sec)
- Latency percentiles (p50, p95, p99, p999)
- Writer lock contention and reopen frequency
- Reader lag and catch-up time

**Configuration Parameters:**
- Entry size: 512B to 1MB+
- Number of concurrent writers: 1-10+
- Number of concurrent readers: 1-50+
- Test duration: 10s to 5min

### 2. Metadata Operations (`metadata_test.go`)

Benchmarks for etcd-based metadata operations:
- Log registration and discovery
- Metadata read/write performance
- Concurrent metadata access patterns

### 3. Storage Layer Performance

#### Local Filesystem (`local_file_rw_perf_test.go`)
- Sequential read/write throughput
- Random access patterns
- File system cache effects
- Direct I/O vs buffered I/O comparison

#### MinIO/Object Storage (`minio_file_rw_perf_test.go`, `minio_test.go`)
- Object upload/download throughput
- Multi-part upload performance
- Concurrent I/O scalability
- Network latency impact

### 4. Low-Level I/O Primitives

#### File Append Operations (`file_append_test.go`)
Tests standard file append performance under various configurations:

**Test Parameters:**
- **Block Size**: 4KB (small), 32KB (medium), 128KB (large)
- **Preallocation**: Impact of preallocating file space
- **Flush Frequency**: No flush, infrequent, frequent
- **Sync Mode**: None, flush, fsync, datasync

**Focus Areas:**
- Impact of write amplification
- File system block alignment
- Buffer cache effectiveness
- Durability vs performance trade-offs

#### Memory-Mapped I/O (`mmap_test.go`)
Benchmarks mmap-based write operations:

**Test Scenarios:**
- Small file + small blocks + frequent flush
- Large file + large blocks + infrequent flush
- Random vs sequential write patterns
- Memory page fault impact

**Key Insights:**
- Optimal mmap region sizes
- Page alignment effects
- Flush/sync overhead with mmap

### 5. Data Processing Operations

#### CRC32 Checksums (`crc_test.go`)
Performance of CRC32 checksum computation across different data sizes:

**Benchmark Sizes**: 100B, 500B, 1KB, 2KB, 4KB, 8KB, 16KB, 32KB, 64KB, 128KB, 256KB, 512KB, 1MB

**Use Cases:**
- Data integrity verification overhead
- Optimal checksum granularity selection

#### Compression (`compression_test.go`)
Comparative analysis of compression algorithms for vector data:

**Algorithms Tested:**
- **Zlib**: Balanced compression ratio and speed
- **LZ4**: Fast compression with moderate ratio
- **Zstd**: Modern algorithm with tunable trade-offs

**Test Data:**
- Vector embeddings (typical ML workload)
- Configurable dimensions and batch sizes
- Normal distribution simulation

**Metrics:**
- Compression ratio (original size / compressed size)
- Compression throughput (MB/sec)
- Decompression throughput (MB/sec)
- CPU efficiency
