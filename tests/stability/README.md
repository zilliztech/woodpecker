# Stability Tests

## Overview

The stability test suite validates Woodpecker's reliability, robustness, and performance under sustained load and real-world operational patterns. These long-running tests simulate production workloads to ensure the system maintains data integrity, consistent performance, and proper resource management over extended periods.

## Prerequisites

### Required External Services
- **Etcd**: For metadata storage and distributed coordination
- **MinIO/S3**: For persistent segment storage

### Optional Monitoring Stack
- **Prometheus**: For metrics collection and analysis
- **Grafana**: For real-time performance visualization
- **Jaeger**: For distributed tracing and request flow analysis

**Recommendation:** Deploy the full monitoring stack for stability tests to observe system behavior, identify bottlenecks, and detect anomalies during long-running scenarios.

## Test Files

### 1. `high_throughput_test.go`

**Purpose:** Validates read/write stability under sustained high-throughput load.

**Test Scenarios:**

#### `TestSimpleHighThroughputWriteAndRead`
Tests basic writer and tail reader collaboration under high load:

**Configuration:**
- **Concurrent Writers**: 4 threads writing simultaneously
- **Message Size**: 1MB per message
- **Total Data Volume**: ~2GB (4 threads × 500 messages × 1MB)
- **Reader Pattern**: Continuous tail reading to keep up with writes

**Validation:**
- All written data is successfully read
- Data integrity maintained across large volumes
- No data loss or corruption under sustained load
- Reader can keep pace with writer throughput
- Memory usage remains stable (no leaks)
- Performance metrics stay within acceptable bounds

**Key Metrics:**
- Write throughput (MB/s, messages/s)
- Read lag (time behind writer)
- Memory consumption over time
- GC pressure and frequency
- Error rates and retry counts

**Storage Backends Tested:**
- Local filesystem storage
- Object storage (MinIO/S3)

---

### 2. `stability_test.go`

**Purpose:** Simulates real-world operational patterns with continuous operations over extended periods.

**Test Scenarios:**

#### `TestWriteAndConcurrentTailRead`
Long-running test with writer restarts and continuous tail reading:

**Writer Behavior:**
- Multiple open/close cycles (simulating restarts)
- Messages written per cycle: 25
- Pause between cycles: 500ms
- Tests writer recovery and continuation

**Reader Behavior:**
- Continuous tail reading throughout test
- Verifies data continuity across writer restarts
- Validates no gaps or duplicates in data stream

**Validation:**
- Data continuity across writer lifecycle events
- Reader resilience to writer disruptions
- Proper segment rolling and compaction
- Memory and resource cleanup on writer close

#### Mixed Operation Patterns
Tests realistic operational scenarios combining:

**Operations:**
- **Tail Read**: Continuous reading of latest data
- **Catchup Read**: Readers catching up from historical positions
- **Truncate**: Log cleanup and space reclamation
- **Write**: Ongoing data ingestion

**Scenarios:**
1. Writer continuously appending while readers tail
2. Truncate operations during active reading
3. New readers catching up from old positions
4. Writer restarts with readers maintaining position
5. Concurrent truncate and catchup reads

**Validation:**
- Data consistency across all operation types
- No interference between concurrent operations
- Proper handling of truncated data during reads
- Catchup readers can reach tail position
- System remains stable over extended runtime

**Duration:** Tests run for extended periods (hours to days) to detect:
- Memory leaks
- Resource exhaustion
- Performance degradation
- Correctness under sustained load
- Recovery from transient failures

---

## Running Stability Tests

### Setup

```bash
# Start external dependencies
docker-compose up -d 

# Verify services are healthy
docker-compose ps
```

### Run Tests

```bash
# Run all stability tests
go test -v ./tests/stability/
```
