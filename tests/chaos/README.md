# Chaos Testing

## Overview

The chaos test suite validates Woodpecker's resilience and fault tolerance in distributed multi-replica deployments. These tests simulate real-world failure scenarios to ensure data consistency, availability, and proper failover behavior under adverse conditions.

## Prerequisites

Before running chaos tests, you must deploy a complete Woodpecker service cluster. Refer to the [`deployments/`](../../deployments) directory for deployment options:

- **Manual Deployment**: Follow deployment instructions for setting up individual components
- **Script-based Deployment**: Use `deploy.sh` for automated cluster provisioning
- **Docker Compose**: Use `docker-compose.yaml` for containerized local testing

### Required Components

A full chaos testing environment requires:
- **Multiple Woodpecker nodes**: At least 3 nodes for quorum testing
- **Etcd cluster**: For metadata storage and coordination
- **MinIO/S3**: For persistent segment storage
- **Network control tools**: For simulating network partitions and latency

## Test Categories

### 1. Basic Functional Tests (`basic_test.go`)

Baseline tests to verify the deployment and environment setup before chaos testing:

**Validation Checks:**
- Docker compose cluster deployment verification
- Service connectivity and health checks
- Basic read/write operations
- Multi-replica quorum functionality
- Data consistency across replicas

**Purpose:** Ensure the system works correctly under normal conditions before introducing chaos.

---

### 2. MinIO Failure Tests

**Status:** TODO

**Planned Test Scenarios:**
- Single MinIO node failure in a cluster
- Complete MinIO service unavailability
- MinIO network partition
- MinIO disk I/O failures
- Recovery behavior after MinIO restoration

**Expected Behavior:**
- In-flight writes should fail gracefully
- Reads should fall back to local cache or replicas
- System should recover automatically when MinIO is restored

---

### 3. Etcd Failure Tests

**Status:** TODO

**Planned Test Scenarios:**
- Single etcd node failure in a cluster
- Etcd leader election disruption
- Complete etcd cluster unavailability
- Etcd network partition (split-brain scenarios)
- Metadata corruption recovery

**Expected Behavior:**
- Existing connections should continue working
- New log operations should fail gracefully
- Metadata consistency should be maintained
- System should recover when etcd quorum is restored

---

### 4. Woodpecker Service Failure Tests

**Status:** TODO

**Planned Test Scenarios:**
- Single writer node failure
- Multiple simultaneous node failures
- Rolling node restarts
- Network partition isolating nodes
- Writer lock lost and recovery
- Graceful vs ungraceful shutdown

**Expected Behavior:**
- Readers should switch to available replicas
- Writers should detect lock loss and reopen
- No data loss for acknowledged writes
- Automatic failover and recovery

---

### 5. Mixed Component Failure Tests

**Status:** TODO

**Planned Test Scenarios:**
- Cascading failures (e.g., MinIO + Woodpecker node)
- Simultaneous multi-component failures
- Partial cluster availability
- Network partition affecting multiple components
- Recovery from complete system failure

**Expected Behavior:**
- System degrades gracefully
- Data consistency is maintained
- Recovery proceeds in correct order
- No data corruption or loss
