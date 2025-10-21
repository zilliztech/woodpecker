# Common Package

## Overview

The `common` package provides a collection of reusable utilities, infrastructure components, and helper libraries that are shared across the entire Woodpecker project. These components form the foundational layer for configuration management, distributed coordination, storage access, observability, and concurrency control.

## Package Structure

### **bitset**
Thread-safe bit manipulation utilities for compact flag storage and operations. Provides a simplified bitset implementation (up to 16 bits) with atomic operations for setting, clearing, and querying individual bits.

**Use cases**: Tracking states, feature flags, and boolean indicators in a memory-efficient manner.

### **channel**
Abstraction layer for asynchronous result communication supporting both local and remote scenarios:
- **Local channels**: Direct Go channel-based implementation for in-process communication
- **Remote channels**: gRPC stream-based implementation for cross-process result delivery

**Use cases**: Handling asynchronous append operations, notification delivery, and request-response patterns.

### **conc**
Concurrency primitives and utilities for managing parallel operations:
- **Future**: Promise-like async computation with result retrieval
- **Pool**: Worker pool with controlled parallelism and graceful shutdown
- **SingleFlight**: Request deduplication to prevent duplicate work for identical operations

**Use cases**: Parallel data processing, resource pooling, and avoiding thundering herd problems.

### **config**
Centralized configuration management system that parses and validates Woodpecker's YAML configuration files. Supports hierarchical configuration with environment-specific overrides.

**Key configurations**: Etcd, storage backends (MinIO, local FS, object storage), logging, tracing, and Woodpecker-specific policies.

### **etcd**
Etcd integration layer providing:
- **Embedded etcd server**: In-process etcd for single-node deployments
- **Client utilities**: Connection management, automatic retries, and session handling
- **Key-value operations**: Helper functions for metadata storage and distributed coordination

**Use cases**: Metadata persistence, distributed locks, and leader election.

### **generic**
Generic utility functions leveraging Go generics:
- Zero value generation
- Type-safe equality checks
- Generic comparisons

**Use cases**: Type-agnostic helper functions to reduce code duplication.

### **hardware**
Hardware resource monitoring and introspection:
- CPU count and usage statistics
- Memory usage tracking
- Disk capacity and I/O metrics

**Use cases**: Resource-aware scheduling, capacity planning, and performance monitoring.

### **logger**
Structured logging infrastructure built on Zap with:
- Custom text encoder for enhanced readability
- Context-aware logging
- Log level configuration
- File rotation support

**Use cases**: Application-wide logging with consistent format and context propagation.

### **membership**
Gossip-based service discovery and cluster membership management:
- **Service Discovery**: High-performance O(1) node lookup with multi-dimensional indexing (availability zone, resource group)
- **Node Management**: Dynamic node registration, health tracking, and failure detection
- **Placement Strategies**: Support for zone-aware, affinity-based, and custom node selection
- **Gossip Protocol**: Peer-to-peer communication for cluster state synchronization

**Use cases**: Distributed cluster formation, node discovery, and quorum selection.

### **metrics**
Prometheus-based metrics collection for observability:
- Client-side metrics (operations, latency, errors)
- Server-side metrics (request counts, throughput, storage stats)
- Log name-to-ID mappings
- Resource utilization tracking

**Use cases**: Production monitoring, SLA tracking, and performance analysis.

### **minio**
MinIO and S3-compatible object storage abstraction:
- Multi-cloud provider support (AWS S3, Aliyun OSS, GCP, Tencent COS)
- File upload/download with retry logic
- Bucket management and lifecycle operations
- Health checks and connectivity validation

**Use cases**: Persistent segment storage in distributed deployments.

### **net**
Network utility functions:
- IP address resolution and validation
- Local IP detection
- Hostname resolution

**Use cases**: Network configuration, service binding, and address management.

### **objectstorage**
Unified object storage interface supporting multiple backends:
- **MinIO/S3**: Standard S3-compatible storage
- **Azure Blob Storage**: Microsoft Azure native integration
- **GCP Native Storage**: Google Cloud Storage direct access

**Use cases**: Cloud-agnostic storage layer for segment persistence.

### **pool**
Byte buffer pooling utilities for reducing memory allocations:
- Reusable byte slice pools
- Size-aware buffer management
- Automatic buffer recycling

**Use cases**: High-throughput data processing with minimal GC pressure.

### **retry**
Configurable retry mechanisms with:
- Exponential backoff strategies
- Maximum retry limits
- Context-aware cancellation
- Custom retry conditions

**Use cases**: Transient failure handling for network operations and external service calls.

### **tracer**
Distributed tracing integration with OpenTelemetry:
- Jaeger exporter support
- OTLP (OpenTelemetry Protocol) support
- Span context propagation
- Configurable sampling rates

**Use cases**: Request tracing across distributed Woodpecker nodes for debugging and performance analysis.

### **werr**
Woodpecker-specific error management system:
- Structured error codes organized by layer (client, server, metadata)
- Retryable vs non-retryable error classification
- Error wrapping with context preservation
- Standardized error messages

**Use cases**: Consistent error handling, error propagation, and client-side retry logic.

## Design Principles

1. **Reusability**: All components are designed to be used across multiple Woodpecker subsystems
2. **Minimal Dependencies**: Common packages avoid heavy dependencies to reduce coupling
3. **Observability**: Built-in support for logging, metrics, and tracing throughout

## Usage

The common package serves as the foundation for:
- **Client Library** (`woodpecker/`): Configuration, logging, and client-side utilities
- **Server Components** (`server/`): Storage backends, metrics, and distributed coordination
- **Command-Line Interface** (`cmd/`): Configuration parsing and service initialization
- **Tests** (`tests/`): Shared test utilities and fixtures