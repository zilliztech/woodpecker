# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Woodpecker is a cloud-native Write-Ahead Log (WAL) storage implementation that uses cloud object storage (S3, Azure, GCP, MinIO) as the durable backend. It's designed for high-throughput, ordered, durable log storage scenarios like distributed databases, streaming, and consensus protocols.

**Go Version:** 1.24.2

## Build Commands

```bash
make build              # Build binary for current platform
make test               # Run all tests with coverage and race detection
make proto              # Generate protobuf code (requires `make install` first)
make install            # Install protobuf code generation tools
make cluster-up         # Start etcd + MinIO + Woodpecker cluster via Docker Compose
make cluster-down       # Stop the cluster
```

## Running Tests

```bash
# All tests
make test

# Single test file
go test -v ./tests/integration/client_test.go

# Single test function
go test -v -run TestOpenWriterMultiTimesInSingleClient ./tests/integration/

# Subtest
go test -v -run TestName/SubtestName ./tests/integration/

# With race detection
go test -race -v ./tests/integration/...
```

Integration tests require Docker (for etcd and MinIO). Use `make cluster-up` to start dependencies.

## Architecture

### Deployment Modes

**Embedded Mode:** Client library with in-process LogStore. Uses etcd for coordination, direct cloud storage access. Minimal dependencies.

**Service Mode:** Dedicated LogStore cluster nodes with gossip-based discovery and quorum-based writes. Better for high-throughput multi-tenant scenarios.

### Key Components

```
woodpecker/          # Client library
├── woodpecker_client.go       # User-facing client interface
├── woodpecker_embed_client.go # Embedded mode client (in-process LogStore)
├── client/         # RPC client for client-server communication in service mode
├── log/            # Log handle and writer/reader operations
├── segment/        # Segment operations (append, rolling, cleanup)
└── quorum/         # Quorum selection for service mode

server/              # LogStore server (AGPLv3/SSPLv1 licensed)
├── logstore.go     # Main LogStore implementation
├── service.go      # gRPC service
├── processor/      # Segment processing
└── storage/        # Storage backends (local, minio, objectstorage, stagedstorage)

common/              # Shared utilities
├── config/         # Configuration management
├── etcd/           # Etcd client and embedded server
├── membership/     # Gossip-based service discovery
├── objectstorage/  # Cloud storage interfaces (S3, Azure, GCP)
├── werr/           # Error codes and wrapping
├── channel/        # Async result channels
└── conc/           # Concurrency primitives (futures, pools)

meta/                # Metadata operations with etcd
proto/               # Protocol Buffer definitions
```

### Data Model

- **Log**: A named log stream containing ordered segments
- **Segment**: Unit of log storage (default 256MB, configurable)
- **Block**: Sub-unit within segment for cloud storage (default 2MB)
- **Entry**: Individual log message with segment ID and entry ID

Storage path format: `root/instance_id/log_id/segment_id/block_id`

### Write Path
1. Client calls `WriteAsync()` or `Write()`
2. Segment routes to internal writer or remote LogStore
3. Data batched and buffered
4. Periodic sync flushes to cloud storage

### Read Path
1. Client calls `ReadNext()` with position
2. Segment reader fetches data (with optional prefetching)
3. Data returned with next read position

## Code Patterns

- All async operations use `context.Context` for cancellation/timeouts
- Custom error codes in `common/werr/` with retryable vs non-retryable classification
- Interface-based design: `Client`, `LogStore`, `MetadataProvider`, `ObjectStorage`
- Structured logging with Zap via `common/logger/`
- Prometheus metrics via `common/metrics/`

## Licensing

- `server/` directory: AGPLv3 or SSPLv1 (dual-licensed)
- All other code: Apache License 2.0
