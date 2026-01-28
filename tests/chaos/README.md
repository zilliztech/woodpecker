# Chaos Testing

## Overview

The chaos test suite validates Woodpecker's resilience and fault tolerance in distributed multi-replica deployments. These tests simulate real-world failure scenarios to ensure data consistency, availability, and proper failover behavior under adverse conditions.

## Quick Start

Run all chaos tests with a single command:

```bash
cd tests/chaos
./run_chaos_tests.sh
```

This will automatically:
1. Build the Docker image (if not already built)
2. Start a 4-node Woodpecker cluster with etcd and MinIO
3. Run all chaos test cases
4. Collect container logs on failure
5. Clean up the cluster

### Run a Specific Test

```bash
./run_chaos_tests.sh -run TestChaos_BasicReadWrite
```

### Keep Cluster Running After Tests

```bash
./run_chaos_tests.sh --no-cleanup
```

### Manual Execution

```bash
# Build image
cd /path/to/woodpecker
./build/build_image.sh ubuntu22.04 auto -t woodpecker:latest

# Start cluster with chaos override
cd deployments
docker compose -f docker-compose.yaml -f ../tests/chaos/docker-compose.chaos.yaml -p woodpecker-chaos up -d

# Wait for cluster to be ready (~15s)
sleep 15

# Run tests
cd ../tests/chaos
go test -v -timeout 600s -run 'TestChaos' ./... -count=1

# Clean up
cd ../../deployments
docker compose -f docker-compose.yaml -f ../tests/chaos/docker-compose.chaos.yaml -p woodpecker-chaos down -v
```

## Prerequisites

- Docker with Compose v2 plugin (`docker compose`)
- Go 1.24.2+
- Sufficient system resources (the cluster uses 4 Woodpecker nodes + etcd + MinIO + Jaeger)

## Architecture

### File Structure

```
tests/chaos/
├── README.md                    # This file
├── basic_test.go                # Baseline read/write smoke test
├── chaos_test.go                # Chaos failure scenario tests
├── docker_cluster.go            # Docker cluster management helper
├── docker-compose.chaos.yaml    # Compose override (disables auto-restart)
└── run_chaos_tests.sh           # One-click test runner
```

### Design Decisions

- **CLI-based Docker management**: Uses `docker` CLI commands (no Go Docker SDK dependency) for simplicity and compatibility with the existing deployment infrastructure.
- **Compose override file**: `docker-compose.chaos.yaml` overrides `restart: "no"` so killed containers stay dead, without modifying the original compose file.
- **Separate project name**: Uses `woodpecker-chaos` project name to avoid interfering with development clusters.
- **Test-managed lifecycle**: Each test creates its own client and log name; cluster lifecycle is managed by `run_chaos_tests.sh` or manually.

### DockerCluster Helper

The `docker_cluster.go` file provides a `DockerCluster` struct with methods for:

| Method | Description |
|---|---|
| `BuildImageIfNeeded(t)` | Build Docker image if missing |
| `Up(t)` / `Down(t)` / `Clean(t)` | Cluster lifecycle |
| `KillNode(t, name)` | `docker kill` — simulates crash |
| `StopNode(t, name)` | `docker stop` — graceful shutdown |
| `StartNode(t, name)` | `docker start` — restart stopped container |
| `PauseNode(t, name)` | `docker pause` — freeze processes |
| `UnpauseNode(t, name)` | `docker unpause` — resume processes |
| `DisconnectNetwork(t, name)` | Simulate network partition |
| `ConnectNetwork(t, name)` | Heal network partition |
| `WaitForHealthy(t, name, timeout)` | Poll until container is running |
| `WaitForClusterReady(t, timeout)` | Wait for all containers + gossip |
| `NewClient(t, ctx)` | Create a Woodpecker client for tests |
| `DumpAllLogs(t, tail)` | Dump container logs to test output |

## Test Cases

### 1. `TestChaos_BasicReadWrite`
Smoke test: writes 1000 entries and reads them back to verify the cluster works under normal conditions.

### 2. `TestChaos_SingleNodeKill_WriteContinues`
Kills one Woodpecker node and verifies that writes continue after segment rolling. Reads back all entries to confirm no data loss.

### 3. `TestChaos_DoubleNodeKill_WriteContinues`
Kills 2 out of 4 nodes and verifies writes can still proceed after recovery/rolling. Tests the quorum tolerance boundary.

### 4. `TestChaos_NodeRestart_RecoveryMode`
Kills a node and restarts it, then verifies the cluster enters recovery mode and continues accepting writes in a new segment.

### 5. `TestChaos_RollingRestart`
Performs a rolling restart of all 4 nodes (kill, restart, write, repeat). Verifies no data gaps across the entire sequence.

### 6. `TestChaos_FullClusterRestart_DataDurability`
Writes data, stops all Woodpecker nodes, restarts them, and verifies all previously written data is readable. Tests durability across a full cluster restart.

### 7. `TestChaos_NetworkPartition`
Disconnects a node from the Docker network, writes entries to remaining nodes, reconnects the node, and verifies all data.

### 8. `TestChaos_MinIOFailure_WriteFailsAndRecovers`
Stops MinIO, verifies writes degrade (some may fail), restarts MinIO, and verifies writes recover and all successful data is readable.

## Extending

To add a new chaos test:

1. Add a new `TestChaos_*` function in `chaos_test.go`
2. Use `newChaosCluster(t)` to get the cluster helper
3. Use `cluster.NewClient(t, ctx)` to create a client (automatically cleaned up)
4. Use `writeEntries()`, `readAllEntries()`, and `verifyEntryOrder()` helpers
5. Use `cluster.KillNode()`, `cluster.StopNode()`, etc. for fault injection
6. Always restore the cluster state at the end of the test (restart killed nodes, reconnect networks)

## Troubleshooting

### Tests fail to start
- Ensure Docker is running and `docker compose` v2 is available
- Check that ports 18080-18083, 17946-17949, 2379, 9000-9001 are not in use
- Run `docker compose -f deployments/docker-compose.yaml -f tests/chaos/docker-compose.chaos.yaml -p woodpecker-chaos down -v` to clean up stale containers

### Tests fail mid-execution
- Check `tests/chaos/logs/` for container logs (collected automatically on failure)
- Run with `--no-cleanup` to inspect the cluster state after failure
- Run a single test with `-run TestChaos_BasicReadWrite` to isolate issues
