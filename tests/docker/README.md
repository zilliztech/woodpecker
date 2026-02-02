# Docker Integration Tests

Docker-based integration test suites for Woodpecker. These tests run a full
4-node Woodpecker cluster using Docker Compose alongside etcd and MinIO,
then execute Go test scenarios against the running cluster.

## Directory Structure

```
tests/docker/
├── common.sh              # Shared shell functions for test runners
├── framework/
│   ├── cluster.go         # DockerCluster: shared cluster management
│   └── helpers.go         # WriteEntries, ReadAllEntries helpers
├── chaos/
│   ├── chaos_cluster.go   # ChaosCluster (embeds framework.DockerCluster)
│   ├── chaos_test.go      # Chaos test scenarios (node kill, partition, etc.)
│   ├── basic_test.go      # Basic read/write smoke test
│   ├── docker-compose.chaos.yaml
│   └── run_chaos_tests.sh
└── monitor/
    ├── monitor_cluster.go # MonitorCluster (embeds framework.DockerCluster)
    ├── monitor_test.go    # Prometheus metrics verification
    ├── docker-compose.monitor.yaml
    ├── prometheus.yml
    ├── run_monitor_tests.sh
    ├── grafana/           # Grafana dashboard auto-setup tool
    │   ├── panels.go      # Go struct types + panel/row builder helpers
    │   ├── dashboard.go   # BuildDashboard: 6-row, 31-panel layout
    │   ├── setup.go       # Grafana API client (datasource + import)
    │   └── cmd/main.go    # Standalone CLI entry point
    └── rolling_restart/   # Rolling restart latency profiling
        └── rolling_restart_test.go
```

## Quick Start

```bash
# Run chaos tests (builds image, starts cluster, runs tests, cleans up)
cd tests/docker/chaos
./run_chaos_tests.sh

# Run monitor tests (includes Prometheus + Grafana)
cd tests/docker/monitor
./run_monitor_tests.sh

# Run a specific test
./run_chaos_tests.sh -run TestChaos_BasicReadWrite

# Keep cluster running after tests for debugging
./run_monitor_tests.sh --no-cleanup

# Clean up resources only (no tests)
./run_chaos_tests.sh --cleanup
```

## Prerequisites

- **Docker** with Docker Compose v2 (`docker compose` CLI plugin)
- **Go** 1.24+
- Available ports: 18080-18083 (Woodpecker), 2379 (etcd), 9000 (MinIO),
  9090 (Prometheus, monitor only), 3000 (Grafana, monitor only)

## Architecture

### Framework Package

`tests/docker/framework/` provides the shared `DockerCluster` struct with all
common cluster operations (Up, Down, StopNode, StartNode, WaitForHealthy,
NewClient, etc.). Test suites embed this struct and add suite-specific methods:

```go
// Chaos suite adds KillNode, PauseNode, DisconnectNetwork, etc.
type ChaosCluster struct {
    *framework.DockerCluster
}

// Monitor suite adds WaitForPrometheusReady, QueryMetric, etc.
type MonitorCluster struct {
    *framework.DockerCluster
}
```

### Cluster Configuration

Each suite passes a `framework.ClusterConfig` to customize the base cluster:

| Field           | Chaos                         | Monitor                       |
|-----------------|-------------------------------|-------------------------------|
| ProjectName     | woodpecker-chaos              | woodpecker-monitor            |
| OverrideFile    | docker-compose.chaos.yaml     | docker-compose.monitor.yaml   |
| ExtraContainers | (none)                        | prometheus, grafana           |
| GossipWait      | 10s                           | 15s                           |

### Shell Scripts

`common.sh` provides shared functions (`parse_args`, `compose_cmd`,
`wait_for_container`, `build_image_if_needed`, etc.). Each runner script
sources `common.sh` and adds suite-specific setup (e.g., Prometheus readiness
check for monitor tests).

## Grafana Dashboard

The monitor suite includes an auto-setup tool that creates a Woodpecker
dashboard in Grafana with 31 panels organized by architecture layer:

| Row | Layer | Panels |
|-----|-------|--------|
| 1 | Client Layer | Append rate, latency, bytes; Read rate; Reader/Writer bytes; Etcd ops |
| 2 | Transport — gRPC | Request rate, error rate, latency P50/P99 |
| 3 | Engine — LogStore | Active logs/segments, instances, operations, latency, processors |
| 4 | Storage — File I/O | Buffer wait, file ops, flush/read/compaction latency, bytes rate |
| 5 | Object Storage | Operations, latency, request size, bytes transferred |
| 6 | Infrastructure | CPU, memory, disk, IO wait |

The dashboard is set up automatically when monitor tests run. You can also
set it up manually against an already-running cluster:

```bash
# Default (localhost:3000 Grafana, prometheus:9090 inside Docker)
cd tests/docker/monitor
go run ./grafana/cmd/

# Custom URLs
go run ./grafana/cmd/ --grafana-url http://localhost:3000 --prometheus-url http://prometheus:9090
```

After setup, open: http://localhost:3000/d/woodpecker-overview/woodpecker

## Adding a New Test Suite

1. Create `tests/docker/<suite>/`
2. Create a `docker-compose.<suite>.yaml` override file
3. Create a Go struct that embeds `framework.DockerCluster`:
   ```go
   type MyCluster struct {
       *framework.DockerCluster
   }

   func NewMyCluster(t *testing.T) *MyCluster {
       base := framework.NewDockerCluster(t, framework.ClusterConfig{
           ProjectName:  "woodpecker-mysuite",
           OverrideFile: "docker-compose.mysuite.yaml",
           NetworkName:  "woodpecker-mysuite_woodpecker",
       })
       return &MyCluster{DockerCluster: base}
   }
   ```
4. Create a `run_<suite>_tests.sh` that sources `../common.sh`
5. Use `framework.WriteEntries` and `framework.ReadAllEntries` for common
   read/write operations
