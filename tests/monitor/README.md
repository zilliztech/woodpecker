# Woodpecker Monitoring E2E Test Suite

This test suite verifies that all Woodpecker Prometheus metrics are correctly reported after read/write operations. It deploys a full 4-node Woodpecker cluster with Prometheus and Grafana, performs data operations, and then queries Prometheus to validate metric reporting.

## Quick Start

Run all monitoring tests with a single command:

```bash
cd tests/monitor
./run_monitor_tests.sh
```

This will automatically:
1. Build the Docker image (if not already built)
2. Start a 4-node Woodpecker cluster with Prometheus and Grafana
3. Wait for all services to be ready
4. Run all monitoring test cases
5. Collect container logs on failure
6. Clean up the cluster

### Run a Specific Test

```bash
./run_monitor_tests.sh -run TestMonitor_MetricsVerification
```

### Keep Cluster Running After Tests

```bash
./run_monitor_tests.sh --no-cleanup
```

Then access:
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000

## Manual Execution

```bash
# Build image
cd /path/to/woodpecker
./build/build_image.sh ubuntu22.04 auto -t woodpecker:latest

# Start cluster with monitor override
cd deployments
docker compose -f docker-compose.yaml -f ../tests/monitor/docker-compose.monitor.yaml -p woodpecker-monitor up -d

# Wait for cluster to be ready (~15s)
sleep 15

# Run tests
cd ../tests/monitor
go test -v -timeout 600s -run 'TestMonitor' ./... -count=1

# Clean up
cd ../../deployments
docker compose -f docker-compose.yaml -f ../tests/monitor/docker-compose.monitor.yaml -p woodpecker-monitor down -v
```

## Prerequisites

- Docker with Compose v2 plugin (`docker compose`)
- Go 1.24.2+
- Sufficient system resources (4 Woodpecker nodes + etcd + MinIO + Jaeger + Prometheus + Grafana)
- Available ports:
  - 18080-18083 (Woodpecker service)
  - 17946-17949 (Woodpecker gossip)
  - 9091-9094 (Woodpecker metrics)
  - 2379 (etcd)
  - 9000-9001 (MinIO)
  - 9090 (Prometheus)
  - 3000 (Grafana)
  - 16686 (Jaeger)

## Architecture

### File Structure

```
tests/monitor/
├── README.md                       # This file
├── prometheus.yml                  # Prometheus scrape configuration
├── docker-compose.monitor.yaml     # Compose override (adds Prometheus + Grafana)
├── docker_monitor.go               # MonitorCluster helper struct
├── monitor_test.go                 # E2E metric verification tests
└── run_monitor_tests.sh            # One-click test runner
```

### Design Decisions

- **Compose override file**: `docker-compose.monitor.yaml` adds Prometheus and Grafana services and sets `restart: "no"` for all containers without modifying the original compose file.
- **Separate project name**: Uses `woodpecker-monitor` project name to avoid interfering with development or chaos test clusters.
- **Client vs Server metrics**: Server metrics (woodpecker_server_*, grpc_server_*) have real non-zero values in the server containers. Client metrics (woodpecker_client_*) are registered but have zero values because client operations run in the Go test process, not in the server containers. Tests verify server metrics have values and client metrics are registered.
- **Prometheus scrape interval**: 5s scrape interval with 15s wait after operations ensures metrics are captured before verification.

## Metrics Reference

### Client Append Module

| Metric Name | Type | Labels | Description | Key Design Metric |
|---|---|---|---|---|
| `woodpecker_client_append_requests_total` | Counter | log_id | Total append requests | Write QPS |
| `woodpecker_client_append_entries_total` | Counter | log_id | Total entries appended | Write throughput (entries) |
| `woodpecker_client_append_bytes` | Histogram | log_id | Append bytes distribution | Message size distribution P50/P99 |
| `woodpecker_client_append_latency` | Histogram | log_id | Append latency | Write latency P50/P99 |

### Client Read Module

| Metric Name | Type | Labels | Description | Key Design Metric |
|---|---|---|---|---|
| `woodpecker_client_read_requests_total` | Counter | log_id | Total read requests | Read QPS |
| `woodpecker_client_read_entries_total` | Counter | log_id | Total entries read | Read throughput (entries) |
| `woodpecker_client_reader_bytes_read` | Counter | log_id, reader_name | Bytes read | Read throughput (bytes) |
| `woodpecker_client_reader_operation_latency` | Histogram | log_id, operation, status | Reader operation latency | Read latency P50/P99 |

### LogHandle / SegmentHandle Module

| Metric Name | Type | Labels | Description | Key Design Metric |
|---|---|---|---|---|
| `woodpecker_client_log_handle_operations_total` | Counter | log_id, operation, status | LogHandle operation count | Metadata operation success rate |
| `woodpecker_client_segment_handle_operations_total` | Counter | log_id, operation, status | Segment operation count | Segment rolling/close frequency |
| `woodpecker_client_segment_handle_pending_append_ops` | Gauge | log_id | Pending append operations | Write backpressure observation |
| `woodpecker_client_writer_bytes_written` | Counter | log_id | Writer bytes written | Per-writer throughput |

### Client Etcd Meta Module

| Metric Name | Type | Labels | Description | Key Design Metric |
|---|---|---|---|---|
| `woodpecker_client_etcd_meta_operations_total` | Counter | operation, status | Etcd metadata operations | Metadata operation success/error rate |
| `woodpecker_client_etcd_meta_operation_latency` | Histogram | operation, status | Etcd operation latency | Metadata latency P99 |

### Server LogStore Module

| Metric Name | Type | Labels | Description | Key Design Metric |
|---|---|---|---|---|
| `woodpecker_server_logstore_active_logs` | Gauge | — | Active log count | Resource usage observation |
| `woodpecker_server_logstore_active_segments` | Gauge | — | Active segment count | Segment lifecycle monitoring |
| `woodpecker_server_logstore_instances_total` | Gauge | node | Node instance count | Cluster topology awareness |
| `woodpecker_server_logstore_operations_total` | Counter | log_id, operation, status | LogStore operation count | Server-side operation success rate |
| `woodpecker_server_logstore_active_segment_processors` | Gauge | log_id | Active processor count | Processor resource observation |

### Server File/Buffer Module

| Metric Name | Type | Labels | Description | Key Design Metric |
|---|---|---|---|---|
| `woodpecker_server_buffer_wait_latency` | Histogram | log_id | Buffer wait latency | Write path queuing latency |
| `woodpecker_server_file_operations_total` | Counter | log_id, operation, status | File operation count | File I/O frequency |
| `woodpecker_server_file_writer` | Gauge | log_id | Active writer count | Writer resource usage |
| `woodpecker_server_file_reader` | Gauge | log_id | Active reader count | Reader resource usage |
| `woodpecker_server_file_flush_latency` | Histogram | log_id | Flush latency | Persistence latency P99 |
| `woodpecker_server_flush_bytes_written` | Counter | log_id | Flush bytes written | Persistence throughput |
| `woodpecker_server_read_batch_latency` | Histogram | log_id | Batch read latency | Server-side read latency |
| `woodpecker_server_read_batch_bytes_total` | Counter | log_id | Batch read bytes | Server-side read throughput |
| `woodpecker_server_file_compaction_latency` | Histogram | log_id | Compaction latency | Compaction performance |
| `woodpecker_server_compact_bytes_written` | Counter | log_id | Compaction bytes written | Compaction throughput |

### Server Object Storage Module

| Metric Name | Type | Labels | Description | Key Design Metric |
|---|---|---|---|---|
| `woodpecker_server_object_storage_operations_total` | Counter | operation, status | Object storage operation count | S3 call frequency/success rate |
| `woodpecker_server_object_storage_operation_latency` | Histogram | operation, status | Object storage latency | S3 latency P50/P99 |
| `woodpecker_server_object_storage_request_bytes` | Histogram | operation | Per-request size | Request size distribution |
| `woodpecker_server_object_storage_bytes_transferred` | Counter | operation | Total bytes transferred | S3 bandwidth usage |

### System Resource Module

| Metric Name | Type | Labels | Description | Key Design Metric |
|---|---|---|---|---|
| `woodpecker_server_system_cpu_usage` | Gauge | — | CPU usage percentage | Node load |
| `woodpecker_server_system_cpu_num` | Gauge | — | CPU core count | Capacity planning |
| `woodpecker_server_system_memory_total_bytes` | Gauge | — | Total memory | Capacity planning |
| `woodpecker_server_system_memory_used_bytes` | Gauge | — | Used memory | Memory pressure |
| `woodpecker_server_system_memory_usage_ratio` | Gauge | — | Memory usage ratio | Memory alert threshold |
| `woodpecker_server_system_disk_total_bytes` | Gauge | path | Disk total bytes | Storage capacity |
| `woodpecker_server_system_disk_used_bytes` | Gauge | path | Disk used bytes | Storage pressure |
| `woodpecker_server_system_io_wait` | Gauge | — | IO wait percentage | I/O bottleneck detection |

### gRPC Module (go-grpc-prometheus auto-generated)

| Metric Name | Type | Labels | Description | Key Design Metric |
|---|---|---|---|---|
| `grpc_server_started_total` | Counter | grpc_type, grpc_service, grpc_method | RPC started count | RPC QPS |
| `grpc_server_handled_total` | Counter | +grpc_code | RPC completed count | RPC success rate/error code distribution |
| `grpc_server_handling_seconds` | Histogram | grpc_type, grpc_service, grpc_method | RPC handling time | RPC latency P50/P99 |
| `grpc_server_msg_received_total` | Counter | grpc_type, grpc_service, grpc_method | Messages received | Streaming message throughput |
| `grpc_server_msg_sent_total` | Counter | grpc_type, grpc_service, grpc_method | Messages sent | Streaming message throughput |

## Troubleshooting

### Port Conflicts

If you see port binding errors, check for other services using the required ports:

```bash
# Check port usage
lsof -i :9090   # Prometheus
lsof -i :3000   # Grafana
lsof -i :9091   # Woodpecker node1 metrics
```

Stop conflicting services or use `--cleanup` to remove stale containers:

```bash
./run_monitor_tests.sh --cleanup
```

### Manually Accessing Prometheus

With `--no-cleanup`, you can query metrics directly:

```bash
# Check if Prometheus is ready
curl http://localhost:9090/-/ready

# Query a specific metric
curl 'http://localhost:9090/api/v1/query?query=woodpecker_server_logstore_operations_total'

# Check scrape targets
curl http://localhost:9090/api/v1/targets
```

### Manually Accessing Grafana

With `--no-cleanup`, open http://localhost:3000 in your browser. Anonymous access is enabled with Admin role. Add Prometheus as a data source with URL `http://prometheus:9090`.

### Viewing Container Logs

```bash
# View specific container logs
docker logs prometheus
docker logs woodpecker-node1

# View all logs
docker compose -f deployments/docker-compose.yaml \
  -f tests/monitor/docker-compose.monitor.yaml \
  -p woodpecker-monitor logs
```

### Tests Fail Mid-Execution

- Check `tests/monitor/logs/` for container logs (collected automatically on failure)
- Run with `--no-cleanup` to inspect the cluster state after failure
- Verify Prometheus targets are up: http://localhost:9090/targets
