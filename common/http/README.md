# Woodpecker Admin HTTP API

Woodpecker exposes an HTTP admin server on each node (default port `9091`, configurable via `METRICS_PORT` env). This server provides health checks, metrics, profiling, and node lifecycle management endpoints.

## Endpoints Overview

| Method | Path | Category | Description |
|--------|------|----------|-------------|
| GET | `/healthz` | Health | Health check (readiness probe) |
| GET | `/metrics` | Monitoring | Prometheus metrics |
| GET | `/log/level` | Ops | Query/update log level at runtime |
| GET | `/admin/memberlist` | Cluster | Gossip memberlist status |
| GET | `/admin/node/status` | Lifecycle | Node status and membership info |
| GET | `/admin/log-health` | Health | Node-wide per-log read/write health (optionally filtered by bucket/rootPath) |
| POST | `/admin/node/decommission` | Lifecycle | Start graceful node decommission |
| GET | `/admin/node/decommission/progress` | Lifecycle | Decommission progress and safe-to-terminate check |
| GET | `/debug/pprof/` | Debug | Pprof index page (enabled by default, disable via `PPROF_ENABLE=false`) |
| GET | `/debug/pprof/profile` | Debug | CPU profile (`?seconds=N`) |
| GET | `/debug/pprof/heap` | Debug | Heap memory profile |
| GET | `/debug/pprof/goroutine` | Debug | Goroutine stack dump (`?debug=1` or `?debug=2`) |
| GET | `/debug/pprof/allocs` | Debug | Memory allocation profile |
| GET | `/debug/pprof/block` | Debug | Blocking profile |
| GET | `/debug/pprof/mutex` | Debug | Mutex contention profile |
| GET | `/debug/pprof/threadcreate` | Debug | Thread creation profile |
| GET | `/debug/pprof/trace` | Debug | Execution trace (`?seconds=N`) |
| GET | `/debug/pprof/cmdline` | Debug | Process command line |
| GET | `/debug/pprof/symbol` | Debug | Symbol lookup |

---

## Health Check

```bash
# Plain text response
curl http://localhost:9091/healthz
```

**Response (plain text, healthy):**
```
OK
```

**Response (plain text, unhealthy):**
```
Not all components are healthy, 1/2
```

```bash
# JSON response with component details
curl -H "Content-Type: application/json" http://localhost:9091/healthz
```

**Response (JSON, healthy):**
```json
{
  "state": "OK",
  "detail": [
    {"name": "LogStore", "code": "Healthy"}
  ]
}
```

**Response (JSON, unhealthy):**
```json
{
  "state": "Not all components are healthy, 1/2",
  "detail": [
    {"name": "LogStore", "code": "Healthy"},
    {"name": "Membership", "code": "Abnormal"}
  ]
}
```

**HTTP Status:** `200` if all healthy, `500` if any component is unhealthy.

**Component health codes:** `Initializing` | `Healthy` | `Abnormal` | `StandBy` | `Stopping`

---

## Log Health

`/admin/log-health` reports the local node's observed per-log read/write health, derived
from real operation results (no synthetic heartbeat logs are created).

```bash
# All logs observed on this node
curl "http://localhost:9091/admin/log-health"

# Filter to a single Woodpecker instance (both params required, otherwise ignored)
curl "http://localhost:9091/admin/log-health?bucket_name=a-bucket&root_path=files"
```

- Both `bucket_name` and `root_path` must be supplied to filter; a partial filter is ignored
  and all tenants are returned.
- Write health is the logstore "accept" outcome (`logstore.add_entry`); read health is
  `logstore.get_batch_entries`. Each log is classified Healthy / Stalled / Failed / Idle.
  Reaching the end of a log (EOF / entry-not-found) is a healthy read, not a failure.
- Independent of `/healthz`: `/healthz` is process liveness, while `/admin/log-health` is
  data-path health — a stalled or failed log never affects the `/healthz` result.
- Cold start, idle, or no observed log activity returns `Healthy` (a quiet node is not
  pulled out of rotation).
- **HTTP Status:** `503` only when every tracked log is Stalled or Failed; `200` otherwise.

---

## Prometheus Metrics

```bash
curl http://localhost:9091/metrics
```

**Response (Prometheus text format, excerpt):**
```
# HELP woodpecker_server_logstore_active_logs Number of active logs in the log store
# TYPE woodpecker_server_logstore_active_logs gauge
woodpecker_server_logstore_active_logs{namespace="mybucket/myroot",node_id="woodpecker-0"} 3

# HELP woodpecker_server_logstore_active_segments Number of active segments in the log store
# TYPE woodpecker_server_logstore_active_segments gauge
woodpecker_server_logstore_active_segments{log_id="1",namespace="mybucket/myroot",node_id="woodpecker-0"} 2

# HELP woodpecker_server_logstore_operations_total Total number of log store operations
# TYPE woodpecker_server_logstore_operations_total counter
woodpecker_server_logstore_operations_total{log_id="1",namespace="mybucket/myroot",node_id="woodpecker-0",operation="add_entry",status="success"} 1024

# HELP woodpecker_server_logstore_operation_latency Latency of log store operations
# TYPE woodpecker_server_logstore_operation_latency histogram
woodpecker_server_logstore_operation_latency_bucket{log_id="1",namespace="mybucket/myroot",node_id="woodpecker-0",operation="add_entry",status="success",le="1"} 500
...
```

**Key metric families:**

| Metric | Type | Description |
|--------|------|-------------|
| `woodpecker_server_logstore_active_logs` | Gauge | Number of active logs |
| `woodpecker_server_logstore_active_segments` | Gauge | Number of active segments |
| `woodpecker_server_logstore_active_segment_processors` | Gauge | Number of active segment processors |
| `woodpecker_server_logstore_instances_total` | Gauge | Total LogStore instances |
| `woodpecker_server_logstore_operations_total` | Counter | Total operations (by operation, status) |
| `woodpecker_server_logstore_operation_latency` | Histogram | Operation latency in ms |
| `woodpecker_server_buffer_wait_latency` | Histogram | Time entries wait in buffer (ms) |
| `woodpecker_server_file_operations_total` | Counter | File I/O operations |
| `woodpecker_server_file_operation_latency` | Histogram | File I/O latency in ms |
| `woodpecker_server_file_flush_latency` | Histogram | Flush latency in ms |
| `woodpecker_server_file_compaction_latency` | Histogram | Compaction latency in ms |
| `woodpecker_server_object_storage_operations_total` | Counter | Object storage operations |
| `woodpecker_server_object_storage_operation_latency` | Histogram | Object storage latency in ms |
| `woodpecker_server_object_storage_bytes_transferred` | Counter | Bytes transferred to/from object storage |
| `woodpecker_server_object_storage_stored_bytes` | Gauge | Current bytes stored in object storage |
| `woodpecker_server_system_cpu_usage` | Gauge | System CPU usage ratio |
| `woodpecker_server_system_memory_used_bytes` | Gauge | System memory used |
| `woodpecker_server_system_disk_used_bytes` | Gauge | Disk usage on data directory |
| `woodpecker_server_system_io_wait` | Gauge | I/O wait ratio |

**Prometheus scrape config example:**
```yaml
scrape_configs:
  - job_name: 'woodpecker'
    static_configs:
      - targets: ['woodpecker-0:9091', 'woodpecker-1:9091', 'woodpecker-2:9091']
```

---

## Log Level

Query or update the runtime log level (TODO: runtime change not yet implemented).

```bash
curl http://localhost:9091/log/level
```

**Response:**
```
Log level change endpoint - TODO
```

**HTTP Status:** `200`

> **Note:** This endpoint is a placeholder. Runtime log level adjustment is planned for a future release.

---

## Cluster Memberlist

```bash
curl http://localhost:9091/admin/memberlist
```

**Response (plain text):**
```
Total Members: 4

[1] Name: woodpecker-0
    Addr: 172.18.0.2:17946
    State: 0

[2] Name: woodpecker-1
    Addr: 172.18.0.3:17946
    State: 0
```

---

## Node Lifecycle APIs

These endpoints enable external orchestration systems (e.g., K8s operators, cluster autoscalers) to manage node lifecycle events such as rolling upgrades, scale-down, and graceful decommissioning.

Each endpoint targets the **local node** receiving the request. The external orchestrator should call each pod's admin port directly.

### GET /admin/node/status

Query the current node's status, membership info, and metadata.

```bash
curl http://localhost:9091/admin/node/status
```

**Response:**
```json
{
  "node_id": "woodpecker-0",
  "state": "active",
  "is_decommissioning": false,
  "member_count": 4,
  "address": "172.18.0.2:18080",
  "resource_group": "default",
  "az": "az-1",
  "tags": {
    "role": "logstore"
  }
}
```

**Fields:**
- `state`: `"active"` | `"decommissioning"` | `"decommissioned"`
- `is_decommissioning`: convenience boolean for quick checks
- `member_count`: number of nodes visible in the gossip cluster
- `address`: gRPC service address for client connections

**Use case:** Post-upgrade verification, scale-up confirmation, monitoring dashboards.

### POST /admin/node/decommission

Mark the node for retirement. The node will stop accepting new write operations while allowing existing tasks (segment processors) to drain.

```bash
curl -X POST http://localhost:9091/admin/node/decommission
```

**Response (success):**
```json
{"status": "decommission started"}
```

**Response (conflict — already decommissioned):**
```json
{"error": "node already decommissioned"}
```

**HTTP Status:** `200` on success, `409` on conflict.

**Behavior:**
- Idempotent: calling multiple times while decommissioning is safe (returns `200`)
- Immediately rejects new `AddEntry` (write) gRPC requests with `ErrLogStoreShutdown`
- Existing reads, segment completions, compactions, and other operations continue normally

### GET /admin/node/decommission/progress

Track remaining tasks on a decommissioning node. Poll this endpoint to determine when it is safe to terminate the pod.

```bash
curl http://localhost:9091/admin/node/decommission/progress
```

**Response (processors active):**
```json
{
  "state": "decommissioning",
  "remaining_processors": 5,
  "has_local_data": true,
  "safe_to_terminate": false
}
```

**Response (processors drained, waiting for data cleanup):**
```json
{
  "state": "decommissioning",
  "remaining_processors": 0,
  "has_local_data": true,
  "safe_to_terminate": false
}
```

**Response (ready to terminate):**
```json
{
  "state": "decommissioned",
  "remaining_processors": 0,
  "has_local_data": false,
  "safe_to_terminate": true
}
```

**Fields:**
- `remaining_processors`: number of active segment processors still running (informational, does not affect `safe_to_terminate`)
- `has_local_data`: whether there are segment data files (`data.log`) remaining on local disk
- `safe_to_terminate`: `true` when `has_local_data == false` — the pod can be killed safely. This is the sole criterion; processor count is not a factor because processors are in-memory objects that disappear on shutdown

---

## K8s Integration Examples

### PreStop Hook (Graceful Drain)

```yaml
lifecycle:
  preStop:
    exec:
      command:
        - /bin/sh
        - -c
        - |
          # Signal the node to stop accepting new writes
          curl -X POST http://localhost:9091/admin/node/decommission
          # Wait until all tasks have drained
          while true; do
            SAFE=$(curl -s http://localhost:9091/admin/node/decommission/progress | jq .safe_to_terminate)
            if [ "$SAFE" = "true" ]; then break; fi
            sleep 2
          done
```

### Readiness Probe

```yaml
readinessProbe:
  httpGet:
    path: /healthz
    port: 9091
  initialDelaySeconds: 5
  periodSeconds: 10
```

### Liveness Probe

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 9091
  initialDelaySeconds: 15
  periodSeconds: 20
```

### Rolling Upgrade Verification Script

```bash
#!/bin/bash
# Wait for a node to join the cluster after upgrade
NODE_ADDR="woodpecker-0.woodpecker:9091"
for i in $(seq 1 30); do
  STATE=$(curl -s http://$NODE_ADDR/admin/node/status | jq -r .state)
  MEMBERS=$(curl -s http://$NODE_ADDR/admin/node/status | jq .member_count)
  if [ "$STATE" = "active" ] && [ "$MEMBERS" -ge 3 ]; then
    echo "Node is active with $MEMBERS members"
    exit 0
  fi
  echo "Waiting... state=$STATE members=$MEMBERS (attempt $i/30)"
  sleep 2
done
echo "Node failed to become ready"
exit 1
```

---

## Debug / Profiling (pprof)

Enabled by default. Disable via `PPROF_ENABLE=false` environment variable.

**Available pprof endpoints:**

| Path | Description |
|------|-------------|
| `/debug/pprof/` | Index page listing all profiles |
| `/debug/pprof/profile` | CPU profile (default 30s, configurable via `?seconds=N`) |
| `/debug/pprof/heap` | Heap memory profile |
| `/debug/pprof/goroutine` | Goroutine stack dump |
| `/debug/pprof/allocs` | Memory allocation profile |
| `/debug/pprof/block` | Blocking profile |
| `/debug/pprof/mutex` | Mutex contention profile |
| `/debug/pprof/threadcreate` | Thread creation profile |
| `/debug/pprof/cmdline` | Process command line |
| `/debug/pprof/symbol` | Symbol lookup |
| `/debug/pprof/trace` | Execution trace (configurable via `?seconds=N`) |

**Usage examples:**

```bash
# Browse pprof index page
curl http://localhost:9091/debug/pprof/

# CPU profile — collect 30 seconds (binary protobuf output, save to file)
curl -o cpu.prof http://localhost:9091/debug/pprof/profile?seconds=30

# Heap profile — current heap snapshot
curl -o heap.prof http://localhost:9091/debug/pprof/heap

# Goroutine dump — human-readable stack traces
curl http://localhost:9091/debug/pprof/goroutine?debug=2

# Goroutine dump — compact summary (one line per goroutine)
curl http://localhost:9091/debug/pprof/goroutine?debug=1

# Memory allocation profile — all allocations since process start
curl -o allocs.prof http://localhost:9091/debug/pprof/allocs

# Block profile — goroutine blocking events
curl -o block.prof http://localhost:9091/debug/pprof/block

# Mutex contention profile
curl -o mutex.prof http://localhost:9091/debug/pprof/mutex

# Execution trace — collect 5 seconds
curl -o trace.out http://localhost:9091/debug/pprof/trace?seconds=5

# Interactive analysis with go tool pprof
go tool pprof http://localhost:9091/debug/pprof/heap
go tool pprof http://localhost:9091/debug/pprof/profile?seconds=30

# Analyze execution trace
go tool trace trace.out
```
