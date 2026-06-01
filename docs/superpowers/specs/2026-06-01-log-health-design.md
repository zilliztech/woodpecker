# Log Health — Lightweight Per-Log R/W Health Check

- **Issue:** [zilliztech/woodpecker#131](https://github.com/zilliztech/woodpecker/issues/131)
- **Date:** 2026-06-01
- **Status:** Approved design, ready for implementation plan

## Problem

The existing `/healthz` probe only confirms the **process** is up and its role is
registered. It does **not** verify that logs can actually be written to and read
from the underlying storage. A node can pass `/healthz` while every write to
storage hangs or fails.

We want a second, **orthogonal** probe that answers: *"can this node still read and
write its logs?"* — derived purely from the read/write activity the node already
performs, with no injected probe traffic and no measurable cost on the hot path.

## Goals

- Per-log read/write health, observed from **real traffic** (no heartbeat/probe
  messages injected — explicitly per the issue).
- A single per-node HTTP endpoint that reports each tracked log's state plus a
  node-level rollup.
- Hot-path cost ≈ a shared `RLock` + a map lookup + one `atomic.Store`. No
  allocation, no exclusive lock, no per-write bookkeeping.

## Non-Goals

- **No change to `/healthz`.** `log-health` does not feed into, gate, or modify the
  existing liveness probe. A stalled log must never make `/healthz` fail —
  otherwise a data-path issue would get escalated into a process restart by k8s.
  The two probes stay fully independent (different package, route, types, data
  source).
- **No active/injected probe** (unlike Pulsar's topic `healthcheck`, which produces
  and consumes a probe message). We only observe traffic that already happens.
- **No idle storage fallback** (no `HeadBucket`). An idle log with no observed
  activity reports `Healthy` — see Idle handling below. Active probing is a
  possible future feature, out of scope here.
- **No cluster-wide aggregation.** Each node answers only for itself; any caller
  (monitor / load balancer / operator) aggregates across nodes. The previously
  drafted `GetClusterRWHealth` / peer HTTP fan-out / memberlist parsing / quorum
  logic is removed.

## Relationship to `/healthz`

| | `/healthz` (existing) | `/admin/log-health` (new) |
|---|---|---|
| Package | `common/http/health/` | `common/http/management/` |
| Meaning | Process liveness — process up, role registered | Data-path health — real R/W path works, per log |
| Data source | role indicators | `LogHealthTracker` atomic timestamps |
| Response type | `health.HealthResponse` | `LogHealthResponse` |

They are two orthogonal layers of liveness and share no code.

## Naming

Renamed from the earlier `rw_health` draft to `log_health` — the granularity is "per
log", matching the granularity (not the mechanism) of Pulsar's per-topic health
check.

| Earlier draft | This design |
|---|---|
| `server/rw_health.go` | `server/log_health.go` |
| `common/http/management/rw_health_handler.go` | `common/http/management/log_health_handler.go` |
| `RWHealthTracker` | `LogHealthTracker` |
| `ServerRWHealthResponse` | `LogHealthResponse` |
| `RWLogHealthSnapshot` | `LogHealthSnapshot` |
| `RWHealthStateHealthy` / `…Unhealthy` | `LogHealthStateHealthy` / `…Unhealthy` |
| route `/admin/server-rw-health` | `/admin/log-health` |
| `ClusterRWHealthResponse`, `GetClusterRWHealth`, `fetchPeerRWHealth`, `rwHealthPeer`, quorum aggregation | **deleted** |

## Architecture

### Components

1. **`LogHealthTracker`** (`server/log_health.go`) — owns the in-memory state, the
   record methods called from the hot path, and snapshot/classification logic.
2. **Hot-path instrumentation** — calls into the tracker from `Server.AddEntry`
   (write) and `Server.GetBatchEntriesAdv` (read).
3. **HTTP handler** (`common/http/management/log_health_handler.go`) — parses the
   optional `bucket_name` / `root_path` filter and serializes the response.
4. **Wiring** — `Server.GetLogHealth` callback, registered at route
   `/admin/log-health` in `common/http/server.go`, exposed from `cmd/main.go`.

### Data model — lock-free hot path

```go
type logInstanceKey struct{ bucketName, rootPath string }

type logHealth struct {
    writeAttemptMS atomic.Int64 // set when AddEntry starts (writes can hang in-flight)
    writeSuccessMS atomic.Int64
    writeFailureMS atomic.Int64
    readSuccessMS  atomic.Int64 // reads are synchronous — no attempt timestamp
    readFailureMS  atomic.Int64
    failureReason  atomic.Pointer[string] // last failure reason, optional/diagnostic
}

type LogHealthTracker struct {
    mu         sync.RWMutex // guards map STRUCTURE only
    stallAfter time.Duration
    logs       map[logInstanceKey]map[int64]*logHealth
}
```

**Hot path:** `RLock` (shared) → map lookup → `atomic.Store` on the relevant field →
`RUnlock`. The first write/read ever seen for a `(bucket, rootPath, logID)` takes the
write lock once to create the `*logHealth`; every subsequent record is shared-lock +
atomic only. No allocation per write, no exclusive lock per write, no per-write ID.

### Record methods

- `RecordWriteAttempt(bucket, rootPath, logID, now)` — store `writeAttemptMS`.
- `RecordWriteSuccess(bucket, rootPath, logID, now)` — store `writeSuccessMS`.
- `RecordWriteFailure(bucket, rootPath, logID, now, reason)` — store `writeFailureMS`
  + `failureReason`.
- `RecordReadSuccess(bucket, rootPath, logID, now)` — store `readSuccessMS`.
- `RecordReadFailure(bucket, rootPath, logID, now, reason)` — store `readFailureMS`
  + `failureReason`.

All methods are no-ops when the tracker is `nil`.

### Instrumentation points

- **Write — `Server.AddEntry`:**
  - `RecordWriteAttempt` at entry start.
  - `RecordWriteSuccess` when the entry is synced.
  - `RecordWriteFailure` on buffer error / result error.
  - **Context cancel / deadline exceeded → no-op** (client gave up; not a storage
    fault). Simpler than the old "abandoned" path because there is no map entry to
    clean up.
- **Read — `Server.GetBatchEntriesAdv`:**
  - `RecordReadFailure` if the call returns `err`, else `RecordReadSuccess`.
  - Reads are synchronous, so they have no attempt timestamp and never reach the
    `Stalled` state — only `Healthy` / `Failed` / `Idle`.

### Per-log classification (snapshot time)

`stallAfter` defaults to 10 minutes. Each direction (read, write) is first classified
independently from its timestamps into a raw direction-state, exposed as
`write_state` / `read_state` for transparency. The log's single overall `state` is
then derived from both directions (see "Log overall state" below).

For a direction with `lastSuccess`, `lastFailure`, and (write only) `lastAttempt`:

| Direction state | Rule |
|---|---|
| **Healthy** | `now - lastSuccess ≤ stallAfter` (succeeded recently) |
| **Stalled** (write only) | `lastSuccess > 0` **AND** `now - lastSuccess > stallAfter` **AND** `lastAttempt > lastSuccess` **AND** `lastAttempt > lastFailure` — was healthy, then work keeps flowing in but nothing completes (catches hung writes even under high QPS, because the rule keys off the age of the last *success*, not the last attempt). A log that has *never* succeeded is never Stalled — it is Failed (recent failure) or Idle. |
| **Failed** | `lastFailure > lastSuccess` **AND** `now - lastFailure ≤ stallAfter` (recent terminal failures) |
| **Idle** | no observed activity, or success & failure both stale (older than `stallAfter`) and not Stalled. Idle is informational — the last terminal outcome is still visible via the `last_*_ms` timestamps in the response, so an operator can read "last known status" directly. |

### Log overall state

A single enum per log, computed from the two direction-states with worst-wins
precedence `Stalled > Failed > Healthy > Idle`:

- `Stalled` if the write direction is Stalled.
- else `Failed` if either direction is Failed.
- else `Healthy` if either direction is Healthy.
- else `Idle` (no recent activity in either direction).

The four counters (`healthy_logs` / `failed_logs` / `stalled_logs` / `idle_logs`) are
mutually exclusive and partition `tracked_logs` by this overall state.

### Node-level rollup

The rollup spans **all logs in scope** (all tenants when unfiltered, or the one
filtered instance). It deliberately keys off **active evidence**, so a merely quiet
node is never pulled out of rotation:

- Node `state = Unhealthy` **iff** `tracked_logs > 0` **AND** every tracked log is
  `Stalled` or `Failed` (zero Healthy, zero Idle — there is current, active evidence
  the data path is broken across all logs).
- Otherwise `state = Healthy`: ≥ 1 Healthy log, **or** all/some logs Idle (a quiet
  node is healthy), **or** no tracked logs at all (reason `no_observed_activity`).

This matches the issue's "at least one successfully reading/writing log = Healthy;
multiple logs stalled simultaneously = Unhealthy", and resolves the idle case
conservatively (idle alone never forces Unhealthy).

### HTTP endpoint

**Node-level scope.** Woodpecker is multi-tenant: a node serves many
`(bucket_name, root_path)` instances at once, and a log is uniquely identified by the
triple `(bucket_name, root_path, log_id)` (there is no `logName` at the logstore
layer). So one probe call returns **every** log the node tracks, across all tenants,
and each log carries its own triple.

`GET /admin/log-health[?bucket_name=<bucket>&root_path=<root>]`

- `bucket_name` / `root_path` are **optional filters** (accepts snake/camel/lower
  variants). When both are supplied, the response is narrowed to that one instance;
  when omitted, all tracked logs across all tenants are returned. Supplying only one
  of the two is treated as "no filter" (or 400 — see open detail below); default is
  to ignore a lone param and return everything.
- 405 on non-GET.
- Body `LogHealthResponse`:

```jsonc
{
  "state": "Healthy",              // node rollup: Healthy | Unhealthy
  "reason": "observed_write_success",
  "node_id": "...",
  "filter_bucket_name": "",        // echoes the filter when one was applied, else ""
  "filter_root_path": "",
  "timestamp_ms": 0,
  "stall_after_ms": 600000,
  "tracked_logs": 2,
  "healthy_logs": 1,
  "failed_logs": 0,
  "stalled_logs": 0,
  "idle_logs": 1,
  "logs": [
    {
      "bucket_name": "tenant-a-bucket",   // the unique triple, per log
      "root_path": "tenant-a/root",
      "log_id": 1,
      "state": "Healthy",          // overall, see "Log overall state"
      "write_state": "Healthy",
      "read_state": "Idle",
      "last_write_success_ms": 0,
      "last_write_failure_ms": 0,
      "last_read_success_ms": 0,
      "last_read_failure_ms": 0,
      "last_failure_reason": ""
    }
  ]
}
```

- HTTP status: `200` when node `state == Healthy`, `503` when `Unhealthy` (so a load
  balancer / probe can act on status code alone). Logs are sorted by
  `(bucket_name, root_path, log_id)` for stable output.

### Wiring

- `Server` gains `logHealthTracker *LogHealthTracker`, constructed in
  `NewServerWithConfig` with `NewLogHealthTracker(defaultLogHealthStallAfter)`.
- `Server.GetLogHealth(ctx, bucketFilter, rootPathFilter) (LogHealthResponse, int)`
  builds the snapshot and returns the response + HTTP status. Empty filter strings
  mean "all tenants"; when both are non-empty the snapshot is narrowed to that
  instance.
- `LogHealthCallback func(ctx, bucketName, rootPath) (any, int)` — the handler passes
  the parsed filter (empty strings when absent); signature unchanged from the draft.
- `AdminCallbacks` gains one field `GetLogHealth management.LogHealthCallback`; the
  cluster callback is removed.
- `common/http/server.go` registers the route when the callback is set.
- `cmd/main.go` wires `srv.GetLogHealth`.
- `common/http/README.md` documents the new route alongside the other admin routes.

## Error handling

- `nil` tracker → all record methods no-op; `GetLogHealth` falls back to an empty
  tracker and reports `Healthy / no_observed_activity`.
- Missing filter params → return all tenants (not an error). Non-GET → `405`.
- Client-cancelled writes are deliberately not counted as failures.

## Testing

- **`server/log_health_test.go`** (table-driven, using a fixed injected `now`):
  - Each direction-state rule: Healthy, Stalled (incl. high-QPS hung-write case where
    `lastAttempt` is recent but `lastSuccess` is stale), Failed, Idle.
  - Read direction never goes Stalled.
  - Log overall state precedence `Stalled > Failed > Healthy > Idle`, including a
    case where write=Healthy and read=Failed → overall Failed.
  - Counters partition `tracked_logs` (mutually exclusive).
  - Node rollup: ≥1 healthy → Healthy; some idle + no failures → Healthy (quiet node
    not pulled out); all stalled/failed → Unhealthy; no logs → Healthy +
    `no_observed_activity`.
  - `nil` tracker safety.
  - Concurrency smoke test: many goroutines recording on the same/different logs
    under `-race` to confirm the RLock + atomic model is data-race free.
- **`server/log_health_test.go`** also: unfiltered snapshot returns logs across
  multiple `(bucket, rootPath)` instances, each tagged with its own triple; filtered
  snapshot narrows to one instance.
- **`common/http/management/log_health_handler_test.go`:** no-filter returns all,
  filter narrows, 405 non-GET, 200 vs 503 by node state, JSON shape (per-log triple
  present).
- **`server/service_test.go`:** adjust existing tests to the renamed
  symbols/endpoint; drop cluster-health expectations.

## Migration / cleanup

The earlier `rw_health` draft (working-tree changes, not yet committed) is
superseded: rename the two files, rename the surviving symbols, and delete the entire
cluster/quorum/peer/`PendingWrites`-map code path.
