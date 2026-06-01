# Log Health — Lightweight Per-Log R/W Health Check

- **Issue:** [zilliztech/woodpecker#131](https://github.com/zilliztech/woodpecker/issues/131)
- **Date:** 2026-06-01
- **Status:** Approved design, ready for implementation plan

## Problem

The existing `/healthz` probe only confirms the **process** is up and its role is
registered. It does **not** verify that logs can actually be written to and read
from the underlying storage/buffer. A node can pass `/healthz` while every write
hangs or fails.

We want a second, **orthogonal** probe that answers: *"can this node still read and
write its logs?"* — derived purely from the read/write activity the node already
performs, with no injected probe traffic and no measurable cost on the hot path.

## Goals

- Per-log read/write health, observed from **real traffic** (no heartbeat/probe
  messages injected — explicitly per the issue).
- A single per-node HTTP endpoint that reports each tracked log's state plus a
  node-level rollup.
- **Minimal code intrusion:** reuse the existing `metrics.Op` / `OpObserver`
  instrumentation that already wraps every logstore operation. The data-path
  business code (`server/service.go`, the body of `logstore` ops) is **not** touched.
- Hot-path cost ≈ a shared `RLock` + a map lookup + one `atomic.Store` per observed
  op. No allocation, no exclusive lock, no per-op bookkeeping in the tracker.

## Non-Goals

- **No change to `/healthz`.** `log-health` does not feed into, gate, or modify the
  existing liveness probe. A stalled log must never make `/healthz` fail —
  otherwise a data-path issue would get escalated into a process restart by k8s.
  The two probes stay fully independent (different package, route, types, data
  source).
- **No active/injected probe** (unlike Pulsar's topic `healthcheck`, which produces
  and consumes a probe message). We only observe traffic that already happens.
- **No idle storage fallback** (no `HeadBucket`). An idle log with no observed
  activity reports `Healthy` — see Idle handling below. Active probing is a possible
  future feature, out of scope here.
- **No cluster-wide aggregation.** Each node answers only for itself; any caller
  (monitor / load balancer / operator) aggregates across nodes. The previously
  drafted `GetClusterRWHealth` / peer HTTP fan-out / memberlist parsing / quorum
  logic is removed.
- **No storage-layer signal.** Write health is taken from the **logstore layer**
  (`logstore.add_entry`), i.e. the client-observed "request accepted" outcome — not
  from `file.flush`/`file.sync`. See "Signal source & semantics" for the accepted
  consequence.

## Relationship to `/healthz`

| | `/healthz` (existing) | `/admin/log-health` (new) |
|---|---|---|
| Package | `common/http/health/` | `common/http/management/` |
| Meaning | Process liveness — process up, role registered | Data-path health — real R/W path works, per log |
| Data source | role indicators | `loghealth.Tracker` atomic timestamps, fed by `OpObserver` |
| Response type | `health.HealthResponse` | `loghealth.Response` |

They are two orthogonal layers of liveness and share no code.

## How we observe (the key design decision)

Woodpecker already instruments every logstore operation. In `server/logstore.go`:

```go
op := metrics.StartOp("logstore.add_entry", nil, nil, metrics.WithLogSegment(logId, entry.SegId))
status := "success"
defer func() { op.End(status); /* ...prometheus... */ }()
```

`metrics.StartOp`/`op.End(status)` notify a **global chain of `OpObserver`s**
(`common/metrics/op_observer.go`); the `opregistry.Registry` is already such an
observer. We add a second observer instead of hand-writing record calls in the
hot path.

### Ownership & lifecycle

The tracker is a self-contained object. Whoever creates it and calls
`metrics.RegisterOpObserver(t)` receives the op stream; the tracker holds the only
state. There is **no hard constraint** on where it lives, only two soft ones:

1. The same instance must be reachable by both the hot path (via the global observer
   chain — automatic once registered) and the admin endpoint (via the
   `GetLogHealth` callback closure).
2. It must be registered **once, at startup, before serving traffic** (per
   `RegisterOpObserver`'s contract) — so registration belongs in `main`, not in a
   per-instance constructor (which would re-register on every test `Server`).

We therefore mirror the existing `opReg` pattern exactly: **create and register the
tracker in `cmd/main.go`**, and wire the same instance into the admin callback. The
tracker lives in its own package `common/runtime/loghealth`, sibling to
`opregistry`. `server/service.go` and `server/logstore.go` business logic are
untouched (the only `logstore.go` change is adding instance labels to two `StartOp`
calls — see Wiring).

```
              cmd/main.go (process scope, runs once)
                 t := loghealth.New(stallAfter)
                 metrics.RegisterOpObserver(t)            ← into global chain
                 GetLogHealth: func(b,r){ return t.Report(b,r) }
                          │ same *t referenced twice
        ┌─────────────────┴──────────────────┐
        ▼                                     ▼
   hot path (write/read)                 admin endpoint (read)
 logstore.add_entry / get_batch_entries   GET /admin/log-health
   → StartOp / op.End(status)               → t.Report(...)
   → metrics fans out to observers          → RLock + read atomics
   → t.OnOpStart / t.OnOpEnd
   → updates t.logs map (atomic stores)  ← the only data structure
```

### Signal source & semantics

- **Write** = `logstore.add_entry` op. Its `End(status)` fires when the entry is
  **accepted into the buffer** (the sync-to-storage is asynchronous and not part of
  this op). So "Healthy write" means "the node is accepting writes", which is the
  client-observed outcome. **Accepted consequence:** a storage backend that hangs on
  flush while buffering still succeeds will *not* show Unhealthy until back-pressure
  makes `add_entry` itself error. This is the deliberate trade-off of choosing the
  logstore layer over `file.flush`.
- **Read** = `logstore.get_batch_entries` op `End(status)`.
- **Success vs failure** = `status == "success"` (the literal logstore uses) →
  success; any other status → failure.
- **Client cancel:** because `add_entry`'s op ends at buffering (it does not span the
  client's sync-wait), client cancellation during the sync-wait is naturally not
  observed as a write failure. A `get_batch_entries` cancelled mid-call ends with a
  non-success status and is counted as a read failure; this minor noise is accepted
  and can be refined later by inspecting the status string if needed.

## Naming

Granularity is "per log", matching the granularity (not the mechanism) of Pulsar's
per-topic health check. The earlier `rw_health` draft is superseded.

| Earlier `rw_health` draft | This design |
|---|---|
| `server/rw_health.go` (Server-owned tracker) | `common/runtime/loghealth/tracker.go` (standalone observer) |
| manual record calls in `server/service.go` | **reverted** — observer instead |
| `RWHealthTracker` | `loghealth.Tracker` |
| `ServerRWHealthResponse` | `loghealth.Response` |
| `RWLogHealthSnapshot` | `loghealth.LogSnapshot` |
| `common/http/management/rw_health_handler.go` | `common/http/management/log_health_handler.go` |
| route `/admin/server-rw-health` | `/admin/log-health` |
| `ClusterRWHealthResponse`, `GetClusterRWHealth`, `fetchPeerRWHealth`, quorum | **deleted** |

## Architecture

### Components

1. **`loghealth.Tracker`** (`common/runtime/loghealth/tracker.go`) — implements
   `metrics.OpObserver`; owns the in-memory state and the classification / snapshot
   logic. The single source of truth.
2. **`metrics.Op` instance fields** (`common/metrics/op.go`) — new `BucketName` /
   `RootPath` fields + a `WithInstance(bucket, root)` option, so observers receive the
   multi-tenant identity. (Today `Op` only carries `LogID`/`SegmentID`.)
3. **Two `StartOp` call sites** (`server/logstore.go`) — pass
   `WithInstance(bucketName, rootPath)` for `add_entry` and `get_batch_entries`.
4. **HTTP handler** (`common/http/management/log_health_handler.go`) — parses the
   optional `bucket_name` / `root_path` filter and serializes the response.
5. **Wiring** (`cmd/main.go`, `common/http/{router,server}.go`) — create + register
   the tracker, expose route `/admin/log-health`.

### Data model — lock-free hot path

```go
type logInstanceKey struct{ bucketName, rootPath string }

type logHealth struct {
    writeAttemptMS atomic.Int64 // OnOpStart(add_entry); writes can hang in-flight
    writeSuccessMS atomic.Int64
    writeFailureMS atomic.Int64
    readSuccessMS  atomic.Int64 // reads: only success/failure, no attempt timestamp
    readFailureMS  atomic.Int64
    failureReason  atomic.Pointer[string] // last failure reason, diagnostic
}

type Tracker struct {
    mu         sync.RWMutex // guards map STRUCTURE only
    stallAfter time.Duration
    logs       map[logInstanceKey]map[int64]*logHealth
}
```

**Hot path** (inside `OnOpStart`/`OnOpEnd`): `RLock` (shared) → map lookup →
`atomic.Store`. The first op ever seen for a `(bucket, rootPath, logID)` takes the
write lock once to create the `*logHealth`; every subsequent record is shared-lock +
atomic only. No allocation, no exclusive lock, no per-op ID.

### Observer integration

The tracker implements `metrics.OpObserver`. The observer methods are thin adapters
that translate an `*metrics.Op` into an internal record call; the internal record
methods take explicit `(bucket, rootPath, logID, now[, reason])` so they are unit
testable without constructing `metrics.Op`.

```go
const (
    opAddEntry        = "logstore.add_entry"        // must match server/logstore.go StartOp
    opGetBatchEntries = "logstore.get_batch_entries"
    statusSuccess     = "success"                   // the literal logstore.go uses
)

func (t *Tracker) OnOpStart(op *metrics.Op) uint64 {
    if op.OpType == opAddEntry {
        t.recordWriteAttempt(op.BucketName, op.RootPath, op.LogID, op.StartedAt())
    }
    return 0 // tracker keeps no per-op handle
}

func (t *Tracker) OnOpEnd(op *metrics.Op, _ uint64, elapsed time.Duration, status string) {
    now := op.StartedAt().Add(elapsed)
    ok := status == statusSuccess
    switch op.OpType {
    case opAddEntry:
        if ok { t.recordWriteSuccess(op.BucketName, op.RootPath, op.LogID, now) } else
        { t.recordWriteFailure(op.BucketName, op.RootPath, op.LogID, now, status) }
    case opGetBatchEntries:
        if ok { t.recordReadSuccess(op.BucketName, op.RootPath, op.LogID, now) } else
        { t.recordReadFailure(op.BucketName, op.RootPath, op.LogID, now, status) }
    }
}
```

Other op types are ignored. All record methods are no-ops when the tracker is `nil`.
Observer methods must be fast and goroutine-safe (per the `OpObserver` contract) —
satisfied by the RLock + atomic model.

### Per-log classification (snapshot time)

`stallAfter` defaults to 10 minutes. Each direction (read, write) is first classified
independently from its timestamps into a raw direction-state, exposed as
`write_state` / `read_state` for transparency. The log's single overall `state` is
then derived from both directions (see "Log overall state").

For a direction with `lastSuccess`, `lastFailure`, and (write only) `lastAttempt`:

| Direction state | Rule |
|---|---|
| **Healthy** | `lastSuccess > 0` **AND** `now - lastSuccess ≤ stallAfter` (succeeded recently) |
| **Stalled** (write only) | `lastSuccess > 0` **AND** `now - lastSuccess > stallAfter` **AND** `lastAttempt > lastSuccess` **AND** `lastAttempt > lastFailure` — was healthy, then ops keep starting but none complete (catches a hung `add_entry` even under high QPS, because the rule keys off the age of the last *success*, not the last attempt). A log that has *never* succeeded is never Stalled — it is Failed or Idle. |
| **Failed** | `lastFailure > lastSuccess` **AND** `now - lastFailure ≤ stallAfter` (recent terminal failures) |
| **Idle** | no observed activity, or success & failure both stale (older than `stallAfter`) and not Stalled. Idle is informational — the last terminal outcome is still visible via the `last_*_ms` timestamps. |

### Log overall state

A single enum per log, worst-wins precedence `Stalled > Failed > Healthy > Idle`:

- `Stalled` if the write direction is Stalled.
- else `Failed` if either direction is Failed.
- else `Healthy` if either direction is Healthy.
- else `Idle`.

The four counters (`healthy_logs` / `failed_logs` / `stalled_logs` / `idle_logs`) are
mutually exclusive and partition `tracked_logs` by this overall state.

### Node-level rollup

The rollup spans **all logs in scope** (all tenants when unfiltered, or the one
filtered instance). It keys off **active evidence**, so a merely quiet node is never
pulled out of rotation:

- Node `state = Unhealthy` **iff** `tracked_logs > 0` **AND** every tracked log is
  `Stalled` or `Failed` (zero Healthy, zero Idle).
- Otherwise `state = Healthy`: ≥ 1 Healthy log, **or** some/all logs Idle (quiet node
  is healthy), **or** no tracked logs at all (reason `no_observed_activity`).

This matches the issue's "at least one successfully reading/writing log = Healthy;
multiple logs stalled simultaneously = Unhealthy", resolving idle conservatively.

### HTTP endpoint

**Node-level scope.** Woodpecker is multi-tenant: a node serves many
`(bucket_name, root_path)` instances at once, and a log is uniquely identified by the
triple `(bucket_name, root_path, log_id)` (there is no `logName` at the logstore
layer). One probe call returns **every** log the node tracks, across all tenants, each
carrying its own triple.

`GET /admin/log-health[?bucket_name=<bucket>&root_path=<root>]`

- `bucket_name` / `root_path` are **optional filters** (accepts snake/camel/lower
  variants). Both supplied → narrowed to that one instance; omitted → all tenants. A
  lone param (only one of the two) is ignored → returns everything.
- 405 on non-GET.
- Body `loghealth.Response`:

```jsonc
{
  "state": "Healthy",              // node rollup: Healthy | Unhealthy
  "reason": "observed_read_write_success",
  "node_id": "...",                // from metrics.NodeID
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

- HTTP status: `200` when node `state == Healthy`, `503` when `Unhealthy`. Logs sorted
  by `(bucket_name, root_path, log_id)` for stable output.

### Public API of `loghealth`

```go
func New(stallAfter time.Duration) *Tracker

// metrics.OpObserver
func (t *Tracker) OnOpStart(op *metrics.Op) uint64
func (t *Tracker) OnOpEnd(op *metrics.Op, handle uint64, elapsed time.Duration, status string)

// Snapshot is pure & deterministic (nodeID and now injected) — used by tests.
func (t *Tracker) Snapshot(bucketFilter, rootPathFilter, nodeID string, now time.Time) Response

// Report is the production wrapper used by the admin callback: it fills nodeID from
// metrics.NodeID and now from time.Now(), and maps state -> HTTP status.
func (t *Tracker) Report(bucketFilter, rootPathFilter string) (Response, int)

// Node rollup states.
const StateHealthy = "Healthy"
const StateUnhealthy = "Unhealthy"
```

### Wiring

- `common/metrics/op.go`: add `BucketName`, `RootPath` to `Op`; add
  `WithInstance(bucketName, rootPath string) OpOption`.
- `server/logstore.go`: add `metrics.WithInstance(bucketName, rootPath)` to the
  `StartOp` calls for `logstore.add_entry` (~:197) and `logstore.get_batch_entries`
  (~:300). No other logic changes.
- `cmd/main.go`: `t := loghealth.New(defaultStallAfter)`;
  `metrics.RegisterOpObserver(t)` (next to the existing `opReg` registration);
  add `GetLogHealth: func(_ context.Context, b, r string) (any, int) { return t.Report(b, r) }`.
- `common/http/server.go`: `AdminCallbacks` gains `GetLogHealth management.LogHealthCallback`;
  register route when set. Remove the two RW callback fields.
- `common/http/router.go`: one const `AdminLogHealthPath = "/admin/log-health"`;
  remove the two RW consts.
- `common/http/management/log_health_handler.go`: handler + `LogHealthCallback`.
- **Revert** the `rw_health` draft's edits to `server/service.go` and
  `server/service_test.go` (restore the upstream `AddEntry`/`GetBatchEntriesAdv` and
  remove the `rwHealthTracker` field); delete `server/rw_health.go` and
  `server/rw_health_test.go`.
- `common/http/README.md`: document the route.

## Error handling

- `nil` tracker → all observer/record methods no-op; `Report` on a `nil` tracker (or
  empty tracker) yields `Healthy / no_observed_activity`.
- Missing/partial filter → return all tenants (not an error). Non-GET → `405`.
- Non-`"success"` op status counts as a failure (see Signal source & semantics).

## Testing

- **`common/runtime/loghealth/tracker_test.go`** (table-driven, fixed injected `now`,
  using the internal record methods + `Snapshot`):
  - Each direction-state rule: Healthy, Stalled (incl. high-QPS hung-write case where
    `lastAttempt` is recent but `lastSuccess` is stale), Failed, Idle.
  - Read direction never goes Stalled.
  - Overall precedence `Stalled > Failed > Healthy > Idle`, incl. write=Healthy +
    read=Failed → Failed.
  - Counters partition `tracked_logs`.
  - Node rollup: ≥1 healthy → Healthy; some idle + no failures → Healthy; all
    stalled/failed → Unhealthy; no logs → Healthy + `no_observed_activity`.
  - Multi-tenant: unfiltered returns logs across multiple `(bucket, rootPath)` each
    tagged with its triple; filter narrows to one instance.
  - `nil` tracker safety.
  - Concurrency smoke test under `-race`.
  - **Observer adapter:** drive via `metrics.StartOp(...WithInstance...)` + `op.End`
    (with `metrics.ResetObservers()` guard) and assert the resulting state, proving
    `OnOpStart`/`OnOpEnd` map op-type + status correctly.
- **`common/metrics/op_test.go`:** `WithInstance` sets `BucketName`/`RootPath`.
- **`common/http/management/log_health_handler_test.go`:** no-filter returns all,
  filter narrows, lone param ignored, 405 non-GET, 200 vs 503 by node state, JSON
  shape (per-log triple present).

## Migration / cleanup

The earlier `rw_health` draft (working-tree changes, not yet committed) is superseded:
revert its `server/service.go` + `server/service_test.go` edits, delete
`server/rw_health.go` + `server/rw_health_test.go`, and implement the observer-based
design above. Net effect on data-path business code: **zero** (only two label-only
args added to existing `StartOp` calls).
