# LogStore related components.

This is the location for LogStore Server related classes. It is to prepare for separating common, protocol, client and
server modules.

## LogStore ObjectStorage layout

layout: `root`/`<instance_id>`/`<log_id>`/`<segment_id>`/`<fregement_id>`

- `root`: the prefix used for prefixing all the log data. The `root` default value is `woodpecker`.
- `instance_id`: identifier for the current woodpecker cluster instance.
- `log_id`: identifier for the log.
- `segment_id`: identifier for the segment of the log.
- `<fregement_id>`: identifier for the object of the logfile, which represents a fragment of the logfile.

## Node Lifecycle State Machine

Each Woodpecker server node has a lifecycle managed by `NodeLifecycleManager` (`lifecycle.go`). The state machine governs how a node participates in the cluster during normal operation, rolling upgrades, and scale-down events.

### States

| State | Description |
|-------|-------------|
| `active` | Node is fully operational. Accepts new writes (`AddEntry`), serves reads, and participates in segment operations (fence, complete, compact). This is the initial state after startup. |
| `decommissioning` | Node is preparing for shutdown. New writes are **rejected** (`ErrLogStoreShutdown`), but all other operations continue normally — reads, segment completions, compactions, and cleanup proceed to allow in-flight tasks to drain. |
| `decommissioned` | Node has completed draining. All segment processors have been closed. The node is safe to terminate. |

### State Transitions

```
                  POST /admin/node/decommission       monitor detects 0 processors
                  (or Decommission() call)            (automatic, every 5s check)
    ┌──────────┐ ──────────────────────────> ┌─────────────────┐ ──────────────> ┌─────────────────┐
    │  active  │                             │ decommissioning │                 │ decommissioned  │
    └──────────┘                             └─────────────────┘                 └─────────────────┘
         │                                     │          ▲
         │                                     │          │ (idempotent, no error)
         │                                     ▼          │
         │                              POST /admin/node/decommission
         │                              ──────> stays in decommissioning
         │
         │  SIGTERM/SIGINT
         ▼
    Server.Stop()
    (immediate full shutdown, bypasses decommission)
```

**Note:** The transition from `decommissioning` to `decommissioned` is **automatic**. A background monitor goroutine starts when decommission is triggered (or on restart if the persisted state is `decommissioning`). It checks `LogStore.GetActiveProcessorCount()` every 5 seconds. When the count reaches 0, it calls `MarkDecommissioned()` and persists the state. The monitor stops after the transition.

### Transition Rules

| From | To | Trigger | Behavior |
|------|----|---------|----------|
| `active` | `decommissioning` | `POST /admin/node/decommission` | Sets `rejectWrites` flag on LogStore. New `AddEntry` calls return `ErrLogStoreShutdown`. Reads and segment operations continue. |
| `decommissioning` | `decommissioning` | `POST /admin/node/decommission` (repeated) | Idempotent. Returns `200 OK`, no state change. |
| `decommissioning` | `decommissioned` | Automatic (background monitor detects `GetActiveProcessorCount() == 0`, checks every 5s) | All segment processors drained and closed. State persisted to `node_state.json`. Node is safe to terminate. |
| `decommissioned` | any | `POST /admin/node/decommission` | Returns `409 Conflict` with error `"node already decommissioned"`. |
| any | (shutdown) | `SIGTERM` / `SIGINT` | `Server.Stop()` performs immediate graceful shutdown: close listener, leave gossip cluster, drain gRPC, stop LogStore. Does not go through decommission states. |

### What Happens During Decommission

When a node enters `decommissioning`:

1. **Writes blocked** — The `rejectWrites` flag (separate from `stopped`) is set on LogStore. `AddEntry` returns `ErrLogStoreShutdown` immediately. Clients will failover to other nodes via quorum selection.

2. **Reads continue** — `GetBatchEntriesAdv` and other read operations are unaffected. This allows consumers to finish reading data from segments hosted on this node.

3. **Segment operations continue** — `FenceSegment`, `CompleteSegment`, `CompactSegment`, `CleanSegment`, `UpdateLastAddConfirmed` all function normally. This is critical because:
   - Active segments need to be fenced and completed so data is sealed.
   - Compaction must finish to ensure data durability in object storage.
   - Cleanup of truncated segments should proceed to free resources.

4. **Processors drain naturally** — The background cleanup goroutine in LogStore continues to collect idle segment processors. As segment operations complete and processors become idle, they are closed and removed.

5. **Wait for local data cleanup** — Even after all processors have been reclaimed, segment data files (`data.log`) may still exist on local disk. This is because compaction only runs on one node in the quorum (uploading to object storage and deleting that node's local data). Local replicas on other quorum nodes must wait for the full Client-side cleanup flow:
   - The Client-side auditor marks the segment as `Truncated`
   - Wait for retention to expire (default 72 hours, configured by `retention_time`)
   - The Client-side `SegmentCleanupManager` sends `CleanSegment` RPC to all quorum nodes
   - Each node deletes its local `data.log` file

6. **Decommission monitor** — A background goroutine (started by `Decommission()` or on restart recovery) checks every 5 seconds whether `HasLocalSegmentData() == false` (i.e., whether any `data.log` files remain on disk). Once all local data has been cleaned up, it automatically calls `MarkDecommissioned()`, persists the state, and stops the monitor.

   The processor count is only exposed as an observability metric in logs and the progress API — **it does not determine decommission completion**. Processors are in-memory objects that naturally disappear when the node shuts down. Moreover, due to the idle cleanup protection mechanism (the processor with the highest segmentId per log is always retained), the processor count will not reach zero during normal cleanup. The sole criterion for decommission completion is: **no segment data files remain on local disk**.

7. **Safe to terminate** — After auto-transition to `decommissioned`, the progress API reports `safe_to_terminate: true`. At this point, the K8s orchestrator (or human operator) can proceed with pod termination. If the node restarts before being terminated, it will load `decommissioned` from the state file.

### Decommission Timeline (Conservative Approach)

```
POST /admin/node/decommission
  │
  ├─ Takes effect immediately: reject new writes, start monitor
  │
  ├─ Wait for Client-side auditor to mark segments as Truncated
  │   (depends on when upstream no longer needs this data)
  │
  ├─ Wait for retention period to expire (default 72h)
  │
  ├─ Wait for Client-side CleanupManager to send CleanSegment to this node
  │   (deletes local data.log files)
  │
  └─ Monitor detects no local data.log files → automatically marks decommissioned
```

**Total decommission duration** depends on the data lifecycle. In the worst case, it may take **retention period + auditor cycle + cleanup cycle** (potentially days).

**Design trade-off:** The current approach uses a conservative strategy — the Server determines decommission completion solely by monitoring whether local disk data has been fully cleaned up, without introducing additional functional code or cross-component interactions (no proactive compaction triggering, no communication with the Client, no modifications to segment lifecycle management logic). The advantage is simplicity, no changes to existing data safety guarantees, and zero risk. The downside is that decommission speed is limited by the retention time configuration and cannot be accelerated.

**Future optimization directions** (out of current scope):
- When the orchestrator triggers decommission, concurrently notify the Client to prioritize compaction + truncation + cleanup for segments on this node
- Provide a `force` parameter that allows skipping the retention wait when data already has a complete copy in object storage
- Have the Server proactively scan and request the Client to clean up its data

### State Persistence

Decommission state is persisted to a local file `<Storage.RootPath>/node_state.json` (e.g., `/woodpecker/data/node_state.json`). This ensures that if a node restarts during the decommission process (e.g., OOM kill, rolling upgrade of the decommission controller), it will resume in the `decommissioning` state and continue rejecting new writes.

**File format:**
```json
{"state":"decommissioning","timestamp":1710000000000}
```

**Behavior on startup:**
- If `node_state.json` does not exist → node starts as `active` (normal fresh boot).
- If `node_state.json` contains `"decommissioning"` → node starts in `decommissioning`, `rejectWrites` is re-applied to LogStore automatically.
- If `node_state.json` contains `"decommissioned"` → node starts in `decommissioned`, further decommission calls return `409`.
- If `node_state.json` is corrupt or contains an unknown state → startup fails with an error (prevents silent data inconsistency).

**Why local file instead of etcd:**
- The Server layer does not interact with etcd by design — etcd access is confined to the Woodpecker Client layer (`MetadataProvider`).
- `Storage.RootPath` is the shared data directory for all tenants, typically backed by a PersistentVolume in K8s, so it survives pod restarts.
- No new external dependency required.

**Clearing state:** After a node has been fully decommissioned and re-provisioned, the state file can be removed manually or via `ClearState()` to reset the node to `active`.

### Key Design Decision: `rejectWrites` vs `stopped`

The LogStore has two separate boolean flags:

- **`stopped`** — Checked by ALL operations (reads, writes, fence, complete, compact, etc.). Set during full `LogStore.Stop()` shutdown.
- **`rejectWrites`** — Checked ONLY by `AddEntry`. Set during decommission via `RejectNewWrites()`.

This separation is essential. If decommission reused `stopped`, all operations would be blocked immediately, preventing graceful task draining — segment processors couldn't complete their work, data could be lost, and the node would be stuck with non-zero `remaining_processors` indefinitely.

### Monitoring Decommission Progress

During decommission, the external orchestrator should poll `GET /admin/node/decommission/progress`:

```json
{
  "state": "decommissioning",
  "remaining_processors": 5,
  "has_local_data": true,
  "safe_to_terminate": false
}
```

- `remaining_processors`: Count of active `SegmentProcessor` instances (informational only, does not affect `safe_to_terminate`). Due to idle cleanup protection (highest segmentId per log is always kept), this number may not reach 0 — processors are in-memory objects that disappear on shutdown.
- `has_local_data`: Whether there are `data.log` files remaining on disk. Even after all processors are idle, local segment data may persist until the Client-side cleanup flow completes (truncation + retention expiry + CleanSegment RPC).
- `safe_to_terminate`: `true` when `has_local_data == false`. This is the **sole criterion** — no local data means the node can be safely terminated.

### Server Shutdown Sequence (Stop)

Regardless of lifecycle state, when `Server.Stop()` is called (via SIGTERM/SIGINT):

```
1. Close TCP listener             — stop accepting new gRPC connections
2. Leave gossip cluster           — broadcast departure to peers (5s timeout)
3. Shutdown gossip                — stop memberlist
4. GracefulStop gRPC              — drain in-flight handlers (10s timeout, force stop on timeout)
5. Wait for gRPC loop goroutine   — ensure clean exit
6. Stop LogStore                  — close all segment processors, stop background cleanup
7. Cancel server context          — propagate cancellation to all goroutines
8. Wait for gossip goroutine      — ensure async seed join loop exits
```

### Related Files

| File | Description |
|------|-------------|
| `lifecycle.go` | `NodeLifecycleManager` — state machine with file persistence (`node_state.json`) |
| `lifecycle_test.go` | Unit tests for state transitions, progress, and persistence (restart survival, corrupt file, clear) |
| `logstore.go` | `LogStore` with `rejectWrites` flag and `GetActiveProcessorCount()` |
| `service.go` | `Server.Decommission()`, `GetNodeStatus()`, `GetDecommissionProgress()`, and `Stop()` |
| `../common/http/management/node_handler.go` | HTTP handlers for the admin lifecycle APIs |
| `../common/http/README.md` | Full HTTP API reference with curl examples |
