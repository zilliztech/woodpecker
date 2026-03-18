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

5. **Wait for local data cleanup** — 即使所有 processor 已回收，本地磁盘上可能仍存在 segment 数据文件（`data.log`）。这是因为 compaction 只在 quorum 的其中一个节点执行（上传到 object storage 并删除该节点的本地数据），其他 quorum 节点的本地副本需要等待 Client 端的完整清理流程：
   - Client 端 auditor 将 segment 标记为 `Truncated`
   - 等待 retention 过期（默认 72 小时，由 `retention_time` 配置）
   - Client 端 `SegmentCleanupManager` 发送 `CleanSegment` RPC 到所有 quorum 节点
   - 各节点删除本地 `data.log` 文件

6. **Decommission monitor** — A background goroutine (started by `Decommission()` or on restart recovery) checks every 5 seconds whether `HasLocalSegmentData() == false`（磁盘上是否还有 `data.log` 文件）。当本地数据全部清理完毕时，自动调用 `MarkDecommissioned()`，持久化状态并停止监控。

   Processor 数量仅作为日志和 progress API 的观测指标输出，**不参与退役完成的判定**。Processor 是内存对象，节点关机后自然消失；而且由于 idle cleanup 的保护机制（每个 log 保留最高 segmentId 的 processor），processor count 在正常清理流程中不会归零。退役完成的唯一判定标准是：**本地磁盘上没有 segment 数据文件**。

7. **Safe to terminate** — After auto-transition to `decommissioned`, the progress API reports `safe_to_terminate: true`. At this point, the K8s orchestrator (or human operator) can proceed with pod termination. If the node restarts before being terminated, it will load `decommissioned` from the state file.

### Decommission Timeline (Conservative Approach)

```
POST /admin/node/decommission
  │
  ├─ 立即生效：拒绝新写入，启动 monitor
  │
  ├─ 等待 Client 端 auditor 标记 segment 为 Truncated
  │   （取决于上游何时不再需要这些数据）
  │
  ├─ 等待 retention 时间过期（默认 72h）
  │
  ├─ 等待 Client 端 CleanupManager 发送 CleanSegment 到本节点
  │   （删除本地 data.log 文件）
  │
  └─ monitor 检测到 processor==0 且无本地 data.log → 自动标记 decommissioned
```

**退役总耗时** 取决于数据生命周期，最坏情况可能需要 **retention 时间 + auditor 周期 + cleanup 周期**（可能数天）。

**设计取舍：** 当前采用保守策略 — Server 端仅通过监控本地磁盘数据是否清理完毕来判定退役完成，不引入额外的功能代码和跨组件交互（不主动触发 compaction、不与 Client 端通信、不修改 segment 生命周期管理逻辑）。优点是实现简单、不改变现有数据安全保障、零风险；缺点是退役速度受限于 retention 时间配置，无法加速。

**后续优化方向**（不在当前 scope 内）：
- 管控系统在触发退役时，同步通知 Client 端优先对该节点的 segment 执行 compaction + truncate + cleanup
- 提供 `force` 参数，允许在数据已有 object storage 完整副本时跳过 retention 等待
- Server 端主动扫描并请求 Client 端清理自身数据

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
