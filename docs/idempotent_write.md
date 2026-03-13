# Idempotent Write Support for Woodpecker

## Background

In distributed systems, producers (clients) may retry writes due to network failures, timeouts, or leader changes. Without idempotent guarantees, these retries can result in duplicate log entries, breaking correctness for downstream consumers and higher-level systems.

Mainstream log systems such as Kafka and Pulsar provide idempotent write semantics to ensure that retries do not introduce duplicates. Currently, Woodpecker does not provide built-in idempotent write support, which means duplicate data may be persisted when clients retry append operations.

## Problem Statement

Under the current design:

- Clients may retry append requests when facing transient failures.
- The storage layer cannot distinguish between:
  - a new write, and
  - a replayed write caused by retry.
- As a result, the same logical record may be appended multiple times to a segment.

This behavior makes it difficult to:

- Build exactly-once or effectively-once semantics on top of Woodpecker.
- Use Woodpecker as a reliable WAL for message queues, stream processing engines, or stateful systems.

## Proposed Feature

Introduce idempotent write support in Woodpecker, similar to Kafka / Pulsar. At a high level, the system should guarantee:

> Multiple retries of the same logical write will result in at most one successful append within a bounded idempotency window.

## Design Overview

### Key Design Decisions

1. **Deduplication happens at the Client (SDK) layer.** The client maintains a dedup window that maps `idempotencyId → (segId, entryId, status)`. On first write, the entryId is assigned and recorded immediately. All subsequent retries of the same idempotencyId reuse the same entryId.
2. **Dedup window is bounded by time + capacity** (whichever limit is reached first). Retries beyond the window are treated as new writes.
3. **`idempotencyId` is persisted with each entry** in the storage layer, enabling dedup state reconstruction on client restart.
4. **Dedup state is checkpointed to object storage** as a per-log `dedup_snapshot` file (compact binary format), supporting both StagedStorage and ObjectStorage modes.
5. **Idempotent writes are optional and opt-in**. Non-idempotent clients work exactly as before with zero overhead.

### Architecture: Why Client-Side Dedup

In Woodpecker, writes go through a quorum path: Client `AppendOp` sends entries to N server nodes and waits for acknowledgment quorum (aq).

If dedup were done at the server side (per-node), different nodes may have inconsistent views of which entryId corresponds to a given idempotencyId, because partial failures leave some nodes with the entry and others without.

By managing idempotencyId → entryId mapping at the client side, the client guarantees that **all retries of the same logical write always use the same (segId, entryId)**. Every server node receives identical entries, and the quorum remains consistent.

## Detailed Design

### Idempotency Identifier

Idempotent writes are identified by a flexible idempotency identifier (`idempotencyId`). The identifier can be:

- **Explicitly provided** by the client, representing business-level idempotency semantics.
- **Implicitly derived** by the system, defaulting to a hash (e.g. MD5) of the message payload when not provided.

This design allows clients to choose the most appropriate idempotency strategy for their workload, while still providing a safe default.

### Write Path (Client Side)

```
Business layer calls Write(msg, idempotencyId="X")
        │
        ▼
┌───────────────────────────────────┐
│ Check local dedup window for "X"  │
└───────┬──────────┬────────────────┘
        │          │
    found       not found
        │          │
        ▼          ▼
  ┌──────────┐  Assign new entryId/segId
  │ committed │  Record "X" → (segId, entryId, pending) in dedup window
  │    ?      │      │
  └──┬────┬──┘      ▼
   yes    no    AppendOp → Quorum write (same entryId for all retries)
     │     │         │
     ▼     ▼         ├─ Success → mark "X" as committed in dedup window
  Return  Wait/      │             return (segId, entryId)
  success Retry      │
  with    with    └─ Failure → "X" stays pending in dedup window
  original same          next retry reuses the same (segId, entryId)
  entryId  entryId
```

Key behaviors:

- **First write**: idempotencyId not in window → assign (segId, entryId), record as `pending`, send to quorum.
- **Retry (not yet committed)**: idempotencyId found with `pending` status → reuse the same (segId, entryId), send to quorum again. Multiple async writes with the same idempotencyId are treated as the same request.
- **Retry (already committed)**: idempotencyId found with `committed` status → return success immediately with the previously committed entryId, no server interaction.
- **Write failure**: the mapping stays in the window as `pending`. Business layer retry will reuse the same (segId, entryId), ensuring all quorum nodes receive identical entries.
- **"Fake failure"** (quorum actually succeeded but client didn't receive ack): the mapping is `pending` in memory. Retry sends the same (segId, entryId) to quorum → server nodes that already have this entry treat it as a no-op, others accept it → quorum ack succeeds → client marks as `committed`. No duplicate data.

### Dedup Window

The client maintains an in-memory dedup window per log, bounded by **time + capacity** (whichever limit is reached first):

| Parameter | Description | Example |
|-----------|-------------|---------|
| `idempotency_window_duration` | Max age of a dedup entry | `10m` |
| `idempotency_window_max_keys` | Max number of tracked idempotencyIds | `100000` |

- **Eviction**: entries are evicted from memory individually as they expire or when capacity is exceeded (oldest-first). Only `committed` entries are eligible for eviction; `pending` entries are retained until they are committed or the client restarts.
- **Window-expired retries**: if a retry arrives after its `idempotencyId` has been evicted, it is treated as a new write. This is acceptable because window-expired retries are an extreme edge case, and accepting them is safer than rejecting with an error.
- **Memory footprint estimate**: each dedup entry stores a 16-byte MD5 raw bytes as key, plus 16 bytes for segId (int64) + entryId (int64) as value, totaling 32 bytes per entry. With the default `window_max_keys = 100,000`, the raw data size is approximately **3 MB**. Including Go map structural overhead (pointers, buckets, etc.), the actual memory usage is estimated at **5~6 MB** per log.

### Proto / API Changes

**`LogEntry` in `proto/logstore.proto`** — add an optional field to persist `idempotencyId` alongside the entry:

```protobuf
string idempotency_id = N;  // Optional. Persisted with entry for dedup state reconstruction.
```

**`WriteMessage` (client side)** — add an optional `IdempotencyId` field, passed through from the business layer.

**Response semantics**: when a duplicate is detected at the client side, the client returns success with the original entryId. No changes to server-side response format are needed, since dedup is handled entirely by the client.

### Compatibility

- Idempotent writes are optional and opt-in.
- Non-idempotent clients continue to work exactly as before, without additional metadata or performance overhead.
- Existing write paths remain unchanged when idempotency is disabled.
- Only StagedStorage and ObjectStorage modes support idempotent writes (both have object storage access for snapshot persistence).

### Design Rationale

- Avoids forcing clients into dense, strictly ordered sequence numbers.
- Enables business-level idempotency semantics when needed.
- Provides a safe and intuitive default behavior.
- Aligns well with append-only storage and WAL-style systems.
- Client-side dedup with entryId reuse is the simplest correct approach given the single-writer-per-log model. All retries of the same logical write are guaranteed to use the same entryId, ensuring quorum consistency.

## Dedup Snapshot Design

### Overview

The dedup window state is periodically checkpointed to object storage as a **`dedup_snapshot`** file, per log. This enables dedup state recovery after client restart.

Storage path:

```
{log_prefix}/dedup_snapshot/{logId}/snapshot_{timestamp}
```

The snapshot uses a **compact binary format** (e.g. Protocol Buffers) to minimize file size. With the default `window_max_keys = 100,000`, the snapshot file is approximately **3~5 MB**.

### Snapshot Data Structure

Since the dedup window is at the **log level** and a log may span multiple segments (some still actively being written), the snapshot tracks **per-segment watermarks** to record exactly where each segment's data has been captured.

The following JSON is for illustration only; the actual format is compact binary:

```json
{
  "log_id": 123,
  "snapshot_timestamp": "2026-02-05T10:00:00Z",
  "config": {
    "window_duration": "10m",
    "window_max_keys": 100000
  },
  "segment_watermarks": [
    {
      "segment_id": 1,
      "last_entry_id": 150
    },
    {
      "segment_id": 2,
      "last_entry_id": 250
    }
  ],
  "dedup_entries": {
    "idempotency_id_abc": {
      "segment_id": 1,
      "entry_id": 120,
      "timestamp": "2026-02-05T09:55:00Z"
    },
    "idempotency_id_def": {
      "segment_id": 2,
      "entry_id": 230,
      "timestamp": "2026-02-05T09:58:00Z"
    }
  }
}
```

| Field | Description |
|-------|-------------|
| `segment_watermarks` | Per-segment recovery point. Records the last entryId that is included in this snapshot. Recovery reads entries after this point via the normal read API. |
| `dedup_entries` | The dedup window contents: mapping of `idempotencyId` → (segmentId, entryId, timestamp). Only `committed` entries are persisted; `pending` entries are not (they will be resolved during gap recovery). |

### Snapshot Timing

Snapshots are written to object storage based on **time + bounded size**:

- **Time-based**: periodically (e.g. every 30s or 1m).
- **Size-based**: when the number of new dedup entries since last snapshot exceeds a threshold.

Only the most recent 1~2 snapshots are retained. Older snapshots are deleted after a new one is successfully written.

### Recovery Flow (Client Restart / Writer Failover)

When a client restarts (or a new writer takes over after failover), the dedup window **must be fully reconstructed before the writer can accept new writes**:

```
Step 1: Load latest dedup_snapshot from object storage
        │
        ▼
Step 2: Fill the gap (entries committed after the snapshot)
        │
        ├─ For each segment in segment_watermarks:
        │     Read entries where entryId > watermark.last_entry_id
        │     via the normal read API
        │     → Extract idempotencyId from each entry
        │     → Add to dedup window as committed
        │
        ├─ For any new segment not in segment_watermarks:
        │     Read the entire segment from the beginning
        │     → Extract idempotencyId from each entry
        │     → Add to dedup window as committed
        │
        ▼
Step 3: Apply window eviction (discard entries older than window_duration)
        │
        ▼
Dedup window fully reconstructed, writer ready to accept new writes
```

This ensures:
- All actually committed entries are in the dedup window with their real (segId, entryId).
- Entries that were `pending` (in-flight) when the old writer crashed are resolved: if they were committed to quorum, they appear in the gap data; if not, they don't appear, and business layer retries will be treated as new writes with new entryIds.

## Edge Cases

### Case 1: Client crash between write and snapshot

```
Timeline:
  Snapshot written (covers up to entryId=500)
      │
      ▼
  Entries 501~520 written successfully (dedup window updated in memory)
      │
      ▼
  Client crashes (snapshot NOT updated)
```

**Recovery**: Load snapshot (watermark at entryId=500), then read entries 501~520 from the log via read API. The `idempotencyId` is persisted in each entry, so the dedup window is fully rebuilt. No duplicates are possible.

### Case 2: "Fake failure" — write succeeds on quorum but client doesn't receive ack

```
Timeline:
  Write(idempotencyId="X") → assigned entryId=100, recorded as pending
      │
      ▼
  Quorum write succeeds on server side
  But ack is lost in network
      │
      ▼
  Client considers write failed, "X" stays pending with entryId=100
      │
      ▼
  Business layer retries Write(idempotencyId="X")
  Client finds "X" → pending, entryId=100
  Sends same entryId=100 to quorum again
      │
      ▼
  Server nodes that already have entryId=100: no-op / ack
  Server nodes that don't: accept it
  → Quorum ack → client marks "X" as committed
```

**Result**: No duplicate. The same entryId=100 is used consistently across all retries.

### Case 3: Writer failover — old writer crashes with in-flight writes

```
Timeline:
  Old writer: Write(idempotencyId="X") → entryId=100, pending
      │
      ▼
  Old writer crashes before receiving ack
      │
      ▼
  Segment is fenced, new writer starts
  New writer loads snapshot + reads gap from server
      │
      ├─ If entryId=100 was committed to quorum:
      │     It appears in the gap data
      │     "X" → (seg, 100, committed) in dedup window
      │     Business retry → returns success immediately
      │
      └─ If entryId=100 was NOT committed (didn't reach quorum):
            It does NOT appear in the gap data
            "X" is NOT in dedup window
            Business retry → treated as new write, assigned new entryId
```

**Result**: Correct in both cases. Committed writes are preserved; uncommitted writes are cleanly retried.

### Case 4: Segment already finalized

A segment referenced in `segment_watermarks` may have been finalized since the snapshot was taken. This is normal — the recovery flow reads from the watermark position onward via the read API, which works the same regardless of whether the segment is active or finalized. If all entries in that segment are older than `window_duration`, the segment can be skipped entirely.

### Case 5: Segment truncated / cleaned up

If a segment has been truncated, all its entries are necessarily older than the dedup window (truncation happens long after finalization). The corresponding `dedup_entries` in the snapshot would have already been evicted by the time-based window. No special handling is needed.

### Case 6: New segment rolled after snapshot

After the snapshot was taken, the log may have rolled to a new segment that is not recorded in `segment_watermarks`. During recovery, the client discovers all active segments from metadata. Any segment not present in `segment_watermarks` is read from the beginning to extract `idempotencyId` values.

### Case 7: Snapshot write fails

If a snapshot write to object storage fails, the previous snapshot remains valid. The client continues operating with the in-memory dedup window. The next scheduled snapshot attempt will capture the latest state. Retaining 1~2 old snapshots ensures there is always a fallback.

### Case 8: Duplicate retry arrives after window expiration

```
Timeline:
  Entry with idempotencyId="X" written at T=0
      │
      ▼
  Window duration passes (e.g. 10 minutes)
      │
      ▼
  idempotencyId="X" evicted from dedup window
      │
      ▼
  Retry with idempotencyId="X" arrives at T=12m
```

**Behavior**: The retry is treated as a new write and accepted. This is by design — window-expired retries represent an extreme edge case (a retry delayed by more than the configured window duration), and accepting the write is safer than rejecting it with an error.
