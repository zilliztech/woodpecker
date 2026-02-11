# Idempotent Write

Idempotent write ensures that duplicate messages are written only once to the log, preventing data duplication caused by client retries during network timeouts or node failures.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   IdempotentWriter                       │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │ DedupWindow │    │ InnerWriter │    │  Snapshot   │  │
│  │  (dedup)    │    │   (write)   │    │   Manager   │  │
│  └─────────────┘    └─────────────┘    └─────────────┘  │
└─────────────────────────────────────────────────────────┘
```

| Component | Responsibility |
|-----------|----------------|
| **IdempotentWriter** | Wrapper layer that intercepts write requests for deduplication |
| **DedupWindow** | In-memory sliding window tracking committed idempotencyIds |
| **DedupSnapshotManager** | Persists dedup window state to object storage |

## Write Flow

```
Client Write Request (with idempotencyId)
         │
         ▼
   ┌─────────────┐
   │ Query DedupWindow │
   └─────────────┘
         │
    ┌────┴────┐
    │         │
  Exists    Not Found
    │         │
    ▼         ▼
 Return     Mark as Pending
 existing   and write to
 EntryId    InnerWriter
              │
              ▼
         Write Success?
         ┌───┴───┐
        Yes      No
         │        │
         ▼        ▼
    Update to   Remove Pending
    Committed   Return error
         │
         ▼
    Return new EntryId
```

## Key Mechanisms

### IdempotencyId Generation

If the client does not provide an idempotencyId, it is automatically derived from the MD5 hash of the payload:

```go
func deriveIdempotencyId(payload []byte) string {
    hash := md5.Sum(payload)
    return hex.EncodeToString(hash[:])
}
```

### Dedup Window

The dedup window is a bounded in-memory cache with two eviction policies:

- **Time-based**: Default 5 minutes. Committed entries older than the window duration are evicted.
- **Capacity-based**: Default 1 million entries. When exceeded, oldest entries are evicted using LRU policy.

Entry states:
- `Pending`: Write in progress. Subsequent requests with the same ID will wait.
- `Committed`: Successfully written. Returns cached result immediately.

### Concurrent Write Handling

Multiple concurrent requests with the same idempotencyId are handled efficiently:

```go
// Multiple concurrent requests with same idempotencyId
goroutine1: Write("id-1") ──┐
goroutine2: Write("id-1") ──┼──► Only one actually writes
goroutine3: Write("id-1") ──┘    Others wait and reuse the result
```

This is implemented using `sync.Cond`: the first request marks the entry as Pending and performs the write; subsequent requests wait until the status changes to Committed.

### Snapshot and Recovery

Snapshots are persisted to object storage for crash recovery:

```
┌────────────────────────────────────────┐
│         Object Storage (S3/MinIO)       │
│  logs/{logId}/dedup_snapshot/{ts}      │
│  ┌──────────────────────────────────┐  │
│  │ - LogId                          │  │
│  │ - SnapshotTimestamp              │  │
│  │ - SegmentWatermarks (high water) │  │
│  │ - Entries[] (committed entries)  │  │
│  │ - Config (window configuration)  │  │
│  └──────────────────────────────────┘  │
└────────────────────────────────────────┘
```

**Recovery Flow**:
1. Load the latest snapshot into DedupWindow
2. Read new entries written after the snapshot using SegmentWatermarks
3. Add those entries' idempotencyIds to the window
4. Recovery complete, ready to handle new requests

## Package Structure

```
woodpecker/log/
├── idempotent_writer.go      # Public interface, implements LogWriter
└── idempotent/               # Internal implementation
    ├── dedup_window.go       # Dedup window data structure
    ├── dedup_snapshot.go     # Snapshot persistence
    └── README.md             # This file
```

This design avoids import cycles:
- `idempotent_writer` is in the `log` package, directly using `WriteMessage`, `WriteResult` types
- `dedup_window` and `dedup_snapshot` are pure data structures with no dependency on the `log` package

## Usage

### Opening an Idempotent Writer

```go
// Open an idempotent writer
writer, err := logHandle.OpenIdempotentWriter(ctx)
if err != nil {
    return err
}
defer writer.Close(ctx)
```

### Writing with Custom IdempotencyId

```go
result := writer.Write(ctx, &log.WriteMessage{
    Payload:       []byte("data"),
    IdempotencyId: "unique-request-id-123",
})
if result.Err != nil {
    return result.Err
}
fmt.Printf("Written to segment %d, entry %d\n",
    result.LogMessageId.SegmentId,
    result.LogMessageId.EntryId)
```

### Retry Behavior

Retrying with the same idempotencyId returns the same EntryId without duplicate writes:

```go
// First write
result1 := writer.Write(ctx, &log.WriteMessage{
    Payload:       []byte("data"),
    IdempotencyId: "request-123",
})

// Retry with same idempotencyId (e.g., after timeout)
result2 := writer.Write(ctx, &log.WriteMessage{
    Payload:       []byte("data"),
    IdempotencyId: "request-123",
})

// result1.LogMessageId == result2.LogMessageId
// Only one entry is written to the log
```

### Auto-derived IdempotencyId

If no idempotencyId is provided, it is derived from the payload hash:

```go
// IdempotencyId will be derived from MD5(payload)
result := writer.Write(ctx, &log.WriteMessage{
    Payload: []byte("data"),
})
```

## Configuration

```go
type IdempotentWriterConfig struct {
    // How often to save snapshots (default: 1 minute)
    SnapshotInterval time.Duration

    // Dedup window duration (default: 5 minutes)
    WindowDuration time.Duration

    // Maximum entries in dedup window (default: 1,000,000)
    MaxKeys int
}
```

## Design Considerations

1. **Memory Efficiency**: The dedup window uses both time and capacity bounds to prevent unbounded memory growth.

2. **Crash Recovery**: Periodic snapshots combined with gap reading ensure no committed entries are lost.

3. **Concurrent Safety**: All operations are thread-safe using fine-grained locking and condition variables.

4. **Exactly-Once Semantics**: Combined with client-provided idempotencyIds, this provides exactly-once write semantics within the dedup window duration.
