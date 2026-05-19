# Design: Staged Writer Sync Policy Scalability

> **Status**: WIP
> **Issue**: [#103](https://github.com/zilliztech/woodpecker/issues/103)
> **Branch**: `enhance_sync_policy`

## Background

In service mode, `StagedFileWriter` is the multi-tenant writer used by server-side segment processors. Before this change, each active writer owned a dedicated `run()` goroutine, ticker, and flush channel. With many idle logs, this produced one goroutine and one ticker per writable segment even when no data was being appended.

That model does not scale to a single server hosting tens of thousands of logs. Idle writers still consume stack memory, scheduler slots, ticker activity, and channel buffers.

## Goal

Optimize only the service-mode sync path:

- Remove the per-writer sync goroutine and ticker from `StagedFileWriter`.
- Keep local and object-storage writers unchanged.
- Preserve write ordering, finalization correctness, and existing public behavior.
- Expose enough sync-pool state to diagnose queue pressure.

Client-side polling, LAC coalescing, gRPC connection limits, and unrelated lifecycle fixes are intentionally outside this PR scope.

## Design

### Shared Sync Pool

`LogStore` owns one shared `conc.Pool[struct{}]` sized at `runtime.NumCPU() * 2`. The pool is passed through `SegmentProcessor` into each service-mode `StagedFileWriter`.

This changes sync execution from:

```text
one writer -> one goroutine + one ticker
```

to:

```text
many writers -> one shared worker pool
```

### Write-Triggered Scheduling

`WriteDataAsync()` still appends into the writer buffer under `mu`. After appending:

- If the buffer exceeds the size or entry-count threshold, it submits a sync task immediately.
- Otherwise, it registers a single delayed sync with `time.AfterFunc`.
- `syncScheduled` is an atomic CAS guard so each writer has at most one pending delayed sync window.
- `syncTaskSubmitted` is an atomic CAS guard so each writer has at most one sync task queued or running in the shared pool.

This keeps idle writers quiet while still batching writes within the configured local-storage sync interval.

If a writer receives another sync request while one is already queued or running, it does not submit another pool task. It schedules one delayed recheck instead. When the current sync task finishes, it also schedules a delayed recheck if the buffer received more data while the flush was in progress. This prevents a hot writer from filling the shared pool with duplicate sync tasks.

### Synchronous Flush Path

`Sync()` now performs the complete flush cycle:

1. Lock `syncMu` to serialize the whole sync cycle for this writer.
2. Lock `mu` and roll the current buffer into a flush task.
3. Swap in a fresh buffer and update submitted flush state.
4. Release `mu`.
5. Write the block to disk under `flushMu`.
6. Notify pending write result channels.

The old `flushTaskChan`, `run()` loop, and `awaitAllFlushTasks()` path are removed.

`syncMu` preserves per-writer block order even if multiple sync requests are triggered by timers, threshold checks, or explicit `Sync()` calls. `WriteDataAsync()` does not take `syncMu`, so new writes can still append to the fresh buffer while the previous buffer is being flushed.

### Finalize Interaction

`Finalize()` first calls `Sync()` to drain pending buffered data. After the drain, it locks both `mu` and `flushMu` before writing index records and footer.

The lock order is:

```text
mu -> flushMu
```

This matches the sync path and prevents an already scheduled sync task from writing blocks concurrently with footer finalization.

### Snapshot And Metrics

The old flush queue fields are now backed by sync-pool state for staged writers:

- `flush_queue_depth` maps to submitted sync-pool work (`running + waiting`).
- `flush_queue_capacity` maps to pool capacity.
- detailed snapshot fields expose submitted, running, waiting, and capacity separately.

Prometheus metrics expose sync-pool running workers and capacity at the server level.

## Expected Impact

The main improvement is eliminating idle per-writer goroutines and tickers. At high log counts, goroutine count becomes bounded by the shared pool and runtime/system goroutines instead of growing linearly with the number of writers.

The trade-off is that many active writers can queue behind the shared pool. This is intentional: the pool converts unbounded goroutine growth into bounded sync concurrency.

## Files

Core implementation:

- `server/storage/stagedstorage/writer_impl.go`
- `server/storage/stagedstorage/snapshot.go`
- `server/storage/snapshot.go`
- `server/logstore.go`
- `server/processor/segment_processor.go`
- `common/conc/pool.go`
- `common/metrics/service_metrics.go`

Validation:

- `server/storage/stagedstorage/writer_impl_test.go`
- `server/processor/segment_processor_test.go`
- `tests/integration/staged_file_crash_test.go`
- `tests/integration/staged_file_rw_test.go`
- `tests/benchmark/scalability_test.go`
