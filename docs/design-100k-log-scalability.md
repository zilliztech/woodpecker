# Design: 100K Log Scalability on Single Server Node

> **Status**: WIP
> **Issue**: [#103](https://github.com/zilliztech/woodpecker/issues/103)
> **Branch**: `enhance_sync_policy`

---

## 1. Background & Goal

### 1.1 Problem

Woodpecker's server-side architecture currently allocates a dedicated goroutine (`run()`) with a 5ms ticker per writable segment for periodic sync. In multi-tenant service mode, a single server node may host 100K+ logs from thousands of clients. Each log has at least one active segment, resulting in 100K goroutines + 100K tickers:

- **Memory exhaustion**: 100K goroutine stacks ~600MB + heap ~4GB, exceeding 8GB VM capacity
- **Scheduler pressure**: Go runtime efficiency degrades severely managing 100K goroutine context switches
- **GC storms**: Idle tickers still generate ~734 MB/sec of memory allocation pressure, saturating GC
- **Unavailable**: Benchmarks show 40K+ logs timeout during creation alone

### 1.2 Goal

**A single server node supports 100K logs, with ~1K active (receiving writes) and 99K idle. Target hardware: 4C8G VM.**

### 1.3 Scope

- **Server-side** is multi-tenant — this is the optimization focus
- **Client-side** is per-user, typically 16 logs per instance (max ~100) — optimization focuses on reducing unnecessary gRPC interactions
- **Only service mode** (`StagedFileWriter`) is modified — the only multi-tenant scenario. Local and MinIO modes are embed/single-tenant and do not face large log counts

### 1.4 Storage Backends

| Backend | Storage Type | Deployment | Tenancy | Modified |
|---------|-------------|-----------|---------|----------|
| Local Disk | `local` | Embed | Single | No |
| Object Storage | `minio` | Embed | Single | No |
| **Service (Staged)** | `service` | **Distributed** | **Multi** | **Yes** |

---

## 2. Problem Analysis

### 2.1 Server-Side: Per-Writer Goroutine Explosion

Each writable segment on the server consumes:

| Component | Goroutines | Tickers | Channels | FDs |
|-----------|-----------|---------|----------|-----|
| `StagedFileWriter.run()` | 1 | 1 (5ms) | flushTaskChan(300) | 1 |
| SegmentProcessor | 0 | 0 | 0 | 0 |
| **Total per segment** | **1** | **1** | **300 slots** | **1** |

At 100K segments: 100K goroutines + 100K tickers + 30M channel buffer slots.

The server cannot distinguish active from idle segments — the `run()` goroutine and ticker start at writer creation and persist until `Close()`, regardless of write activity.

### 2.2 Client-Side: Unnecessary gRPC Interactions

| Issue | Description | Impact |
|-------|-------------|--------|
| **Tail read empty polling** | Fixed 200ms polling interval, 5 RPCs/sec when no data | Wastes server CPU and bandwidth |
| **Frequent LAC sync** | `UpdateLastAddConfirmed` sent to all quorum nodes per completed entry batch | ~1000 RPCs/sec under high throughput |
| **Redundant compaction RPCs** | Already-compacted segments still receive `CompactSegment` RPCs each auditor cycle | Idle-period gRPC waste |

### 2.3 Connection Layer: Missing Protection

The gRPC server had no keepalive configuration, idle connection reclamation, concurrent stream limits, or connection count caps. The client connection pool is per-Client instance (not per-process), so a per-log Client anti-pattern would create connections proportional to log count.

---

## 3. Optimization Design

### 3.1 Server Core: AfterFunc + conc.Pool Replacing Per-Writer Goroutine

**Core idea**: Remove `StagedFileWriter`'s `run()` goroutine and internal ticker. Write-triggered sync scheduling uses `time.AfterFunc` + CAS on-demand registration. Sync execution uses a shared `conc.Pool` (existing ants-based goroutine pool).

```
BEFORE:                               AFTER:
 Per writer:                            WriteDataAsync():
   run() goroutine                        buffer.Append(entry)
   + 5ms ticker                           if CAS(syncScheduled, false->true):
   + flushTaskChan(300)                       time.AfterFunc(5ms, ...)

 N goroutines, N tickers               AfterFunc fires:
                                          syncPool.Submit(writer.Sync)

                                        syncPool = conc.Pool (NumCPU x 2)
                                        Constant ~16 goroutines, independent of log count
```

**CAS ensures batching within each 5ms window**: The `syncScheduled` CAS guarantees at most one pending timer per writer. All writes within the window are batched into a single `Sync()` call (one block), improving both throughput and disk efficiency.

**`Sync()` redesign**: Merges buffer roll + processFlushTask into one synchronous operation. Phase 1 (roll buffer under `mu`) then Phase 2 (write to disk + fsync under `flushMu`) then notify callers.

### 3.2 Client gRPC Reduction

| Optimization | Approach | Effect |
|-------------|----------|--------|
| **Tail read backoff** | Adaptive exponential backoff: 200ms to 5s, reset on data received | 25x fewer empty polls |
| **LAC sync coalescing** | 50ms window with CAS + AfterFunc, same pattern as writer sync | 50x fewer RPCs |
| **Auditor skip** | Track compacted segments client-side, skip redundant CompactSegment RPCs | Near-zero idle gRPC |

### 3.3 Connection Layer Hardening

| Optimization | Approach |
|-------------|----------|
| **gRPC Keepalive** | MaxConnectionIdle=5min, MaxConnectionAge=30min, ping 30s/timeout 10s |
| **Connection limit** | `LimitedListener` semaphore wrapper, configurable `MaxConnections` |
| **Concurrency control** | MaxConcurrentStreams=200 |
| **Client idle cleanup** | Scan every 1min, close connections unused for 5min |

### 3.4 Server Resource Management

| Optimization | Approach |
|-------------|----------|
| **Aggressive idle cleanup** | MaxIdleTime 5min to 1min, CleanupInterval 60s to 30s |
| **SyncPool metrics** | New `sync_pool_running` / `sync_pool_capacity` Prometheus gauges |

---

## 4. Implementation Steps & Status

### Phase 0: Connection Layer Hardening DONE

| Task | Description | Files Changed |
|------|-------------|---------------|
| T0.1 | gRPC server keepalive + MaxConcurrentStreams | `server/service.go` |
| T0.2 | Server connection count limit (LimitedListener) | `server/limited_listener.go` (new), `server/service.go`, `common/config/configuration.go` |
| T0.3 | Client connection pool idle cleanup (5min) | `woodpecker/client/logstore_client_pool_remote.go` |
| T0.4 | Client usage best-practice documentation | `woodpecker/client/logstore_client_pool.go`, `woodpecker/woodpecker_client.go` |

### Phase 1+2: StagedFileWriter Core Refactor DONE

| Task | Description | Files Changed |
|------|-------------|---------------|
| T1.1 | Create shared syncPool (conc.Pool, NumCPU x 2) in LogStore | `server/logstore.go` |
| T1.2 | Rewrite Sync() to merge roll buffer + processFlushTask | `server/storage/stagedstorage/writer_impl.go` |
| T1.3 | Add AfterFunc + CAS trigger in WriteDataAsync() | `server/storage/stagedstorage/writer_impl.go` |
| T1.4 | Remove run() goroutine + flushTaskChan + awaitAllFlushTasks | `server/storage/stagedstorage/writer_impl.go` |
| T1.5 | Thread syncPool through SegmentProcessor | `server/processor/segment_processor.go` |
| T1.6 | Test validation + benchmark comparison | `writer_impl_test.go`, `tests/benchmark/scalability_test.go` |

### Phase 3: Client Tail Read Optimization DONE

| Task | Description | Files Changed |
|------|-------------|---------------|
| T3.1 | Adaptive exponential backoff (200ms to 5s, 1.5x rate) | `woodpecker/log/log_reader.go`, `log_reader_test.go` |
| T3.2 | Server-side long poll (future, deferred) | — |

### Phase 4: Client gRPC Reduction DONE

| Task | Description | Files Changed |
|------|-------------|---------------|
| T4.1 | LAC sync 50ms window coalescing (CAS + AfterFunc) | `woodpecker/segment/segment_handle.go` |
| T4.2 | Auditor skip already-compacted segments | `woodpecker/log/log_writer.go`, `internal_log_writer.go` |
| T4.3 | GetBlockCount: no change needed (returns 0 for multi-node) | — |

### Phase 5: Server Resource Limits DONE

| Task | Description | Files Changed |
|------|-------------|---------------|
| T5.1 | Aggressive idle cleanup (MaxIdleTime 5min to 1min) | `common/config/configuration.go`, `server/logstore.go` |
| T5.2 | SyncPool utilization Prometheus metrics | `common/metrics/service_metrics.go`, `server/logstore.go` |

### Phase 6: Client 10K Validation (TODO, requires cluster)

| Task | Description |
|------|-------------|
| T6.1 | Single client with 10K logs: goroutine/memory/etcd pressure benchmark |
| T6.2 | If needed: consolidate client per-log goroutines (auditor/session/cleanup) |

### Phase 7: Integration Testing (TODO, requires cluster)

| Task | Description |
|------|-------------|
| T7.1 | 100K logs, 1K active on 4C8G VM: end-to-end validation |
| T7.2 | Tail read optimization effectiveness verification |
| T7.3 | Correctness tests: no data loss, ordering guarantees |
| T7.4 | Mixed workload stress test: 1K writes + 10K tail readers + 89K idle |

---

## 5. Results: Before vs After

> **Test environment**: macOS Darwin 23.6.0, Apple Silicon, single node
> **Benchmark code**: `tests/benchmark/scalability_test.go`
> **Run**: `go test -v -timeout 30m -run TestScalability ./tests/benchmark/`
>
> - Before = pre-optimization (local mode, per-writer run() goroutine + 5ms ticker)
> - After = fully optimized (service mode, AfterFunc + conc.Pool)

### 5.1 Goroutine Growth

| Log Count | Before goroutines | Before avg/log | After goroutines | After avg/log | Reduction |
|-----------|-------------------|---------------|------------------|--------------|-----------|
| 100 | 105 | 1.00 | 23 | 0.16 | 78% |
| 500 | 505 | 1.00 | 23 | 0.03 | 95% |
| 1,000 | 1,005 | 1.00 | 23 | 0.02 | 98% |
| 5,000 | 5,005 | 1.00 | 22 | 0.00 | 99.6% |
| 10,000 | 10,005 | 1.00 | 17 | 0.00 | 99.8% |
| 20,000 | 20,005 | 1.00 | 11 | 0.00 | **99.95%** |
| 40,000 | (timeout) | — | ~24 | 0.00 | **N/A** |

Before: 1 goroutine per log, linear growth. After: constant ~10-23 (conc.Pool workers + system), **O(1)**.

### 5.2 Memory Usage

| Log Count | Before Heap | After Heap | Heap Reduction | Before Stack | After Stack | Stack Reduction |
|-----------|-------------|------------|---------------|-------------|-------------|----------------|
| 1,000 | 42.0 MB | 32.5 MB | 23% | 5.5 MB | 1.2 MB | 78% |
| 5,000 | 174.4 MB | 146.0 MB | 16% | 25.3 MB | 1.9 MB | 92% |
| 10,000 | 384.7 MB | 288.0 MB | 25% | 57.2 MB | 2.4 MB | 96% |
| 20,000 | 849.9 MB | 572.3 MB | **33%** | 123.3 MB | 3.6 MB | **97%** |
| 40,000 | (timeout) | 1,213 MB | **available** | — | 6.0 MB | **N/A** |

Stack memory shows the largest improvement (no per-writer goroutine stacks). Heap reduction comes from eliminating flushTaskChan buffers.

### 5.3 Write Latency Under Scale

| Total Logs | Active | Before avg | After avg | Before Sys | After Sys | After Throughput | After Create Rate |
|-----------|--------|-----------|-----------|------------|-----------|-----------------|------------------|
| 100 | 1 | 33 us | 17 us | 26 MB | 27 MB | 57K/s | 398/s |
| 1,000 | 10 | 47 us | 17 us | 96 MB | 79 MB | 449K/s | 340/s |
| 5,000 | 50 | 50 us | 183 us | 433 MB | 251 MB | 162K/s | 274/s |
| 10,000 | 100 | 74 us | 269 us | **976 MB** | **508 MB** | 157K/s | 286/s |
| 20,000 | 200 | 105 us | 445 us | **2.1 GB** | **955 MB** | 185K/s | 300/s |
| 40,000 | 400 | **TIMEOUT** | 245 us | — | **1.7 GB** | 199K/s | 300/s |
| 100,000 | 1,000 | **TIMEOUT** | creation OK | — | — | write timeout | ~250/s |

Small scale (1K or fewer): latency improved. Large scale (5K+): avg latency increases due to shared pool queuing, but Sys memory drops 42-55%. 40K logs goes from timeout to passing.

### 5.4 Segment Creation Rate

| Log Count | Before | After | Improvement |
|-----------|--------|-------|-------------|
| 1,000 | 111/s | 340/s | 3.1x |
| 10,000 | 100/s | 286/s | 2.9x |
| 20,000 | 89/s (degrading) | 300/s (stable) | 3.4x |
| 40,000 | timeout | 300/s | N/A |

Before: creation rate degrades with log count (scheduler pressure). After: stable ~300/s regardless of scale.

### 5.5 Cold Start Latency

| Operation | Before | After | Improvement |
|-----------|--------|-------|-------------|
| Cold start (first write) | 10.0 ms | **3.2 ms** | 69% |

No longer needs to spawn a goroutine on cold start.

### 5.6 Idle CPU / GC Pressure (1,000 logs, 5-second window)

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Goroutines | 1,005 | **7** | 99.3% |
| GC runs | 122 (24.4/sec) | **0** | **100% eliminated** |
| Allocation rate | 734 MB/sec | **0.0 MB/sec** | **100% eliminated** |

GC pressure and idle allocation **completely eliminated**.

### 5.7 Summary

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Goroutine growth** | O(N) per log | **O(1) constant ~20** | Architectural |
| **20K logs goroutines** | 20,005 | 11 | **99.95%** |
| **20K logs Sys memory** | 2.1 GB | 955 MB | **55%** |
| **20K logs stack** | 123 MB | 3.6 MB | **97%** |
| **Idle GC pressure** | 734 MB/sec | 0 MB/sec | **100%** |
| **Cold start latency** | 10 ms | 3.2 ms | **69%** |
| **Creation rate** | 89-119/s (degrading) | 274-398/s (stable) | **3x** |
| **40K logs** | TIMEOUT | PASS | **Available** |

### 5.8 Remaining Bottleneck

100K logs can be created (~6.5 minutes, ~250/s) but write phase times out within 30min. The bottleneck is no longer goroutines but:
- **Heap memory**: 100K writer structs + buffers consume ~2.9 GB
- **Disk I/O**: fsync contention across 100K file handles

Future optimization directions: writer memory pooling, buffer compression, FD LRU cache.

---

## 6. Risks & Open Questions

| # | Issue | Status |
|---|-------|--------|
| 1 | Shared pool queuing delay: avg latency increases at 5K+ logs | **Confirmed**: Expected trade-off, slight latency for major resource savings |
| 2 | Race between AfterFunc-triggered Sync and Close | **Resolved**: Sync checks `w.file == nil` under `mu`, Close sets file to nil under `mu` |
| 3 | Tail read backoff max latency 5s | **Confirmed**: Configurable, acceptable for most use cases |
| 4 | LAC coalescing adds up to 50ms read visibility delay | **Confirmed**: Acceptable for log tailing scenarios |
| 5 | Connection pool is per-Client instance (not per-process) | **Mitigated**: Documentation + connection limit + idle cleanup |
| 6 | Client-side 10K log goroutine/etcd pressure | **Pending**: Phase 6 validation |
| 7 | 100K log write timeout | **Pending**: Heap memory is the next bottleneck to address |
