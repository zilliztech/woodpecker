# StagedFileWriter Goroutine Elimination — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the per-writer `run()` goroutine and `flushTaskChan` from `StagedFileWriter`, replacing with `time.AfterFunc` + shared `conc.Pool` to reduce goroutine count from O(N_logs) to O(NumCPU).

**Architecture:** `WriteDataAsync()` appends to buffer. On first write, a `time.AfterFunc(syncInterval)` is registered via CAS on `syncScheduled`. When the timer fires, it submits `Sync()` to a shared `conc.Pool`. `Sync()` now does the full cycle: roll buffer + processFlushTask (disk I/O) + notify callers. The `flushTaskChan` and `run()` goroutine are eliminated entirely.

**Tech Stack:** Go stdlib (`time.AfterFunc`, `sync/atomic`), existing `common/conc.Pool` (ants-based)

**Scope:** Only `StagedFileWriter` (service mode, multi-tenant). `LocalFileWriter` and `MinioFileWriter` unchanged.

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `server/storage/stagedstorage/writer_impl.go` | **Modify** | Core refactor: remove `run()`, `flushTaskChan`, `awaitAllFlushTasks`; refactor `Sync()`, `WriteDataAsync()`, `Close()`; add `syncScheduled`, `syncPool` fields |
| `server/storage/stagedstorage/writer_impl_test.go` | **Modify** | Update tests for new constructor signature (syncPool param) |
| `server/processor/segment_processor.go` | **Modify** | Accept `syncPool` param, pass to `StagedFileWriter` |
| `server/logstore.go` | **Modify** | Create/own `syncPool`, pass to `SegmentProcessor` |
| `tests/benchmark/scalability_test.go` | **Run** | Re-run baseline benchmarks to validate improvement |

---

### Task 1: Add `syncPool` to LogStore

**Files:**
- Modify: `server/logstore.go:70-103` (struct + constructor + Start/Stop)

- [ ] **Step 1: Add syncPool field and import**

In `server/logstore.go`, add to imports:

```go
"runtime"

"github.com/zilliztech/woodpecker/common/conc"
```

Add field to `logStore` struct (after `rejectWrites` field, ~line 84):

```go
syncPool *conc.Pool[struct{}] // shared worker pool for StagedFileWriter sync operations
```

- [ ] **Step 2: Initialize syncPool in NewLogStore**

In `NewLogStore()` (~line 87-103), add pool creation before the return:

```go
func NewLogStore(ctx context.Context, cfg *config.Configuration, storageClient storageclient.ObjectStorage) LogStore {
	ctx, cancel := context.WithCancel(ctx)
	logStore := &logStore{
		cfg:               cfg,
		ctx:               ctx,
		cancel:            cancel,
		storageClient:     storageClient,
		segmentProcessors: make(map[string]map[int64]processor.SegmentProcessor),
		address:           net.GetIP(""),
		cleanupDone:       make(chan struct{}),
		syncPool:          conc.NewPool[struct{}](runtime.NumCPU() * 2),
	}
	logStore.stopped.Store(true)

	logger.Ctx(ctx).Info("LogStore created successfully",
		zap.String("address", logStore.address),
		zap.Int("syncPoolSize", runtime.NumCPU()*2))

	return logStore
}
```

- [ ] **Step 3: Release syncPool in Stop()**

In `Stop()` (~line 122-163), add pool release before clearing the maps:

```go
// Release the sync worker pool
if l.syncPool != nil {
	l.syncPool.Release()
}
```

Add this after the segment processor cleanup loop and before `l.segmentProcessors = make(...)`.

- [ ] **Step 4: Verify compilation**

Run: `go build ./server/...`
Expected: Clean compilation (syncPool is created but not yet passed anywhere)

- [ ] **Step 5: Commit**

```bash
git add server/logstore.go
git commit -m "feat(logstore): add shared syncPool for StagedFileWriter sync operations"
```

---

### Task 2: Thread syncPool through SegmentProcessor

**Files:**
- Modify: `server/processor/segment_processor.go:65-79,83-100,384-456`
- Modify: `server/logstore.go:215-266`

- [ ] **Step 1: Add syncPool field to segmentProcessor**

In `segment_processor.go`, add import:

```go
"github.com/zilliztech/woodpecker/common/conc"
```

Add field to `segmentProcessor` struct (~line 83-100):

```go
syncPool *conc.Pool[struct{}] // shared sync worker pool (passed from LogStore)
```

- [ ] **Step 2: Update NewSegmentProcessor signature**

Change `NewSegmentProcessor` (~line 65) to accept `syncPool`:

```go
func NewSegmentProcessor(ctx context.Context, cfg *config.Configuration, userBucketName string, userRootPath string, logId int64, segId int64, storageClient storageclient.ObjectStorage, syncPool *conc.Pool[struct{}]) SegmentProcessor {
	ctime := time.Now().UnixMilli()
	logger.Ctx(ctx).Info("new segment processor created", zap.Int64("ctime", ctime), zap.Int64("logId", logId), zap.Int64("segId", segId))
	s := &segmentProcessor{
		cfg:           cfg,
		bucketName:    userBucketName,
		rootPath:      userRootPath,
		logId:         logId,
		segId:         segId,
		storageClient: storageClient,
		createTime:    ctime,
		syncPool:      syncPool,
	}
	s.lastAccessTime.Store(ctime)
	return s
}
```

- [ ] **Step 3: Pass syncPool when creating StagedFileWriter**

In `getOrCreateSegmentWriter()` (~line 407-416), update the `StagedFileWriter` creation call within the `case "service":` block. The exact change depends on Task 3 (which adds the syncPool parameter to StagedFileWriter). For now, note the location — this step will be completed after Task 3.

- [ ] **Step 4: Update LogStore.getOrCreateSegmentProcessor to pass syncPool**

In `server/logstore.go`, `getOrCreateSegmentProcessor()` (~line 247):

Change:
```go
processor.NewSegmentProcessor(ctx, l.cfg, bucketName, rootPath, logId, segmentId, l.storageClient)
```

To:
```go
processor.NewSegmentProcessor(ctx, l.cfg, bucketName, rootPath, logId, segmentId, l.storageClient, l.syncPool)
```

- [ ] **Step 5: Verify compilation**

Run: `go build ./server/...`
Expected: May have errors because StagedFileWriter doesn't accept syncPool yet — that's OK, will be fixed in Task 3.

- [ ] **Step 6: Commit (after Task 3 compiles)**

This will be committed together with Task 3.

---

### Task 3: Refactor StagedFileWriter — Core Changes

**Files:**
- Modify: `server/storage/stagedstorage/writer_impl.go`

This is the largest task. We modify the struct, constructor, `Sync()`, `WriteDataAsync()`, `Close()`, and remove `run()`, `flushTaskChan`, `awaitAllFlushTasks()`, `rollBufferAndSubmitFlushTaskUnsafe()`.

- [ ] **Step 1: Add new fields, remove old fields from struct**

In the `StagedFileWriter` struct (~line 64-121):

**Remove** these fields:
```go
flushTaskChan                chan *blockFlushTask
allUploadingTaskDone         atomic.Bool
lastSubmittedFlushingBlockID atomic.Int64
fileClose                    chan struct{}
runCtx                       context.Context
runCancel                    context.CancelFunc
```

**Add** these fields (in place of the removed ones):
```go
// Shared sync worker pool (owned by LogStore, shared across all writers)
syncPool      *conc.Pool[struct{}]
// CAS flag: true = a timer is pending, prevents duplicate AfterFunc registration
syncScheduled atomic.Bool
// Sync interval for periodic flush (from config: MaxIntervalForLocalStorage)
syncInterval  time.Duration
```

**Keep** these fields (they are still used):
```go
lastSubmittedFlushingEntryID atomic.Int64
flushMu                      sync.Mutex
storageWritable              atomic.Bool
```

- [ ] **Step 2: Update constructor — remove `run()`, add syncPool param**

Change `NewStagedFileWriter` signature (~line 124-126):

```go
func NewStagedFileWriter(ctx context.Context, bucket string, rootPath string, localBaseDir string, logId int64, segmentId int64, storageCli objectstorage.ObjectStorage, cfg *config.Configuration, syncPool *conc.Pool[struct{}]) (*StagedFileWriter, error) {
	return NewStagedFileWriterWithMode(ctx, bucket, rootPath, localBaseDir, logId, segmentId, storageCli, cfg, false, syncPool)
}
```

Change `NewStagedFileWriterWithMode` signature (~line 129):

```go
func NewStagedFileWriterWithMode(ctx context.Context, bucket string, rootPath string, localBaseDir string, logId int64, segmentId int64, storageCli objectstorage.ObjectStorage, cfg *config.Configuration, recoveryMode bool, syncPool *conc.Pool[struct{}]) (*StagedFileWriter, error) {
```

In the writer struct initialization (~line 166-185), **remove**:
```go
flushTaskChan:       make(chan *blockFlushTask, flushQueueSize),
runCtx:              runCtx,
runCancel:           runCancel,
```

**Add**:
```go
syncPool:     syncPool,
syncInterval: time.Duration(maxInterval) * time.Millisecond,
```

Remove `runCtx, runCancel := context.WithCancel(context.Background())` (~line 164).

Remove the `go writer.run()` line (~line 283).

Remove the init of `allUploadingTaskDone` and `lastSubmittedFlushingBlockID` (~lines 198-199).

Add import for `"time"` if not already present, and `"github.com/zilliztech/woodpecker/common/conc"`.

- [ ] **Step 3: Rewrite `Sync()` — merge roll buffer + processFlushTask**

Replace `Sync()` (~line 354-389) and `rollBufferAndSubmitFlushTaskUnsafe()` (~line 392-467):

```go
// Sync forces immediate sync of all buffered data.
// This method performs the full cycle: roll buffer → write to disk (fsync) → notify callers.
// It is safe to call from multiple goroutines; flushMu serializes disk writes.
func (w *StagedFileWriter) Sync(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "Sync")
	defer sp.End()
	startTime := time.Now()

	// Phase 1: Roll buffer (under mu)
	w.mu.Lock()

	if !w.storageWritable.Load() {
		w.mu.Unlock()
		return nil
	}

	currentBuffer := w.buffer.Load()
	if currentBuffer == nil {
		w.mu.Unlock()
		return nil
	}

	expectedNextEntryId := currentBuffer.ExpectedNextEntryId.Load()
	if expectedNextEntryId-currentBuffer.FirstEntryId == 0 {
		w.mu.Unlock()
		return nil
	}

	// Read entries to flush
	toFlushEntries, err := currentBuffer.ReadEntriesRange(currentBuffer.GetFirstEntryId(), currentBuffer.GetExpectedNextEntryId())
	if err != nil {
		w.mu.Unlock()
		logger.Ctx(ctx).Warn("Sync: error reading entries from buffer", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Error(err))
		return err
	}
	if len(toFlushEntries) == 0 {
		w.mu.Unlock()
		return nil
	}

	// Create flush task
	blockNumber := w.currentBlockNumber.Load()
	flushTask := &blockFlushTask{
		entries:      toFlushEntries,
		firstEntryId: toFlushEntries[0].EntryId,
		lastEntryId:  toFlushEntries[len(toFlushEntries)-1].EntryId,
		blockNumber:  int32(blockNumber),
	}

	// Switch to new buffer
	restData, err := currentBuffer.ReadEntriesToLast(expectedNextEntryId)
	if err != nil {
		w.mu.Unlock()
		return err
	}
	newBuffer := cache.NewSequentialBufferWithData(w.logId, w.segmentId, expectedNextEntryId, w.maxBufferEntries, restData, w.nsStr)

	w.lastSubmittedFlushingEntryID.Store(flushTask.lastEntryId)
	w.currentBlockNumber.Add(1)
	w.lastSyncTimestamp.Store(time.Now().UnixMilli())
	w.buffer.Store(newBuffer)

	w.mu.Unlock()

	metrics.WpFileOperationsTotal.WithLabelValues(metrics.NodeID, w.nsStr, w.logIdStr, "rollBuffer", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(metrics.NodeID, w.nsStr, w.logIdStr, "rollBuffer", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	// Phase 2: Write to disk (under flushMu — serializes disk I/O per writer)
	w.flushMu.Lock()
	w.processFlushTask(ctx, flushTask)
	w.flushMu.Unlock()

	metrics.WpFileOperationsTotal.WithLabelValues(metrics.NodeID, w.nsStr, w.logIdStr, "sync", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(metrics.NodeID, w.nsStr, w.logIdStr, "sync", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return nil
}
```

**Delete** the old `rollBufferAndSubmitFlushTaskUnsafe()` method entirely (~line 392-467).

- [ ] **Step 4: Rewrite `WriteDataAsync()` — add AfterFunc trigger**

In `WriteDataAsync()` (~line 713-818), replace the sync trigger section. After `w.mu.Unlock()` (~line 789) and the debug log, replace lines 799-816:

```go
	// Immediate sync: buffer size or entry count exceeded threshold
	if bufferSize >= w.maxFlushSize || entryCount >= w.maxBufferEntries {
		logger.Ctx(ctx).Info("Triggering immediate sync from WriteDataAsync",
			zap.Int64("bufferSize", bufferSize),
			zap.Int64("entryCount", entryCount))
		if w.syncPool != nil {
			w.syncPool.Submit(func() (struct{}, error) {
				w.Sync(context.Background())
				return struct{}{}, nil
			})
		} else {
			// Fallback for tests without pool
			w.Sync(ctx)
		}
		return entryId, nil
	}

	// Periodic sync: register AfterFunc on first write in this window
	if w.syncScheduled.CompareAndSwap(false, true) {
		syncInterval := w.syncInterval
		if w.syncPool != nil {
			time.AfterFunc(syncInterval, func() {
				w.syncScheduled.Store(false)
				w.syncPool.Submit(func() (struct{}, error) {
					w.Sync(context.Background())
					return struct{}{}, nil
				})
			})
		} else {
			// Fallback for tests without pool
			time.AfterFunc(syncInterval, func() {
				w.syncScheduled.Store(false)
				w.Sync(context.Background())
			})
		}
	}
```

- [ ] **Step 5: Rewrite `Close()` — simplified without run() goroutine**

Replace `Close()` (~line 962-1003):

```go
func (w *StagedFileWriter) Close(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "Close")
	defer sp.End()
	startTime := time.Now()
	if !w.closed.CompareAndSwap(false, true) {
		logger.Ctx(ctx).Info("Close: already closed, skip", zap.String("inst", fmt.Sprintf("%p", w)))
		return nil
	}

	defer func() {
		if w.file != nil {
			w.file.Close()
			w.file = nil
		}
	}()

	logger.Ctx(ctx).Info("Close: trigger final sync", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId))

	// Final sync: flush any remaining buffered data directly (not via pool)
	err := w.Sync(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("sync error during close",
			zap.String("segmentFilePath", w.segmentFilePath),
			zap.Error(err))
		metrics.WpFileOperationsTotal.WithLabelValues(metrics.NodeID, w.nsStr, w.logIdStr, "close", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(metrics.NodeID, w.nsStr, w.logIdStr, "close", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return err
	}

	metrics.WpFileOperationsTotal.WithLabelValues(metrics.NodeID, w.nsStr, w.logIdStr, "close", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(metrics.NodeID, w.nsStr, w.logIdStr, "close", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}
```

- [ ] **Step 6: Delete `run()` and `awaitAllFlushTasks()`**

Delete `run()` method (~line 299-351) entirely.
Delete `awaitAllFlushTasks()` method (~line 638-690) entirely.
Delete the `blockFlushTask` struct if it's only used internally — but keep it since `processFlushTask` still uses it as a local data transfer type.

- [ ] **Step 7: Update `GetBlockCount()`**

`GetBlockCount()` (~line 957-959) currently reads `lastSubmittedFlushingBlockID` which we removed. Change it to use `currentBlockNumber`:

```go
func (w *StagedFileWriter) GetBlockCount(ctx context.Context) int64 {
	return w.currentBlockNumber.Load()
}
```

- [ ] **Step 8: Verify compilation**

Run: `go build ./server/...`
Expected: Clean compilation

- [ ] **Step 9: Commit**

```bash
git add server/storage/stagedstorage/writer_impl.go server/processor/segment_processor.go server/logstore.go
git commit -m "feat(staged-writer): replace run() goroutine with AfterFunc + conc.Pool

Remove per-writer goroutine and flushTaskChan from StagedFileWriter.
Sync is now triggered by time.AfterFunc (CAS-guarded, one per 5ms window)
and executed in a shared conc.Pool (NumCPU*2 workers).
This reduces goroutine count from O(N_logs) to O(NumCPU) for service mode."
```

---

### Task 4: Update SegmentProcessor to pass syncPool to StagedFileWriter

**Files:**
- Modify: `server/processor/segment_processor.go:407-422`

- [ ] **Step 1: Update StagedFileWriter creation call**

In `getOrCreateSegmentWriter()`, within the `case "service":` block (~line 407-416), update the call:

Change:
```go
stagedstorage.NewStagedFileWriterWithMode(ctx, s.getInstanceBucket(), s.getLogBaseDir(), path.Join(s.cfg.Woodpecker.Storage.RootPath, s.getInstanceBucket(), s.getLogBaseDir()), s.logId, s.segId, s.storageClient, s.cfg, recoverMode)
```

To:
```go
stagedstorage.NewStagedFileWriterWithMode(ctx, s.getInstanceBucket(), s.getLogBaseDir(), path.Join(s.cfg.Woodpecker.Storage.RootPath, s.getInstanceBucket(), s.getLogBaseDir()), s.logId, s.segId, s.storageClient, s.cfg, recoverMode, s.syncPool)
```

- [ ] **Step 2: Verify full compilation**

Run: `go build ./...`
Expected: Clean compilation of entire project

- [ ] **Step 3: Commit**

```bash
git add server/processor/segment_processor.go
git commit -m "feat(processor): pass syncPool to StagedFileWriter in service mode"
```

---

### Task 5: Update Tests

**Files:**
- Modify: `server/storage/stagedstorage/writer_impl_test.go`

- [ ] **Step 1: Update all test helper calls to pass syncPool=nil**

All tests that call `NewStagedFileWriter` or `NewStagedFileWriterWithMode` need to pass `nil` as the `syncPool` parameter. When `syncPool` is nil, `WriteDataAsync()` falls back to direct `Sync()` call and `time.AfterFunc` with direct execution.

Search for all calls and add `nil` as the last argument:

```go
// Before:
NewStagedFileWriter(ctx, "test-bucket", "test-root", dir, 1, 0, mockStorage, cfg)
// After:
NewStagedFileWriter(ctx, "test-bucket", "test-root", dir, 1, 0, mockStorage, cfg, nil)

// Before:
NewStagedFileWriterWithMode(ctx, "test-bucket", "test-root", dir, 1, 0, mockStorage, cfg, false)
// After:
NewStagedFileWriterWithMode(ctx, "test-bucket", "test-root", dir, 1, 0, mockStorage, cfg, false, nil)
```

- [ ] **Step 2: Run tests**

Run: `go test -short -count=1 -v ./server/storage/stagedstorage/...`
Expected: All existing tests pass

- [ ] **Step 3: Add new test for AfterFunc sync behavior**

Add to `writer_impl_test.go`:

```go
func TestStagedFileWriter_AfterFuncSync(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	// Create writer without pool (nil) - uses fallback direct sync
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, mockStorage, cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, writer)
	defer writer.Close(context.Background())

	// Write entry - should trigger AfterFunc
	resultCh := channel.NewDefaultResultChannel()
	_, err = writer.WriteDataAsync(context.Background(), 0, []byte("test-data"), resultCh)
	require.NoError(t, err)

	// syncScheduled should be true (timer pending)
	assert.True(t, writer.syncScheduled.Load())

	// Wait for AfterFunc to fire + sync to complete
	time.Sleep(time.Duration(cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage.Milliseconds()+50) * time.Millisecond)

	// syncScheduled should be false (timer fired)
	assert.False(t, writer.syncScheduled.Load())

	// Data should be flushed
	assert.Greater(t, writer.lastEntryID.Load(), int64(-1))
}
```

- [ ] **Step 4: Add test for CAS deduplication**

```go
func TestStagedFileWriter_CAS_Dedup(t *testing.T) {
	dir := t.TempDir()
	cfg := newTestConfig(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, 1, 0, mockStorage, cfg, nil)
	require.NoError(t, err)
	defer writer.Close(context.Background())

	// Write multiple entries rapidly — only one AfterFunc should be registered
	for i := int64(0); i < 10; i++ {
		_, err := writer.WriteDataAsync(context.Background(), i, []byte(fmt.Sprintf("data-%d", i)), nil)
		require.NoError(t, err)
	}

	// syncScheduled should be true (one timer pending)
	assert.True(t, writer.syncScheduled.Load())

	// Wait for sync
	time.Sleep(time.Duration(cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage.Milliseconds()+100) * time.Millisecond)

	// All 10 entries should be flushed in a single sync
	assert.Equal(t, int64(9), writer.lastEntryID.Load())
}
```

- [ ] **Step 5: Run all tests**

Run: `go test -short -count=1 ./server/storage/stagedstorage/...`
Expected: All tests pass

- [ ] **Step 6: Commit**

```bash
git add server/storage/stagedstorage/writer_impl_test.go
git commit -m "test(staged-writer): update tests for AfterFunc sync mechanism"
```

---

### Task 6: Run Scalability Benchmarks

**Files:**
- Run: `tests/benchmark/scalability_test.go`

- [ ] **Step 1: Run goroutine growth benchmark**

Run: `go test -v -timeout 30m -run TestScalability_GoroutineGrowth ./tests/benchmark/`
Expected: Goroutines/log should drop from 1.00 to ~0.00 (no per-writer goroutine)

- [ ] **Step 2: Run write latency benchmark**

Run: `go test -v -timeout 30m -run 'TestScalability_WriteLatency' ./tests/benchmark/`
Expected: 10K+ log scenarios should no longer timeout. Memory usage should be significantly reduced.

- [ ] **Step 3: Run idle CPU overhead benchmark**

Run: `go test -v -timeout 5m -run TestScalability_IdleCPUOverhead ./tests/benchmark/`
Expected: Allocation rate should drop from 734 MB/sec to near 0 (no ticker wake-ups).

- [ ] **Step 4: Run cold start latency**

Run: `go test -v -timeout 5m -run TestScalability_ColdStartLatency ./tests/benchmark/`
Expected: Should improve slightly (no goroutine spawn on cold start).

- [ ] **Step 5: Record results and update design doc**

Update `docs/design-100k-log-scalability.md` Section 0 with "After Phase 1+2" column in the benchmark tables. Mark Phase 1+2 as DONE in the progress section.

- [ ] **Step 6: Commit**

```bash
git add docs/design-100k-log-scalability.md
git commit -m "docs: update baseline benchmarks with Phase 1+2 results"
```

---

### Task 7: Run Full Test Suite

- [ ] **Step 1: Run all unit tests**

Run: `go test -short -count=1 ./...`
Expected: All tests pass. Watch for any test that creates `StagedFileWriter` directly.

- [ ] **Step 2: Fix any broken tests**

If any test outside `stagedstorage/` calls `NewStagedFileWriter` or `NewStagedFileWriterWithMode`, add the `nil` syncPool argument.

Search for call sites:
```bash
grep -r "NewStagedFileWriter" --include="*.go" .
```

- [ ] **Step 3: Final commit**

```bash
git add -A
git commit -m "fix: update remaining StagedFileWriter call sites for syncPool param"
```
