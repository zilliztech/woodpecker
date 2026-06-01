# Log Health Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the in-progress `rw_health` draft with a lightweight, node-wide, multi-tenant per-log read/write health probe (`log_health`) derived from real traffic, exposed at `GET /admin/log-health`.

**Architecture:** A `LogHealthTracker` keeps a few `atomic.Int64` timestamps per `(bucket, rootPath, logID)`. The write path (`AddEntry`) and read path (`GetBatchEntriesAdv`) record attempt/success/failure timestamps with a shared `RLock` + atomic store (no per-write allocation, no exclusive lock). At snapshot time each log is classified Healthy/Stalled/Failed/Idle from those timestamps. One HTTP endpoint returns all tracked logs (optionally filtered to one tenant) plus a node rollup. The earlier cluster fan-out / quorum / pending-map code is deleted.

**Tech Stack:** Go, `sync/atomic`, `net/http`, `testify`.

**Spec:** `docs/superpowers/specs/2026-06-01-log-health-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `server/log_health.go` | Create (replaces `server/rw_health.go`) | `LogHealthTracker`, record methods, classification, `Snapshot`, `Server.GetLogHealth` |
| `server/rw_health.go` | Delete | superseded |
| `server/log_health_test.go` | Create (replaces `server/rw_health_test.go`) | tracker unit tests |
| `server/rw_health_test.go` | Delete | superseded |
| `server/service.go` | Modify | rename field; simplify write instrumentation; add read instrumentation |
| `server/service_test.go` | Modify | rename symbols in helpers + assertions |
| `common/http/management/log_health_handler.go` | Create (replaces `rw_health_handler.go`) | HTTP handler + optional filter parsing |
| `common/http/management/rw_health_handler.go` | Delete | superseded |
| `common/http/management/log_health_handler_test.go` | Create (replaces `rw_health_handler_test.go`) | handler tests |
| `common/http/management/rw_health_handler_test.go` | Delete | superseded |
| `common/http/router.go` | Modify | one route const `AdminLogHealthPath`, drop the two RW consts |
| `common/http/server.go` | Modify | one callback field + one registration block |
| `cmd/main.go` | Modify | one callback wiring |
| `common/http/README.md` | Modify | document `/admin/log-health`, drop the two RW rows |

**Compilation note (Go packages compile as a unit):** `cmd` imports `server` and `common/http`, which imports `common/http/management`. Renaming symbols therefore breaks the full module build until every task is done. So:
- **Task 1** keeps package `server` self-consistent → verify with `go test ./server/...` (NOT a full `go build ./...`, which stays red until Task 2).
- **Task 2** completes `management` + `common/http` + `cmd` → `go build ./...` goes green.
- **Task 3** is docs + final full verification.

---

## Task 1: Rewrite the tracker and instrument the data path (package `server`)

**Files:**
- Create: `server/log_health.go`
- Delete: `server/rw_health.go`
- Create: `server/log_health_test.go`
- Delete: `server/rw_health_test.go`
- Modify: `server/service.go` (field at `:54`, constructor at `:129`, `AddEntry` `:365-441`, `GetBatchEntriesAdv` `:452-460`)
- Modify: `server/service_test.go` (`:55`, `:509`, `:974-977`, `:1001-1004`)

- [ ] **Step 1: Write the new tracker test file**

Delete the old test and write the new one:

```bash
git rm server/rw_health_test.go
```

Create `server/log_health_test.go`:

```go
package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/membership"
)

const testStall = 10 * time.Minute

func TestLogHealth_NilTrackerSafe(t *testing.T) {
	var tr *LogHealthTracker
	// none of these should panic
	tr.RecordWriteAttempt("b", "r", 1, time.UnixMilli(1000))
	tr.RecordWriteSuccess("b", "r", 1, time.UnixMilli(1000))
	tr.RecordWriteFailure("b", "r", 1, time.UnixMilli(1000), "x")
	tr.RecordReadSuccess("b", "r", 1, time.UnixMilli(1000))
	tr.RecordReadFailure("b", "r", 1, time.UnixMilli(1000), "x")
}

func TestLogHealth_NoActivityHealthy(t *testing.T) {
	tr := NewLogHealthTracker(testStall)
	snap := tr.Snapshot("", "", "node-1", time.UnixMilli(1000))
	require.Equal(t, LogHealthStateHealthy, snap.State)
	require.Equal(t, "no_observed_activity", snap.Reason)
	require.Equal(t, 0, snap.TrackedLogs)
	require.Equal(t, "node-1", snap.NodeID)
}

func TestLogHealth_RecentWriteSuccessIsHealthy(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := NewLogHealthTracker(testStall)
	tr.RecordWriteAttempt("b", "r", 1, now)
	tr.RecordWriteSuccess("b", "r", 1, now.Add(time.Second))

	snap := tr.Snapshot("", "", "node-1", now.Add(2*time.Second))
	require.Equal(t, 1, snap.TrackedLogs)
	require.Equal(t, 1, snap.HealthyLogs)
	require.Equal(t, LogHealthStateHealthy, snap.State)
	require.Equal(t, logStateHealthy, snap.Logs[0].WriteState)
	require.Equal(t, "b", snap.Logs[0].BucketName)
	require.Equal(t, "r", snap.Logs[0].RootPath)
	require.Equal(t, int64(1), snap.Logs[0].LogID)
}

func TestLogHealth_RecentReadFailureIsFailed(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := NewLogHealthTracker(testStall)
	tr.RecordWriteSuccess("b", "r", 1, now)            // write healthy
	tr.RecordReadFailure("b", "r", 1, now.Add(time.Second), "read boom")

	snap := tr.Snapshot("", "", "node-1", now.Add(2*time.Second))
	// overall = worst(Healthy write, Failed read) = Failed
	require.Equal(t, logStateHealthy, snap.Logs[0].WriteState)
	require.Equal(t, logStateFailed, snap.Logs[0].ReadState)
	require.Equal(t, logStateFailed, snap.Logs[0].State)
	require.Equal(t, "read boom", snap.Logs[0].LastFailureReason)
}

func TestLogHealth_HungWriteBecomesStalledUnderLoad(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := NewLogHealthTracker(testStall)
	// One real success long ago, then attempts keep coming but never complete.
	tr.RecordWriteSuccess("b", "r", 1, now)
	tr.RecordWriteAttempt("b", "r", 1, now.Add(12*time.Minute)) // recent attempt, no success/failure after

	snap := tr.Snapshot("", "", "node-1", now.Add(12*time.Minute+time.Second))
	require.Equal(t, logStateStalled, snap.Logs[0].WriteState)
	require.Equal(t, 1, snap.StalledLogs)
	require.Equal(t, LogHealthStateUnhealthy, snap.State)
	require.Equal(t, "all_logs_failed_or_stalled", snap.Reason)
}

func TestLogHealth_ReadNeverStalls(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := NewLogHealthTracker(testStall)
	tr.RecordReadSuccess("b", "r", 1, now) // success then 20 min of silence on reads

	snap := tr.Snapshot("", "", "node-1", now.Add(20*time.Minute))
	// reads have no attempt timestamp; stale success -> Idle, never Stalled
	require.Equal(t, logStateIdle, snap.Logs[0].ReadState)
	require.Equal(t, logStateIdle, snap.Logs[0].State)
	require.Equal(t, 1, snap.IdleLogs)
	require.Equal(t, LogHealthStateHealthy, snap.State) // idle never forces Unhealthy
}

func TestLogHealth_IdleNodeStaysHealthy(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := NewLogHealthTracker(testStall)
	tr.RecordWriteSuccess("b", "r", 1, now) // healthy log, goes quiet
	tr.RecordWriteFailure("b", "r", 2, now, "old fail") // a stale failure

	snap := tr.Snapshot("", "", "node-1", now.Add(30*time.Minute))
	require.Equal(t, 2, snap.TrackedLogs)
	require.Equal(t, 2, snap.IdleLogs) // both went idle (stale)
	require.Equal(t, 0, snap.HealthyLogs)
	require.Equal(t, 0, snap.FailedLogs)
	require.Equal(t, LogHealthStateHealthy, snap.State) // quiet node not pulled out
}

func TestLogHealth_UnhealthyOnlyWhenAllActiveFailures(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := NewLogHealthTracker(testStall)
	tr.RecordWriteFailure("b", "r", 1, now, "f1")
	tr.RecordWriteFailure("b", "r", 2, now, "f2")

	snap := tr.Snapshot("", "", "node-1", now.Add(time.Second))
	require.Equal(t, 2, snap.FailedLogs)
	require.Equal(t, LogHealthStateUnhealthy, snap.State)
	require.Equal(t, "all_logs_failed_or_stalled", snap.Reason)
}

func TestLogHealth_HealthyIfAnyLogHealthy(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := NewLogHealthTracker(testStall)
	tr.RecordWriteSuccess("b", "r", 1, now)
	tr.RecordWriteFailure("b", "r", 2, now, "f2")

	snap := tr.Snapshot("", "", "node-1", now.Add(time.Second))
	require.Equal(t, 1, snap.HealthyLogs)
	require.Equal(t, 1, snap.FailedLogs)
	require.Equal(t, LogHealthStateHealthy, snap.State)
	require.Equal(t, "observed_read_write_success", snap.Reason)
}

func TestLogHealth_MultiTenantAndFilter(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := NewLogHealthTracker(testStall)
	tr.RecordWriteSuccess("bucket-a", "root-a", 1, now)
	tr.RecordWriteSuccess("bucket-b", "root-b", 7, now)

	all := tr.Snapshot("", "", "node-1", now.Add(time.Second))
	require.Equal(t, 2, all.TrackedLogs)
	// sorted by (bucket, root, logID)
	require.Equal(t, "bucket-a", all.Logs[0].BucketName)
	require.Equal(t, "bucket-b", all.Logs[1].BucketName)

	filtered := tr.Snapshot("bucket-b", "root-b", "node-1", now.Add(time.Second))
	require.Equal(t, 1, filtered.TrackedLogs)
	require.Equal(t, int64(7), filtered.Logs[0].LogID)
	require.Equal(t, "bucket-b", filtered.FilterBucketName)
}

func TestLogHealth_ConcurrentRecordsRaceFree(t *testing.T) {
	tr := NewLogHealthTracker(testStall)
	now := time.UnixMilli(1_000_000)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				logID := int64(i % 4)
				tr.RecordWriteAttempt("b", "r", logID, now)
				tr.RecordWriteSuccess("b", "r", logID, now)
				_ = tr.Snapshot("", "", "node-1", now)
			}
		}(g)
	}
	wg.Wait()
	snap := tr.Snapshot("", "", "node-1", now)
	require.Equal(t, 4, snap.TrackedLogs)
}

func TestServer_GetLogHealth_StatusCode(t *testing.T) {
	s := createTestServer(context.Background(), &membership.ServerConfig{NodeID: "node-1"})
	defer s.cancel()

	s.logHealthTracker.RecordWriteFailure("bucket-a", "root-a", 1, time.Now(), "sync failed")

	resp, statusCode := s.GetLogHealth(context.Background(), "", "")
	assert.Equal(t, LogHealthStateUnhealthy, resp.State)
	assert.Equal(t, 503, statusCode)
}
```

- [ ] **Step 2: Replace the implementation file**

```bash
git rm server/rw_health.go
```

Create `server/log_health.go`:

```go
package server

import (
	"context"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// Node-level rollup states.
	LogHealthStateHealthy   = "Healthy"
	LogHealthStateUnhealthy = "Unhealthy"

	// Per-log / per-direction states.
	logStateHealthy = "Healthy"
	logStateFailed  = "Failed"
	logStateStalled = "Stalled"
	logStateIdle    = "Idle"

	defaultLogHealthStallAfter = 10 * time.Minute
)

type logInstanceKey struct {
	bucketName string
	rootPath   string
}

// logHealth holds the lock-free per-log state. All fields are written from the
// hot path with atomic stores under a shared RLock.
type logHealth struct {
	writeAttemptMS atomic.Int64 // when a write started (writes can hang in-flight)
	writeSuccessMS atomic.Int64
	writeFailureMS atomic.Int64
	readSuccessMS  atomic.Int64 // reads are synchronous -> no attempt timestamp
	readFailureMS  atomic.Int64
	failureReason  atomic.Pointer[string]
}

// LogHealthTracker tracks read/write outcomes per (bucket, rootPath, logID).
type LogHealthTracker struct {
	mu         sync.RWMutex // guards map STRUCTURE only
	stallAfter time.Duration
	logs       map[logInstanceKey]map[int64]*logHealth
}

// LogHealthSnapshot is the per-log view. The triple (BucketName, RootPath, LogID)
// uniquely identifies a log on this node.
type LogHealthSnapshot struct {
	BucketName         string `json:"bucket_name"`
	RootPath           string `json:"root_path"`
	LogID              int64  `json:"log_id"`
	State              string `json:"state"`
	WriteState         string `json:"write_state"`
	ReadState          string `json:"read_state"`
	LastWriteSuccessMS int64  `json:"last_write_success_ms,omitempty"`
	LastWriteFailureMS int64  `json:"last_write_failure_ms,omitempty"`
	LastReadSuccessMS  int64  `json:"last_read_success_ms,omitempty"`
	LastReadFailureMS  int64  `json:"last_read_failure_ms,omitempty"`
	LastFailureReason  string `json:"last_failure_reason,omitempty"`
}

// LogHealthResponse is the node-wide response, optionally filtered to one tenant.
type LogHealthResponse struct {
	State            string              `json:"state"`
	Reason           string              `json:"reason"`
	NodeID           string              `json:"node_id,omitempty"`
	FilterBucketName string              `json:"filter_bucket_name,omitempty"`
	FilterRootPath   string              `json:"filter_root_path,omitempty"`
	TimestampMS      int64               `json:"timestamp_ms"`
	StallAfterMS     int64               `json:"stall_after_ms"`
	TrackedLogs      int                 `json:"tracked_logs"`
	HealthyLogs      int                 `json:"healthy_logs"`
	FailedLogs       int                 `json:"failed_logs"`
	StalledLogs      int                 `json:"stalled_logs"`
	IdleLogs         int                 `json:"idle_logs"`
	Logs             []LogHealthSnapshot `json:"logs"`
}

func NewLogHealthTracker(stallAfter time.Duration) *LogHealthTracker {
	if stallAfter <= 0 {
		stallAfter = defaultLogHealthStallAfter
	}
	return &LogHealthTracker{
		stallAfter: stallAfter,
		logs:       make(map[logInstanceKey]map[int64]*logHealth),
	}
}

// get returns the *logHealth for a log, creating it on first use. The common case
// (log already exists) takes only a shared RLock; creation takes the write lock once.
func (t *LogHealthTracker) get(bucketName, rootPath string, logID int64) *logHealth {
	key := logInstanceKey{bucketName: bucketName, rootPath: rootPath}
	t.mu.RLock()
	lh := t.logs[key][logID] // indexing a nil inner map is safe and returns nil
	t.mu.RUnlock()
	if lh != nil {
		return lh
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	instanceLogs := t.logs[key]
	if instanceLogs == nil {
		instanceLogs = make(map[int64]*logHealth)
		t.logs[key] = instanceLogs
	}
	if lh = instanceLogs[logID]; lh == nil {
		lh = &logHealth{}
		instanceLogs[logID] = lh
	}
	return lh
}

func (t *LogHealthTracker) RecordWriteAttempt(bucketName, rootPath string, logID int64, now time.Time) {
	if t == nil {
		return
	}
	t.get(bucketName, rootPath, logID).writeAttemptMS.Store(now.UnixMilli())
}

func (t *LogHealthTracker) RecordWriteSuccess(bucketName, rootPath string, logID int64, now time.Time) {
	if t == nil {
		return
	}
	t.get(bucketName, rootPath, logID).writeSuccessMS.Store(now.UnixMilli())
}

func (t *LogHealthTracker) RecordWriteFailure(bucketName, rootPath string, logID int64, now time.Time, reason string) {
	if t == nil {
		return
	}
	lh := t.get(bucketName, rootPath, logID)
	lh.writeFailureMS.Store(now.UnixMilli())
	lh.failureReason.Store(&reason)
}

func (t *LogHealthTracker) RecordReadSuccess(bucketName, rootPath string, logID int64, now time.Time) {
	if t == nil {
		return
	}
	t.get(bucketName, rootPath, logID).readSuccessMS.Store(now.UnixMilli())
}

func (t *LogHealthTracker) RecordReadFailure(bucketName, rootPath string, logID int64, now time.Time, reason string) {
	if t == nil {
		return
	}
	lh := t.get(bucketName, rootPath, logID)
	lh.readFailureMS.Store(now.UnixMilli())
	lh.failureReason.Store(&reason)
}

// Snapshot returns the health of all tracked logs. When both bucketFilter and
// rootPathFilter are non-empty, the result is narrowed to that one instance;
// otherwise all tenants are returned.
func (t *LogHealthTracker) Snapshot(bucketFilter, rootPathFilter, nodeID string, now time.Time) LogHealthResponse {
	resp := LogHealthResponse{
		State:            LogHealthStateHealthy,
		Reason:           "no_observed_activity",
		NodeID:           nodeID,
		FilterBucketName: bucketFilter,
		FilterRootPath:   rootPathFilter,
		TimestampMS:      now.UnixMilli(),
		StallAfterMS:     int64(t.stallAfter / time.Millisecond),
		Logs:             []LogHealthSnapshot{},
	}
	filtered := bucketFilter != "" && rootPathFilter != ""

	t.mu.RLock()
	for key, instanceLogs := range t.logs {
		if filtered && (key.bucketName != bucketFilter || key.rootPath != rootPathFilter) {
			continue
		}
		for logID, lh := range instanceLogs {
			resp.Logs = append(resp.Logs, t.snapshotLog(key, logID, lh, now))
		}
	}
	t.mu.RUnlock()

	sort.Slice(resp.Logs, func(i, j int) bool {
		a, b := resp.Logs[i], resp.Logs[j]
		if a.BucketName != b.BucketName {
			return a.BucketName < b.BucketName
		}
		if a.RootPath != b.RootPath {
			return a.RootPath < b.RootPath
		}
		return a.LogID < b.LogID
	})

	resp.TrackedLogs = len(resp.Logs)
	for _, s := range resp.Logs {
		switch s.State {
		case logStateHealthy:
			resp.HealthyLogs++
		case logStateFailed:
			resp.FailedLogs++
		case logStateStalled:
			resp.StalledLogs++
		case logStateIdle:
			resp.IdleLogs++
		}
	}

	switch {
	case resp.TrackedLogs == 0:
		// keep Healthy / no_observed_activity
	case resp.HealthyLogs > 0:
		resp.Reason = "observed_read_write_success"
	case resp.IdleLogs == 0: // all logs are Stalled or Failed
		resp.State = LogHealthStateUnhealthy
		resp.Reason = "all_logs_failed_or_stalled"
	default: // no healthy logs, but some idle -> quiet, not actively broken
		resp.Reason = "idle_logs_present"
	}
	return resp
}

func (t *LogHealthTracker) snapshotLog(key logInstanceKey, logID int64, lh *logHealth, now time.Time) LogHealthSnapshot {
	snap := LogHealthSnapshot{
		BucketName:         key.bucketName,
		RootPath:           key.rootPath,
		LogID:              logID,
		LastWriteSuccessMS: lh.writeSuccessMS.Load(),
		LastWriteFailureMS: lh.writeFailureMS.Load(),
		LastReadSuccessMS:  lh.readSuccessMS.Load(),
		LastReadFailureMS:  lh.readFailureMS.Load(),
	}
	if r := lh.failureReason.Load(); r != nil {
		snap.LastFailureReason = *r
	}
	writeAttempt := lh.writeAttemptMS.Load()
	snap.WriteState = t.classify(snap.LastWriteSuccessMS, snap.LastWriteFailureMS, writeAttempt, now, true)
	snap.ReadState = t.classify(snap.LastReadSuccessMS, snap.LastReadFailureMS, 0, now, false)
	snap.State = overallLogState(snap.WriteState, snap.ReadState)
	return snap
}

// classify derives one direction's state from its timestamps. Stalled is only
// reachable when allowStall is true (writes) AND the log was previously healthy
// (lastSuccess > 0): a never-succeeded log is Failed or Idle, never Stalled.
func (t *LogHealthTracker) classify(lastSuccess, lastFailure, lastAttempt int64, now time.Time, allowStall bool) string {
	if lastSuccess == 0 && lastFailure == 0 && lastAttempt == 0 {
		return logStateIdle // no activity in this direction
	}
	nowMS := now.UnixMilli()
	stallMS := int64(t.stallAfter / time.Millisecond)

	if lastSuccess > 0 && nowMS-lastSuccess <= stallMS {
		return logStateHealthy
	}
	if allowStall && lastSuccess > 0 && nowMS-lastSuccess > stallMS &&
		lastAttempt > lastSuccess && lastAttempt > lastFailure {
		return logStateStalled
	}
	if lastFailure > lastSuccess && nowMS-lastFailure <= stallMS {
		return logStateFailed
	}
	return logStateIdle
}

// overallLogState combines the two direction-states, worst-wins:
// Stalled > Failed > Healthy > Idle.
func overallLogState(writeState, readState string) string {
	switch {
	case writeState == logStateStalled || readState == logStateStalled:
		return logStateStalled
	case writeState == logStateFailed || readState == logStateFailed:
		return logStateFailed
	case writeState == logStateHealthy || readState == logStateHealthy:
		return logStateHealthy
	default:
		return logStateIdle
	}
}

// GetLogHealth builds the node-wide snapshot. Empty filters mean "all tenants".
func (s *Server) GetLogHealth(_ context.Context, bucketFilter, rootPathFilter string) (LogHealthResponse, int) {
	tracker := s.logHealthTracker
	if tracker == nil {
		tracker = NewLogHealthTracker(defaultLogHealthStallAfter)
	}
	nodeID := ""
	if s.serverConfig != nil {
		nodeID = s.serverConfig.NodeID
	}
	resp := tracker.Snapshot(bucketFilter, rootPathFilter, nodeID, time.Now())
	if resp.State == LogHealthStateUnhealthy {
		return resp, http.StatusServiceUnavailable
	}
	return resp, http.StatusOK
}
```

- [ ] **Step 3: Update `server/service.go`**

3a. Rename the struct field at `server/service.go:54`:

```go
	logHealthTracker      *LogHealthTracker
```

3b. Rename the constructor field at `server/service.go:129`:

```go
		logHealthTracker: NewLogHealthTracker(defaultLogHealthStallAfter),
```

3c. Replace the write attempt block at `:365-368`:

```go
	if s.logHealthTracker != nil {
		s.logHealthTracker.RecordWriteAttempt(request.BucketName, request.RootPath, request.LogId, time.Now())
	}
```

3d. Replace the buffer-error block at `:375-377`:

```go
		if s.logHealthTracker != nil {
			s.logHealthTracker.RecordWriteFailure(request.BucketName, request.RootPath, request.LogId, time.Now(), err.Error())
		}
```

3e. Remove the send-buffered-failure tracking entirely (old `:399-401`). A transport send failure is the client giving up, not a storage fault — so the block becomes just the existing log line:

```go
	if sendErr != nil {
		logger.Ctx(streamCtx).Warn("failed to send buffered response", zap.Error(sendErr))
		return sendErr
	}
```

3f. Replace the ReadResult-error block at `:408-414`:

```go
		if s.logHealthTracker != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// client gave up — not a storage fault, no-op
			} else {
				s.logHealthTracker.RecordWriteFailure(request.BucketName, request.RootPath, request.LogId, time.Now(), err.Error())
			}
		}
```

3g. Replace the result.Err block at `:428-430`:

```go
		if s.logHealthTracker != nil {
			s.logHealthTracker.RecordWriteFailure(request.BucketName, request.RootPath, request.LogId, time.Now(), result.Err.Error())
		}
```

3h. Replace the success block at `:439-441`:

```go
	if s.logHealthTracker != nil {
		s.logHealthTracker.RecordWriteSuccess(request.BucketName, request.RootPath, request.LogId, time.Now())
	}
```

3i. Add read instrumentation in `GetBatchEntriesAdv` (`:452-460`). Replace the whole function body:

```go
func (s *Server) GetBatchEntriesAdv(ctx context.Context, request *proto.GetBatchEntriesAdvRequest) (*proto.GetBatchEntriesAdvResponse, error) {
	result, err := s.logStore.GetBatchEntriesAdv(ctx, request.BucketName, request.RootPath, request.LogId, request.SegmentId, request.FromEntryId, request.MaxEntries, request.LastReadState)
	if err != nil {
		if s.logHealthTracker != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// client gave up — not a storage fault, no-op
			} else {
				s.logHealthTracker.RecordReadFailure(request.BucketName, request.RootPath, request.LogId, time.Now(), err.Error())
			}
		}
		return &proto.GetBatchEntriesAdvResponse{
			Status: werr.Status(err),
		}, nil
	}
	if s.logHealthTracker != nil {
		s.logHealthTracker.RecordReadSuccess(request.BucketName, request.RootPath, request.LogId, time.Now())
	}
	return &proto.GetBatchEntriesAdvResponse{Status: werr.Success(), Result: result}, nil
}
```

(The `errors` import added by the draft at `server/service.go:22` stays — it's still used.)

- [ ] **Step 4: Update `server/service_test.go`**

4a. Rename the field in both helpers (`:55` and `:509`):

```go
		logHealthTracker: NewLogHealthTracker(defaultLogHealthStallAfter),
```

4b. Replace the assertions in `TestServer_AddEntry_Success` (`:974-977`):

```go
	health, statusCode := s.GetLogHealth(context.Background(), "", "")
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, LogHealthStateHealthy, health.State)
	assert.Equal(t, 1, health.HealthyLogs)
```

4c. Replace the assertions in `TestServer_AddEntry_AddEntryError` (`:1001-1004`):

```go
	health, statusCode := s.GetLogHealth(context.Background(), "", "")
	assert.Equal(t, 503, statusCode)
	assert.Equal(t, LogHealthStateUnhealthy, health.State)
	assert.Equal(t, 1, health.FailedLogs)
```

- [ ] **Step 5: Run the server-package tests (with race detector)**

Run: `go test -race ./server/...`
Expected: PASS (all `TestLogHealth_*`, `TestServer_GetLogHealth_StatusCode`, and the two `TestServer_AddEntry_*` health assertions). Note: `go build ./...` is still expected to fail here because `cmd` and `common/http` reference the old symbols — that is fixed in Task 2.

- [ ] **Step 6: Commit**

```bash
git add server/log_health.go server/log_health_test.go server/service.go server/service_test.go
git add -u server/   # stages the rw_health.go / rw_health_test.go deletions
git commit -m "feat(server): lightweight per-log r/w health tracker (#131)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Rename the HTTP handler, route, and wiring (`management` + `common/http` + `cmd`)

**Files:**
- Create: `common/http/management/log_health_handler.go`
- Delete: `common/http/management/rw_health_handler.go`
- Create: `common/http/management/log_health_handler_test.go`
- Delete: `common/http/management/rw_health_handler_test.go`
- Modify: `common/http/router.go:49-53`
- Modify: `common/http/server.go:62-63`, `:195-206`
- Modify: `cmd/main.go:224-229`

- [ ] **Step 1: Write the handler test**

```bash
git rm common/http/management/rw_health_handler_test.go
```

Create `common/http/management/log_health_handler_test.go`:

```go
package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogHealthHandler_NoFilterReturnsAll(t *testing.T) {
	var gotBucket, gotRoot string
	handler := NewLogHealthHandler(func(_ context.Context, bucketName, rootPath string) (any, int) {
		gotBucket, gotRoot = bucketName, rootPath
		return map[string]string{"state": "Healthy"}, http.StatusOK
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/log-health", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Empty(t, gotBucket)
	require.Empty(t, gotRoot)
}

func TestLogHealthHandler_PartialFilterIgnored(t *testing.T) {
	var gotBucket, gotRoot string
	handler := NewLogHealthHandler(func(_ context.Context, bucketName, rootPath string) (any, int) {
		gotBucket, gotRoot = bucketName, rootPath
		return map[string]string{}, http.StatusOK
	})

	// only bucket_name -> treated as "no filter"
	req := httptest.NewRequest(http.MethodGet, "/admin/log-health?bucket_name=b", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	require.Empty(t, gotBucket)
	require.Empty(t, gotRoot)
}

func TestLogHealthHandler_FilterPassedThrough(t *testing.T) {
	var gotBucket, gotRoot string
	handler := NewLogHealthHandler(func(_ context.Context, bucketName, rootPath string) (any, int) {
		gotBucket, gotRoot = bucketName, rootPath
		return map[string]string{}, http.StatusServiceUnavailable
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/log-health?bucket_name=b&root_path=r", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	require.Equal(t, "b", gotBucket)
	require.Equal(t, "r", gotRoot)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestLogHealthHandler_RejectsNonGet(t *testing.T) {
	handler := NewLogHealthHandler(func(context.Context, string, string) (any, int) {
		return nil, http.StatusOK
	})
	req := httptest.NewRequest(http.MethodPost, "/admin/log-health", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestLogHealthHandler_EncodesJSON(t *testing.T) {
	handler := NewLogHealthHandler(func(context.Context, string, string) (any, int) {
		return map[string]any{"state": "Healthy", "tracked_logs": 0}, http.StatusOK
	})
	req := httptest.NewRequest(http.MethodGet, "/admin/log-health", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "Healthy", body["state"])
}
```

- [ ] **Step 2: Run the handler test to verify it fails to compile**

Run: `go test ./common/http/management/...`
Expected: FAIL — `undefined: NewLogHealthHandler`.

- [ ] **Step 3: Write the handler implementation**

```bash
git rm common/http/management/rw_health_handler.go
```

Create `common/http/management/log_health_handler.go`:

```go
package management

import (
	"context"
	"encoding/json"
	"net/http"
)

// LogHealthCallback returns the node's log health, optionally filtered to one
// (bucketName, rootPath) instance. Empty strings mean "all tenants".
type LogHealthCallback func(ctx context.Context, bucketName, rootPath string) (any, int)

// NewLogHealthHandler serves the node-wide log health endpoint.
// Optional query params: ?bucket_name=<bucket>&root_path=<root>.
// A partial filter (only one of the two) is ignored and all tenants are returned.
func NewLogHealthHandler(get LogHealthCallback) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		bucketName := firstNonEmpty(
			r.URL.Query().Get("bucket_name"),
			r.URL.Query().Get("bucketName"),
			r.URL.Query().Get("bucketname"),
		)
		rootPath := firstNonEmpty(
			r.URL.Query().Get("root_path"),
			r.URL.Query().Get("rootPath"),
			r.URL.Query().Get("rootpath"),
		)
		if bucketName == "" || rootPath == "" {
			bucketName, rootPath = "", "" // partial filter -> no filter
		}

		result, statusCode := get(r.Context(), bucketName, rootPath)
		if statusCode == 0 {
			statusCode = http.StatusOK
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_ = json.NewEncoder(w).Encode(result)
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
```

- [ ] **Step 4: Update `common/http/router.go`**

Replace the two RW consts at `:49-53` with one:

```go
// AdminLogHealthPath is the path for node-wide per-log read/write health.
const AdminLogHealthPath = "/admin/log-health"
```

- [ ] **Step 5: Update `common/http/server.go`**

5a. Replace the two callback fields at `:62-63`:

```go
	GetLogHealth            management.LogHealthCallback
```

5b. Replace the two registration blocks at `:195-206`:

```go
	if callbacks.GetLogHealth != nil {
		Register(&Handler{
			Path:        AdminLogHealthPath,
			HandlerFunc: management.NewLogHealthHandler(callbacks.GetLogHealth),
		})
	}
```

- [ ] **Step 6: Update `cmd/main.go`**

Replace the two callbacks at `:224-229`:

```go
		GetLogHealth: func(ctx context.Context, bucketName, rootPath string) (any, int) {
			return srv.GetLogHealth(ctx, bucketName, rootPath)
		},
```

- [ ] **Step 7: Build the whole module and run the management tests**

Run: `go build ./... && go test ./common/http/...`
Expected: build succeeds (module is now consistent), management handler tests PASS.

- [ ] **Step 8: Commit**

```bash
git add common/http/management/log_health_handler.go common/http/management/log_health_handler_test.go common/http/router.go common/http/server.go cmd/main.go
git add -u common/http/management/   # stages the rw_health_handler*.go deletions
git commit -m "feat(http): node-wide /admin/log-health endpoint (#131)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Documentation and final verification

**Files:**
- Modify: `common/http/README.md:14-15`, `:86-91`

- [ ] **Step 1: Update the endpoints table**

Replace the two RW rows at `common/http/README.md:14-15` with one:

```markdown
| GET | `/admin/log-health` | Health | Node-wide per-log read/write health (optional `?bucket_name=&root_path=` filter) |
```

- [ ] **Step 2: Update the examples / notes section**

Replace the RW curl examples and notes at `:86-91`:

```markdown
curl "http://localhost:9091/admin/log-health"
curl "http://localhost:9091/admin/log-health?bucket_name=a-bucket&root_path=files"
```

```markdown
- `/admin/log-health` reports each log's observed read/write health on this node,
  derived from real traffic (no injected probe). With no query params it returns all
  tenants; passing both `bucket_name` and `root_path` narrows to one instance.
- It is independent of `/healthz`: `/healthz` is process liveness; `/admin/log-health`
  is data-path health and never affects the `/healthz` result.
- Returns HTTP `503` when every tracked log is Stalled or Failed, `200` otherwise.
```

- [ ] **Step 3: Final full verification**

Run: `go build ./... && go vet ./server/... ./common/http/... ./cmd/... && go test -race ./server/... ./common/http/...`
Expected: build clean, vet clean, all tests PASS.

- [ ] **Step 4: Confirm no stale RW references remain**

Run: `grep -rIn "RWHealth\|rwHealth\|server-rw-health\|cluster-rw-health\|GetClusterRWHealth\|RWWriteAttempt" cmd common server || echo "clean"`
Expected: `clean`.

- [ ] **Step 5: Commit**

```bash
git add common/http/README.md
git commit -m "docs(http): document /admin/log-health endpoint (#131)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review Notes

- **Spec coverage:** atomic-timestamp model (Task 1 Step 2), read+write instrumentation (Task 1 Step 3), per-log classification incl. high-QPS stall (Task 1 tests `TestLogHealth_HungWriteBecomesStalledUnderLoad`), node rollup + idle-stays-healthy (`TestLogHealth_IdleNodeStaysHealthy`), node-wide scope + per-log triple + filter (`TestLogHealth_MultiTenantAndFilter`), single endpoint / cluster code deleted (Task 2), `/healthz` independence documented (Task 3), no idle storage probe (not implemented, by design).
- **Refinement vs spec:** `classify` requires `lastSuccess > 0` for both Healthy and Stalled — a log that has *never* succeeded is Failed (if a recent failure exists) or Idle, never Stalled. This avoids the `now - 0` bootstrap false-positive and matches the intuitive meaning of "stalled" (was healthy, stopped completing). Spec's Stalled row is read with this precondition.
- **Type consistency:** record methods take `(bucket, rootPath, logID, now[, reason])` everywhere (no more `*RWWriteAttempt`); field is `logHealthTracker`; callback type `LogHealthCallback`; route const `AdminLogHealthPath`; response types `LogHealthResponse` / `LogHealthSnapshot`. Status code is `503` (was `500` in the draft) per spec.
