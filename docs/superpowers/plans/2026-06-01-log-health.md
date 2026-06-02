# Log Health Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a lightweight, node-wide, multi-tenant per-log read/write health probe (`/admin/log-health`) that observes the existing `metrics.Op` stream — with zero changes to data-path business logic.

**Architecture:** A `loghealth.Tracker` implements `metrics.OpObserver`. It is created and registered once in `cmd/main.go` (mirroring `opReg`). Every logstore op already calls `metrics.StartOp`/`op.End(status)`; the tracker's `OnOpStart`/`OnOpEnd` translate `logstore.add_entry` (write) and `logstore.get_batch_entries` (read) ops into a few `atomic.Int64` timestamps per `(bucket, rootPath, logID)`. At snapshot time each log is classified Healthy/Stalled/Failed/Idle. One HTTP endpoint returns all tracked logs (optionally filtered) plus a node rollup. The earlier `rw_health` draft is reverted.

**Tech Stack:** Go, `sync/atomic`, `net/http`, `testify`.

**Spec:** `docs/superpowers/specs/2026-06-01-log-health-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `common/metrics/op.go` | Modify | add `BucketName`/`RootPath` to `Op` + `WithInstance` option |
| `common/metrics/op_test.go` | Modify | test `WithInstance` |
| `common/runtime/loghealth/tracker.go` | Create | `Tracker` (OpObserver), classification, `Snapshot`, `Report` |
| `common/runtime/loghealth/tracker_test.go` | Create | tracker + observer-adapter tests |
| `server/logstore.go` | Modify | add `WithInstance` to 2 `StartOp` calls |
| `server/service.go` | Revert | restore upstream (remove draft instrumentation) |
| `server/service_test.go` | Revert | restore upstream |
| `server/rw_health.go` | Delete | superseded |
| `server/rw_health_test.go` | Delete | superseded |
| `common/http/management/log_health_handler.go` | Create (replaces `rw_health_handler.go`) | handler + optional filter |
| `common/http/management/rw_health_handler.go` | Delete | superseded |
| `common/http/management/log_health_handler_test.go` | Create (replaces `rw_health_handler_test.go`) | handler tests |
| `common/http/management/rw_health_handler_test.go` | Delete | superseded |
| `common/http/router.go` | Modify | one route const `AdminLogHealthPath` |
| `common/http/server.go` | Modify | one callback field + registration |
| `cmd/main.go` | Modify | create + register tracker; wire `GetLogHealth` |
| `common/http/README.md` | Modify | document `/admin/log-health` |

**Compilation ordering (Go compiles packages as a unit):**
- **Task 1** (`common/metrics`) — standalone → `go test ./common/metrics/...`.
- **Task 2** (`common/runtime/loghealth`) — needs Task 1 → `go test ./common/runtime/loghealth/...`.
- **Task 3** (`server`) — reverts the draft + adds `WithInstance` (needs Task 1) → `go test ./server/...`. (Full `go build ./...` is still red here: `cmd` references the now-removed `GetServerRWHealth`, fixed in Task 4.)
- **Task 4** (`management` + `common/http` + `cmd`) — full module green.
- **Task 5** — docs + final verification.

---

## Task 1: Add instance identity to `metrics.Op`

**Files:**
- Modify: `common/metrics/op.go` (struct ~:11-23, options ~:28-34)
- Modify: `common/metrics/op_test.go`

- [ ] **Step 1: Write the failing test**

Add to `common/metrics/op_test.go`:

```go
func TestWithInstance_SetsBucketAndRoot(t *testing.T) {
	ResetObservers()
	defer ResetObservers()
	op := StartOp("test.op", nil, nil, WithInstance("bucket-a", "root-a"), WithLogSegment(7, 3))
	if op.BucketName != "bucket-a" || op.RootPath != "root-a" {
		t.Fatalf("got bucket=%q root=%q", op.BucketName, op.RootPath)
	}
	if op.LogID != 7 || op.SegmentID != 3 {
		t.Fatalf("WithLogSegment regressed: log=%d seg=%d", op.LogID, op.SegmentID)
	}
}
```

- [ ] **Step 2: Run it — verify it fails**

Run: `go test ./common/metrics/ -run TestWithInstance_SetsBucketAndRoot`
Expected: FAIL — `op.BucketName undefined` / `undefined: WithInstance`.

- [ ] **Step 3: Implement the field + option**

In `common/metrics/op.go`, add two fields to `Op` (after `Labels`):

```go
type Op struct {
	OpType     string
	Labels     prometheus.Labels
	BucketName string
	RootPath   string
	LogID      int64
	SegmentID  int64
	TraceID    string
	SpanID     string

	histo   prometheus.Observer // may be nil
	start   time.Time
	ended   atomic.Bool
	handles []uint64 // one per observer
}
```

Add the option next to `WithLogSegment`:

```go
// WithInstance sets the multi-tenant identity (bucket + root path) for the op.
func WithInstance(bucketName, rootPath string) OpOption {
	return func(op *Op) {
		op.BucketName = bucketName
		op.RootPath = rootPath
	}
}
```

- [ ] **Step 4: Run tests — verify pass**

Run: `go test ./common/metrics/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add common/metrics/op.go common/metrics/op_test.go
git commit -m "feat(metrics): add instance identity (bucket/root) to Op (#131)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: The `loghealth.Tracker` observer

**Files:**
- Create: `common/runtime/loghealth/tracker.go`
- Create: `common/runtime/loghealth/tracker_test.go`

- [ ] **Step 1: Write the test file**

Create `common/runtime/loghealth/tracker_test.go`:

```go
package loghealth

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/metrics"
)

const testStall = 10 * time.Minute

func TestTracker_NilSafe(t *testing.T) {
	var tr *Tracker
	tr.OnOpStart(&metrics.Op{OpType: opAddEntry})
	tr.OnOpEnd(&metrics.Op{OpType: opAddEntry}, 0, time.Second, "success")
	resp, code := tr.Report("", "")
	require.Equal(t, StateHealthy, resp.State)
	require.Equal(t, 200, code)
}

func TestTracker_NoActivityHealthy(t *testing.T) {
	tr := New(testStall)
	snap := tr.Snapshot("", "", "node-1", time.UnixMilli(1000))
	require.Equal(t, StateHealthy, snap.State)
	require.Equal(t, "no_observed_activity", snap.Reason)
	require.Equal(t, 0, snap.TrackedLogs)
	require.Equal(t, "node-1", snap.NodeID)
}

func TestTracker_RecentWriteSuccessHealthy(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := New(testStall)
	tr.recordWriteAttempt("b", "r", 1, now)
	tr.recordWriteSuccess("b", "r", 1, now.Add(time.Second))
	snap := tr.Snapshot("", "", "n", now.Add(2*time.Second))
	require.Equal(t, 1, snap.HealthyLogs)
	require.Equal(t, logStateHealthy, snap.Logs[0].WriteState)
	require.Equal(t, "b", snap.Logs[0].BucketName)
	require.Equal(t, "r", snap.Logs[0].RootPath)
	require.Equal(t, int64(1), snap.Logs[0].LogID)
}

func TestTracker_OverallWorstOfReadWrite(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := New(testStall)
	tr.recordWriteSuccess("b", "r", 1, now)
	tr.recordReadFailure("b", "r", 1, now.Add(time.Second), "read boom")
	snap := tr.Snapshot("", "", "n", now.Add(2*time.Second))
	require.Equal(t, logStateHealthy, snap.Logs[0].WriteState)
	require.Equal(t, logStateFailed, snap.Logs[0].ReadState)
	require.Equal(t, logStateFailed, snap.Logs[0].State)
	require.Equal(t, "read boom", snap.Logs[0].LastFailureReason)
}

func TestTracker_HungWriteStalledUnderLoad(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := New(testStall)
	tr.recordWriteSuccess("b", "r", 1, now)                    // last success
	tr.recordWriteAttempt("b", "r", 1, now.Add(12*time.Minute)) // recent attempt, never completes
	snap := tr.Snapshot("", "", "n", now.Add(12*time.Minute+time.Second))
	require.Equal(t, logStateStalled, snap.Logs[0].WriteState)
	require.Equal(t, 1, snap.StalledLogs)
	require.Equal(t, StateUnhealthy, snap.State)
	require.Equal(t, "all_logs_failed_or_stalled", snap.Reason)
}

func TestTracker_NeverSucceededNotStalled(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := New(testStall)
	tr.recordWriteAttempt("b", "r", 1, now) // only an attempt, never success/failure
	snap := tr.Snapshot("", "", "n", now.Add(30*time.Minute))
	require.Equal(t, logStateIdle, snap.Logs[0].WriteState) // not Stalled
}

func TestTracker_ReadNeverStalls(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := New(testStall)
	tr.recordReadSuccess("b", "r", 1, now)
	snap := tr.Snapshot("", "", "n", now.Add(20*time.Minute))
	require.Equal(t, logStateIdle, snap.Logs[0].ReadState)
	require.Equal(t, logStateIdle, snap.Logs[0].State)
	require.Equal(t, StateHealthy, snap.State)
}

func TestTracker_IdleNodeStaysHealthy(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := New(testStall)
	tr.recordWriteSuccess("b", "r", 1, now)
	tr.recordWriteFailure("b", "r", 2, now, "old fail")
	snap := tr.Snapshot("", "", "n", now.Add(30*time.Minute))
	require.Equal(t, 2, snap.IdleLogs)
	require.Equal(t, 0, snap.HealthyLogs)
	require.Equal(t, 0, snap.FailedLogs)
	require.Equal(t, StateHealthy, snap.State)
}

func TestTracker_UnhealthyOnlyWhenAllActiveFailures(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := New(testStall)
	tr.recordWriteFailure("b", "r", 1, now, "f1")
	tr.recordWriteFailure("b", "r", 2, now, "f2")
	snap := tr.Snapshot("", "", "n", now.Add(time.Second))
	require.Equal(t, 2, snap.FailedLogs)
	require.Equal(t, StateUnhealthy, snap.State)
	require.Equal(t, "all_logs_failed_or_stalled", snap.Reason)
}

func TestTracker_HealthyIfAnyHealthy(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := New(testStall)
	tr.recordWriteSuccess("b", "r", 1, now)
	tr.recordWriteFailure("b", "r", 2, now, "f2")
	snap := tr.Snapshot("", "", "n", now.Add(time.Second))
	require.Equal(t, 1, snap.HealthyLogs)
	require.Equal(t, 1, snap.FailedLogs)
	require.Equal(t, StateHealthy, snap.State)
	require.Equal(t, "observed_read_write_success", snap.Reason)
}

func TestTracker_MultiTenantAndFilter(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := New(testStall)
	tr.recordWriteSuccess("bucket-a", "root-a", 1, now)
	tr.recordWriteSuccess("bucket-b", "root-b", 7, now)
	all := tr.Snapshot("", "", "n", now.Add(time.Second))
	require.Equal(t, 2, all.TrackedLogs)
	require.Equal(t, "bucket-a", all.Logs[0].BucketName) // sorted by (bucket, root, logID)
	require.Equal(t, "bucket-b", all.Logs[1].BucketName)
	filtered := tr.Snapshot("bucket-b", "root-b", "n", now.Add(time.Second))
	require.Equal(t, 1, filtered.TrackedLogs)
	require.Equal(t, int64(7), filtered.Logs[0].LogID)
	require.Equal(t, "bucket-b", filtered.FilterBucketName)
}

func TestTracker_ReportStatusCode(t *testing.T) {
	now := time.Now()
	tr := New(testStall)
	tr.recordWriteFailure("b", "r", 1, now, "boom")
	resp, code := tr.Report("", "")
	require.Equal(t, StateUnhealthy, resp.State)
	require.Equal(t, 503, code)
}

func TestTracker_ConcurrentRaceFree(t *testing.T) {
	tr := New(testStall)
	now := time.UnixMilli(1_000_000)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				id := int64(i % 4)
				tr.recordWriteAttempt("b", "r", id, now)
				tr.recordWriteSuccess("b", "r", id, now)
				_ = tr.Snapshot("", "", "n", now)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 4, tr.Snapshot("", "", "n", now).TrackedLogs)
}

// Observer adapter: drive through the real StartOp/End path.
func TestTracker_ObserverMapsOps(t *testing.T) {
	metrics.ResetObservers()
	defer metrics.ResetObservers()
	tr := New(testStall)
	metrics.RegisterOpObserver(tr)

	w := metrics.StartOp(opAddEntry, nil, nil, metrics.WithInstance("b", "r"), metrics.WithLogSegment(1, 0))
	w.End("success")
	r := metrics.StartOp(opGetBatchEntries, nil, nil, metrics.WithInstance("b", "r"), metrics.WithLogSegment(2, 0))
	r.End("error")

	snap := tr.Snapshot("", "", "n", time.Now())
	require.Equal(t, 2, snap.TrackedLogs)
	byLog := map[int64]LogSnapshot{}
	for _, l := range snap.Logs {
		byLog[l.LogID] = l
	}
	require.Equal(t, logStateHealthy, byLog[1].WriteState)
	require.Equal(t, logStateFailed, byLog[2].ReadState)
}
```

- [ ] **Step 2: Run it — verify it fails to compile**

Run: `go test ./common/runtime/loghealth/...`
Expected: FAIL — package/`New`/`Tracker` undefined.

- [ ] **Step 3: Write the implementation**

Create `common/runtime/loghealth/tracker.go`:

```go
package loghealth

import (
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zilliztech/woodpecker/common/metrics"
)

const (
	// Node rollup states.
	StateHealthy   = "Healthy"
	StateUnhealthy = "Unhealthy"

	// Per-log / per-direction states.
	logStateHealthy = "Healthy"
	logStateFailed  = "Failed"
	logStateStalled = "Stalled"
	logStateIdle    = "Idle"

	// DefaultStallAfter is the no-success window after which an active-but-not-
	// completing log is considered Stalled.
	DefaultStallAfter = 10 * time.Minute

	// Observed op types — MUST match the opType strings passed to metrics.StartOp
	// in server/logstore.go.
	opAddEntry        = "logstore.add_entry"
	opGetBatchEntries = "logstore.get_batch_entries"
	// statusSuccess is the literal status server/logstore.go sets on success.
	statusSuccess = "success"
)

// Compile-time assertion that Tracker is an OpObserver.
var _ metrics.OpObserver = (*Tracker)(nil)

type logInstanceKey struct {
	bucketName string
	rootPath   string
}

// logHealth is the lock-free per-log state, written via atomic stores under RLock.
type logHealth struct {
	writeAttemptMS atomic.Int64
	writeSuccessMS atomic.Int64
	writeFailureMS atomic.Int64
	readSuccessMS  atomic.Int64
	readFailureMS  atomic.Int64
	failureReason  atomic.Pointer[string]
}

// Tracker observes the metrics.Op stream and derives per-log read/write health.
type Tracker struct {
	mu         sync.RWMutex // guards map STRUCTURE only
	stallAfter time.Duration
	logs       map[logInstanceKey]map[int64]*logHealth
}

// LogSnapshot is the per-log view; (BucketName, RootPath, LogID) is the unique key.
type LogSnapshot struct {
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

// Response is the node-wide payload, optionally filtered to one tenant.
type Response struct {
	State            string        `json:"state"`
	Reason           string        `json:"reason"`
	NodeID           string        `json:"node_id,omitempty"`
	FilterBucketName string        `json:"filter_bucket_name,omitempty"`
	FilterRootPath   string        `json:"filter_root_path,omitempty"`
	TimestampMS      int64         `json:"timestamp_ms"`
	StallAfterMS     int64         `json:"stall_after_ms"`
	TrackedLogs      int           `json:"tracked_logs"`
	HealthyLogs      int           `json:"healthy_logs"`
	FailedLogs       int           `json:"failed_logs"`
	StalledLogs      int           `json:"stalled_logs"`
	IdleLogs         int           `json:"idle_logs"`
	Logs             []LogSnapshot `json:"logs"`
}

// New creates a Tracker. stallAfter <= 0 uses DefaultStallAfter.
func New(stallAfter time.Duration) *Tracker {
	if stallAfter <= 0 {
		stallAfter = DefaultStallAfter
	}
	return &Tracker{
		stallAfter: stallAfter,
		logs:       make(map[logInstanceKey]map[int64]*logHealth),
	}
}

// --- metrics.OpObserver ---

func (t *Tracker) OnOpStart(op *metrics.Op) uint64 {
	if t == nil || op == nil {
		return 0
	}
	if op.OpType == opAddEntry {
		t.recordWriteAttempt(op.BucketName, op.RootPath, op.LogID, op.StartedAt())
	}
	return 0 // the tracker keeps no per-op handle
}

func (t *Tracker) OnOpEnd(op *metrics.Op, _ uint64, elapsed time.Duration, status string) {
	if t == nil || op == nil {
		return
	}
	now := op.StartedAt().Add(elapsed)
	ok := status == statusSuccess
	switch op.OpType {
	case opAddEntry:
		if ok {
			t.recordWriteSuccess(op.BucketName, op.RootPath, op.LogID, now)
		} else {
			t.recordWriteFailure(op.BucketName, op.RootPath, op.LogID, now, status)
		}
	case opGetBatchEntries:
		if ok {
			t.recordReadSuccess(op.BucketName, op.RootPath, op.LogID, now)
		} else {
			t.recordReadFailure(op.BucketName, op.RootPath, op.LogID, now, status)
		}
	}
}

// --- internal record methods (unit-testable without metrics.Op) ---

func (t *Tracker) get(bucketName, rootPath string, logID int64) *logHealth {
	key := logInstanceKey{bucketName: bucketName, rootPath: rootPath}
	t.mu.RLock()
	lh := t.logs[key][logID] // indexing a nil inner map is safe -> nil
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

func (t *Tracker) recordWriteAttempt(b, r string, logID int64, now time.Time) {
	if t == nil {
		return
	}
	t.get(b, r, logID).writeAttemptMS.Store(now.UnixMilli())
}

func (t *Tracker) recordWriteSuccess(b, r string, logID int64, now time.Time) {
	if t == nil {
		return
	}
	t.get(b, r, logID).writeSuccessMS.Store(now.UnixMilli())
}

func (t *Tracker) recordWriteFailure(b, r string, logID int64, now time.Time, reason string) {
	if t == nil {
		return
	}
	lh := t.get(b, r, logID)
	lh.writeFailureMS.Store(now.UnixMilli())
	lh.failureReason.Store(&reason)
}

func (t *Tracker) recordReadSuccess(b, r string, logID int64, now time.Time) {
	if t == nil {
		return
	}
	t.get(b, r, logID).readSuccessMS.Store(now.UnixMilli())
}

func (t *Tracker) recordReadFailure(b, r string, logID int64, now time.Time, reason string) {
	if t == nil {
		return
	}
	lh := t.get(b, r, logID)
	lh.readFailureMS.Store(now.UnixMilli())
	lh.failureReason.Store(&reason)
}

// --- snapshot / classification ---

// Snapshot is pure and deterministic (nodeID + now injected). Empty filters = all.
func (t *Tracker) Snapshot(bucketFilter, rootPathFilter, nodeID string, now time.Time) Response {
	resp := Response{
		State:            StateHealthy,
		Reason:           "no_observed_activity",
		NodeID:           nodeID,
		FilterBucketName: bucketFilter,
		FilterRootPath:   rootPathFilter,
		TimestampMS:      now.UnixMilli(),
		StallAfterMS:     int64(t.stallAfter / time.Millisecond),
		Logs:             []LogSnapshot{},
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
		// Healthy / no_observed_activity
	case resp.HealthyLogs > 0:
		resp.Reason = "observed_read_write_success"
	case resp.IdleLogs == 0: // all Stalled or Failed
		resp.State = StateUnhealthy
		resp.Reason = "all_logs_failed_or_stalled"
	default: // no healthy, but some idle -> quiet, not actively broken
		resp.Reason = "idle_logs_present"
	}
	return resp
}

func (t *Tracker) snapshotLog(key logInstanceKey, logID int64, lh *logHealth, now time.Time) LogSnapshot {
	snap := LogSnapshot{
		BucketName:         key.bucketName,
		RootPath:           key.rootPath,
		LogID:              logID,
		LastWriteSuccessMS: lh.writeSuccessMS.Load(),
		LastWriteFailureMS: lh.writeFailureMS.Load(),
		LastReadSuccessMS:  lh.readSuccessMS.Load(),
		LastReadFailureMS:  lh.readFailureMS.Load(),
	}
	if rp := lh.failureReason.Load(); rp != nil {
		snap.LastFailureReason = *rp
	}
	writeAttempt := lh.writeAttemptMS.Load()
	snap.WriteState = t.classify(snap.LastWriteSuccessMS, snap.LastWriteFailureMS, writeAttempt, now, true)
	snap.ReadState = t.classify(snap.LastReadSuccessMS, snap.LastReadFailureMS, 0, now, false)
	snap.State = overallLogState(snap.WriteState, snap.ReadState)
	return snap
}

// classify derives one direction's state. Stalled requires allowStall (writes) AND a
// prior success (lastSuccess > 0): a never-succeeded log is Failed or Idle, not Stalled.
func (t *Tracker) classify(lastSuccess, lastFailure, lastAttempt int64, now time.Time, allowStall bool) string {
	if lastSuccess == 0 && lastFailure == 0 && lastAttempt == 0 {
		return logStateIdle
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

// overallLogState combines the two directions, worst-wins: Stalled > Failed > Healthy > Idle.
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

// Report is the production entry point for the admin callback: nodeID from
// metrics.NodeID, now from time.Now(), and state -> HTTP status.
func (t *Tracker) Report(bucketFilter, rootPathFilter string) (Response, int) {
	if t == nil {
		return Response{State: StateHealthy, Reason: "no_observed_activity", Logs: []LogSnapshot{}}, http.StatusOK
	}
	resp := t.Snapshot(bucketFilter, rootPathFilter, metrics.NodeID, time.Now())
	if resp.State == StateUnhealthy {
		return resp, http.StatusServiceUnavailable
	}
	return resp, http.StatusOK
}
```

- [ ] **Step 4: Run tests (with race detector)**

Run: `go test -race ./common/runtime/loghealth/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add common/runtime/loghealth/
git commit -m "feat(loghealth): per-log r/w health tracker as OpObserver (#131)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Label the logstore ops; revert the draft in package `server`

**Files:**
- Modify: `server/logstore.go:197`, `:300`
- Revert: `server/service.go`, `server/service_test.go`
- Delete: `server/rw_health.go`, `server/rw_health_test.go`

- [ ] **Step 1: Add `WithInstance` to the two observed StartOp calls**

`server/logstore.go:197` (add_entry) — add `metrics.WithInstance(bucketName, rootPath)`:

```go
	op := metrics.StartOp("logstore.add_entry", nil, nil, metrics.WithInstance(bucketName, rootPath), metrics.WithLogSegment(logId, entry.SegId))
```

`server/logstore.go:300` (get_batch_entries):

```go
	op := metrics.StartOp("logstore.get_batch_entries", nil, nil, metrics.WithInstance(bucketName, rootPath), metrics.WithLogSegment(logId, segmentId))
```

- [ ] **Step 2: Revert the draft instrumentation and delete the superseded files**

```bash
git checkout HEAD -- server/service.go server/service_test.go
git rm server/rw_health.go server/rw_health_test.go
```

This restores the upstream `AddEntry` / `GetBatchEntriesAdv` (no `rwHealthTracker`
field, no manual record calls) and removes the old tracker + its tests. The health
feature now lives entirely in the observer wired from `main` (Task 4) — package
`server` carries no health logic beyond emitting the already-labelled ops.

- [ ] **Step 3: Build and test package `server`**

Run: `go build ./server/... && go test ./server/...`
Expected: PASS. (Full `go build ./...` is still red — `cmd` references the removed
`srv.GetServerRWHealth`; fixed in Task 4.)

- [ ] **Step 4: Commit**

```bash
git add server/logstore.go
git add -u server/   # stages service.go/service_test.go revert + rw_health*.go deletions
git commit -m "feat(logstore): tag add_entry/get_batch_entries ops with instance; drop rw_health draft (#131)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: HTTP handler, route, and `main` wiring

**Files:**
- Create: `common/http/management/log_health_handler.go`
- Delete: `common/http/management/rw_health_handler.go`
- Create: `common/http/management/log_health_handler_test.go`
- Delete: `common/http/management/rw_health_handler_test.go`
- Modify: `common/http/router.go:49-53`
- Modify: `common/http/server.go:62-63`, `:195-206`
- Modify: `cmd/main.go` (imports, `:201` area, `:224-229`)

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
	h := NewLogHealthHandler(func(_ context.Context, b, r string) (any, int) {
		gotBucket, gotRoot = b, r
		return map[string]string{"state": "Healthy"}, http.StatusOK
	})
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/admin/log-health", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Empty(t, gotBucket)
	require.Empty(t, gotRoot)
}

func TestLogHealthHandler_PartialFilterIgnored(t *testing.T) {
	var gotBucket, gotRoot string
	h := NewLogHealthHandler(func(_ context.Context, b, r string) (any, int) {
		gotBucket, gotRoot = b, r
		return map[string]string{}, http.StatusOK
	})
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/admin/log-health?bucket_name=b", nil))
	require.Empty(t, gotBucket)
	require.Empty(t, gotRoot)
}

func TestLogHealthHandler_FilterPassedThrough(t *testing.T) {
	var gotBucket, gotRoot string
	h := NewLogHealthHandler(func(_ context.Context, b, r string) (any, int) {
		gotBucket, gotRoot = b, r
		return map[string]string{}, http.StatusServiceUnavailable
	})
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/admin/log-health?bucket_name=b&root_path=r", nil))
	require.Equal(t, "b", gotBucket)
	require.Equal(t, "r", gotRoot)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestLogHealthHandler_RejectsNonGet(t *testing.T) {
	h := NewLogHealthHandler(func(context.Context, string, string) (any, int) { return nil, http.StatusOK })
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodPost, "/admin/log-health", nil))
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestLogHealthHandler_EncodesJSON(t *testing.T) {
	h := NewLogHealthHandler(func(context.Context, string, string) (any, int) {
		return map[string]any{"state": "Healthy", "tracked_logs": 0}, http.StatusOK
	})
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/admin/log-health", nil))
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "Healthy", body["state"])
}
```

- [ ] **Step 2: Run it — verify it fails to compile**

Run: `go test ./common/http/management/...`
Expected: FAIL — `undefined: NewLogHealthHandler`.

- [ ] **Step 3: Write the handler**

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
// A partial filter (only one of the two) is ignored — all tenants are returned.
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

- [ ] **Step 6: Wire `cmd/main.go`**

6a. Add the import (with the other `common/runtime/...` imports):

```go
	"github.com/zilliztech/woodpecker/common/runtime/loghealth"
```

6b. Create + register the tracker next to `opReg` (after `cmd/main.go:201`):

```go
	logHealth := loghealth.New(loghealth.DefaultStallAfter)
	metrics.RegisterOpObserver(logHealth)
```

6c. Replace the two RW callbacks at `:224-229` with:

```go
		GetLogHealth: func(_ context.Context, bucketName, rootPath string) (any, int) {
			return logHealth.Report(bucketName, rootPath)
		},
```

- [ ] **Step 7: Build the module and run http tests**

Run: `go build ./... && go test ./common/http/...`
Expected: build succeeds (module consistent), handler tests PASS.

- [ ] **Step 8: Commit**

```bash
git add common/http/management/log_health_handler.go common/http/management/log_health_handler_test.go common/http/router.go common/http/server.go cmd/main.go
git add -u common/http/management/   # stages rw_health_handler*.go deletions
git commit -m "feat(http): /admin/log-health endpoint wired to loghealth observer (#131)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Documentation and final verification

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
  derived from real traffic via the metrics op stream (no injected probe). With no
  query params it returns all tenants; passing both `bucket_name` and `root_path`
  narrows to one instance.
- Write health is the logstore "accept" outcome (`logstore.add_entry`); read health is
  `logstore.get_batch_entries`. Independent of `/healthz` (process liveness) — a
  stalled log never affects the `/healthz` result.
- Returns HTTP `503` when every tracked log is Stalled or Failed, `200` otherwise.
```

- [ ] **Step 3: Final full verification**

Run: `go build ./... && go vet ./common/... ./server/... ./cmd/... && go test -race ./common/metrics/... ./common/runtime/loghealth/... ./common/http/... ./server/...`
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

- **Spec coverage:** observer-based data source + ownership in main (Task 2 + Task 4
  Step 6); `WithInstance` for the multi-tenant triple (Task 1, Task 3 Step 1);
  zero data-path intrusion / draft reverted (Task 3 Step 2); classification incl.
  high-QPS stall and never-succeeded≠stalled (Task 2 tests); node rollup + idle-stays-
  healthy; node-wide scope + filter; single endpoint, cluster code deleted (Task 4);
  `/healthz` independence + logstore-layer semantics documented (Task 5); no idle
  storage probe (by design, not implemented).
- **Placeholder scan:** none — every code/test step contains full content.
- **Type consistency:** package `loghealth`; `Tracker`, `Response`, `LogSnapshot`;
  `New`/`Snapshot`/`Report`; observer methods `OnOpStart`/`OnOpEnd`; op-type consts
  match `server/logstore.go` strings; `metrics.WithInstance`/`Op.BucketName`/`RootPath`;
  callback `management.LogHealthCallback`; route `AdminLogHealthPath`; HTTP status 503
  on Unhealthy.
- **Observer registration** lives only in `main` (not a constructor) → no global
  observer-chain pollution across test `Server`s; tracker tests use
  `metrics.ResetObservers()`.
```
