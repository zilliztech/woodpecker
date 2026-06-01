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
