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
	tr.recordWriteSuccess("b", "r", 1, now)
	tr.recordWriteAttempt("b", "r", 1, now.Add(12*time.Minute))
	snap := tr.Snapshot("", "", "n", now.Add(12*time.Minute+time.Second))
	require.Equal(t, logStateStalled, snap.Logs[0].WriteState)
	require.Equal(t, 1, snap.StalledLogs)
	require.Equal(t, StateUnhealthy, snap.State)
	require.Equal(t, "all_logs_failed_or_stalled", snap.Reason)
}

func TestTracker_NeverSucceededNotStalled(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := New(testStall)
	tr.recordWriteAttempt("b", "r", 1, now)
	snap := tr.Snapshot("", "", "n", now.Add(30*time.Minute))
	require.Equal(t, logStateIdle, snap.Logs[0].WriteState)
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
	require.Equal(t, "bucket-a", all.Logs[0].BucketName)
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

func TestTracker_FailureReasonPerDirection(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	tr := New(testStall)
	// Write fails; later read succeeds. The write reason must survive.
	tr.recordWriteFailure("b", "r", 1, now, "write boom")
	tr.recordReadSuccess("b", "r", 1, now.Add(time.Second))
	snap := tr.Snapshot("", "", "n", now.Add(2*time.Second))
	require.Equal(t, "write boom", snap.Logs[0].LastFailureReason)

	// A newer read failure should win over the older write failure.
	tr.recordReadFailure("b", "r", 1, now.Add(3*time.Second), "read boom")
	snap = tr.Snapshot("", "", "n", now.Add(4*time.Second))
	require.Equal(t, "read boom", snap.Logs[0].LastFailureReason)
}

func TestTracker_ReadEndOfFileIsHealthy(t *testing.T) {
	metrics.ResetObservers()
	defer metrics.ResetObservers()
	tr := New(testStall)
	metrics.RegisterOpObserver(tr)

	// A caught-up tail read ends with the end_of_file status — must count as a
	// healthy read, not a failure.
	r := metrics.StartOp(opGetBatchEntries, nil, nil, metrics.WithInstance("b", "r"), metrics.WithLogSegment(5, 0))
	r.End("end_of_file")

	snap := tr.Snapshot("", "", "n", time.Now())
	require.Equal(t, 1, snap.TrackedLogs)
	require.Equal(t, logStateHealthy, snap.Logs[0].ReadState)
	require.Equal(t, 0, snap.FailedLogs)
	require.Equal(t, StateHealthy, snap.State)
}
