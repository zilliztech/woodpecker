// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"context"
	"errors"
	"math"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

func segMeta(segNo int64, state proto.SegmentState) *meta.SegmentMeta {
	return &meta.SegmentMeta{Metadata: &proto.SegmentMetadata{SegNo: segNo, State: state}}
}

// countingNotifyManager records which segments EnsureSegmentNotified was called for and
// reports each as "advanced" (real work) so the per-cycle budget accounting can be exercised.
// All fields are mutex-guarded: the distributor tests invoke it from a separate goroutine
// while the test polls with require.Eventually (the race detector flags unguarded access).
type countingNotifyManager struct {
	mu          sync.Mutex
	called      []int64
	sweepBounds []int64
	reaped      []int64
	advanced    bool
	err         error
}

func (c *countingNotifyManager) EnsureSegmentNotified(_ context.Context, _ string, _ int64, segmentId int64) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.called = append(c.called, segmentId)
	return c.advanced, c.err
}

func (c *countingNotifyManager) CleanupOrphanedStatuses(_ context.Context, _ int64, minSegmentId int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sweepBounds = append(c.sweepBounds, minSegmentId)
	return nil
}

func (c *countingNotifyManager) MarkSegmentReaped(segmentId int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reaped = append(c.reaped, segmentId)
}

func (c *countingNotifyManager) calledSegments() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int64(nil), c.called...)
}

func (c *countingNotifyManager) sweepBoundsSnapshot() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int64(nil), c.sweepBounds...)
}

// recordingCleanupManager records the orphan-sweep bounds it was invoked with.
type recordingCleanupManager struct {
	sweepBounds []int64
}

func (r *recordingCleanupManager) CleanupSegment(_ context.Context, _ string, _ int64, _ int64) error {
	return nil
}

func (r *recordingCleanupManager) CleanupOrphanedStatuses(_ context.Context, _ int64, minSegmentId int64) error {
	r.sweepBounds = append(r.sweepBounds, minSegmentId)
	return nil
}

func TestCollectTruncatedSegments(t *testing.T) {
	segs := map[int64]*meta.SegmentMeta{
		1: segMeta(1, proto.SegmentState_Completed),
		2: segMeta(2, proto.SegmentState_Truncated),
		3: segMeta(3, proto.SegmentState_Sealed),
		4: segMeta(4, proto.SegmentState_Truncated),
	}
	got := collectTruncatedSegments(segs)
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	assert.Equal(t, []int64{2, 4}, got)

	assert.Empty(t, collectTruncatedSegments(map[int64]*meta.SegmentMeta{
		1: segMeta(1, proto.SegmentState_Sealed),
	}))
}

// TestDistributeCompactedMarks_OnlySealedAndDrivenCount verifies the pass only touches Sealed
// segments and returns the count that did real work (advanced==true).
func TestDistributeCompactedMarks_OnlySealedAndDrivenCount(t *testing.T) {
	lh := &testLogHandleMock{}
	lh.On("GetName").Return("test-log").Maybe()
	lh.On("GetId").Return(int64(1)).Maybe()

	segs := map[int64]*meta.SegmentMeta{
		1: segMeta(1, proto.SegmentState_Completed),
		2: segMeta(2, proto.SegmentState_Sealed),
		3: segMeta(3, proto.SegmentState_Truncated),
		4: segMeta(4, proto.SegmentState_Sealed),
	}
	nm := &countingNotifyManager{advanced: true}
	driven := distributeCompactedMarks(context.Background(), lh, nm, segs, true)

	assert.Equal(t, 2, driven, "both Sealed segments did real work")
	called := nm.calledSegments()
	sort.Slice(called, func(i, j int) bool { return called[i] < called[j] })
	assert.Equal(t, []int64{2, 4}, called, "only Sealed segments are notified")
}

// TestDistributeCompactedMarks_NonServiceModeIsNoOp verifies the client-side storage-mode gate:
// outside service (staged) storage, mark distribution is skipped entirely — no notify RPC and
// no root/marking record is ever created.
func TestDistributeCompactedMarks_NonServiceModeIsNoOp(t *testing.T) {
	lh := &testLogHandleMock{}
	lh.On("GetName").Return("test-log").Maybe()
	lh.On("GetId").Return(int64(1)).Maybe()

	segs := map[int64]*meta.SegmentMeta{
		1: segMeta(1, proto.SegmentState_Sealed),
		2: segMeta(2, proto.SegmentState_Sealed),
	}
	nm := &countingNotifyManager{advanced: true}
	driven := distributeCompactedMarks(context.Background(), lh, nm, segs, false)

	assert.Equal(t, 0, driven, "non-service mode drives nothing")
	assert.Empty(t, nm.calledSegments(), "non-service mode must not notify any segment")
}

// TestDistributeCompactedMarks_SettledNotCounted verifies settled (advanced==false) segments
// are still notified (the manager decides) but do NOT consume the driven count.
func TestDistributeCompactedMarks_SettledNotCounted(t *testing.T) {
	lh := &testLogHandleMock{}
	lh.On("GetName").Return("test-log").Maybe()
	lh.On("GetId").Return(int64(1)).Maybe()

	segs := map[int64]*meta.SegmentMeta{
		1: segMeta(1, proto.SegmentState_Sealed),
		2: segMeta(2, proto.SegmentState_Sealed),
	}
	nm := &countingNotifyManager{advanced: false} // all settled fast-path
	driven := distributeCompactedMarks(context.Background(), lh, nm, segs, true)

	assert.Equal(t, 0, driven, "settled segments don't consume the budget")
	assert.Len(t, nm.calledSegments(), 2, "but they are still asked (the manager fast-paths internally)")
}

// TestDistributeCompactedMarks_ErrorDoesNotAbort verifies a per-segment notify error is
// tolerated: the pass continues and the erroring segment is not counted as driven.
func TestDistributeCompactedMarks_ErrorDoesNotAbort(t *testing.T) {
	lh := &testLogHandleMock{}
	lh.On("GetName").Return("test-log").Maybe()
	lh.On("GetId").Return(int64(1)).Maybe()

	segs := map[int64]*meta.SegmentMeta{
		1: segMeta(1, proto.SegmentState_Sealed),
		2: segMeta(2, proto.SegmentState_Sealed),
	}
	nm := &countingNotifyManager{advanced: false, err: errors.New("etcd down")}
	driven := distributeCompactedMarks(context.Background(), lh, nm, segs, true)

	assert.Equal(t, 0, driven)
	assert.Len(t, nm.calledSegments(), 2, "both segments attempted despite the error")
}

// TestCompactCompletedSegments_CountsAndSkips verifies the compact pass only touches Completed
// segments, and that a GetRecoverableSegmentHandle failure is counted as a failure and skipped
// without aborting the pass.
func TestCompactCompletedSegments_CountsAndSkips(t *testing.T) {
	lh := &testLogHandleMock{}
	lh.On("GetName").Return("test-log").Maybe()
	lh.On("GetId").Return(int64(1)).Maybe()
	// Segment 1 (Completed): handle lookup fails -> counted processed + failed, skipped.
	lh.On("GetRecoverableSegmentHandle", mock.Anything, int64(1)).Return(nil, errors.New("not recoverable"))

	segs := map[int64]*meta.SegmentMeta{
		1: segMeta(1, proto.SegmentState_Completed),
		2: segMeta(2, proto.SegmentState_Sealed),    // ignored by this pass
		3: segMeta(3, proto.SegmentState_Truncated), // ignored by this pass
	}
	st := compactCompletedSegments(context.Background(), lh, segs)
	require.Equal(t, 1, st.processed)
	assert.Equal(t, 0, st.compacted)
	assert.Equal(t, 1, st.failed)
}

// TestRunNotifyDistributor_NonServiceReturnsImmediately verifies the distributor goroutine is a
// no-op outside service storage: it returns at once rather than waiting on the snapshot channel.
func TestRunNotifyDistributor_NonServiceReturnsImmediately(t *testing.T) {
	lh := &testLogHandleMock{}
	lh.On("GetName").Return("test-log").Maybe()
	lh.On("GetId").Return(int64(1)).Maybe()
	nm := &countingNotifyManager{}

	done := make(chan struct{})
	go func() {
		runNotifyDistributor(lh, nm, false /*serviceMode*/, make(chan map[int64]*meta.SegmentMeta, 1), make(chan struct{}))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runNotifyDistributor should return immediately in non-service mode")
	}
	assert.Empty(t, nm.calledSegments())
}

// TestRunNotifyDistributor_ConsumesSnapshotsThenStops verifies the service-mode loop consumes
// auditor-published snapshots (no etcd scan of its own: the mock log handle has NO GetSegments
// expectation, so a re-list would fail the test) and exits when the close channel fires.
func TestRunNotifyDistributor_ConsumesSnapshotsThenStops(t *testing.T) {
	lh := &testLogHandleMock{}
	lh.On("GetName").Return("test-log").Maybe()
	lh.On("GetId").Return(int64(1)).Maybe()
	nm := &countingNotifyManager{advanced: true}

	segsCh := make(chan map[int64]*meta.SegmentMeta, 1)
	closeCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		runNotifyDistributor(lh, nm, true /*serviceMode*/, segsCh, closeCh)
		close(done)
	}()

	publishSegmentsSnapshot(segsCh, map[int64]*meta.SegmentMeta{
		1: segMeta(1, proto.SegmentState_Sealed),
	})
	require.Eventually(t, func() bool {
		lh.Mock.Test(t)
		return len(nm.calledSegments()) > 0
	}, 3*time.Second, 10*time.Millisecond, "the distributor consumed the published snapshot")

	close(closeCh)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runNotifyDistributor should stop after closeCh fires")
	}
	assert.Contains(t, nm.calledSegments(), int64(1))
}

// TestPublishSegmentsSnapshot_ReplacesStale verifies the non-blocking publish: an unconsumed
// snapshot in the buffer is replaced by the fresher one instead of blocking the auditor.
func TestPublishSegmentsSnapshot_ReplacesStale(t *testing.T) {
	segsCh := make(chan map[int64]*meta.SegmentMeta, 1)
	publishSegmentsSnapshot(segsCh, map[int64]*meta.SegmentMeta{1: segMeta(1, proto.SegmentState_Sealed)})
	publishSegmentsSnapshot(segsCh, map[int64]*meta.SegmentMeta{2: segMeta(2, proto.SegmentState_Sealed)})

	got := <-segsCh
	_, hasFresh := got[2]
	assert.True(t, hasFresh, "the buffered stale snapshot must be replaced by the fresh one")
	select {
	case <-segsCh:
		t.Fatal("channel must hold at most one snapshot")
	default:
	}
}

// TestOrphanSweepBound verifies the sweep bound derivation: segments are cleaned in ascending
// order, so any cleanup-domain record below the smallest EXISTING segment id is an orphan;
// with no segments left, everything is (MaxInt64).
func TestOrphanSweepBound(t *testing.T) {
	assert.Equal(t, int64(5), orphanSweepBound(map[int64]*meta.SegmentMeta{
		5: segMeta(5, proto.SegmentState_Sealed),
		7: segMeta(7, proto.SegmentState_Truncated),
		9: segMeta(9, proto.SegmentState_Active),
	}))
	assert.Equal(t, int64(math.MaxInt64), orphanSweepBound(map[int64]*meta.SegmentMeta{}))
}

// TestSweepOrphanedCleanupRecords verifies the periodic sweep drives BOTH cleanup-domain
// record types (cleaning + marking) with the live-list bound — the reclaim path for a
// best-effort record delete that failed after the segment metadata was already gone (the
// idle-log case the batch-time sweeps never reach).
func TestSweepOrphanedCleanupRecords(t *testing.T) {
	lh := &testLogHandleMock{}
	lh.On("GetName").Return("test-log").Maybe()
	lh.On("GetId").Return(int64(1)).Maybe()
	cm := &recordingCleanupManager{}
	nm := &countingNotifyManager{}

	// Live segments {5,9}: both sweeps run with bound 5.
	sweepOrphanedCleanupRecords(context.Background(), lh, cm, nm, map[int64]*meta.SegmentMeta{
		5: segMeta(5, proto.SegmentState_Sealed),
		9: segMeta(9, proto.SegmentState_Truncated),
	})
	assert.Equal(t, []int64{5}, cm.sweepBounds)
	assert.Equal(t, []int64{5}, nm.sweepBoundsSnapshot())

	// Empty log: both sweeps run with MaxInt64 — every leftover record (e.g. a PENDING_MANUAL
	// marking record whose delete failed while reaping the log's last segment) is reclaimed.
	sweepOrphanedCleanupRecords(context.Background(), lh, cm, nm, map[int64]*meta.SegmentMeta{})
	assert.Equal(t, []int64{5, math.MaxInt64}, cm.sweepBounds)
	assert.Equal(t, []int64{5, math.MaxInt64}, nm.sweepBoundsSnapshot())
}
