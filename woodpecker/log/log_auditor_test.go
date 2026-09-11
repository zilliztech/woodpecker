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

func (c *countingNotifyManager) reapedSegments() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int64(nil), c.reaped...)
}

type blockingNotifyManager struct {
	mu        sync.Mutex
	called    []int64
	started   chan struct{}
	canceled  chan struct{}
	release   chan struct{}
	startOne  sync.Once
	cancelOne sync.Once
}

func newBlockingNotifyManager() *blockingNotifyManager {
	return &blockingNotifyManager{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
		release:  make(chan struct{}),
	}
}

func (b *blockingNotifyManager) EnsureSegmentNotified(ctx context.Context, _ string, _ int64, segmentId int64) (bool, error) {
	b.mu.Lock()
	b.called = append(b.called, segmentId)
	b.mu.Unlock()
	b.startOne.Do(func() { close(b.started) })
	<-ctx.Done()
	b.cancelOne.Do(func() { close(b.canceled) })
	<-b.release
	return true, ctx.Err()
}

func (b *blockingNotifyManager) CleanupOrphanedStatuses(context.Context, int64, int64) error {
	return nil
}

func (b *blockingNotifyManager) MarkSegmentReaped(int64) {}

func (b *blockingNotifyManager) calledSegments() []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int64(nil), b.called...)
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
	st := compactCompletedSegments(context.Background(), lh, segs, false)
	require.Equal(t, 1, st.processed)
	assert.Equal(t, 0, st.compacted)
	assert.Equal(t, 1, st.failed)
}

func TestCompactCompletedSegments_CanceledWriterDoesNotStartCompaction(t *testing.T) {
	lh := &testLogHandleMock{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	st := compactCompletedSegments(ctx, lh, map[int64]*meta.SegmentMeta{
		1: segMeta(1, proto.SegmentState_Completed),
	}, false)

	assert.Equal(t, compactStats{}, st)
	lh.AssertNotCalled(t, "GetRecoverableSegmentHandle", mock.Anything, mock.Anything)
}

// TestCompactCompletedSegments_LocalModeSkipsPass verifies the compaction pass does no per-segment
// work on local storage: Compact() is a no-op there that never advances a segment out of Completed,
// so running the pass would re-walk the same set every auditor cycle -- taking the log handle's
// write lock and emitting a log line per segment -- forever.
func TestCompactCompletedSegments_LocalModeSkipsPass(t *testing.T) {
	lh := &testLogHandleMock{}
	lh.On("GetName").Return("test-log").Maybe()
	lh.On("GetId").Return(int64(1)).Maybe()

	st := compactCompletedSegments(context.Background(), lh, map[int64]*meta.SegmentMeta{
		1: segMeta(1, proto.SegmentState_Completed),
		2: segMeta(2, proto.SegmentState_Completed),
	}, true)

	assert.Equal(t, compactStats{}, st)
	lh.AssertNotCalled(t, "GetRecoverableSegmentHandle", mock.Anything, mock.Anything)
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
		runNotifyDistributor(context.Background(), lh, nm, false /*serviceMode*/, make(chan map[int64]*meta.SegmentMeta, 1))
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
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runNotifyDistributor(ctx, lh, nm, true /*serviceMode*/, segsCh)
		close(done)
	}()

	publishSegmentsSnapshot(segsCh, map[int64]*meta.SegmentMeta{
		1: segMeta(1, proto.SegmentState_Sealed),
	})
	require.Eventually(t, func() bool {
		lh.Mock.Test(t)
		return len(nm.calledSegments()) > 0
	}, 3*time.Second, 10*time.Millisecond, "the distributor consumed the published snapshot")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runNotifyDistributor should stop after its lifecycle context is canceled")
	}
	assert.Contains(t, nm.calledSegments(), int64(1))
}

func TestRunNotifyDistributor_CanceledContextSkipsBufferedSnapshot(t *testing.T) {
	lh := &testLogHandleMock{}
	lh.On("GetName").Return("test-log").Maybe()
	lh.On("GetId").Return(int64(1)).Maybe()
	nm := &countingNotifyManager{advanced: true}
	segsCh := make(chan map[int64]*meta.SegmentMeta, 1)
	segsCh <- map[int64]*meta.SegmentMeta{1: segMeta(1, proto.SegmentState_Sealed)}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		runNotifyDistributor(ctx, lh, nm, true, segsCh)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled distributor did not stop with a buffered snapshot")
	}
	assert.Empty(t, nm.calledSegments(), "a buffered snapshot must not start after cancellation")
}

func TestRunNotifyDistributor_CancellationStopsInFlightPass(t *testing.T) {
	lh := &testLogHandleMock{}
	lh.On("GetName").Return("test-log").Maybe()
	lh.On("GetId").Return(int64(1)).Maybe()
	nm := newBlockingNotifyManager()
	segsCh := make(chan map[int64]*meta.SegmentMeta, 1)
	segsCh <- map[int64]*meta.SegmentMeta{
		1: segMeta(1, proto.SegmentState_Sealed),
		2: segMeta(2, proto.SegmentState_Sealed),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runNotifyDistributor(ctx, lh, nm, true, segsCh)
		close(done)
	}()

	select {
	case <-nm.started:
	case <-time.After(2 * time.Second):
		t.Fatal("distributor did not start the first segment")
	}
	cancel()
	select {
	case <-nm.canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight notify did not observe lifecycle cancellation")
	}
	close(nm.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("distributor did not exit after the canceled pass returned")
	}
	assert.Len(t, nm.calledSegments(), 1, "cancellation must stop the pass before another segment")
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

// TestCompactCompletedSegments_BoundedPerCycle verifies one cycle compacts at most
// maxCompactedPerCycle segments and reports the rest as deferred, so a backlog cannot swamp a
// single cycle -- the exposure being a recovery stampede, where compaction has been failing, the
// Completed set has grown, and every log tries to drain it the moment storage comes back.
func TestCompactCompletedSegments_BoundedPerCycle(t *testing.T) {
	lh := &testLogHandleMock{}
	lh.On("GetName").Return("test-log").Maybe()
	lh.On("GetId").Return(int64(1)).Maybe()
	// Every lookup fails: the pass still counts the segment as processed, which is what the
	// bound is being measured against, and no segment handle is needed.
	lh.On("GetRecoverableSegmentHandle", mock.Anything, mock.Anything).Return(nil, errors.New("boom"))

	const total = maxCompactedPerCycle + 25
	segs := map[int64]*meta.SegmentMeta{}
	for i := int64(1); i <= total; i++ {
		segs[i] = segMeta(i, proto.SegmentState_Completed)
	}

	st := compactCompletedSegments(context.Background(), lh, segs, false)
	assert.Equal(t, maxCompactedPerCycle, st.processed)
	assert.Equal(t, total-maxCompactedPerCycle, st.deferred)
}

// TestCompactCompletedSegments_OldestFirst verifies the bounded pass drains in ascending segment
// order. Ranging a map is randomly ordered; unbounded that was harmless because every Completed
// segment was visited, but under a bound it would let a segment be skipped cycle after cycle by
// chance while its local data.log stayed on disk.
func TestCompactCompletedSegments_OldestFirst(t *testing.T) {
	lh := &testLogHandleMock{}
	lh.On("GetName").Return("test-log").Maybe()
	lh.On("GetId").Return(int64(1)).Maybe()

	var mu sync.Mutex
	var seen []int64
	lh.On("GetRecoverableSegmentHandle", mock.Anything, mock.Anything).
		Return(nil, errors.New("boom")).
		Run(func(args mock.Arguments) {
			mu.Lock()
			defer mu.Unlock()
			seen = append(seen, args.Get(1).(int64))
		})

	// Insert high segment numbers first so a map-order pass would be unlikely to come out sorted.
	segs := map[int64]*meta.SegmentMeta{}
	for i := int64(maxCompactedPerCycle * 2); i >= 1; i-- {
		segs[i] = segMeta(i, proto.SegmentState_Completed)
	}

	compactCompletedSegments(context.Background(), lh, segs, false)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, maxCompactedPerCycle)
	for i, segNo := range seen {
		require.Equal(t, int64(i+1), segNo, "expected the oldest %d segments in order", maxCompactedPerCycle)
	}
}

// TestWaitAuditorStartJitter covers the phase spreading that keeps writers created together from
// ticking in lockstep, and the two shutdown signals the auditor loop itself watches: a writer
// closed during the wait must not be held up by it.
func TestWaitAuditorStartJitter(t *testing.T) {
	closed := make(chan struct{})

	t.Run("spreads the start across the interval", func(t *testing.T) {
		const interval = 50 * time.Millisecond
		buckets := map[int]int{}
		for range 60 {
			start := time.Now()
			require.True(t, waitAuditorStartJitter(context.Background(), closed, interval))
			waited := time.Since(start)
			// Generous upper bound: the delay is under one interval by construction, the slack
			// only absorbs scheduler wake-up latency on a loaded CI machine.
			require.Less(t, waited, interval+100*time.Millisecond)
			if b := int(waited * 4 / interval); b < 4 {
				buckets[b]++ // 4 quarters of the interval
			}
		}
		// A fixed phase would land every sample in one bucket; spreading is the whole point.
		assert.GreaterOrEqual(t, len(buckets), 3, "start should spread across the interval, got %v", buckets)
	})

	t.Run("returns immediately when the writer is closing", func(t *testing.T) {
		writerClose := make(chan struct{}, 1)
		writerClose <- struct{}{}
		start := time.Now()
		assert.False(t, waitAuditorStartJitter(context.Background(), writerClose, time.Hour))
		assert.Less(t, time.Since(start), time.Second)
	})

	t.Run("returns immediately when the writer is invalidated", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		start := time.Now()
		assert.False(t, waitAuditorStartJitter(ctx, closed, time.Hour))
		assert.Less(t, time.Since(start), time.Second)
	})

	t.Run("no wait when the interval is not positive", func(t *testing.T) {
		start := time.Now()
		assert.True(t, waitAuditorStartJitter(context.Background(), closed, 0))
		assert.True(t, waitAuditorStartJitter(context.Background(), closed, -time.Second))
		assert.Less(t, time.Since(start), 50*time.Millisecond)
	})
}
