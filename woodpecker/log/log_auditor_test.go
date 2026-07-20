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
	"sort"
	"testing"

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
type countingNotifyManager struct {
	called   []int64
	advanced bool
	err      error
}

func (c *countingNotifyManager) EnsureSegmentNotified(_ context.Context, _ string, _ int64, segmentId int64) (bool, error) {
	c.called = append(c.called, segmentId)
	return c.advanced, c.err
}

func (c *countingNotifyManager) CleanupOrphanedStatuses(_ context.Context, _ int64, _ int64) error {
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
	driven := distributeCompactedMarks(context.Background(), lh, nm, segs)

	assert.Equal(t, 2, driven, "both Sealed segments did real work")
	sort.Slice(nm.called, func(i, j int) bool { return nm.called[i] < nm.called[j] })
	assert.Equal(t, []int64{2, 4}, nm.called, "only Sealed segments are notified")
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
	driven := distributeCompactedMarks(context.Background(), lh, nm, segs)

	assert.Equal(t, 0, driven, "settled segments don't consume the budget")
	assert.Len(t, nm.called, 2, "but they are still asked (the manager fast-paths internally)")
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
	driven := distributeCompactedMarks(context.Background(), lh, nm, segs)

	assert.Equal(t, 0, driven)
	assert.Len(t, nm.called, 2, "both segments attempted despite the error")
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
