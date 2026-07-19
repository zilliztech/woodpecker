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

package segment

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

// newNotifyTestManager builds a manager over fresh mocks with an empty seed List
// (the common case: no pre-existing marking records for the log).
func newNotifyTestManager(t *testing.T) (*segmentCompactedNotifyManagerImpl, *mocks_meta.MetadataProvider, *mocks_logstore_client.LogStoreClientPool) {
	t.Helper()
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockMeta.EXPECT().ListSegmentCompactedNotifyStatus(mock.Anything, mock.Anything).
		Return([]*proto.SegmentCompactedNotifyStatus{}, nil).Maybe()
	mgr := NewSegmentCompactedNotifyManager("bucket", "root", mockMeta, mockPool).(*segmentCompactedNotifyManagerImpl)
	return mgr, mockMeta, mockPool
}

// expectNotifyToNode wires the pool to hand out a client for addr whose
// NotifySegmentCompacted returns rpcErr.
func expectNotifyToNode(t *testing.T, mockPool *mocks_logstore_client.LogStoreClientPool, addr string, logId, segId int64, rpcErr error) {
	t.Helper()
	cli := mocks_logstore_client.NewLogStoreClient(t)
	cli.EXPECT().NotifySegmentCompacted(mock.Anything, "bucket", "root", logId, segId).Return(rpcErr)
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, addr).Return(cli, nil)
}

// TestNotifyManager_NewRecord_AllAck_Completes covers the fresh path: no record yet ->
// record created with every quorum node false -> fanout -> all ack -> COMPLETED + settled
// (the second call is an in-memory fast path with no etcd read).
func TestNotifyManager_NewRecord_AllAck_Completes(t *testing.T) {
	mgr, mockMeta, mockPool := newNotifyTestManager(t)
	ctx := context.Background()
	logId, segId := int64(7), int64(3)

	mockMeta.EXPECT().GetSegmentCompactedNotifyStatus(mock.Anything, logId, segId).Return(nil, nil).Once()
	mockSegmentMetaWithQuorum(mockMeta, "test-log", segId, []string{"node1", "node2"})
	mockMeta.EXPECT().CreateSegmentCompactedNotifyStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCompactedNotifyStatus) bool {
		return s.LogId == logId && s.SegmentId == segId &&
			s.State == proto.SegmentCompactedNotifyState_NOTIFY_IN_PROGRESS &&
			len(s.QuorumNotifyStatus) == 2 && !s.QuorumNotifyStatus["node1"] && !s.QuorumNotifyStatus["node2"]
	})).Return(nil).Once()
	expectNotifyToNode(t, mockPool, "node1", logId, segId, nil)
	expectNotifyToNode(t, mockPool, "node2", logId, segId, nil)
	mockMeta.EXPECT().UpdateSegmentCompactedNotifyStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCompactedNotifyStatus) bool {
		return s.State == proto.SegmentCompactedNotifyState_NOTIFY_COMPLETED &&
			s.QuorumNotifyStatus["node1"] && s.QuorumNotifyStatus["node2"]
	})).Return(nil).Once()

	advanced, err := mgr.EnsureSegmentNotified(ctx, "test-log", logId, segId)
	require.NoError(t, err)
	assert.True(t, advanced)

	// Second call: settled fast path — no further etcd/RPC expectations may fire.
	advanced, err = mgr.EnsureSegmentNotified(ctx, "test-log", logId, segId)
	require.NoError(t, err)
	assert.False(t, advanced)
}

// TestNotifyManager_Resume_OnlyUnackedNodesRetried covers exact per-node resume: an
// IN_PROGRESS record with node1 already acked -> only node2 gets an RPC -> COMPLETED.
func TestNotifyManager_Resume_OnlyUnackedNodesRetried(t *testing.T) {
	mgr, mockMeta, mockPool := newNotifyTestManager(t)
	ctx := context.Background()
	logId, segId := int64(7), int64(4)

	mockMeta.EXPECT().GetSegmentCompactedNotifyStatus(mock.Anything, logId, segId).Return(&proto.SegmentCompactedNotifyStatus{
		LogId: logId, SegmentId: segId,
		State:              proto.SegmentCompactedNotifyState_NOTIFY_IN_PROGRESS,
		StartTime:          uint64(time.Now().UnixMilli()),
		QuorumNotifyStatus: map[string]bool{"node1": true, "node2": false},
	}, nil).Once()
	mockSegmentMetaWithQuorum(mockMeta, "test-log", segId, []string{"node1", "node2"})
	// Only node2 may be dialed: no expectation exists for node1, so a call to it fails the test.
	expectNotifyToNode(t, mockPool, "node2", logId, segId, nil)
	mockMeta.EXPECT().UpdateSegmentCompactedNotifyStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCompactedNotifyStatus) bool {
		return s.State == proto.SegmentCompactedNotifyState_NOTIFY_COMPLETED
	})).Return(nil).Once()

	advanced, err := mgr.EnsureSegmentNotified(ctx, "test-log", logId, segId)
	require.NoError(t, err)
	assert.True(t, advanced)
}

// TestNotifyManager_FailureWithinBudget_StaysInProgress covers the retry path: a node
// failing while the record is younger than the budget keeps the record IN_PROGRESS (so the
// next auditor cycle retries) and does NOT settle the segment.
func TestNotifyManager_FailureWithinBudget_StaysInProgress(t *testing.T) {
	mgr, mockMeta, mockPool := newNotifyTestManager(t)
	ctx := context.Background()
	logId, segId := int64(7), int64(5)

	mockMeta.EXPECT().GetSegmentCompactedNotifyStatus(mock.Anything, logId, segId).Return(&proto.SegmentCompactedNotifyStatus{
		LogId: logId, SegmentId: segId,
		State:              proto.SegmentCompactedNotifyState_NOTIFY_IN_PROGRESS,
		StartTime:          uint64(time.Now().UnixMilli()), // fresh: well within the budget
		QuorumNotifyStatus: map[string]bool{"node1": false},
	}, nil).Once()
	mockSegmentMetaWithQuorum(mockMeta, "test-log", segId, []string{"node1"})
	expectNotifyToNode(t, mockPool, "node1", logId, segId, errors.New("connection refused"))
	mockMeta.EXPECT().UpdateSegmentCompactedNotifyStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCompactedNotifyStatus) bool {
		return s.State == proto.SegmentCompactedNotifyState_NOTIFY_IN_PROGRESS && !s.QuorumNotifyStatus["node1"]
	})).Return(nil).Once()

	advanced, err := mgr.EnsureSegmentNotified(ctx, "test-log", logId, segId)
	require.NoError(t, err)
	assert.True(t, advanced)

	// Not settled: a subsequent call must drive it again (Get expected again).
	mgr.mu.Lock()
	_, settled := mgr.settled[segId]
	mgr.mu.Unlock()
	assert.False(t, settled, "a within-budget failure must keep the segment unsettled for retry")
}

// TestNotifyManager_BudgetSpent_ParksPendingManual covers the manual-ops escalation: the
// record's StartTime is past the retry budget and a node still fails -> PENDING_MANUAL with
// the stuck node recorded -> settled (no further auto-retry; the next call is a fast path).
func TestNotifyManager_BudgetSpent_ParksPendingManual(t *testing.T) {
	mgr, mockMeta, mockPool := newNotifyTestManager(t)
	ctx := context.Background()
	logId, segId := int64(7), int64(6)

	started := time.Now().Add(-notifyPendingManualAfter - time.Minute) // budget spent
	mockMeta.EXPECT().GetSegmentCompactedNotifyStatus(mock.Anything, logId, segId).Return(&proto.SegmentCompactedNotifyStatus{
		LogId: logId, SegmentId: segId,
		State:              proto.SegmentCompactedNotifyState_NOTIFY_IN_PROGRESS,
		StartTime:          uint64(started.UnixMilli()),
		QuorumNotifyStatus: map[string]bool{"node1": true, "node2": false},
	}, nil).Once()
	mockSegmentMetaWithQuorum(mockMeta, "test-log", segId, []string{"node1", "node2"})
	expectNotifyToNode(t, mockPool, "node2", logId, segId, errors.New("still down"))
	mockMeta.EXPECT().UpdateSegmentCompactedNotifyStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCompactedNotifyStatus) bool {
		return s.State == proto.SegmentCompactedNotifyState_NOTIFY_PENDING_MANUAL &&
			s.ErrorMessage != "" && !s.QuorumNotifyStatus["node2"]
	})).Return(nil).Once()

	advanced, err := mgr.EnsureSegmentNotified(ctx, "test-log", logId, segId)
	require.NoError(t, err)
	assert.True(t, advanced)

	// Parked: subsequent calls are fast-path skips (no Get/RPC expectations set for them).
	advanced, err = mgr.EnsureSegmentNotified(ctx, "test-log", logId, segId)
	require.NoError(t, err)
	assert.False(t, advanced, "PENDING_MANUAL must be excluded from auto-retry")
}

// TestNotifyManager_ExistingCompletedRecord_FastPath covers restart-with-record: the first
// call reads the COMPLETED record once, settles it, and later calls never touch etcd again.
func TestNotifyManager_ExistingCompletedRecord_FastPath(t *testing.T) {
	mgr, mockMeta, _ := newNotifyTestManager(t)
	ctx := context.Background()
	logId, segId := int64(7), int64(8)

	mockMeta.EXPECT().GetSegmentCompactedNotifyStatus(mock.Anything, logId, segId).Return(&proto.SegmentCompactedNotifyStatus{
		LogId: logId, SegmentId: segId,
		State: proto.SegmentCompactedNotifyState_NOTIFY_COMPLETED,
	}, nil).Once()

	advanced, err := mgr.EnsureSegmentNotified(ctx, "test-log", logId, segId)
	require.NoError(t, err)
	assert.False(t, advanced)

	advanced, err = mgr.EnsureSegmentNotified(ctx, "test-log", logId, segId)
	require.NoError(t, err)
	assert.False(t, advanced)
}

// TestNotifyManager_SeedSkipsSettledRecords covers writer-restart seeding: the one-time List
// pre-settles COMPLETED/PENDING_MANUAL records so those segments are skipped without even a
// per-segment Get, while IN_PROGRESS ones still get driven.
func TestNotifyManager_SeedSkipsSettledRecords(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mgr := NewSegmentCompactedNotifyManager("bucket", "root", mockMeta, mockPool).(*segmentCompactedNotifyManagerImpl)
	ctx := context.Background()
	logId := int64(9)

	mockMeta.EXPECT().ListSegmentCompactedNotifyStatus(mock.Anything, logId).Return([]*proto.SegmentCompactedNotifyStatus{
		{LogId: logId, SegmentId: 1, State: proto.SegmentCompactedNotifyState_NOTIFY_COMPLETED},
		{LogId: logId, SegmentId: 2, State: proto.SegmentCompactedNotifyState_NOTIFY_PENDING_MANUAL},
		{
			LogId: logId, SegmentId: 3, State: proto.SegmentCompactedNotifyState_NOTIFY_IN_PROGRESS,
			StartTime: uint64(time.Now().UnixMilli()), QuorumNotifyStatus: map[string]bool{"node1": false},
		},
	}, nil).Once()

	// Segments 1 and 2: seeded as settled — no Get may be issued for them.
	advanced, err := mgr.EnsureSegmentNotified(ctx, "test-log", logId, 1)
	require.NoError(t, err)
	assert.False(t, advanced)
	advanced, err = mgr.EnsureSegmentNotified(ctx, "test-log", logId, 2)
	require.NoError(t, err)
	assert.False(t, advanced)

	// Segment 3: still IN_PROGRESS — driven normally.
	mockMeta.EXPECT().GetSegmentCompactedNotifyStatus(mock.Anything, logId, int64(3)).Return(&proto.SegmentCompactedNotifyStatus{
		LogId: logId, SegmentId: 3,
		State:              proto.SegmentCompactedNotifyState_NOTIFY_IN_PROGRESS,
		StartTime:          uint64(time.Now().UnixMilli()),
		QuorumNotifyStatus: map[string]bool{"node1": false},
	}, nil).Once()
	mockSegmentMetaWithQuorum(mockMeta, "test-log", 3, []string{"node1"})
	expectNotifyToNode(t, mockPool, "node1", logId, 3, nil)
	mockMeta.EXPECT().UpdateSegmentCompactedNotifyStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCompactedNotifyStatus) bool {
		return s.SegmentId == 3 && s.State == proto.SegmentCompactedNotifyState_NOTIFY_COMPLETED
	})).Return(nil).Once()

	advanced, err = mgr.EnsureSegmentNotified(ctx, "test-log", logId, 3)
	require.NoError(t, err)
	assert.True(t, advanced)
}

// TestNotifyManager_QuorumChange_AddsMissingNode covers the defensive resume path: a node
// present in the segment's quorum but absent from the record map is added (as unacked) and
// notified.
func TestNotifyManager_QuorumChange_AddsMissingNode(t *testing.T) {
	mgr, mockMeta, mockPool := newNotifyTestManager(t)
	ctx := context.Background()
	logId, segId := int64(7), int64(10)

	mockMeta.EXPECT().GetSegmentCompactedNotifyStatus(mock.Anything, logId, segId).Return(&proto.SegmentCompactedNotifyStatus{
		LogId: logId, SegmentId: segId,
		State:              proto.SegmentCompactedNotifyState_NOTIFY_IN_PROGRESS,
		StartTime:          uint64(time.Now().UnixMilli()),
		QuorumNotifyStatus: map[string]bool{"node1": true}, // node2 missing from the record
	}, nil).Once()
	mockSegmentMetaWithQuorum(mockMeta, "test-log", segId, []string{"node1", "node2"})
	expectNotifyToNode(t, mockPool, "node2", logId, segId, nil)
	mockMeta.EXPECT().UpdateSegmentCompactedNotifyStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCompactedNotifyStatus) bool {
		return s.State == proto.SegmentCompactedNotifyState_NOTIFY_COMPLETED && s.QuorumNotifyStatus["node2"]
	})).Return(nil).Once()

	advanced, err := mgr.EnsureSegmentNotified(ctx, "test-log", logId, segId)
	require.NoError(t, err)
	assert.True(t, advanced)
}

// TestNotifyManager_CleanupOrphanedStatuses covers the reap: records below minSegmentId
// (their segment meta already deleted) are removed — including PENDING_MANUAL ones — and
// their settled-cache entries dropped; records at/above minSegmentId are untouched.
func TestNotifyManager_CleanupOrphanedStatuses(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mgr := NewSegmentCompactedNotifyManager("bucket", "root", mockMeta, mockPool).(*segmentCompactedNotifyManagerImpl)
	ctx := context.Background()
	logId := int64(11)

	mgr.settled[1] = struct{}{}
	mgr.settled[2] = struct{}{}
	mgr.settled[5] = struct{}{}

	mockMeta.EXPECT().ListSegmentCompactedNotifyStatus(mock.Anything, logId).Return([]*proto.SegmentCompactedNotifyStatus{
		{LogId: logId, SegmentId: 1, State: proto.SegmentCompactedNotifyState_NOTIFY_COMPLETED},
		{LogId: logId, SegmentId: 2, State: proto.SegmentCompactedNotifyState_NOTIFY_PENDING_MANUAL},
		{LogId: logId, SegmentId: 5, State: proto.SegmentCompactedNotifyState_NOTIFY_IN_PROGRESS},
	}, nil).Once()
	mockMeta.EXPECT().DeleteSegmentCompactedNotifyStatus(mock.Anything, logId, int64(1)).Return(nil).Once()
	mockMeta.EXPECT().DeleteSegmentCompactedNotifyStatus(mock.Anything, logId, int64(2)).Return(nil).Once()
	// Segment 5 >= minSegmentId: must NOT be deleted (no expectation set).

	require.NoError(t, mgr.CleanupOrphanedStatuses(ctx, logId, 3))

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	assert.NotContains(t, mgr.settled, int64(1))
	assert.NotContains(t, mgr.settled, int64(2))
	assert.Contains(t, mgr.settled, int64(5))
}
