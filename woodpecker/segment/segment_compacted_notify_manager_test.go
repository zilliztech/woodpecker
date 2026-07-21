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

	"github.com/zilliztech/woodpecker/meta"
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

// === error-branch coverage ===

// TestNotifyManager_SeedListError verifies a seed (List) failure DEGRADES instead of blocking:
// distribution proceeds via per-segment Gets with an empty cache — a single failing List (e.g.
// transient etcd trouble, or one corrupt record before the List was hardened) must never
// disable mark distribution for the whole log nor bypass the caller's per-cycle budget.
func TestNotifyManager_SeedListError(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mgr := NewSegmentCompactedNotifyManager("bucket", "root", mockMeta, mockPool).(*segmentCompactedNotifyManagerImpl)

	// Seed fails, but the settled record is still discovered via the Get path.
	mockMeta.EXPECT().ListSegmentCompactedNotifyStatus(mock.Anything, int64(1)).Return(nil, errors.New("etcd down")).Once()
	mockMeta.EXPECT().GetSegmentCompactedNotifyStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCompactedNotifyStatus{
		LogId: 1, SegmentId: 2, State: proto.SegmentCompactedNotifyState_NOTIFY_COMPLETED,
	}, nil).Once()
	advanced, err := mgr.EnsureSegmentNotified(context.Background(), "test-log", 1, 2)
	require.NoError(t, err, "a seed failure must degrade, not block")
	assert.False(t, advanced)

	// Seed is retried on a later call (not latched by the failure) and succeeds.
	mockMeta.EXPECT().ListSegmentCompactedNotifyStatus(mock.Anything, int64(1)).Return([]*proto.SegmentCompactedNotifyStatus{
		{LogId: 1, SegmentId: 3, State: proto.SegmentCompactedNotifyState_NOTIFY_COMPLETED},
	}, nil).Once()
	advanced, err = mgr.EnsureSegmentNotified(context.Background(), "test-log", 1, 3)
	require.NoError(t, err)
	assert.False(t, advanced, "seeded settled record is a fast path (no Get expected)")
}

// TestNotifyManager_MarkSegmentReaped verifies the in-process reap sync: a segment marked
// reaped (its truncate reclamation started on this writer) is skipped entirely — no etcd
// read, no record creation, no RPC — even if a stale snapshot still shows it Sealed.
func TestNotifyManager_MarkSegmentReaped(t *testing.T) {
	mgr, _, _ := newNotifyTestManager(t)
	mgr.MarkSegmentReaped(42)

	advanced, err := mgr.EnsureSegmentNotified(context.Background(), "test-log", 1, 42)
	require.NoError(t, err)
	assert.False(t, advanced, "a reaped segment must never be driven again")
}

func TestNotifyManager_GetStatusError(t *testing.T) {
	mgr, mockMeta, _ := newNotifyTestManager(t)
	mockMeta.EXPECT().GetSegmentCompactedNotifyStatus(mock.Anything, int64(1), int64(2)).Return(nil, errors.New("etcd timeout")).Once()
	advanced, err := mgr.EnsureSegmentNotified(context.Background(), "test-log", 1, 2)
	require.Error(t, err)
	assert.True(t, advanced)
}

func TestNotifyManager_CreateRecordError(t *testing.T) {
	mgr, mockMeta, _ := newNotifyTestManager(t)
	mockMeta.EXPECT().GetSegmentCompactedNotifyStatus(mock.Anything, int64(1), int64(3)).Return(nil, nil).Once()
	mockSegmentMetaWithQuorum(mockMeta, "test-log", 3, []string{"node1"})
	mockMeta.EXPECT().CreateSegmentCompactedNotifyStatus(mock.Anything, mock.Anything).Return(errors.New("txn failed")).Once()
	advanced, err := mgr.EnsureSegmentNotified(context.Background(), "test-log", 1, 3)
	require.Error(t, err)
	assert.True(t, advanced)
}

func TestNotifyManager_GetSegmentMetadataError(t *testing.T) {
	mgr, mockMeta, _ := newNotifyTestManager(t)
	mockMeta.EXPECT().GetSegmentCompactedNotifyStatus(mock.Anything, int64(1), int64(4)).Return(nil, nil).Once()
	mockMeta.EXPECT().GetSegmentMetadata(mock.Anything, "test-log", int64(4)).Return(nil, errors.New("meta gone")).Once()
	advanced, err := mgr.EnsureSegmentNotified(context.Background(), "test-log", 1, 4)
	require.Error(t, err)
	assert.True(t, advanced)
}

// TestNotifyManager_NilQuorumFallsBackToEmbedNode covers the embed/standalone fallback: a
// segment meta without quorum info distributes to the single 127.0.0.1 node.
func TestNotifyManager_NilQuorumFallsBackToEmbedNode(t *testing.T) {
	mgr, mockMeta, mockPool := newNotifyTestManager(t)
	logId, segId := int64(1), int64(5)

	mockMeta.EXPECT().GetSegmentCompactedNotifyStatus(mock.Anything, logId, segId).Return(nil, nil).Once()
	mockMeta.EXPECT().GetSegmentMetadata(mock.Anything, "test-log", segId).Return(&meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{}, // no quorum info
	}, nil).Once()
	mockMeta.EXPECT().CreateSegmentCompactedNotifyStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCompactedNotifyStatus) bool {
		_, ok := s.QuorumNotifyStatus["127.0.0.1"]
		return len(s.QuorumNotifyStatus) == 1 && ok
	})).Return(nil).Once()
	expectNotifyToNode(t, mockPool, "127.0.0.1", logId, segId, nil)
	mockMeta.EXPECT().UpdateSegmentCompactedNotifyStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCompactedNotifyStatus) bool {
		return s.State == proto.SegmentCompactedNotifyState_NOTIFY_COMPLETED
	})).Return(nil).Once()

	advanced, err := mgr.EnsureSegmentNotified(context.Background(), "test-log", logId, segId)
	require.NoError(t, err)
	assert.True(t, advanced)
}

// TestNotifyManager_ClientLookupFailure covers the GetLogStoreClient error path: the node
// stays unacked and the record stays IN_PROGRESS for the next cycle.
func TestNotifyManager_ClientLookupFailure(t *testing.T) {
	mgr, mockMeta, mockPool := newNotifyTestManager(t)
	logId, segId := int64(1), int64(6)

	mockMeta.EXPECT().GetSegmentCompactedNotifyStatus(mock.Anything, logId, segId).Return(&proto.SegmentCompactedNotifyStatus{
		LogId: logId, SegmentId: segId,
		State:              proto.SegmentCompactedNotifyState_NOTIFY_IN_PROGRESS,
		StartTime:          uint64(time.Now().UnixMilli()),
		QuorumNotifyStatus: map[string]bool{"node1": false},
	}, nil).Once()
	mockSegmentMetaWithQuorum(mockMeta, "test-log", segId, []string{"node1"})
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(nil, errors.New("no route")).Once()
	mockMeta.EXPECT().UpdateSegmentCompactedNotifyStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCompactedNotifyStatus) bool {
		return s.State == proto.SegmentCompactedNotifyState_NOTIFY_IN_PROGRESS && !s.QuorumNotifyStatus["node1"]
	})).Return(nil).Once()

	advanced, err := mgr.EnsureSegmentNotified(context.Background(), "test-log", logId, segId)
	require.NoError(t, err)
	assert.True(t, advanced)
}

func TestNotifyManager_FinalizeUpdateError(t *testing.T) {
	mgr, mockMeta, mockPool := newNotifyTestManager(t)
	logId, segId := int64(1), int64(7)

	mockMeta.EXPECT().GetSegmentCompactedNotifyStatus(mock.Anything, logId, segId).Return(&proto.SegmentCompactedNotifyStatus{
		LogId: logId, SegmentId: segId,
		State:              proto.SegmentCompactedNotifyState_NOTIFY_IN_PROGRESS,
		StartTime:          uint64(time.Now().UnixMilli()),
		QuorumNotifyStatus: map[string]bool{"node1": false},
	}, nil).Once()
	mockSegmentMetaWithQuorum(mockMeta, "test-log", segId, []string{"node1"})
	expectNotifyToNode(t, mockPool, "node1", logId, segId, nil)
	mockMeta.EXPECT().UpdateSegmentCompactedNotifyStatus(mock.Anything, mock.Anything).Return(errors.New("etcd txn failed")).Once()

	advanced, err := mgr.EnsureSegmentNotified(context.Background(), "test-log", logId, segId)
	require.Error(t, err)
	assert.True(t, advanced)
}

// TestNotifyManager_ResumedFullyAckedRecord_CompletesWithoutRPC covers the nodesToNotify==0
// path: a resumed record whose nodes are all acked completes directly, with no RPC.
func TestNotifyManager_ResumedFullyAckedRecord_CompletesWithoutRPC(t *testing.T) {
	mgr, mockMeta, _ := newNotifyTestManager(t)
	logId, segId := int64(1), int64(8)

	mockMeta.EXPECT().GetSegmentCompactedNotifyStatus(mock.Anything, logId, segId).Return(&proto.SegmentCompactedNotifyStatus{
		LogId: logId, SegmentId: segId,
		State:              proto.SegmentCompactedNotifyState_NOTIFY_IN_PROGRESS,
		StartTime:          uint64(time.Now().UnixMilli()),
		QuorumNotifyStatus: map[string]bool{"node1": true},
	}, nil).Once()
	mockSegmentMetaWithQuorum(mockMeta, "test-log", segId, []string{"node1"})
	mockMeta.EXPECT().UpdateSegmentCompactedNotifyStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCompactedNotifyStatus) bool {
		return s.State == proto.SegmentCompactedNotifyState_NOTIFY_COMPLETED
	})).Return(nil).Once()

	advanced, err := mgr.EnsureSegmentNotified(context.Background(), "test-log", logId, segId)
	require.NoError(t, err)
	assert.True(t, advanced)
}

func TestNotifyManager_CleanupOrphaned_ListError(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mgr := NewSegmentCompactedNotifyManager("bucket", "root", mockMeta, mockPool).(*segmentCompactedNotifyManagerImpl)
	mockMeta.EXPECT().ListSegmentCompactedNotifyStatus(mock.Anything, int64(1)).Return(nil, errors.New("etcd down")).Once()
	require.Error(t, mgr.CleanupOrphanedStatuses(context.Background(), 1, 5))
}

// TestNotifyManager_CleanupOrphaned_DeleteError verifies a per-record delete failure is
// logged and skipped (kept for the next sweep) without failing the whole sweep.
func TestNotifyManager_CleanupOrphaned_DeleteError(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mgr := NewSegmentCompactedNotifyManager("bucket", "root", mockMeta, mockPool).(*segmentCompactedNotifyManagerImpl)
	mgr.settled[1] = struct{}{}

	mockMeta.EXPECT().ListSegmentCompactedNotifyStatus(mock.Anything, int64(1)).Return([]*proto.SegmentCompactedNotifyStatus{
		{LogId: 1, SegmentId: 1, State: proto.SegmentCompactedNotifyState_NOTIFY_COMPLETED},
	}, nil).Once()
	mockMeta.EXPECT().DeleteSegmentCompactedNotifyStatus(mock.Anything, int64(1), int64(1)).Return(errors.New("etcd down")).Once()

	require.NoError(t, mgr.CleanupOrphanedStatuses(context.Background(), 1, 5))
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	assert.Contains(t, mgr.settled, int64(1), "settled entry kept when the delete failed (retried next sweep)")
}

// TestNotifyManager_OperatorConfirmed_IsSettled verifies #4: an OPERATOR_CONFIRMED record is
// terminal/settled — EnsureSegmentNotified returns (false, nil) without creating or notifying,
// even across a writer restart (fresh manager re-seeds it as settled).
func TestNotifyManager_OperatorConfirmed_IsSettled(t *testing.T) {
	mgr, mockMeta, _ := newNotifyTestManager(t)
	logId, segId := int64(1), int64(20)

	mockMeta.EXPECT().GetSegmentCompactedNotifyStatus(mock.Anything, logId, segId).Return(&proto.SegmentCompactedNotifyStatus{
		LogId: logId, SegmentId: segId,
		State: proto.SegmentCompactedNotifyState_NOTIFY_OPERATOR_CONFIRMED,
	}, nil).Once()

	advanced, err := mgr.EnsureSegmentNotified(context.Background(), "test-log", logId, segId)
	require.NoError(t, err)
	assert.False(t, advanced)

	// Settled fast path on the next call: no Get expected.
	advanced, err = mgr.EnsureSegmentNotified(context.Background(), "test-log", logId, segId)
	require.NoError(t, err)
	assert.False(t, advanced)
}

// TestNotifyManager_Seed_OperatorConfirmedIsSettled verifies a restart re-seeds an
// OPERATOR_CONFIRMED record as settled (so a confirmed segment is never rebuilt).
func TestNotifyManager_Seed_OperatorConfirmedIsSettled(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mgr := NewSegmentCompactedNotifyManager("bucket", "root", mockMeta, mockPool).(*segmentCompactedNotifyManagerImpl)
	logId := int64(1)

	mockMeta.EXPECT().ListSegmentCompactedNotifyStatus(mock.Anything, logId).Return([]*proto.SegmentCompactedNotifyStatus{
		{LogId: logId, SegmentId: 5, State: proto.SegmentCompactedNotifyState_NOTIFY_OPERATOR_CONFIRMED},
	}, nil).Once()

	// Seeded as settled -> no Get for segment 5.
	advanced, err := mgr.EnsureSegmentNotified(context.Background(), "test-log", logId, 5)
	require.NoError(t, err)
	assert.False(t, advanced)
}

// TestNotifyManager_Resume_PrunesRemovedNode verifies #5: a node no longer in the quorum is
// pruned from the persisted map on resume, so it can never block NOTIFY_COMPLETED. Here node1
// (unacked, removed from quorum) is dropped and only node2 remains -> COMPLETED.
func TestNotifyManager_Resume_PrunesRemovedNode(t *testing.T) {
	mgr, mockMeta, _ := newNotifyTestManager(t)
	logId, segId := int64(1), int64(21)

	mockMeta.EXPECT().GetSegmentCompactedNotifyStatus(mock.Anything, logId, segId).Return(&proto.SegmentCompactedNotifyStatus{
		LogId: logId, SegmentId: segId,
		State:              proto.SegmentCompactedNotifyState_NOTIFY_IN_PROGRESS,
		StartTime:          uint64(time.Now().UnixMilli()),
		QuorumNotifyStatus: map[string]bool{"node1": false, "node2": true}, // node1 removed, unacked
	}, nil).Once()
	// Current quorum no longer contains node1.
	mockSegmentMetaWithQuorum(mockMeta, "test-log", segId, []string{"node2"})
	// node2 already acked, node1 pruned -> nothing to notify -> COMPLETED, node1 dropped.
	mockMeta.EXPECT().UpdateSegmentCompactedNotifyStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCompactedNotifyStatus) bool {
		_, node1Present := s.QuorumNotifyStatus["node1"]
		return s.State == proto.SegmentCompactedNotifyState_NOTIFY_COMPLETED && !node1Present
	})).Return(nil).Once()

	advanced, err := mgr.EnsureSegmentNotified(context.Background(), "test-log", logId, segId)
	require.NoError(t, err)
	assert.True(t, advanced)
}

// TestNotifyManager_CleanupOrphaned_DeletesByStatusLogId verifies #6: deletion uses the
// record's own LogId (defense-in-depth), not the caller's.
func TestNotifyManager_CleanupOrphaned_DeletesByStatusLogId(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mgr := NewSegmentCompactedNotifyManager("bucket", "root", mockMeta, mockPool).(*segmentCompactedNotifyManagerImpl)
	callerLogId := int64(1)

	// A record whose own LogId differs from the caller's (would only happen under a scan bug,
	// but the delete must still target the record's own LogId).
	mockMeta.EXPECT().ListSegmentCompactedNotifyStatus(mock.Anything, callerLogId).Return([]*proto.SegmentCompactedNotifyStatus{
		{LogId: 99, SegmentId: 2, State: proto.SegmentCompactedNotifyState_NOTIFY_COMPLETED},
	}, nil).Once()
	mockMeta.EXPECT().DeleteSegmentCompactedNotifyStatus(mock.Anything, int64(99), int64(2)).Return(nil).Once()

	require.NoError(t, mgr.CleanupOrphanedStatuses(context.Background(), callerLogId, 5))
}
