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

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

// === updateCleanupStatusWithResults tests ===

func TestUpdateCleanupStatusWithResults_AllSuccess(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		QuorumCleanupStatus: map[string]bool{"node1": false, "node2": false, "node3": false},
	}, nil)

	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCleanupStatus) bool {
		return s.State == proto.SegmentCleanupState_CLEANUP_COMPLETED &&
			s.QuorumCleanupStatus["node1"] == true &&
			s.QuorumCleanupStatus["node2"] == true &&
			s.QuorumCleanupStatus["node3"] == true
	})).Return(nil)

	results := []nodeCleanupResult{
		{nodeAddress: "node1", success: true},
		{nodeAddress: "node2", success: true},
		{nodeAddress: "node3", success: true},
	}
	status, err := mgr.updateCleanupStatusWithResults(context.Background(), 1, 2, results)
	assert.NoError(t, err)
	assert.Equal(t, proto.SegmentCleanupState_CLEANUP_COMPLETED, status.State)
}

func TestUpdateCleanupStatusWithResults_PartialSuccess(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		QuorumCleanupStatus: map[string]bool{"node1": false, "node2": false, "node3": false},
	}, nil)

	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCleanupStatus) bool {
		return s.State == proto.SegmentCleanupState_CLEANUP_FAILED &&
			s.QuorumCleanupStatus["node1"] == true &&
			s.QuorumCleanupStatus["node2"] == true &&
			s.QuorumCleanupStatus["node3"] == false &&
			s.ErrorMessage != ""
	})).Return(nil)

	results := []nodeCleanupResult{
		{nodeAddress: "node1", success: true},
		{nodeAddress: "node2", success: true},
		{nodeAddress: "node3", success: false, errorMsg: "connection refused"},
	}
	status, err := mgr.updateCleanupStatusWithResults(context.Background(), 1, 2, results)
	assert.NoError(t, err)
	assert.Equal(t, proto.SegmentCleanupState_CLEANUP_FAILED, status.State)
	assert.Contains(t, status.ErrorMessage, "node3")
}

func TestUpdateCleanupStatusWithResults_WithPreviouslyCompletedNodes(t *testing.T) {
	// Simulates resume: node1 already completed, only node2 was retried
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		QuorumCleanupStatus: map[string]bool{"node1": true, "node2": false},
	}, nil)

	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCleanupStatus) bool {
		return s.State == proto.SegmentCleanupState_CLEANUP_COMPLETED &&
			s.QuorumCleanupStatus["node1"] == true &&
			s.QuorumCleanupStatus["node2"] == true
	})).Return(nil)

	// Only node2 was retried
	results := []nodeCleanupResult{
		{nodeAddress: "node2", success: true},
	}
	status, err := mgr.updateCleanupStatusWithResults(context.Background(), 1, 2, results)
	assert.NoError(t, err)
	assert.Equal(t, proto.SegmentCleanupState_CLEANUP_COMPLETED, status.State)
}

func TestUpdateCleanupStatusWithResults_GetStatusError(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(nil, werr.ErrInternalError)

	_, err := mgr.updateCleanupStatusWithResults(context.Background(), 1, 2, nil)
	assert.Error(t, err)
}

func TestUpdateCleanupStatusWithResults_NilStatus(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(nil, nil)

	_, err := mgr.updateCleanupStatusWithResults(context.Background(), 1, 2, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
}

func TestUpdateCleanupStatusWithResults_NilQuorumMap(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		QuorumCleanupStatus: nil,
	}, nil)

	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCleanupStatus) bool {
		return s.QuorumCleanupStatus["node1"] == true && s.State == proto.SegmentCleanupState_CLEANUP_COMPLETED
	})).Return(nil)

	results := []nodeCleanupResult{
		{nodeAddress: "node1", success: true},
	}
	status, err := mgr.updateCleanupStatusWithResults(context.Background(), 1, 2, results)
	assert.NoError(t, err)
	assert.Equal(t, proto.SegmentCleanupState_CLEANUP_COMPLETED, status.State)
}

func TestUpdateCleanupStatusWithResults_UpdateError(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		QuorumCleanupStatus: map[string]bool{"node1": false},
	}, nil)
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.Anything).Return(werr.ErrInternalError)

	results := []nodeCleanupResult{
		{nodeAddress: "node1", success: true},
	}
	_, err := mgr.updateCleanupStatusWithResults(context.Background(), 1, 2, results)
	assert.Error(t, err)
}

func TestUpdateCleanupStatusWithResults_EmptyResults_NoNodes(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		QuorumCleanupStatus: map[string]bool{},
	}, nil)

	// totalNodes == 0 → FAILED
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCleanupStatus) bool {
		return s.State == proto.SegmentCleanupState_CLEANUP_FAILED
	})).Return(nil)

	status, err := mgr.updateCleanupStatusWithResults(context.Background(), 1, 2, nil)
	assert.NoError(t, err)
	assert.Equal(t, proto.SegmentCleanupState_CLEANUP_FAILED, status.State)
}

func TestUpdateCleanupStatusWithResults_ErrorMessageAppending(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		QuorumCleanupStatus: map[string]bool{"node1": false, "node2": false},
	}, nil)

	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCleanupStatus) bool {
		return s.State == proto.SegmentCleanupState_CLEANUP_FAILED &&
			s.ErrorMessage != ""
	})).Return(nil)

	results := []nodeCleanupResult{
		{nodeAddress: "node1", success: false, errorMsg: "timeout"},
		{nodeAddress: "node2", success: false, errorMsg: "refused"},
	}
	status, err := mgr.updateCleanupStatusWithResults(context.Background(), 1, 2, results)
	assert.NoError(t, err)
	assert.Equal(t, proto.SegmentCleanupState_CLEANUP_FAILED, status.State)
	assert.Contains(t, status.ErrorMessage, "node1")
}

// === sendCleanupRequestToNode tests ===

func TestSendCleanupRequestToNode_GetClientError(t *testing.T) {
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", nil, mockPool).(*segmentCleanupManagerImpl)

	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(nil, errors.New("connection failed"))

	err := mgr.sendCleanupRequestToNode(context.Background(), "test-log", 1, 2, "node1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection failed")
}

func TestSendCleanupRequestToNode_SegmentCleanError(t *testing.T) {
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)

	mgr := NewSegmentCleanupManager("bucket", "root", nil, mockPool).(*segmentCleanupManagerImpl)

	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClient.EXPECT().SegmentClean(mock.Anything, "bucket", "root", int64(1), int64(2), 0).Return(errors.New("clean failed"))

	err := mgr.sendCleanupRequestToNode(context.Background(), "test-log", 1, 2, "node1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "clean failed")
}

func TestSendCleanupRequestToNode_Success(t *testing.T) {
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)

	mgr := NewSegmentCleanupManager("bucket", "root", nil, mockPool).(*segmentCleanupManagerImpl)

	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClient.EXPECT().SegmentClean(mock.Anything, "bucket", "root", int64(1), int64(2), 0).Return(nil)

	err := mgr.sendCleanupRequestToNode(context.Background(), "test-log", 1, 2, "node1")
	assert.NoError(t, err)
}

// === CleanupSegment entry point tests ===

func TestCleanupSegment_AlreadyInProgress(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	// Mark cleanup as in progress
	mgr.cleanupMutex.Lock()
	mgr.inProgressCleanups["1-2"] = true
	mgr.cleanupMutex.Unlock()

	err := mgr.CleanupSegment(context.Background(), "test-log", 1, 2)
	assert.NoError(t, err)
}

func TestCleanupSegment_GetCleanupStatusError(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(nil, werr.ErrInternalError)

	err := mgr.CleanupSegment(context.Background(), "test-log", 1, 2)
	assert.Error(t, err)

	// Verify in-progress flag was cleaned up
	mgr.cleanupMutex.Lock()
	assert.False(t, mgr.inProgressCleanups["1-2"])
	mgr.cleanupMutex.Unlock()
}

func TestCleanupSegment_NewCleanupTask_Success(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	// No existing cleanup status
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(nil, nil).Once()

	// Create cleanup status
	mockMeta.EXPECT().CreateSegmentCleanupStatus(mock.Anything, mock.Anything).Return(nil)

	// Get client and cleanup
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "127.0.0.1").Return(mockClient, nil)
	mockClient.EXPECT().SegmentClean(mock.Anything, "bucket", "root", int64(1), int64(2), 0).Return(nil)

	// updateCleanupStatusWithResults: read status, update with results, write back
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		QuorumCleanupStatus: map[string]bool{"127.0.0.1": false},
	}, nil).Once()
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCleanupStatus) bool {
		return s.State == proto.SegmentCleanupState_CLEANUP_COMPLETED &&
			s.QuorumCleanupStatus["127.0.0.1"] == true
	})).Return(nil).Once()

	err := mgr.CleanupSegment(context.Background(), "test-log", 1, 2)
	assert.NoError(t, err)
}

func TestCleanupSegment_NewCleanupTask_CreateStatusError(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(nil, nil)
	mockMeta.EXPECT().CreateSegmentCleanupStatus(mock.Anything, mock.Anything).Return(werr.ErrInternalError)

	err := mgr.CleanupSegment(context.Background(), "test-log", 1, 2)
	assert.Error(t, err)
	assert.True(t, werr.ErrLogHandleTruncateFailed.Is(err))
}

func TestCleanupSegment_ExistingStatus_Completed(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	// Existing status is COMPLETED
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:     1,
		SegmentId: 2,
		State:     proto.SegmentCleanupState_CLEANUP_COMPLETED,
	}, nil)

	// Should delete segment metadata and cleanup status
	mockMeta.EXPECT().DeleteSegmentMetadata(mock.Anything, "test-log", int64(1), int64(2), proto.SegmentState_Truncated).Return(nil)
	mockMeta.EXPECT().DeleteSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(nil)

	err := mgr.CleanupSegment(context.Background(), "test-log", 1, 2)
	assert.NoError(t, err)
}

func TestCleanupSegment_ExistingStatus_Completed_DeleteMetadataError_ReturnsError(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:     1,
		SegmentId: 2,
		State:     proto.SegmentCleanupState_CLEANUP_COMPLETED,
	}, nil)

	// DeleteSegmentMetadata fails with non-NotFound error (e.g. etcd timeout)
	// Should return error immediately without deleting cleanup status
	mockMeta.EXPECT().DeleteSegmentMetadata(mock.Anything, "test-log", int64(1), int64(2), proto.SegmentState_Truncated).Return(werr.ErrInternalError)
	// DeleteSegmentCleanupStatus should NOT be called

	err := mgr.CleanupSegment(context.Background(), "test-log", 1, 2)
	assert.Error(t, err)
	assert.True(t, werr.ErrInternalError.Is(err))
}

func TestCleanupSegment_ExistingStatus_Completed_SegmentNotFoundIgnored(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:     1,
		SegmentId: 2,
		State:     proto.SegmentCleanupState_CLEANUP_COMPLETED,
	}, nil)

	// Delete returns ErrSegmentNotFound - should be ignored
	mockMeta.EXPECT().DeleteSegmentMetadata(mock.Anything, "test-log", int64(1), int64(2), proto.SegmentState_Truncated).Return(werr.ErrSegmentNotFound)
	mockMeta.EXPECT().DeleteSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(nil)

	err := mgr.CleanupSegment(context.Background(), "test-log", 1, 2)
	assert.NoError(t, err)
}

func TestCleanupSegment_ExistingStatus_Failed_Resume(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	// Existing status is FAILED
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:          1,
		SegmentId:      2,
		State:          proto.SegmentCleanupState_CLEANUP_FAILED,
		ErrorMessage:   "previous error",
		LastUpdateTime: uint64(time.Now().Add(-20 * time.Minute).UnixMilli()),
		QuorumCleanupStatus: map[string]bool{
			"127.0.0.1": false,
		},
	}, nil).Once()

	// handleExistingCleanupStatus updates status to IN_PROGRESS
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCleanupStatus) bool {
		return s.State == proto.SegmentCleanupState_CLEANUP_IN_PROGRESS
	})).Return(nil).Once()

	// Continue cleanup - RPC succeeds
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "127.0.0.1").Return(mockClient, nil)
	mockClient.EXPECT().SegmentClean(mock.Anything, "bucket", "root", int64(1), int64(2), 0).Return(nil)

	// updateCleanupStatusWithResults: read status, apply results, set COMPLETED
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		QuorumCleanupStatus: map[string]bool{"127.0.0.1": false},
	}, nil).Once()
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCleanupStatus) bool {
		return s.State == proto.SegmentCleanupState_CLEANUP_COMPLETED &&
			s.QuorumCleanupStatus["127.0.0.1"] == true
	})).Return(nil).Once()

	err := mgr.CleanupSegment(context.Background(), "test-log", 1, 2)
	assert.NoError(t, err)
}

func TestCleanupSegment_ExistingStatus_UpdateStatusError(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:          1,
		SegmentId:      2,
		State:          proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		LastUpdateTime: uint64(time.Now().UnixMilli()),
		QuorumCleanupStatus: map[string]bool{
			"127.0.0.1": false,
		},
	}, nil)

	// Update status fails
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.Anything).Return(werr.ErrInternalError)

	err := mgr.CleanupSegment(context.Background(), "test-log", 1, 2)
	assert.Error(t, err)
}

func TestCleanupSegment_ExistingStatus_AllNodesAlreadyCompleted(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	// Existing status with the only node already completed
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:          1,
		SegmentId:      2,
		State:          proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		LastUpdateTime: uint64(time.Now().UnixMilli()),
		QuorumCleanupStatus: map[string]bool{
			"127.0.0.1": true, // Already completed
		},
	}, nil).Once()

	// handleExistingCleanupStatus updates to IN_PROGRESS
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCleanupStatus) bool {
		return s.State == proto.SegmentCleanupState_CLEANUP_IN_PROGRESS
	})).Return(nil).Once()

	// No goroutines launched (all nodes done), updateCleanupStatusWithResults called with empty results
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		QuorumCleanupStatus: map[string]bool{"127.0.0.1": true},
	}, nil).Once()
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCleanupStatus) bool {
		return s.State == proto.SegmentCleanupState_CLEANUP_COMPLETED
	})).Return(nil).Once()

	err := mgr.CleanupSegment(context.Background(), "test-log", 1, 2)
	assert.NoError(t, err)
}

// === sendCleanupRequestsToQuorumNodes tests ===

func TestSendCleanupRequestsToQuorumNodes_NodeFailure(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	// GetLogStoreClient fails
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(nil, errors.New("conn failed"))

	// updateCleanupStatusWithResults: read status, apply failed result, set FAILED
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		QuorumCleanupStatus: map[string]bool{"node1": false},
	}, nil)
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCleanupStatus) bool {
		return s.State == proto.SegmentCleanupState_CLEANUP_FAILED
	})).Return(nil)

	quorum := &proto.QuorumInfo{Id: 0, Nodes: []string{"node1"}}
	err := mgr.sendCleanupRequestsToQuorumNodes(context.Background(), "test-log", 1, 2, quorum)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "segment cleanup failed")
}
