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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

// syncMutex is a type alias for sync.Mutex for use in tests
type syncMutex = sync.Mutex

func TestCleanupSegment_AllNodesRequiredForSuccess(t *testing.T) {
	// Test data
	logId := int64(123)
	segmentId := int64(456)

	// Test when 2 out of 3 nodes succeed - should still be considered failed
	t.Run("PartialSuccess", func(t *testing.T) {
		mockMetadataProvider := mocks_meta.NewMetadataProvider(t)
		mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

		mockMetadataProvider.EXPECT().
			GetSegmentCleanupStatus(mock.Anything, logId, segmentId).
			Return(&proto.SegmentCleanupStatus{
				LogId:     logId,
				SegmentId: segmentId,
				State:     proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
				QuorumCleanupStatus: map[string]bool{
					"node1": true,  // Succeeded
					"node2": true,  // Succeeded
					"node3": false, // Failed
				},
			}, nil)

		mockMetadataProvider.EXPECT().
			UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(status *proto.SegmentCleanupStatus) bool {
				return status.LogId == logId &&
					status.SegmentId == segmentId &&
					status.State == proto.SegmentCleanupState_CLEANUP_FAILED &&
					len(status.ErrorMessage) > 0
			})).
			Return(nil)

		cleanupManager := NewSegmentCleanupManager("a-bucket", "files", mockMetadataProvider, mockClientPool).(*segmentCleanupManagerImpl)
		finalStatus, err := cleanupManager.updateFinalCleanupStatus(context.Background(), logId, segmentId)

		assert.NoError(t, err, "Update should succeed")
		assert.Equal(t, proto.SegmentCleanupState_CLEANUP_FAILED, finalStatus.State, "State should be FAILED with partial success")
		assert.Contains(t, finalStatus.ErrorMessage, "Not all quorum nodes succeeded")
	})

	// Test behavior when all nodes succeed - should be CLEANUP_COMPLETED
	t.Run("AllSuccess", func(t *testing.T) {
		mockMetadataProvider := mocks_meta.NewMetadataProvider(t)
		mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

		mockMetadataProvider.EXPECT().
			GetSegmentCleanupStatus(mock.Anything, logId, segmentId).
			Return(&proto.SegmentCleanupStatus{
				LogId:     logId,
				SegmentId: segmentId,
				State:     proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
				QuorumCleanupStatus: map[string]bool{
					"node1": true, // All nodes succeeded
					"node2": true,
					"node3": true,
				},
			}, nil)

		mockMetadataProvider.EXPECT().
			UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(status *proto.SegmentCleanupStatus) bool {
				return status.LogId == logId &&
					status.SegmentId == segmentId &&
					status.State == proto.SegmentCleanupState_CLEANUP_COMPLETED
			})).
			Return(nil)

		cleanupManager := NewSegmentCleanupManager("a-bucket", "files", mockMetadataProvider, mockClientPool).(*segmentCleanupManagerImpl)
		finalStatus, err := cleanupManager.updateFinalCleanupStatus(context.Background(), logId, segmentId)

		assert.NoError(t, err, "Update should succeed")
		assert.Equal(t, proto.SegmentCleanupState_CLEANUP_COMPLETED, finalStatus.State,
			"State should be CLEANUP_COMPLETED when all nodes succeed")
		assert.Empty(t, finalStatus.ErrorMessage, "No error message expected when all nodes succeed")
	})

	// Test when not all nodes succeed - should be considered failed
	t.Run("PartialSuccess_WithCorrectSuccessCount", func(t *testing.T) {
		mockMetadataProvider := mocks_meta.NewMetadataProvider(t)
		mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

		mockMetadataProvider.EXPECT().
			GetSegmentCleanupStatus(mock.Anything, logId, segmentId).
			Return(&proto.SegmentCleanupStatus{
				LogId:     logId,
				SegmentId: segmentId,
				State:     proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
				QuorumCleanupStatus: map[string]bool{
					"node1": true,  // Succeeded
					"node2": true,  // Succeeded
					"node3": false, // Failed
				},
			}, nil)

		mockMetadataProvider.EXPECT().
			UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(status *proto.SegmentCleanupStatus) bool {
				return status.LogId == logId &&
					status.SegmentId == segmentId &&
					status.State == proto.SegmentCleanupState_CLEANUP_FAILED &&
					len(status.ErrorMessage) > 0
			})).
			Return(nil)

		cleanupManager := NewSegmentCleanupManager("a-bucket", "files", mockMetadataProvider, mockClientPool).(*segmentCleanupManagerImpl)
		finalStatus, err := cleanupManager.updateFinalCleanupStatus(context.Background(), logId, segmentId)

		assert.NoError(t, err, "Update should succeed")
		assert.Equal(t, proto.SegmentCleanupState_CLEANUP_FAILED, finalStatus.State,
			"State should be CLEANUP_FAILED when not all nodes succeed")
		assert.Contains(t, finalStatus.ErrorMessage, "succeeded",
			"Error message should indicate partial success")
	})
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

	// processCleanupResult
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		QuorumCleanupStatus: map[string]bool{"127.0.0.1": false},
	}, nil).Once()
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.Anything).Return(nil).Once() // processCleanupResult sets COMPLETED; updateFinalCleanupStatus sees COMPLETED and returns early

	// updateFinalCleanupStatus
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_COMPLETED,
		QuorumCleanupStatus: map[string]bool{"127.0.0.1": true},
	}, nil).Once()

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

	// Update status to IN_PROGRESS
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.Anything).Return(nil).Times(2) // handleExistingCleanupStatus + processCleanupResult; updateFinalCleanupStatus sees COMPLETED and returns early

	// Continue cleanup
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "127.0.0.1").Return(mockClient, nil)
	mockClient.EXPECT().SegmentClean(mock.Anything, "bucket", "root", int64(1), int64(2), 0).Return(nil)

	// processCleanupResult
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		QuorumCleanupStatus: map[string]bool{"127.0.0.1": false},
	}, nil).Once()

	// updateFinalCleanupStatus
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_COMPLETED,
		QuorumCleanupStatus: map[string]bool{"127.0.0.1": true},
	}, nil).Once()

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

func TestCleanupSegment_ExistingStatus_NewNodeAdded(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	// Existing status with some node already completed
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:          1,
		SegmentId:      2,
		State:          proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		LastUpdateTime: uint64(time.Now().UnixMilli()),
		QuorumCleanupStatus: map[string]bool{
			"127.0.0.1": true, // Already completed
		},
	}, nil).Once()

	// Update status + processResult + final
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.Anything).Return(nil).Maybe()

	// Node already completed, no client needed
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().SegmentClean(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Final status check
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_COMPLETED,
		QuorumCleanupStatus: map[string]bool{"127.0.0.1": true},
	}, nil).Maybe()

	err := mgr.CleanupSegment(context.Background(), "test-log", 1, 2)
	assert.NoError(t, err)
}

// === sendCleanupRequestToNode tests ===

func TestSendCleanupRequestToNode_GetClientError(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(nil, errors.New("connection failed"))

	// processCleanupResult should be called with failure
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		QuorumCleanupStatus: map[string]bool{"node1": false},
	}, nil)
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.Anything).Return(nil)

	mu := &syncMutex{}
	err := mgr.sendCleanupRequestToNode(context.Background(), "test-log", 1, 2, 0, "node1", mu)
	assert.Error(t, err)
}

func TestSendCleanupRequestToNode_SegmentCleanError(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClient.EXPECT().SegmentClean(mock.Anything, "bucket", "root", int64(1), int64(2), 0).Return(errors.New("clean failed"))

	// processCleanupResult with failure
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		QuorumCleanupStatus: map[string]bool{"node1": false},
	}, nil)
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.Anything).Return(nil)

	mu := &syncMutex{}
	err := mgr.sendCleanupRequestToNode(context.Background(), "test-log", 1, 2, 0, "node1", mu)
	assert.Error(t, err)
}

func TestSendCleanupRequestToNode_Success(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClient.EXPECT().SegmentClean(mock.Anything, "bucket", "root", int64(1), int64(2), 0).Return(nil)

	// processCleanupResult with success
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		QuorumCleanupStatus: map[string]bool{"node1": false},
	}, nil)
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.Anything).Return(nil)

	mu := &syncMutex{}
	err := mgr.sendCleanupRequestToNode(context.Background(), "test-log", 1, 2, 0, "node1", mu)
	assert.NoError(t, err)
}

// === processCleanupResult tests ===

func TestProcessCleanupResult_GetStatusError(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(nil, werr.ErrInternalError)

	err := mgr.processCleanupResult(context.Background(), 1, 2, 0, "node1", true, "")
	assert.Error(t, err)
}

func TestProcessCleanupResult_NilStatus(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(nil, nil)

	err := mgr.processCleanupResult(context.Background(), 1, 2, 0, "node1", true, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
}

func TestProcessCleanupResult_NilQuorumMap(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		QuorumCleanupStatus: nil, // nil map
	}, nil)
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.Anything).Return(nil)

	err := mgr.processCleanupResult(context.Background(), 1, 2, 0, "node1", true, "")
	assert.NoError(t, err)
}

func TestProcessCleanupResult_Failure_RecordsErrorMessage(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		QuorumCleanupStatus: map[string]bool{"node1": false},
	}, nil)

	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCleanupStatus) bool {
		return s.ErrorMessage != "" && s.QuorumCleanupStatus["node1"] == false && s.State == proto.SegmentCleanupState_CLEANUP_FAILED
	})).Return(nil)

	err := mgr.processCleanupResult(context.Background(), 1, 2, 0, "node1", false, "cleanup error")
	assert.NoError(t, err)
}

func TestProcessCleanupResult_Failure_AppendsErrorMessage(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		ErrorMessage:        "existing error",
		QuorumCleanupStatus: map[string]bool{"node1": false, "node2": false},
	}, nil)

	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCleanupStatus) bool {
		return s.ErrorMessage != "" && len(s.ErrorMessage) > len("existing error")
	})).Return(nil)

	err := mgr.processCleanupResult(context.Background(), 1, 2, 0, "node1", false, "new error")
	assert.NoError(t, err)
}

func TestProcessCleanupResult_AllSuccess(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		QuorumCleanupStatus: map[string]bool{"node1": true}, // one node already done
	}, nil)

	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCleanupStatus) bool {
		return s.State == proto.SegmentCleanupState_CLEANUP_COMPLETED
	})).Return(nil)

	// Mark the only remaining node as success (it was already true)
	err := mgr.processCleanupResult(context.Background(), 1, 2, 0, "node1", true, "")
	assert.NoError(t, err)
}

func TestProcessCleanupResult_UpdateStatusError(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		QuorumCleanupStatus: map[string]bool{"node1": false},
	}, nil)
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.Anything).Return(werr.ErrInternalError)

	err := mgr.processCleanupResult(context.Background(), 1, 2, 0, "node1", true, "")
	assert.Error(t, err)
}

// === updateFinalCleanupStatus additional tests ===

func TestUpdateFinalCleanupStatus_GetStatusError(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(nil, werr.ErrInternalError)

	_, err := mgr.updateFinalCleanupStatus(context.Background(), 1, 2)
	assert.Error(t, err)
}

func TestUpdateFinalCleanupStatus_NilStatus(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(nil, nil)

	_, err := mgr.updateFinalCleanupStatus(context.Background(), 1, 2)
	assert.Error(t, err)
}

func TestUpdateFinalCleanupStatus_AlreadyCompleted(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:     1,
		SegmentId: 2,
		State:     proto.SegmentCleanupState_CLEANUP_COMPLETED,
	}, nil)

	status, err := mgr.updateFinalCleanupStatus(context.Background(), 1, 2)
	assert.NoError(t, err)
	assert.Equal(t, proto.SegmentCleanupState_CLEANUP_COMPLETED, status.State)
}

func TestUpdateFinalCleanupStatus_AlreadyFailed(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:     1,
		SegmentId: 2,
		State:     proto.SegmentCleanupState_CLEANUP_FAILED,
	}, nil)

	status, err := mgr.updateFinalCleanupStatus(context.Background(), 1, 2)
	assert.NoError(t, err)
	assert.Equal(t, proto.SegmentCleanupState_CLEANUP_FAILED, status.State)
}

func TestUpdateFinalCleanupStatus_NilQuorumMap(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		QuorumCleanupStatus: nil,
	}, nil)

	// Should set to FAILED because totalNodes == 0
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.MatchedBy(func(s *proto.SegmentCleanupStatus) bool {
		return s.State == proto.SegmentCleanupState_CLEANUP_FAILED
	})).Return(nil)

	status, err := mgr.updateFinalCleanupStatus(context.Background(), 1, 2)
	assert.NoError(t, err)
	assert.Equal(t, proto.SegmentCleanupState_CLEANUP_FAILED, status.State)
}

func TestUpdateFinalCleanupStatus_UpdateError(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		QuorumCleanupStatus: map[string]bool{"node1": true},
	}, nil)
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.Anything).Return(werr.ErrInternalError)

	_, err := mgr.updateFinalCleanupStatus(context.Background(), 1, 2)
	assert.Error(t, err)
}

// === sendCleanupRequestsToQuorumNodes tests ===

func TestSendCleanupRequestsToQuorumNodes_NodeFailure(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mgr := NewSegmentCleanupManager("bucket", "root", mockMeta, mockPool).(*segmentCleanupManagerImpl)

	// GetLogStoreClient fails
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(nil, errors.New("conn failed"))

	// processCleanupResult for failure
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		QuorumCleanupStatus: map[string]bool{"node1": false},
	}, nil).Once()
	// processCleanupResult updates status once
	mockMeta.EXPECT().UpdateSegmentCleanupStatus(mock.Anything, mock.Anything).Return(nil).Once()

	// updateFinalCleanupStatus - already CLEANUP_FAILED, returns early without calling Update again
	mockMeta.EXPECT().GetSegmentCleanupStatus(mock.Anything, int64(1), int64(2)).Return(&proto.SegmentCleanupStatus{
		LogId:               1,
		SegmentId:           2,
		State:               proto.SegmentCleanupState_CLEANUP_FAILED,
		ErrorMessage:        "some error",
		QuorumCleanupStatus: map[string]bool{"node1": false},
	}, nil).Once()

	quorum := &proto.QuorumInfo{Id: 0, Nodes: []string{"node1"}}
	err := mgr.sendCleanupRequestsToQuorumNodes(context.Background(), "test-log", 1, 2, quorum)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "segment cleanup failed")
}
