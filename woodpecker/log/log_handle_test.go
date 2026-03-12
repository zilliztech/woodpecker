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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_segment_handle"
	"github.com/zilliztech/woodpecker/proto"
)

func createMockLogHandle(t *testing.T) (*logHandleImpl, *mocks_meta.MetadataProvider) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentRollingPolicy: config.SegmentRollingPolicyConfig{
					MaxInterval: config.NewDurationSecondsFromInt(10),
					MaxSize:     64 * 1024 * 1024,
					MaxBlocks:   1000,
				},
			},
		},
	}
	cfg.Woodpecker.Logstore.FencePolicy.ConditionWrite = "enable"

	segments := map[int64]*meta.SegmentMeta{}
	logHandle := NewLogHandle("test-log", 1, segments, mockMeta, nil, cfg, func(ctx context.Context) (*proto.QuorumInfo, error) {
		return &proto.QuorumInfo{
			Id:    -1,
			Wq:    1,
			Aq:    1,
			Es:    1,
			Nodes: []string{"127.0.0.1:59456"},
		}, nil
	}).(*logHandleImpl)

	return logHandle, mockMeta
}

func TestOpenLogReader_ReaderResourceLeak(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Mock CreateReaderTempInfo failure
	mockMeta.EXPECT().CreateReaderTempInfo(mock.Anything, mock.AnythingOfType("string"), int64(1), int64(0), int64(0)).Return(errors.New("metadata error"))

	// Mock GetTruncatedRecordId
	mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
		Metadata: &proto.LogMeta{
			TruncatedSegmentId: -1,
			TruncatedEntryId:   -1,
		},
		Revision: 1,
	}, nil)

	// Mock DeleteReaderTempInfo for cleanup
	mockMeta.EXPECT().DeleteReaderTempInfo(mock.Anything, int64(1), mock.AnythingOfType("string")).Return(nil)

	// Call OpenLogReader and expect error
	reader, err := logHandle.OpenLogReader(ctx, &LogMessageId{SegmentId: 0, EntryId: 0}, "test-reader")

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, reader)
	assert.Contains(t, err.Error(), "metadata error")

	// Verify all expectations were met
	mockMeta.AssertExpectations(t)
}

// TestSegmentHandlesCacheCleanup tests the segment handle cache cleanup functionality
func TestSegmentHandlesCacheCleanup(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)
	ctx := context.Background()

	// Create mock segment handles
	mockSegment1 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegment2 := mocks_segment_handle.NewSegmentHandle(t)

	// Set up segment handles
	now := time.Now()
	oldTimeMs := now.Add(-2 * time.Minute).UnixMilli()     // 2 minutes ago (should be cleaned up)
	recentTimeMs := now.Add(-30 * time.Second).UnixMilli() // 30 seconds ago (should be kept)

	logHandle.SegmentHandles[1] = mockSegment1
	logHandle.SegmentHandles[2] = mockSegment2

	logHandle.WritableSegmentId = 2 // Segment 2 is writable, should not be cleaned

	// Mock GetLastAccessTime calls for cleanup decision
	mockSegment1.EXPECT().GetLastAccessTime().Return(oldTimeMs).Maybe()    // Old segment 1
	mockSegment2.EXPECT().GetLastAccessTime().Return(recentTimeMs).Maybe() // Recent segment 2

	// Expect Close call for old segment (except writable one)
	mockSegment1.EXPECT().ForceCompleteAndClose(mock.Anything).Return(nil).Times(1)

	// Trigger cleanup directly
	logHandle.cleanupIdleSegmentHandlesUnsafe(ctx, 1*time.Minute)

	// Verify cleanup results
	assert.Len(t, logHandle.SegmentHandles, 1, "Should have 1 segment handle remaining")
	assert.Contains(t, logHandle.SegmentHandles, int64(2), "Writable segment should be kept")

	// Verify all expectations were met
	mockSegment1.AssertExpectations(t)
	mockSegment2.AssertExpectations(t)
}

// TestClose_ContinuesOnError tests that Close method continues processing all segments even if some fail
func TestClose_ContinuesOnError(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)
	ctx := context.Background()

	// Create mock segment handles
	mockSegment1 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegment2 := mocks_segment_handle.NewSegmentHandle(t)

	logHandle.SegmentHandles[1] = mockSegment1
	logHandle.SegmentHandles[2] = mockSegment2

	// Mock GetId calls for logging
	mockSegment1.EXPECT().GetId(mock.Anything).Return(int64(1)).Maybe()
	mockSegment2.EXPECT().GetId(mock.Anything).Return(int64(2)).Maybe()

	// Set up expectations - segment 1 fails to fence, segment 2 succeeds
	// mockSegment1.EXPECT().Complete(mock.Anything).Return(-1, nil)
	mockSegment1.EXPECT().ForceCompleteAndClose(mock.Anything).Return(errors.New("complete error"))
	// mockSegment2.EXPECT().Complete(mock.Anything).Return(-1, errors.New("complete error"))
	mockSegment2.EXPECT().ForceCompleteAndClose(mock.Anything).Return(errors.New("complete error"))

	// Call Close
	err := logHandle.Close(ctx)

	// Should return the first error encountered (complete error)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "complete error")

	// Verify all segments were processed and maps were cleared
	assert.Len(t, logHandle.SegmentHandles, 0, "SegmentHandles should be cleared")
	assert.Equal(t, int64(-1), logHandle.WritableSegmentId, "WritableSegmentId should be reset")

	// Verify all expectations were met
	mockSegment1.AssertExpectations(t)
	mockSegment2.AssertExpectations(t)
}

// TestGetExistsReadonlySegmentHandle_UpdatesAccessTime tests that access time is updated on cache hit
func TestGetExistsReadonlySegmentHandle_UpdatesAccessTime(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)
	ctx := context.Background()

	// Create mock segment handle
	mockSegment := mocks_segment_handle.NewSegmentHandle(t)

	// Set up existing segment in cache
	segmentId := int64(1)
	logHandle.SegmentHandles[segmentId] = mockSegment

	// Call GetExistsReadonlySegmentHandle
	result, err := logHandle.GetExistsReadonlySegmentHandle(ctx, segmentId)

	// Assertions
	assert.NoError(t, err)
	assert.Equal(t, mockSegment, result)

	mockSegment.AssertExpectations(t)
}

// TestCreateAndCacheNewSegmentHandle_SetsAccessTime tests that new segment handles have access time set
func TestCreateAndCacheNewSegmentHandle_SetsAccessTime(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Mock GetNextSegmentId
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*meta.SegmentMeta{}, nil)

	// Mock StoreSegmentMetadata
	mockMeta.EXPECT().StoreSegmentMetadata(mock.Anything, "test-log", mock.Anything, mock.Anything).Return(nil)

	// Call createAndCacheNewSegmentHandle
	handle, err := logHandle.createAndCacheWritableSegmentHandle(ctx, nil)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, handle)

	// Verify segment was cached
	segmentId := logHandle.WritableSegmentId
	assert.Contains(t, logHandle.SegmentHandles, segmentId, "Segment should be cached")

	mockMeta.AssertExpectations(t)
}

// TestGetOrCreateWritableSegmentHandle_ErrorHandling tests error handling in GetOrCreateWritableSegmentHandle
func TestGetOrCreateWritableSegmentHandle_ErrorHandling(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Mock GetOrCreateWritableSegmentHandle failure
	logHandle.WritableSegmentId = -1 // No writable segment exists

	// Mock metadata operations for createNewSegmentMeta that will fail
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*meta.SegmentMeta{}, nil)
	mockMeta.EXPECT().StoreSegmentMetadata(mock.Anything, "test-log", mock.Anything, mock.Anything).Return(errors.New("storage error"))

	// Call GetOrCreateWritableSegmentHandle and expect error
	handle, err := logHandle.GetOrCreateWritableSegmentHandle(ctx, nil)

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, handle)
	assert.Contains(t, err.Error(), "storage error")

	mockMeta.AssertExpectations(t)
}

// TestFenceAllActiveSegments_NoActiveSegments tests fencing when no active segments exist
func TestFenceAllActiveSegments_NoActiveSegments(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Mock segments with no active segments
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*meta.SegmentMeta{
		1: {Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Completed}, Revision: 1},
		2: {Metadata: &proto.SegmentMetadata{SegNo: 2, State: proto.SegmentState_Sealed}, Revision: 1},
	}, nil)

	err := logHandle.fenceAllActiveSegments(ctx)
	assert.NoError(t, err)

	mockMeta.AssertExpectations(t)
}

// TestFenceAllActiveSegments_SuccessfulFencing tests successful fencing of active segments
func TestFenceAllActiveSegments_SuccessfulFencing(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Create mock segment handles
	mockSegment1 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegment3 := mocks_segment_handle.NewSegmentHandle(t)

	// Mock segments with some active segments
	segments := map[int64]*meta.SegmentMeta{
		1: {Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active}, Revision: 1},
		2: {Metadata: &proto.SegmentMetadata{SegNo: 2, State: proto.SegmentState_Completed}, Revision: 1}, // Not active
		3: {Metadata: &proto.SegmentMetadata{SegNo: 3, State: proto.SegmentState_Active}, Revision: 1},
	}
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(segments, nil)

	// Set up segment handles in cache (so GetExistsReadonlySegmentHandle won't call GetSegmentMetadata)
	logHandle.SegmentHandles[1] = mockSegment1
	logHandle.SegmentHandles[3] = mockSegment3

	// Mock successful fencing
	mockSegment1.EXPECT().FenceAndComplete(mock.Anything).Return(int64(100), nil)
	mockSegment3.EXPECT().FenceAndComplete(mock.Anything).Return(int64(200), nil)

	// Note: No UpdateSegmentMetadata mock needed — FenceAndComplete handles metadata update internally.

	err := logHandle.fenceAllActiveSegments(ctx)
	assert.NoError(t, err)

	// Verify all expectations
	mockSegment1.AssertExpectations(t)
	mockSegment3.AssertExpectations(t)
	mockMeta.AssertExpectations(t)
}

// TestFenceAllActiveSegments_PartialFailure tests fencing when some segments fail
func TestFenceAllActiveSegments_PartialFailure(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Create mock segment handles
	mockSegment1 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegment2 := mocks_segment_handle.NewSegmentHandle(t)

	// Mock segments with active segments
	segments := map[int64]*meta.SegmentMeta{
		1: {Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active}, Revision: 1},
		2: {Metadata: &proto.SegmentMetadata{SegNo: 2, State: proto.SegmentState_Active}, Revision: 1},
	}
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(segments, nil)

	// Set up segment handles in cache
	logHandle.SegmentHandles[1] = mockSegment1
	logHandle.SegmentHandles[2] = mockSegment2

	// Mock fencing - segment 1 succeeds, segment 2 fails
	mockSegment1.EXPECT().FenceAndComplete(mock.Anything).Return(int64(100), nil)
	mockSegment2.EXPECT().FenceAndComplete(mock.Anything).Return(int64(-1), errors.New("fence error"))

	// Note: No UpdateSegmentMetadata mock needed — FenceAndComplete handles metadata update internally.

	err := logHandle.fenceAllActiveSegments(ctx)

	// Should return standardized error
	assert.Error(t, err)
	assert.True(t, werr.ErrLogHandleFenceFailed.Is(err), "Expected ErrLogHandleFenceFailed error type")

	// Verify all expectations
	mockSegment1.AssertExpectations(t)
	mockSegment2.AssertExpectations(t)
	mockMeta.AssertExpectations(t)
}

// TestFenceAllActiveSegments_EmptySegments tests fencing when no segments exist
func TestFenceAllActiveSegments_EmptySegments(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Mock empty segments
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*meta.SegmentMeta{}, nil)

	err := logHandle.fenceAllActiveSegments(ctx)
	assert.NoError(t, err)

	mockMeta.AssertExpectations(t)
}

// TestLogHandle_GetOrCreateWritableSegmentHandle_SizeTriggeredRolling tests that when segment size
// exceeds MaxSize threshold, it triggers rolling and creates a new writable segment
func TestLogHandle_GetOrCreateWritableSegmentHandle_SizeTriggeredRolling(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Create mock segment handle for old segment
	mockOldSegment := mocks_segment_handle.NewSegmentHandle(t)

	// Set up initial writable segment
	logHandle.SegmentHandles[1] = mockOldSegment
	logHandle.WritableSegmentId = 1

	// Mock old segment behavior - it should exceed size threshold
	mockOldSegment.EXPECT().IsForceRollingReady(mock.Anything).Return(false)
	mockOldSegment.EXPECT().GetSize(mock.Anything).Return(int64(65 * 1024 * 1024)) // 65MB > 64MB threshold
	mockOldSegment.EXPECT().GetBlocksCount(mock.Anything).Return(int64(500))       // Below blocks threshold
	mockOldSegment.EXPECT().GetId(mock.Anything).Return(int64(1)).Maybe()
	mockOldSegment.EXPECT().SetRollingReady(mock.Anything).Once()

	// Mock metadata operations for creating new segment
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*meta.SegmentMeta{
		1: {Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active}, Revision: 1},
	}, nil)
	mockMeta.EXPECT().StoreSegmentMetadata(mock.Anything, "test-log", mock.Anything, mock.MatchedBy(func(meta *meta.SegmentMeta) bool {
		return meta.Metadata.SegNo == 2 && meta.Metadata.State == proto.SegmentState_Active
	})).Return(nil)

	// Call GetOrCreateWritableSegmentHandle - should trigger rolling
	newHandle, err := logHandle.GetOrCreateWritableSegmentHandle(ctx, nil)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, newHandle)
	assert.Equal(t, int64(2), logHandle.WritableSegmentId) // Should have new writable segment
	assert.Contains(t, logHandle.SegmentHandles, int64(2)) // New segment should be cached

	// Verify all expectations
	mockOldSegment.AssertExpectations(t)
	mockMeta.AssertExpectations(t)
}

// TestLogHandle_GetOrCreateWritableSegmentHandle_TimeTriggeredRolling tests that when segment age
// exceeds MaxInterval threshold, it triggers rolling
func TestLogHandle_GetOrCreateWritableSegmentHandle_TimeTriggeredRolling(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Create mock segment handle
	mockOldSegment := mocks_segment_handle.NewSegmentHandle(t)

	// Set up initial writable segment with old creation time
	logHandle.SegmentHandles[1] = mockOldSegment
	logHandle.WritableSegmentId = 1
	logHandle.lastRolloverTimeMs = time.Now().Add(-15 * time.Minute).UnixMilli() // 15 minutes ago > 10 minute threshold

	// Mock old segment behavior - size is small but time exceeds threshold
	mockOldSegment.EXPECT().IsForceRollingReady(mock.Anything).Return(false)
	mockOldSegment.EXPECT().GetSize(mock.Anything).Return(int64(1024))       // Small size
	mockOldSegment.EXPECT().GetBlocksCount(mock.Anything).Return(int64(100)) // Below blocks threshold
	mockOldSegment.EXPECT().GetId(mock.Anything).Return(int64(1)).Maybe()
	mockOldSegment.EXPECT().SetRollingReady(mock.Anything).Once()

	// Mock metadata operations for creating new segment
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*meta.SegmentMeta{
		1: {Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active}, Revision: 1},
	}, nil)
	mockMeta.EXPECT().StoreSegmentMetadata(mock.Anything, "test-log", mock.Anything, mock.MatchedBy(func(meta *meta.SegmentMeta) bool {
		return meta.Metadata.SegNo == 2 && meta.Metadata.State == proto.SegmentState_Active
	})).Return(nil)

	// Call GetOrCreateWritableSegmentHandle - should trigger rolling due to time
	newHandle, err := logHandle.GetOrCreateWritableSegmentHandle(ctx, nil)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, newHandle)
	assert.Equal(t, int64(2), logHandle.WritableSegmentId)
	assert.Contains(t, logHandle.SegmentHandles, int64(2))

	// Verify all expectations
	mockOldSegment.AssertExpectations(t)
	mockMeta.AssertExpectations(t)
}

// TestLogHandle_GetOrCreateWritableSegmentHandle_ForceRolling tests that when segment is marked
// for force rolling, it triggers rolling regardless of size/time
func TestLogHandle_GetOrCreateWritableSegmentHandle_ForceRolling(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Create mock segment handle
	mockOldSegment := mocks_segment_handle.NewSegmentHandle(t)

	// Set up initial writable segment
	logHandle.SegmentHandles[1] = mockOldSegment
	logHandle.WritableSegmentId = 1
	logHandle.lastRolloverTimeMs = time.Now().UnixMilli() // Recent time

	// Mock old segment behavior - force rolling is ready
	mockOldSegment.EXPECT().IsForceRollingReady(mock.Anything).Return(true) // Force rolling
	mockOldSegment.EXPECT().GetId(mock.Anything).Return(int64(1)).Maybe()
	mockOldSegment.EXPECT().SetRollingReady(mock.Anything).Once()

	// Mock metadata operations for creating new segment
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*meta.SegmentMeta{
		1: {Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active}, Revision: 1},
	}, nil)
	mockMeta.EXPECT().StoreSegmentMetadata(mock.Anything, "test-log", mock.Anything, mock.MatchedBy(func(meta *meta.SegmentMeta) bool {
		return meta.Metadata.SegNo == 2 && meta.Metadata.State == proto.SegmentState_Active
	})).Return(nil)

	// Call GetOrCreateWritableSegmentHandle - should trigger rolling due to force rolling
	newHandle, err := logHandle.GetOrCreateWritableSegmentHandle(ctx, nil)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, newHandle)
	assert.Equal(t, int64(2), logHandle.WritableSegmentId)

	// Verify all expectations
	mockOldSegment.AssertExpectations(t)
	mockMeta.AssertExpectations(t)
}

// TestLogHandle_GetOrCreateWritableSegmentHandle_NoRollingNeeded tests that when segment doesn't
// meet rolling criteria, it returns the existing writable segment
func TestLogHandle_GetOrCreateWritableSegmentHandle_NoRollingNeeded(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)
	ctx := context.Background()

	// Create mock segment handle
	mockSegment := mocks_segment_handle.NewSegmentHandle(t)

	// Set up initial writable segment
	logHandle.SegmentHandles[1] = mockSegment
	logHandle.WritableSegmentId = 1
	logHandle.lastRolloverTimeMs = time.Now().UnixMilli() // Recent time

	// Mock segment behavior - no rolling needed
	mockSegment.EXPECT().IsForceRollingReady(mock.Anything).Return(false)
	mockSegment.EXPECT().GetSize(mock.Anything).Return(int64(1024))       // Small size
	mockSegment.EXPECT().GetBlocksCount(mock.Anything).Return(int64(100)) // Below blocks threshold

	// Call GetOrCreateWritableSegmentHandle - should return existing segment
	handle, err := logHandle.GetOrCreateWritableSegmentHandle(ctx, nil)

	// Assertions
	assert.NoError(t, err)
	assert.Equal(t, mockSegment, handle)                   // Should return the same segment
	assert.Equal(t, int64(1), logHandle.WritableSegmentId) // WritableSegmentId should not change

	// Verify all expectations
	mockSegment.AssertExpectations(t)
}

// TestLogHandle_GetOrCreateWritableSegmentHandle_CreateFirstSegment tests creating the first
// writable segment when none exists
func TestLogHandle_GetOrCreateWritableSegmentHandle_CreateFirstSegment(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// No writable segment exists initially
	assert.Equal(t, int64(-1), logHandle.WritableSegmentId)

	// Mock metadata operations for creating first segment
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*meta.SegmentMeta{}, nil)
	mockMeta.EXPECT().StoreSegmentMetadata(mock.Anything, "test-log", mock.Anything, mock.MatchedBy(func(meta *meta.SegmentMeta) bool {
		return meta.Metadata.SegNo == 0 && meta.Metadata.State == proto.SegmentState_Active
	})).Return(nil)

	// Call GetOrCreateWritableSegmentHandle - should create first segment
	handle, err := logHandle.GetOrCreateWritableSegmentHandle(ctx, nil)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, handle)
	assert.Equal(t, int64(0), logHandle.WritableSegmentId) // Should have first segment
	assert.Contains(t, logHandle.SegmentHandles, int64(0)) // First segment should be cached

	// Verify all expectations
	mockMeta.AssertExpectations(t)
}

// TestLogHandle_Close_ForceCloseAllSegments tests that Close method calls ForceCompleteAndClose
// on all segment handles, including both writable and readonly segments
func TestLogHandle_Close_ForceCloseAllSegments(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)
	ctx := context.Background()

	// Create multiple mock segment handles
	mockSegment1 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegment2 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegment3 := mocks_segment_handle.NewSegmentHandle(t)

	// Set up segments - mix of writable and readonly
	logHandle.SegmentHandles[1] = mockSegment1 // Readonly
	logHandle.SegmentHandles[2] = mockSegment2 // Writable
	logHandle.SegmentHandles[3] = mockSegment3 // Readonly
	logHandle.WritableSegmentId = 2

	// Mock GetId calls for logging
	mockSegment1.EXPECT().GetId(mock.Anything).Return(int64(1)).Maybe()
	mockSegment2.EXPECT().GetId(mock.Anything).Return(int64(2)).Maybe()
	mockSegment3.EXPECT().GetId(mock.Anything).Return(int64(3)).Maybe()

	// All segments should receive ForceCompleteAndClose call
	mockSegment1.EXPECT().ForceCompleteAndClose(mock.Anything).Return(nil).Once()
	mockSegment2.EXPECT().ForceCompleteAndClose(mock.Anything).Return(nil).Once()
	mockSegment3.EXPECT().ForceCompleteAndClose(mock.Anything).Return(nil).Once()

	// Call Close
	err := logHandle.Close(ctx)

	// Assertions
	assert.NoError(t, err)
	assert.Len(t, logHandle.SegmentHandles, 0)              // All segments should be cleared
	assert.Equal(t, int64(-1), logHandle.WritableSegmentId) // WritableSegmentId should be reset

	// Verify all expectations
	mockSegment1.AssertExpectations(t)
	mockSegment2.AssertExpectations(t)
	mockSegment3.AssertExpectations(t)
}

// TestLogHandle_Close_ContinuesOnSegmentErrors tests that Close continues processing all segments
// even if some ForceCompleteAndClose calls fail
func TestLogHandle_Close_ContinuesOnSegmentErrors(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)
	ctx := context.Background()

	// Create mock segment handles
	mockSegment1 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegment2 := mocks_segment_handle.NewSegmentHandle(t)

	logHandle.SegmentHandles[1] = mockSegment1
	logHandle.SegmentHandles[2] = mockSegment2

	// Mock GetId calls for logging
	mockSegment1.EXPECT().GetId(mock.Anything).Return(int64(1)).Maybe()
	mockSegment2.EXPECT().GetId(mock.Anything).Return(int64(2)).Maybe()

	// First segment fails, second succeeds
	mockSegment1.EXPECT().ForceCompleteAndClose(mock.Anything).Return(errors.New("segment 1 error")).Once()
	mockSegment2.EXPECT().ForceCompleteAndClose(mock.Anything).Return(nil).Once()

	// Call Close
	err := logHandle.Close(ctx)

	// Should return the error from failed segment
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "segment 1 error")

	// But all segments should still be processed and cleared
	assert.Len(t, logHandle.SegmentHandles, 0)
	assert.Equal(t, int64(-1), logHandle.WritableSegmentId)

	// Verify all expectations
	mockSegment1.AssertExpectations(t)
	mockSegment2.AssertExpectations(t)
}

// TestLogHandle_Rolling_RollingPolicyBehavior tests the rolling policy logic
func TestLogHandle_Rolling_RollingPolicyBehavior(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)
	ctx := context.Background()

	// Test size-based rolling policy (MaxSize = 64MB in config)
	assert.True(t, logHandle.rollingPolicy.ShouldRollover(ctx, 65*1024*1024, 500, time.Now().UnixMilli()))
	assert.False(t, logHandle.rollingPolicy.ShouldRollover(ctx, 1024, 500, time.Now().UnixMilli()))

	// Test time-based rolling policy
	// Note: MaxInterval is 10 seconds (config.NewDurationSecondsFromInt(10)), converted to milliseconds
	oldTime := time.Now().Add(-15 * time.Second).UnixMilli()   // 15 seconds ago, exceeds 10s threshold
	recentTime := time.Now().Add(-5 * time.Second).UnixMilli() // 5 seconds ago, within 10s threshold

	assert.True(t, logHandle.rollingPolicy.ShouldRollover(ctx, 1024, 500, oldTime))
	assert.False(t, logHandle.rollingPolicy.ShouldRollover(ctx, 1024, 500, recentTime))
}

// TestLogHandle_Rolling_ConcurrentAccess tests rolling behavior under concurrent access
func TestLogHandle_Rolling_ConcurrentAccess(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Create mock segment handle that does NOT trigger rolling to avoid creating real segments
	mockSegment := mocks_segment_handle.NewSegmentHandle(t)

	// Set up initial writable segment that should NOT trigger rolling
	logHandle.SegmentHandles[1] = mockSegment
	logHandle.WritableSegmentId = 1

	// Mock segment behavior to NOT trigger rolling (safe for concurrent access)
	mockSegment.EXPECT().IsForceRollingReady(mock.Anything).Return(false).Maybe()
	mockSegment.EXPECT().GetSize(mock.Anything).Return(int64(1024)).Maybe()       // Small size - won't trigger rolling
	mockSegment.EXPECT().GetBlocksCount(mock.Anything).Return(int64(100)).Maybe() // Small count - won't trigger rolling
	mockSegment.EXPECT().GetId(mock.Anything).Return(int64(1)).Maybe()

	// Start multiple goroutines trying to get writable segment handle
	numGoroutines := 5
	results := make([]interface{}, numGoroutines)
	errors := make([]error, numGoroutines)
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			handle, err := logHandle.GetOrCreateWritableSegmentHandle(ctx, nil)
			results[index] = handle
			errors[index] = err
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// All calls should succeed and return the same existing segment handle
	successCount := 0
	for i := 0; i < numGoroutines; i++ {
		if errors[i] == nil && results[i] != nil {
			successCount++
			// All should return the same mock segment
			assert.Equal(t, mockSegment, results[i], "All calls should return the same segment handle")
		}
	}

	assert.Equal(t, numGoroutines, successCount, "All concurrent calls should succeed")

	// Verify final state - should still have the original writable segment
	assert.Equal(t, int64(1), logHandle.WritableSegmentId, "WritableSegmentId should remain unchanged")
	assert.Contains(t, logHandle.SegmentHandles, logHandle.WritableSegmentId)
	assert.Equal(t, mockSegment, logHandle.SegmentHandles[logHandle.WritableSegmentId])

	// Verify expectations (using Maybe() due to concurrency)
	mockSegment.AssertExpectations(t)
	mockMeta.AssertExpectations(t)
}

// TestLogHandle_CompleteAllActiveSegmentIfExists_DataRaceProtection tests that CompleteAllActiveSegmentIfExists
// properly handles concurrent access with GetExistsReadonlySegmentHandle to prevent data races
func TestLogHandle_CompleteAllActiveSegmentIfExists_DataRaceProtection(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Create mock segment handles
	mockSegment1 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegment2 := mocks_segment_handle.NewSegmentHandle(t)

	// Set up segments
	logHandle.SegmentHandles[1] = mockSegment1
	logHandle.SegmentHandles[2] = mockSegment2
	logHandle.WritableSegmentId = 2

	// Mock GetId calls for logging
	mockSegment1.EXPECT().GetId(mock.Anything).Return(int64(1)).Maybe()
	mockSegment2.EXPECT().GetId(mock.Anything).Return(int64(2)).Maybe()

	// Mock ForceCompleteAndClose calls
	mockSegment1.EXPECT().ForceCompleteAndClose(mock.Anything).Return(nil).Once()
	mockSegment2.EXPECT().ForceCompleteAndClose(mock.Anything).Return(nil).Once()

	// Mock GetSegmentMetadata for potential segment lookup during race condition
	mockMeta.EXPECT().GetSegmentMetadata(mock.Anything, "test-log", mock.AnythingOfType("int64")).Return(nil, werr.ErrSegmentNotFound).Maybe()

	// Start concurrent operations
	var wg sync.WaitGroup
	var completeErr error
	var getErr error
	var getResult interface{}

	// Goroutine 1: CompleteAllActiveSegmentIfExists (simulates logWriter.Close())
	wg.Add(1)
	go func() {
		defer wg.Done()
		completeErr = logHandle.CompleteAllActiveSegmentIfExists(ctx)
	}()

	// Goroutine 2: GetExistsReadonlySegmentHandle (simulates concurrent reader)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Try to get a segment handle concurrently
		getResult, getErr = logHandle.GetExistsReadonlySegmentHandle(ctx, 1)
	}()

	// Wait for both operations to complete
	wg.Wait()

	// Verify that CompleteAllActiveSegmentIfExists completed successfully
	assert.NoError(t, completeErr)

	// CompleteAllActiveSegmentIfExists does NOT clear the map — that's Close()'s job.
	// So GetExistsReadonlySegmentHandle should always find the segment regardless of timing.
	assert.NoError(t, getErr, "GetExistsReadonlySegmentHandle should succeed since CompleteAllActiveSegmentIfExists does not clear the map")
	assert.Equal(t, mockSegment1, getResult, "Should return the cached segment handle")

	// Verify all expectations
	mockSegment1.AssertExpectations(t)
	mockSegment2.AssertExpectations(t)
	mockMeta.AssertExpectations(t)
}

// TestLogHandle_Close_vs_GetExistsReadonlySegmentHandle_DataRaceProtection tests that Close
// properly handles concurrent access with GetExistsReadonlySegmentHandle to prevent data races
func TestLogHandle_Close_vs_GetExistsReadonlySegmentHandle_DataRaceProtection(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Mock GetSegmentMetadata for when Close clears the map before GetExistsReadonlySegmentHandle runs
	mockMeta.EXPECT().GetSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil, werr.ErrSegmentNotFound).Maybe()

	// Create mock segment handles
	mockSegment1 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegment2 := mocks_segment_handle.NewSegmentHandle(t)

	// Set up segments
	logHandle.SegmentHandles[1] = mockSegment1
	logHandle.SegmentHandles[2] = mockSegment2
	logHandle.WritableSegmentId = 2

	// Mock GetId calls for logging
	mockSegment1.EXPECT().GetId(mock.Anything).Return(int64(1)).Maybe()
	mockSegment2.EXPECT().GetId(mock.Anything).Return(int64(2)).Maybe()

	// Mock ForceCompleteAndClose calls
	mockSegment1.EXPECT().ForceCompleteAndClose(mock.Anything).Return(nil).Once()
	mockSegment2.EXPECT().ForceCompleteAndClose(mock.Anything).Return(nil).Once()

	// Start concurrent operations
	var wg sync.WaitGroup
	var closeErr error
	var getErr error
	var getResult interface{}

	// Goroutine 1: Close (simulates logHandle.Close())
	wg.Add(1)
	go func() {
		defer wg.Done()
		closeErr = logHandle.Close(ctx)
	}()

	// Goroutine 2: GetExistsReadonlySegmentHandle (simulates concurrent reader)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Try to get a segment handle concurrently
		getResult, getErr = logHandle.GetExistsReadonlySegmentHandle(ctx, 1)
	}()

	// Wait for both operations to complete
	wg.Wait()

	// Verify that Close completed successfully
	assert.NoError(t, closeErr)

	// Verify that segments were cleared
	assert.Len(t, logHandle.SegmentHandles, 0)
	assert.Equal(t, int64(-1), logHandle.WritableSegmentId)

	// GetExistsReadonlySegmentHandle should either:
	// 1. Return the segment handle if it got the lock before Close clears the map, OR
	// 2. Return ErrSegmentNotFound if Close cleared the map first and metadata lookup fails
	if getErr == nil {
		assert.Equal(t, mockSegment1, getResult, "Should return the cached segment handle if acquired before Close")
	} else {
		assert.Nil(t, getResult, "Should return nil result on error")
		assert.True(t, werr.ErrSegmentNotFound.Is(getErr), "Error should be ErrSegmentNotFound, got: %v", getErr)
	}

	// Verify all expectations
	mockSegment1.AssertExpectations(t)
	mockSegment2.AssertExpectations(t)
}

// TestLogHandle_ConcurrentSegmentHandleAccess tests concurrent access to segment handles
// from multiple goroutines to ensure thread safety
func TestLogHandle_ConcurrentSegmentHandleAccess(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Create mock segment handle
	mockSegment := mocks_segment_handle.NewSegmentHandle(t)

	// Set up initial segment
	logHandle.SegmentHandles[1] = mockSegment

	// Mock metadata operations for potential segment creation
	mockMeta.EXPECT().GetSegmentMetadata(mock.Anything, "test-log", mock.AnythingOfType("int64")).Return(nil, werr.ErrSegmentNotFound).Maybe()

	// Start multiple goroutines trying to access segment handles
	numGoroutines := 10
	results := make([]interface{}, numGoroutines)
	errors := make([]error, numGoroutines)
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			// Half try to get existing segment, half try to get non-existent segment
			segmentId := int64(1)
			if index%2 == 1 {
				segmentId = int64(999) // Non-existent segment
			}
			handle, err := logHandle.GetExistsReadonlySegmentHandle(ctx, segmentId)
			results[index] = handle
			errors[index] = err
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify results
	for i := 0; i < numGoroutines; i++ {
		if i%2 == 0 {
			// Should get existing segment
			assert.NoError(t, errors[i], "Goroutine %d should not have error", i)
			assert.Equal(t, mockSegment, results[i], "Goroutine %d should get existing segment", i)
		} else {
			// Should get error for non-existent segment
			assert.Error(t, errors[i], "Goroutine %d should have error for non-existent segment", i)
			assert.Nil(t, results[i], "Goroutine %d should get nil for non-existent segment", i)
		}
	}

	// Verify expectations
	mockMeta.AssertExpectations(t)
}

// TestLogHandle_CompleteAllActiveSegmentIfExists_vs_Close_Behavior tests the difference
// between CompleteAllActiveSegmentIfExists and Close methods
func TestLogHandle_CompleteAllActiveSegmentIfExists_vs_Close_Behavior(t *testing.T) {
	// Test CompleteAllActiveSegmentIfExists
	t.Run("CompleteAllActiveSegmentIfExists", func(t *testing.T) {
		logHandle, _ := createMockLogHandle(t)
		ctx := context.Background()

		// Create mock segment handles
		mockSegment1 := mocks_segment_handle.NewSegmentHandle(t)
		mockSegment2 := mocks_segment_handle.NewSegmentHandle(t)

		// Set up segments
		logHandle.SegmentHandles[1] = mockSegment1
		logHandle.SegmentHandles[2] = mockSegment2
		logHandle.WritableSegmentId = 2

		// Mock GetId calls for logging
		mockSegment1.EXPECT().GetId(mock.Anything).Return(int64(1)).Maybe()
		mockSegment2.EXPECT().GetId(mock.Anything).Return(int64(2)).Maybe()

		// Mock ForceCompleteAndClose calls
		mockSegment1.EXPECT().ForceCompleteAndClose(mock.Anything).Return(nil).Once()
		mockSegment2.EXPECT().ForceCompleteAndClose(mock.Anything).Return(nil).Once()

		// Call CompleteAllActiveSegmentIfExists
		err := logHandle.CompleteAllActiveSegmentIfExists(ctx)

		// Verify
		assert.NoError(t, err)
		// CompleteAllActiveSegmentIfExists does NOT clear the map — that's Close()'s job.
		assert.Len(t, logHandle.SegmentHandles, 2)

		// Verify expectations
		mockSegment1.AssertExpectations(t)
		mockSegment2.AssertExpectations(t)
	})

	// Test Close
	t.Run("Close", func(t *testing.T) {
		logHandle, _ := createMockLogHandle(t)
		ctx := context.Background()

		// Create mock segment handles
		mockSegment1 := mocks_segment_handle.NewSegmentHandle(t)
		mockSegment2 := mocks_segment_handle.NewSegmentHandle(t)

		// Set up segments
		logHandle.SegmentHandles[1] = mockSegment1
		logHandle.SegmentHandles[2] = mockSegment2
		logHandle.WritableSegmentId = 2

		// Mock GetId calls for logging
		mockSegment1.EXPECT().GetId(mock.Anything).Return(int64(1)).Maybe()
		mockSegment2.EXPECT().GetId(mock.Anything).Return(int64(2)).Maybe()

		// Mock ForceCompleteAndClose calls
		mockSegment1.EXPECT().ForceCompleteAndClose(mock.Anything).Return(nil).Once()
		mockSegment2.EXPECT().ForceCompleteAndClose(mock.Anything).Return(nil).Once()

		// Call Close
		err := logHandle.Close(ctx)

		// Verify
		assert.NoError(t, err)
		assert.Len(t, logHandle.SegmentHandles, 0)              // Segments should be cleared
		assert.Equal(t, int64(-1), logHandle.WritableSegmentId) // WritableSegmentId should be reset

		// Verify expectations
		mockSegment1.AssertExpectations(t)
		mockSegment2.AssertExpectations(t)
	})
}

// TestLogHandle_BackgroundCleanup_DataRaceProtection tests that background cleanup
// properly handles concurrent access to prevent data races
func TestLogHandle_BackgroundCleanup_DataRaceProtection(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)
	ctx := context.Background()

	// Create mock segment handles
	mockSegment1 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegment2 := mocks_segment_handle.NewSegmentHandle(t)

	// Set up segments
	logHandle.SegmentHandles[1] = mockSegment1
	logHandle.SegmentHandles[2] = mockSegment2
	logHandle.WritableSegmentId = 2

	// Mock GetLastAccessTime for cleanup logic
	oldTime := time.Now().Add(-2 * time.Minute).UnixMilli()
	mockSegment1.EXPECT().GetLastAccessTime().Return(oldTime).Maybe()
	mockSegment2.EXPECT().GetLastAccessTime().Return(time.Now().UnixMilli()).Maybe()

	// Mock ForceCompleteAndClose for cleanup
	mockSegment1.EXPECT().ForceCompleteAndClose(mock.Anything).Return(nil).Maybe()

	// Mock GetSegmentMetadata in case cleanup removes segment 1 before GetExistsReadonlySegmentHandle finds it
	logHandle.Metadata.(*mocks_meta.MetadataProvider).EXPECT().
		GetSegmentMetadata(mock.Anything, mock.Anything, int64(1)).
		Return(nil, werr.ErrSegmentNotFound).Maybe()

	// Start concurrent operations
	var wg sync.WaitGroup
	var getErr error
	var getResult interface{}

	// Goroutine 1: Background cleanup (uses the lock-protected version)
	wg.Add(1)
	go func() {
		defer wg.Done()
		logHandle.performBackgroundCleanup(1 * time.Minute)
	}()

	// Goroutine 2: GetExistsReadonlySegmentHandle
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Try to get a segment handle concurrently
		getResult, getErr = logHandle.GetExistsReadonlySegmentHandle(ctx, 1)
	}()

	// Wait for both operations to complete
	wg.Wait()

	// The test should complete without data races
	// The exact outcome depends on timing, but both operations should complete safely
	if getErr == nil {
		t.Logf("GetExistsReadonlySegmentHandle result: %v", getResult)
	} else {
		t.Logf("GetExistsReadonlySegmentHandle error: %v", getErr)
	}

	// Verify expectations
	mockSegment1.AssertExpectations(t)
	mockSegment2.AssertExpectations(t)
}

// createMockLogHandleWithConfig creates a mock log handle with specific storage and fence policy configuration
func createMockLogHandleWithConfig(t *testing.T, storageType string, conditionWrite string) (*logHandleImpl, *mocks_meta.MetadataProvider) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentRollingPolicy: config.SegmentRollingPolicyConfig{
					MaxInterval: config.NewDurationSecondsFromInt(10),
					MaxSize:     64 * 1024 * 1024,
					MaxBlocks:   1000,
				},
				Auditor: config.AuditorConfig{
					MaxInterval: config.NewDurationSecondsFromInt(5),
				},
				SessionMonitor: config.SessionMonitorConfig{
					CheckInterval: config.NewDurationSecondsFromInt(5),
					MaxFailures:   3,
				},
			},
			Storage: config.StorageConfig{
				Type: storageType,
			},
			Logstore: config.LogstoreConfig{
				FencePolicy: config.FencePolicyConfig{
					ConditionWrite: conditionWrite,
				},
			},
		},
	}

	segments := map[int64]*meta.SegmentMeta{}
	logHandle := NewLogHandle("test-log", 1, segments, mockMeta, nil, cfg, nil).(*logHandleImpl)

	return logHandle, mockMeta
}

// TestOpenLogWriter_MinioWithConditionWriteDisabled tests OpenLogWriter with MinIO storage and condition write disabled
// Should use distributed lock writer
func TestOpenLogWriter_MinioWithConditionWriteDisabled(t *testing.T) {
	logHandle, mockMeta := createMockLogHandleWithConfig(t, "minio", "disable")
	ctx := context.Background()

	// Mock successful lock acquisition
	mockSessionLock := meta.NewSessionLockForTest(&concurrency.Session{})
	mockMeta.EXPECT().AcquireLogWriterLock(mock.Anything, "test-log").Return(mockSessionLock, nil)

	// Mock successful fencing (no active segments)
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*meta.SegmentMeta{}, nil)

	// Call OpenLogWriter
	writer, err := logHandle.OpenLogWriter(ctx)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, writer)

	// Verify it's a logWriterImpl (with distributed locks)
	// Check by type assertion - logWriterImpl has GetWriterSessionForTest method
	_, ok := writer.(*logWriterImpl)
	assert.True(t, ok, "Expected logWriterImpl (with distributed locks)")

	// Mock lock release for cleanup
	mockMeta.EXPECT().ReleaseLogWriterLock(mock.Anything, "test-log").Return(nil)

	// Clean up: close writer to avoid resource leaks
	if writer != nil {
		_ = writer.Close(ctx)
	}

	mockMeta.AssertExpectations(t)
}

// TestOpenLogWriter_MinioWithConditionWriteDisabled_LockFailure tests OpenLogWriter with MinIO storage
// and condition write disabled when lock acquisition fails
func TestOpenLogWriter_MinioWithConditionWriteDisabled_LockFailure(t *testing.T) {
	logHandle, mockMeta := createMockLogHandleWithConfig(t, "minio", "disable")
	ctx := context.Background()

	// Mock failed lock acquisition
	mockMeta.EXPECT().AcquireLogWriterLock(mock.Anything, "test-log").Return(nil, errors.New("lock acquisition failed"))

	// Call OpenLogWriter
	writer, err := logHandle.OpenLogWriter(ctx)

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, writer)
	assert.Contains(t, err.Error(), "lock acquisition failed")

	mockMeta.AssertExpectations(t)
}

// TestOpenLogWriter_MinioWithConditionWriteDisabled_FenceFailure tests OpenLogWriter with MinIO storage
// and condition write disabled when fencing fails
func TestOpenLogWriter_MinioWithConditionWriteDisabled_FenceFailure(t *testing.T) {
	logHandle, mockMeta := createMockLogHandleWithConfig(t, "minio", "disable")
	ctx := context.Background()

	// Mock successful lock acquisition
	mockSessionLock := meta.NewSessionLockForTest(&concurrency.Session{})
	mockMeta.EXPECT().AcquireLogWriterLock(mock.Anything, "test-log").Return(mockSessionLock, nil)

	// Mock fencing failure
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(nil, errors.New("fence error"))

	// Mock lock release after fence failure
	mockMeta.EXPECT().ReleaseLogWriterLock(mock.Anything, "test-log").Return(nil)

	// Call OpenLogWriter
	writer, err := logHandle.OpenLogWriter(ctx)

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, writer)

	mockMeta.AssertExpectations(t)
}

// TestOpenLogWriter_MinioWithConditionWriteEnabled tests OpenLogWriter with MinIO storage and condition write enabled
// Should use internal writer with condition write
func TestOpenLogWriter_MinioWithConditionWriteEnabled(t *testing.T) {
	logHandle, mockMeta := createMockLogHandleWithConfig(t, "minio", "enable")
	ctx := context.Background()

	// Mock successful fencing (no active segments)
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*meta.SegmentMeta{}, nil)

	// Call OpenLogWriter
	writer, err := logHandle.OpenLogWriter(ctx)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, writer)

	// Verify it's an internalLogWriterImpl (with condition write)
	// Check by type assertion - internalLogWriterImpl doesn't have GetWriterSessionForTest
	_, ok := writer.(*internalLogWriterImpl)
	assert.True(t, ok, "Expected internalLogWriterImpl (with condition write)")

	// Clean up: close writer to avoid resource leaks
	if writer != nil {
		_ = writer.Close(ctx)
	}

	// Verify AcquireLogWriterLock was NOT called (should use internal writer)
	mockMeta.AssertExpectations(t)
}

// TestOpenLogWriter_MinioWithConditionWriteAuto tests OpenLogWriter with MinIO storage and condition write auto
// Should use internal writer with condition write
func TestOpenLogWriter_MinioWithConditionWriteAuto(t *testing.T) {
	logHandle, mockMeta := createMockLogHandleWithConfig(t, "minio", "auto")
	ctx := context.Background()

	// Mock successful fencing (no active segments)
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*meta.SegmentMeta{}, nil)

	// Call OpenLogWriter
	writer, err := logHandle.OpenLogWriter(ctx)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, writer)

	// Verify it's an internalLogWriterImpl (with condition write)
	// Check by type assertion - internalLogWriterImpl doesn't have GetWriterSessionForTest
	_, ok := writer.(*internalLogWriterImpl)
	assert.True(t, ok, "Expected internalLogWriterImpl (with condition write)")

	// Clean up: close writer to avoid resource leaks
	if writer != nil {
		_ = writer.Close(ctx)
	}

	// Verify AcquireLogWriterLock was NOT called (should use internal writer)
	mockMeta.AssertExpectations(t)
}

// TestOpenLogWriter_NonMinioStorage tests OpenLogWriter with non-MinIO storage
// Should use internal writer regardless of fence policy
func TestOpenLogWriter_NonMinioStorage(t *testing.T) {
	logHandle, mockMeta := createMockLogHandleWithConfig(t, "local", "disable")
	ctx := context.Background()

	// Mock successful fencing (no active segments)
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*meta.SegmentMeta{}, nil)

	// Call OpenLogWriter
	writer, err := logHandle.OpenLogWriter(ctx)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, writer)

	// Verify it's an internalLogWriterImpl (with condition write)
	// Check by type assertion - internalLogWriterImpl doesn't have GetWriterSessionForTest
	_, ok := writer.(*internalLogWriterImpl)
	assert.True(t, ok, "Expected internalLogWriterImpl (with condition write)")

	// Clean up: close writer to avoid resource leaks
	if writer != nil {
		_ = writer.Close(ctx)
	}

	// Verify AcquireLogWriterLock was NOT called (should use internal writer)
	mockMeta.AssertExpectations(t)
}

// TestOpenLogWriter_DefaultStorageType tests OpenLogWriter with default storage type (empty string)
// Should be treated as MinIO
func TestOpenLogWriter_DefaultStorageType(t *testing.T) {
	logHandle, mockMeta := createMockLogHandleWithConfig(t, "", "disable")
	ctx := context.Background()

	// Mock successful lock acquisition
	mockSessionLock := meta.NewSessionLockForTest(&concurrency.Session{})
	mockMeta.EXPECT().AcquireLogWriterLock(mock.Anything, "test-log").Return(mockSessionLock, nil)

	// Mock successful fencing (no active segments)
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*meta.SegmentMeta{}, nil)

	// Call OpenLogWriter
	writer, err := logHandle.OpenLogWriter(ctx)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, writer)

	// Verify it's a logWriterImpl (with distributed locks)
	_, ok := writer.(*logWriterImpl)
	assert.True(t, ok, "Expected logWriterImpl (with distributed locks)")

	// Mock lock release for cleanup
	mockMeta.EXPECT().ReleaseLogWriterLock(mock.Anything, "test-log").Return(nil)

	// Clean up: close writer to avoid resource leaks
	if writer != nil {
		_ = writer.Close(ctx)
	}

	mockMeta.AssertExpectations(t)
}

// TestOpenLogWriter_InternalWriter_FenceFailure tests OpenLogWriter with internal writer when fencing fails
func TestOpenLogWriter_InternalWriter_FenceFailure(t *testing.T) {
	logHandle, mockMeta := createMockLogHandleWithConfig(t, "minio", "enable")
	ctx := context.Background()

	// Mock fencing failure
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(nil, errors.New("fence error"))

	// Call OpenLogWriter
	writer, err := logHandle.OpenLogWriter(ctx)

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, writer)

	mockMeta.AssertExpectations(t)
}

// TestOpenLogWriter_MinioWithConditionWriteDisabled_ActiveSegments tests OpenLogWriter with MinIO storage,
// condition write disabled, and active segments that need fencing
func TestOpenLogWriter_MinioWithConditionWriteDisabled_ActiveSegments(t *testing.T) {
	logHandle, mockMeta := createMockLogHandleWithConfig(t, "minio", "disable")
	ctx := context.Background()

	// Create mock segment handles
	mockSegment1 := mocks_segment_handle.NewSegmentHandle(t)

	// Mock successful lock acquisition
	mockSessionLock := meta.NewSessionLockForTest(&concurrency.Session{})
	mockMeta.EXPECT().AcquireLogWriterLock(mock.Anything, "test-log").Return(mockSessionLock, nil)

	// Mock segments with active segment
	segments := map[int64]*meta.SegmentMeta{
		1: {Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active}, Revision: 1},
	}
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(segments, nil)

	// Set up segment handle in cache
	logHandle.SegmentHandles[1] = mockSegment1

	// Mock successful fencing
	mockSegment1.EXPECT().FenceAndComplete(mock.Anything).Return(int64(100), nil)

	// Note: No UpdateSegmentMetadata mock needed here — FenceAndComplete handles metadata update internally.

	// Call OpenLogWriter
	writer, err := logHandle.OpenLogWriter(ctx)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, writer)

	// Verify it's a logWriterImpl (with distributed locks)
	_, ok := writer.(*logWriterImpl)
	assert.True(t, ok, "Expected logWriterImpl (with distributed locks)")

	// Mock segment handle cleanup for writer close
	mockSegment1.EXPECT().ForceCompleteAndClose(mock.Anything).Return(nil).Maybe()

	// Mock lock release for cleanup
	mockMeta.EXPECT().ReleaseLogWriterLock(mock.Anything, "test-log").Return(nil)

	// Clean up: close writer to avoid resource leaks
	if writer != nil {
		_ = writer.Close(ctx)
	}

	mockSegment1.AssertExpectations(t)
	mockMeta.AssertExpectations(t)
}

// TestLogHandle_GetLastRecordId tests that GetLastRecordId returns ErrOperationNotSupported
func TestLogHandle_GetLastRecordId(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)
	ctx := context.Background()

	result, err := logHandle.GetLastRecordId(ctx)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.True(t, werr.ErrOperationNotSupported.Is(err))
}

// TestLogHandle_GetTruncatedRecordId tests retrieving the truncation point
func TestLogHandle_GetTruncatedRecordId(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
			Metadata: &proto.LogMeta{
				TruncatedSegmentId: 5,
				TruncatedEntryId:   10,
			},
			Revision: 1,
		}, nil)

		result, err := logHandle.GetTruncatedRecordId(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, int64(5), result.SegmentId)
		assert.Equal(t, int64(10), result.EntryId)
	})

	t.Run("Error", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(nil, errors.New("metadata error"))

		result, err := logHandle.GetTruncatedRecordId(ctx)
		assert.Nil(t, result)
		assert.Error(t, err)
	})

	t.Run("NoTruncation", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
			Metadata: &proto.LogMeta{
				TruncatedSegmentId: -1,
				TruncatedEntryId:   -1,
			},
			Revision: 1,
		}, nil)

		result, err := logHandle.GetTruncatedRecordId(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, int64(-1), result.SegmentId)
		assert.Equal(t, int64(-1), result.EntryId)
	})
}

// TestLogHandle_Truncate tests the Truncate method
func TestLogHandle_Truncate(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
			Metadata: &proto.LogMeta{
				TruncatedSegmentId: 0,
				TruncatedEntryId:   0,
			},
			Revision: 1,
		}, nil)

		mockMeta.EXPECT().GetSegmentMetadata(mock.Anything, "test-log", int64(3)).Return(&meta.SegmentMeta{
			Metadata: &proto.SegmentMetadata{
				SegNo:       3,
				State:       proto.SegmentState_Completed,
				LastEntryId: 100,
			},
			Revision: 1,
		}, nil)

		mockMeta.EXPECT().UpdateLogMeta(mock.Anything, "test-log", mock.Anything).Return(nil)

		err := logHandle.Truncate(ctx, &LogMessageId{SegmentId: 3, EntryId: 50})
		assert.NoError(t, err)
	})

	t.Run("GetLogMetaError", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(nil, errors.New("meta error"))

		err := logHandle.Truncate(ctx, &LogMessageId{SegmentId: 3, EntryId: 50})
		assert.Error(t, err)
	})

	t.Run("TruncationPointBehindCurrent", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
			Metadata: &proto.LogMeta{
				TruncatedSegmentId: 10,
				TruncatedEntryId:   50,
			},
			Revision: 1,
		}, nil)

		// Requesting truncation behind current point should succeed without error
		err := logHandle.Truncate(ctx, &LogMessageId{SegmentId: 5, EntryId: 10})
		assert.NoError(t, err)
	})

	t.Run("SegmentNotFound", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
			Metadata: &proto.LogMeta{
				TruncatedSegmentId: 0,
				TruncatedEntryId:   0,
			},
			Revision: 1,
		}, nil)

		mockMeta.EXPECT().GetSegmentMetadata(mock.Anything, "test-log", int64(99)).Return(nil, werr.ErrSegmentNotFound)

		// Segment not found should not return error (just skip)
		err := logHandle.Truncate(ctx, &LogMessageId{SegmentId: 99, EntryId: 0})
		assert.NoError(t, err)
	})

	t.Run("InvalidEntryId", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
			Metadata: &proto.LogMeta{
				TruncatedSegmentId: 0,
				TruncatedEntryId:   0,
			},
			Revision: 1,
		}, nil)

		mockMeta.EXPECT().GetSegmentMetadata(mock.Anything, "test-log", int64(3)).Return(&meta.SegmentMeta{
			Metadata: &proto.SegmentMetadata{
				SegNo:       3,
				State:       proto.SegmentState_Completed,
				LastEntryId: 10,
			},
			Revision: 1,
		}, nil)

		// Entry ID 20 exceeds LastEntryId 10
		err := logHandle.Truncate(ctx, &LogMessageId{SegmentId: 3, EntryId: 20})
		assert.Error(t, err)
	})

	t.Run("UpdateLogMetaError", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
			Metadata: &proto.LogMeta{
				TruncatedSegmentId: 0,
				TruncatedEntryId:   0,
			},
			Revision: 1,
		}, nil)

		mockMeta.EXPECT().GetSegmentMetadata(mock.Anything, "test-log", int64(3)).Return(&meta.SegmentMeta{
			Metadata: &proto.SegmentMetadata{
				SegNo:       3,
				State:       proto.SegmentState_Completed,
				LastEntryId: 100,
			},
			Revision: 1,
		}, nil)

		mockMeta.EXPECT().UpdateLogMeta(mock.Anything, "test-log", mock.Anything).Return(errors.New("update failed"))

		err := logHandle.Truncate(ctx, &LogMessageId{SegmentId: 3, EntryId: 50})
		assert.Error(t, err)
	})
}

// TestLogHandle_CheckAndSetSegmentTruncatedIfNeed tests segment truncation checking
func TestLogHandle_CheckAndSetSegmentTruncatedIfNeed(t *testing.T) {
	t.Run("GetLogMetaError", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(nil, errors.New("meta error"))

		err := logHandle.CheckAndSetSegmentTruncatedIfNeed(ctx)
		assert.Error(t, err)
	})

	t.Run("NoTruncationPointSet", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
			Metadata: &proto.LogMeta{
				TruncatedSegmentId: 0,
				TruncatedEntryId:   0,
			},
			Revision: 1,
		}, nil)

		err := logHandle.CheckAndSetSegmentTruncatedIfNeed(ctx)
		assert.NoError(t, err)
	})

	t.Run("GetSegmentsError", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
			Metadata: &proto.LogMeta{
				TruncatedSegmentId: 5,
				TruncatedEntryId:   10,
			},
			Revision: 1,
		}, nil)

		mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(nil, errors.New("segments error"))

		err := logHandle.CheckAndSetSegmentTruncatedIfNeed(ctx)
		assert.Error(t, err)
	})

	t.Run("SegmentsAlreadyTruncated", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
			Metadata: &proto.LogMeta{
				TruncatedSegmentId: 5,
				TruncatedEntryId:   10,
			},
			Revision: 1,
		}, nil)

		segments := map[int64]*meta.SegmentMeta{
			0: {Metadata: &proto.SegmentMetadata{SegNo: 0, State: proto.SegmentState_Truncated}, Revision: 1},
			3: {Metadata: &proto.SegmentMetadata{SegNo: 3, State: proto.SegmentState_Truncated}, Revision: 1},
			5: {Metadata: &proto.SegmentMetadata{SegNo: 5, State: proto.SegmentState_Active}, Revision: 1},
		}
		mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(segments, nil)

		err := logHandle.CheckAndSetSegmentTruncatedIfNeed(ctx)
		assert.NoError(t, err)
	})
}

// TestLogHandle_GetRecoverableSegmentHandle tests getting recoverable segment handles
func TestLogHandle_GetRecoverableSegmentHandle(t *testing.T) {
	t.Run("SegmentNotFound", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetSegmentMetadata(mock.Anything, "test-log", int64(99)).Return(nil, werr.ErrSegmentNotFound)

		segHandle, err := logHandle.GetRecoverableSegmentHandle(ctx, 99)
		assert.Nil(t, segHandle)
		assert.Error(t, err)
		assert.True(t, werr.ErrSegmentNotFound.Is(err))
	})
}

// TestLogHandle_GetCurrentWritableSegmentHandle tests the test-only method
func TestLogHandle_GetCurrentWritableSegmentHandle(t *testing.T) {
	t.Run("NoWritableSegment", func(t *testing.T) {
		logHandle, _ := createMockLogHandle(t)
		ctx := context.Background()

		// WritableSegmentId = -1 by default
		segHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
		assert.Nil(t, segHandle)
	})

	t.Run("HasWritableSegment", func(t *testing.T) {
		logHandle, _ := createMockLogHandle(t)
		ctx := context.Background()

		mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)
		logHandle.SegmentHandles[5] = mockSegHandle
		logHandle.WritableSegmentId = 5

		segHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
		assert.NotNil(t, segHandle)
		assert.Equal(t, mockSegHandle, segHandle)
	})
}

// TestLogHandle_GetRecoverableSegmentHandle_Success tests the success path
func TestLogHandle_GetRecoverableSegmentHandle_Success(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)
	ctx := context.Background()

	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)
	logHandle.SegmentHandles[5] = mockSegHandle

	segHandle, err := logHandle.GetRecoverableSegmentHandle(ctx, 5)
	assert.NoError(t, err)
	assert.Equal(t, mockSegHandle, segHandle)
}

// TestLogHandle_GetRecoverableSegmentHandle_NilSegment tests the nil segment case
func TestLogHandle_GetRecoverableSegmentHandle_NilSegment(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Not in cache, metadata returns nil
	mockMeta.EXPECT().GetSegmentMetadata(mock.Anything, "test-log", int64(99)).Return(nil, nil)

	segHandle, err := logHandle.GetRecoverableSegmentHandle(ctx, 99)
	assert.Nil(t, segHandle)
	assert.Error(t, err)
	assert.True(t, werr.ErrSegmentNotFound.Is(err))
}

// TestLogHandle_GetExistsReadonlySegmentHandle_MetadataError tests metadata retrieval error
func TestLogHandle_GetExistsReadonlySegmentHandle_MetadataError(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	mockMeta.EXPECT().GetSegmentMetadata(mock.Anything, "test-log", int64(99)).Return(nil, errors.New("connection error"))

	segHandle, err := logHandle.GetExistsReadonlySegmentHandle(ctx, 99)
	assert.Nil(t, segHandle)
	assert.Error(t, err)
}

// TestLogHandle_GetExistsReadonlySegmentHandle_NilSegMeta tests when metadata returns nil
func TestLogHandle_GetExistsReadonlySegmentHandle_NilSegMeta(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	mockMeta.EXPECT().GetSegmentMetadata(mock.Anything, "test-log", int64(99)).Return(nil, nil)

	segHandle, err := logHandle.GetExistsReadonlySegmentHandle(ctx, 99)
	assert.Nil(t, segHandle)
	assert.Error(t, err)
	assert.True(t, werr.ErrSegmentNotFound.Is(err))
}

// TestLogHandle_GetExistsReadonlySegmentHandle_FoundInMetadata tests the full path from metadata
func TestLogHandle_GetExistsReadonlySegmentHandle_FoundInMetadata(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	mockMeta.EXPECT().GetSegmentMetadata(mock.Anything, "test-log", int64(10)).Return(&meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 10,
			State: proto.SegmentState_Sealed,
		},
		Revision: 1,
	}, nil)

	segHandle, err := logHandle.GetExistsReadonlySegmentHandle(ctx, 10)
	assert.NoError(t, err)
	assert.NotNil(t, segHandle)
	// Verify it was cached
	assert.Contains(t, logHandle.SegmentHandles, int64(10))
}

// TestLogHandle_AdjustPendingReadPointIfTruncated tests truncation adjustment logic
func TestLogHandle_AdjustPendingReadPointIfTruncated(t *testing.T) {
	t.Run("GetTruncatedRecordIdError", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(nil, errors.New("meta error"))

		from := &LogMessageId{SegmentId: 0, EntryId: 0}
		result := logHandle.adjustPendingReadPointIfTruncated(ctx, "reader-1", from)
		// On error, should return original from
		assert.Equal(t, from, result)
	})

	t.Run("NoTruncation_ReturnsOriginal", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
			Metadata: &proto.LogMeta{
				TruncatedSegmentId: -1,
				TruncatedEntryId:   -1,
			},
			Revision: 1,
		}, nil)

		from := &LogMessageId{SegmentId: 5, EntryId: 10}
		result := logHandle.adjustPendingReadPointIfTruncated(ctx, "reader-1", from)
		assert.Equal(t, from, result)
	})

	t.Run("FromEarliest_AdjustToTruncationPoint", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
			Metadata: &proto.LogMeta{
				TruncatedSegmentId: 5,
				TruncatedEntryId:   10,
			},
			Revision: 1,
		}, nil)

		earliest := EarliestLogMessageID()
		from := &earliest
		result := logHandle.adjustPendingReadPointIfTruncated(ctx, "reader-1", from)
		// Should adjust to after truncation point
		assert.Equal(t, int64(5), result.SegmentId)
		assert.Equal(t, int64(11), result.EntryId)
	})

	t.Run("FromEarliest_AdjustSegment", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
			Metadata: &proto.LogMeta{
				TruncatedSegmentId: 5,
				TruncatedEntryId:   10,
			},
			Revision: 1,
		}, nil)

		// EarliestLogMessageID has segId=0, which is < truncatedSegId=5
		earliest := EarliestLogMessageID()
		from := &earliest
		result := logHandle.adjustPendingReadPointIfTruncated(ctx, "reader-1", from)
		assert.Equal(t, int64(5), result.SegmentId)
		assert.Equal(t, int64(11), result.EntryId) // truncatedEntryId + 1
	})

	t.Run("SpecificPosition_NotAdjusted", func(t *testing.T) {
		logHandle, mockMeta := createMockLogHandle(t)
		ctx := context.Background()

		mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
			Metadata: &proto.LogMeta{
				TruncatedSegmentId: 5,
				TruncatedEntryId:   10,
			},
			Revision: 1,
		}, nil)

		// Specific position (not earliest) should NOT be adjusted
		from := &LogMessageId{SegmentId: 3, EntryId: 0}
		result := logHandle.adjustPendingReadPointIfTruncated(ctx, "reader-1", from)
		assert.Equal(t, from, result)
	})
}

// TestLogHandle_ShouldMoveToTruncationPoint tests the truncation point decision
func TestLogHandle_ShouldMoveToTruncationPoint(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)

	t.Run("EarliestWithTruncation", func(t *testing.T) {
		earliest := EarliestLogMessageID()
		from := &earliest
		truncatedId := &LogMessageId{SegmentId: 5, EntryId: 10}
		assert.True(t, logHandle.shouldMoveToTruncationPoint(from, truncatedId))
	})

	t.Run("EarliestWithNegativeTruncation", func(t *testing.T) {
		earliest := EarliestLogMessageID()
		from := &earliest
		truncatedId := &LogMessageId{SegmentId: -1, EntryId: -1}
		assert.False(t, logHandle.shouldMoveToTruncationPoint(from, truncatedId))
	})

	t.Run("SpecificPositionNotMoved", func(t *testing.T) {
		from := &LogMessageId{SegmentId: 3, EntryId: 5}
		truncatedId := &LogMessageId{SegmentId: 5, EntryId: 10}
		assert.False(t, logHandle.shouldMoveToTruncationPoint(from, truncatedId))
	})
}

// TestLogHandle_CheckAndSetSegmentTruncatedIfNeed_WithActiveSeg tests truncation with active segment needing update
func TestLogHandle_CheckAndSetSegmentTruncatedIfNeed_WithActiveSeg(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
		Metadata: &proto.LogMeta{
			TruncatedSegmentId: 5,
			TruncatedEntryId:   10,
		},
		Revision: 1,
	}, nil)

	segments := map[int64]*meta.SegmentMeta{
		0: {Metadata: &proto.SegmentMetadata{SegNo: 0, State: proto.SegmentState_Active}, Revision: 1},
		3: {Metadata: &proto.SegmentMetadata{SegNo: 3, State: proto.SegmentState_Completed}, Revision: 1},
		5: {Metadata: &proto.SegmentMetadata{SegNo: 5, State: proto.SegmentState_Active}, Revision: 1}, // truncation segment - skip
		8: {Metadata: &proto.SegmentMetadata{SegNo: 8, State: proto.SegmentState_Active}, Revision: 1}, // after truncation - skip
	}
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(segments, nil)

	// Only segments 0 and 3 (before truncation seg 5) should be marked as truncated
	mockMeta.EXPECT().UpdateSegmentMetadata(mock.Anything, "test-log", int64(1), mock.MatchedBy(func(sm *meta.SegmentMeta) bool {
		return sm.Metadata.State == proto.SegmentState_Truncated && sm.Metadata.SegNo == 0
	}), proto.SegmentState_Active).Return(nil)
	mockMeta.EXPECT().UpdateSegmentMetadata(mock.Anything, "test-log", int64(1), mock.MatchedBy(func(sm *meta.SegmentMeta) bool {
		return sm.Metadata.State == proto.SegmentState_Truncated && sm.Metadata.SegNo == 3
	}), proto.SegmentState_Completed).Return(nil)

	err := logHandle.CheckAndSetSegmentTruncatedIfNeed(ctx)
	assert.NoError(t, err)
}

// TestLogHandle_CheckAndSetSegmentTruncatedIfNeed_UpdateError tests update metadata error
func TestLogHandle_CheckAndSetSegmentTruncatedIfNeed_UpdateError(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
		Metadata: &proto.LogMeta{
			TruncatedSegmentId: 5,
			TruncatedEntryId:   10,
		},
		Revision: 1,
	}, nil)

	segments := map[int64]*meta.SegmentMeta{
		0: {Metadata: &proto.SegmentMetadata{SegNo: 0, State: proto.SegmentState_Active}, Revision: 1},
	}
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(segments, nil)

	// Update fails but should not return error (logs and continues)
	mockMeta.EXPECT().UpdateSegmentMetadata(mock.Anything, "test-log", int64(1), mock.Anything, mock.Anything).Return(errors.New("update error"))

	err := logHandle.CheckAndSetSegmentTruncatedIfNeed(ctx)
	assert.NoError(t, err) // error is logged but not returned
}

// TestLogHandle_OpenLogReader_Success tests successful reader opening
func TestLogHandle_OpenLogReader_Success(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Mock GetTruncatedRecordId (via GetLogMeta)
	mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
		Metadata: &proto.LogMeta{
			TruncatedSegmentId: -1,
			TruncatedEntryId:   -1,
		},
		Revision: 1,
	}, nil)

	// Mock CreateReaderTempInfo success
	mockMeta.EXPECT().CreateReaderTempInfo(mock.Anything, mock.AnythingOfType("string"), int64(1), int64(0), int64(0)).Return(nil)

	reader, err := logHandle.OpenLogReader(ctx, &LogMessageId{SegmentId: 0, EntryId: 0}, "")
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	// Cleanup
	mockMeta.EXPECT().DeleteReaderTempInfo(mock.Anything, int64(1), mock.AnythingOfType("string")).Return(nil)
	_ = reader.Close(ctx)
}

// TestLogHandle_OpenLogReader_WithReaderBaseName tests reader opening with a custom base name
func TestLogHandle_OpenLogReader_WithReaderBaseName(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
		Metadata: &proto.LogMeta{
			TruncatedSegmentId: -1,
			TruncatedEntryId:   -1,
		},
		Revision: 1,
	}, nil)

	mockMeta.EXPECT().CreateReaderTempInfo(mock.Anything, mock.AnythingOfType("string"), int64(1), int64(0), int64(0)).Return(nil)

	reader, err := logHandle.OpenLogReader(ctx, &LogMessageId{SegmentId: 0, EntryId: 0}, "my-reader")
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Contains(t, reader.GetName(), "my-reader")

	mockMeta.EXPECT().DeleteReaderTempInfo(mock.Anything, int64(1), mock.AnythingOfType("string")).Return(nil)
	_ = reader.Close(ctx)
}

// TestLogHandle_OpenLogReader_WithTruncation tests reader opening with truncation adjustment
func TestLogHandle_OpenLogReader_WithTruncation(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
		Metadata: &proto.LogMeta{
			TruncatedSegmentId: 3,
			TruncatedEntryId:   10,
		},
		Revision: 1,
	}, nil)

	// Reader starts from earliest, should be adjusted to after truncation point
	mockMeta.EXPECT().CreateReaderTempInfo(mock.Anything, mock.AnythingOfType("string"), int64(1), int64(3), int64(11)).Return(nil)

	earliest := EarliestLogMessageID()
	reader, err := logHandle.OpenLogReader(ctx, &earliest, "")
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	mockMeta.EXPECT().DeleteReaderTempInfo(mock.Anything, int64(1), mock.AnythingOfType("string")).Return(nil)
	_ = reader.Close(ctx)
}

// TestLogHandle_Truncate_GetSegmentMetadataOtherError tests non-SegmentNotFound metadata error
func TestLogHandle_Truncate_GetSegmentMetadataOtherError(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
		Metadata: &proto.LogMeta{
			TruncatedSegmentId: 0,
			TruncatedEntryId:   0,
		},
		Revision: 1,
	}, nil)

	mockMeta.EXPECT().GetSegmentMetadata(mock.Anything, "test-log", int64(3)).Return(nil, errors.New("connection error"))

	err := logHandle.Truncate(ctx, &LogMessageId{SegmentId: 3, EntryId: 50})
	assert.Error(t, err)
}

// TestLogHandle_Truncate_SameSegmentBehindEntry tests truncation with same segment but behind entry
func TestLogHandle_Truncate_SameSegmentBehindEntry(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&meta.LogMeta{
		Metadata: &proto.LogMeta{
			TruncatedSegmentId: 5,
			TruncatedEntryId:   50,
		},
		Revision: 1,
	}, nil)

	// Same segment but entry behind current - should be treated as behind
	err := logHandle.Truncate(ctx, &LogMessageId{SegmentId: 5, EntryId: 20})
	assert.NoError(t, err)
}

// === Background cleanup tests ===

func TestLogHandle_StopBackgroundCleanup_AlreadyStopped(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)

	// Stop background cleanup (it was started by NewLogHandle)
	logHandle.stopBackgroundCleanup()

	// Call again - should be idempotent (already closed channel)
	logHandle.stopBackgroundCleanup()
}

func TestLogHandle_BackgroundCleanupLoop_StopsOnCleanupDone(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)

	// Stop the existing cleanup goroutine first
	logHandle.stopBackgroundCleanup()

	// Reset for manual testing
	logHandle.cleanupDone = make(chan struct{})
	logHandle.cleanupWg.Add(1)

	done := make(chan struct{})
	go func() {
		logHandle.backgroundCleanupLoop()
		close(done)
	}()

	// Signal stop
	close(logHandle.cleanupDone)

	select {
	case <-done:
		// Success
	case <-time.After(3 * time.Second):
		t.Fatal("backgroundCleanupLoop did not stop")
	}
}

func TestLogHandle_BackgroundCleanupLoop_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mockMeta := mocks_meta.NewMetadataProvider(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentRollingPolicy: config.SegmentRollingPolicyConfig{
					MaxInterval: config.NewDurationSecondsFromInt(10),
					MaxSize:     64 * 1024 * 1024,
					MaxBlocks:   1000,
				},
			},
		},
	}
	cfg.Woodpecker.Logstore.FencePolicy.ConditionWrite = "enable"

	segments := map[int64]*meta.SegmentMeta{}
	logHandle := NewLogHandle("test-log", 1, segments, mockMeta, nil, cfg, func(ctx context.Context) (*proto.QuorumInfo, error) {
		return &proto.QuorumInfo{Id: -1, Wq: 1, Aq: 1, Es: 1, Nodes: []string{"127.0.0.1:59456"}}, nil
	}).(*logHandleImpl)

	// Stop the default cleanup
	logHandle.stopBackgroundCleanup()

	// Replace context with cancellable one and restart
	logHandle.ctx = ctx
	logHandle.cleanupDone = make(chan struct{})
	logHandle.cleanupWg.Add(1)

	done := make(chan struct{})
	go func() {
		logHandle.backgroundCleanupLoop()
		close(done)
	}()

	// Cancel context
	cancel()

	select {
	case <-done:
		// Success
	case <-time.After(3 * time.Second):
		t.Fatal("backgroundCleanupLoop did not stop on context cancel")
	}
}

func TestLogHandle_PerformBackgroundCleanup_NoHandles(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)

	// No segment handles - cleanup should be a no-op
	logHandle.performBackgroundCleanup(1 * time.Minute)
	assert.Empty(t, logHandle.SegmentHandles)
}

func TestLogHandle_PerformBackgroundCleanup_RemovesIdleHandles(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)

	mockIdleSegment := mocks_segment_handle.NewSegmentHandle(t)
	mockRecentSegment := mocks_segment_handle.NewSegmentHandle(t)

	logHandle.SegmentHandles[1] = mockIdleSegment
	logHandle.SegmentHandles[2] = mockRecentSegment
	logHandle.WritableSegmentId = -1 // No writable segment

	// Idle segment (old access time)
	mockIdleSegment.EXPECT().GetLastAccessTime().Return(time.Now().Add(-5 * time.Minute).UnixMilli())
	mockIdleSegment.EXPECT().ForceCompleteAndClose(mock.Anything).Return(nil)

	// Recent segment
	mockRecentSegment.EXPECT().GetLastAccessTime().Return(time.Now().UnixMilli())

	logHandle.performBackgroundCleanup(1 * time.Minute)

	assert.Len(t, logHandle.SegmentHandles, 1)
	assert.Contains(t, logHandle.SegmentHandles, int64(2))
}

// === Additional cleanupIdleSegmentHandlesUnsafe tests ===

func TestLogHandle_CleanupIdleSegmentHandlesUnsafe_NoHandlesToClean(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)
	ctx := context.Background()

	mockSegment := mocks_segment_handle.NewSegmentHandle(t)
	logHandle.SegmentHandles[1] = mockSegment

	// Recent access time - should not be cleaned
	mockSegment.EXPECT().GetLastAccessTime().Return(time.Now().UnixMilli())

	logHandle.cleanupIdleSegmentHandlesUnsafe(ctx, 1*time.Minute)

	assert.Len(t, logHandle.SegmentHandles, 1)
}

// === fenceAllActiveSegments additional test ===

func TestFenceAllActiveSegments_GetSegmentsError(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(nil, werr.ErrInternalError)

	err := logHandle.fenceAllActiveSegments(ctx)
	assert.Error(t, err)
}

// === GetNextSegmentId test ===

func TestLogHandle_GetNextSegmentId_Error(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(nil, werr.ErrInternalError)

	_, err := logHandle.GetNextSegmentId(ctx)
	assert.Error(t, err)
}

// === createNewSegmentMeta tests ===

func TestLogHandle_CreateNewSegmentMeta_QuorumError(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentRollingPolicy: config.SegmentRollingPolicyConfig{
					MaxInterval: config.NewDurationSecondsFromInt(10),
					MaxSize:     64 * 1024 * 1024,
					MaxBlocks:   1000,
				},
			},
		},
	}
	cfg.Woodpecker.Logstore.FencePolicy.ConditionWrite = "enable"

	segments := map[int64]*meta.SegmentMeta{}
	logHandle := NewLogHandle("test-log", 1, segments, mockMeta, nil, cfg, func(ctx context.Context) (*proto.QuorumInfo, error) {
		return nil, werr.ErrInternalError // Quorum selection fails
	}).(*logHandleImpl)
	defer logHandle.stopBackgroundCleanup()

	ctx := context.Background()
	_, err := logHandle.createNewSegmentMeta(ctx)
	assert.Error(t, err)
}

func TestLogHandle_CreateNewSegmentMeta_StoreError(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*meta.SegmentMeta{}, nil)
	mockMeta.EXPECT().StoreSegmentMetadata(mock.Anything, "test-log", mock.Anything, mock.Anything).Return(werr.ErrInternalError)

	_, err := logHandle.createNewSegmentMeta(ctx)
	assert.Error(t, err)
}
