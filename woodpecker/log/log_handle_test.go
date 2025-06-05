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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
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
					MaxInterval: 10,               // 10 minutes
					MaxSize:     64 * 1024 * 1024, // 64MB
				},
			},
		},
	}

	segments := make(map[int64]*proto.SegmentMetadata)
	logHandle := NewLogHandle("test-log", 1, segments, mockMeta, nil, cfg).(*logHandleImpl)

	return logHandle, mockMeta
}

// TestOpenLogReader_ReaderResourceLeak tests that reader is properly cleaned up on CreateReaderTempInfo error
func TestOpenLogReader_ReaderResourceLeak(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Mock GetTruncatedRecordId (called by adjustPendingReadPointIfTruncated)
	mockLogMeta := &proto.LogMeta{
		TruncatedSegmentId: 0,
		TruncatedEntryId:   0,
	}
	mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(mockLogMeta, nil)

	// Mock DeleteReaderTempInfo (called by reader.Close)
	mockMeta.EXPECT().DeleteReaderTempInfo(mock.Anything, int64(1), mock.AnythingOfType("string")).Return(nil)

	// Mock CreateReaderTempInfo failure
	from := &LogMessageId{SegmentId: 1, EntryId: 0}
	mockMeta.EXPECT().CreateReaderTempInfo(mock.Anything, mock.AnythingOfType("string"), int64(1), int64(1), int64(0)).Return(errors.New("temp info error"))

	// Call OpenLogReader and expect error
	reader, err := logHandle.OpenLogReader(ctx, from, "test-reader")

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, reader)
	assert.True(t, werr.ErrReaderTempInfoError.Is(err))

	// Verify all expectations were met
	mockMeta.AssertExpectations(t)
}

// TestSegmentHandlesCacheCleanup tests the segment handles cache cleanup mechanism
func TestSegmentHandlesCacheCleanup(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Mock GetSegmentMetadata for non-existent segment
	mockMeta.EXPECT().GetSegmentMetadata(mock.Anything, "test-log", int64(999)).Return(nil, werr.ErrSegmentNotFound)

	// Create mock segment handles
	mockSegment1 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegment2 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegment3 := mocks_segment_handle.NewSegmentHandle(t)

	// Set up segment handles with different access times
	now := time.Now()
	oldTime := now.Add(-2 * time.Minute)     // 2 minutes ago (should be cleaned up)
	recentTime := now.Add(-30 * time.Second) // 30 seconds ago (should be kept)

	logHandle.SegmentHandles[1] = mockSegment1
	logHandle.SegmentHandles[2] = mockSegment2
	logHandle.SegmentHandles[3] = mockSegment3
	logHandle.SegmentHandles[4] = mockSegment1
	logHandle.SegmentHandles[5] = mockSegment2
	logHandle.SegmentHandles[6] = mockSegment3

	logHandle.SegmentHandleLastAccess[1] = oldTime    // Old, should be cleaned
	logHandle.SegmentHandleLastAccess[2] = recentTime // Recent, should be kept (also writable)
	logHandle.SegmentHandleLastAccess[3] = oldTime    // Old, should be cleaned
	logHandle.SegmentHandleLastAccess[4] = oldTime    // Old, should be cleaned
	logHandle.SegmentHandleLastAccess[5] = recentTime // Recent, should be kept
	logHandle.SegmentHandleLastAccess[6] = oldTime    // Old, should be cleaned

	logHandle.WritableSegmentId = 2 // Segment 2 is writable, should not be cleaned

	// Set lastCleanupTime to trigger cleanup
	logHandle.lastCleanupTime = now.Add(-1 * time.Minute) // 1 minute ago

	// Expect Close calls for old segments (except writable one)
	// Segments 1, 3, 4, 6 should be cleaned (4 calls total)
	mockSegment1.EXPECT().Close(mock.Anything).Return(nil).Times(2) // segments 1, 4
	mockSegment3.EXPECT().Close(mock.Anything).Return(nil).Times(2) // segments 3, 6

	// Call GetExistsReadonlySegmentHandle to trigger cleanup
	_, _ = logHandle.GetExistsReadonlySegmentHandle(ctx, 999) // Non-existent segment to trigger cleanup

	// Verify cleanup results
	assert.Len(t, logHandle.SegmentHandles, 2, "Should have 2 segment handles remaining")
	assert.Len(t, logHandle.SegmentHandleLastAccess, 2, "Should have 2 access time records remaining")
	assert.Contains(t, logHandle.SegmentHandles, int64(2), "Writable segment should be kept")
	assert.Contains(t, logHandle.SegmentHandles, int64(5), "Recent segment should be kept")

	// Verify all expectations were met
	mockMeta.AssertExpectations(t)
	mockSegment1.AssertExpectations(t)
	mockSegment2.AssertExpectations(t)
	mockSegment3.AssertExpectations(t)
}

// TestSegmentHandlesCacheCleanup_CloseError tests cleanup behavior when Close fails
func TestSegmentHandlesCacheCleanup_CloseError(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)
	ctx := context.Background()

	// Create mock segment handle that fails to close
	mockSegment := mocks_segment_handle.NewSegmentHandle(t)

	// Add enough segments to trigger cleanup and set old access time
	now := time.Now()
	oldTime := now.Add(-2 * time.Minute)

	for i := int64(1); i <= 6; i++ {
		logHandle.SegmentHandles[i] = mockSegment
		logHandle.SegmentHandleLastAccess[i] = oldTime
	}
	logHandle.WritableSegmentId = -1 // No writable segment

	// Set lastCleanupTime to trigger cleanup
	logHandle.lastCleanupTime = now.Add(-1 * time.Minute)

	// Expect Close calls that return error
	mockSegment.EXPECT().Close(mock.Anything).Return(errors.New("close error")).Times(6)

	// Call cleanup directly
	logHandle.cleanupIdleSegmentHandles(ctx, 1*time.Minute)

	// Verify cleanup results - should still remove from maps even if Close fails
	assert.Len(t, logHandle.SegmentHandles, 0, "Should have no segment handles remaining")
	assert.Len(t, logHandle.SegmentHandleLastAccess, 0, "Should have no access time records remaining")

	mockSegment.AssertExpectations(t)
}

// TestClose_ContinuesOnError tests that Close method continues processing all segments even if some fail
func TestClose_ContinuesOnError(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)
	ctx := context.Background()

	// Create mock segment handles
	mockSegment1 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegment2 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegment3 := mocks_segment_handle.NewSegmentHandle(t)

	logHandle.SegmentHandles[1] = mockSegment1
	logHandle.SegmentHandles[2] = mockSegment2
	logHandle.SegmentHandles[3] = mockSegment3

	// Mock GetId calls for logging
	mockSegment1.EXPECT().GetId(mock.Anything).Return(int64(1)).Maybe()
	mockSegment2.EXPECT().GetId(mock.Anything).Return(int64(2)).Maybe()
	mockSegment3.EXPECT().GetId(mock.Anything).Return(int64(3)).Maybe()

	// Set up expectations - segment 1 fails to fence, segment 2 fails to close, segment 3 succeeds
	mockSegment1.EXPECT().Fence(mock.Anything).Return(errors.New("fence error"))
	mockSegment1.EXPECT().Close(mock.Anything).Return(nil)

	mockSegment2.EXPECT().Fence(mock.Anything).Return(nil)
	mockSegment2.EXPECT().Close(mock.Anything).Return(errors.New("close error"))

	mockSegment3.EXPECT().Fence(mock.Anything).Return(nil)
	mockSegment3.EXPECT().Close(mock.Anything).Return(nil)

	// Call Close
	err := logHandle.Close(ctx)

	// Should return the first error encountered (fence error)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fence error")

	// Verify all segments were processed and maps were cleared
	assert.Len(t, logHandle.SegmentHandles, 0, "SegmentHandles should be cleared")
	assert.Len(t, logHandle.SegmentHandleLastAccess, 0, "SegmentHandleLastAccess should be cleared")
	assert.Equal(t, int64(-1), logHandle.WritableSegmentId, "WritableSegmentId should be reset")

	// Verify all expectations were met
	mockSegment1.AssertExpectations(t)
	mockSegment2.AssertExpectations(t)
	mockSegment3.AssertExpectations(t)
}

// TestGetExistsReadonlySegmentHandle_UpdatesAccessTime tests that access time is updated on cache hit
func TestGetExistsReadonlySegmentHandle_UpdatesAccessTime(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)
	ctx := context.Background()

	// Create mock segment handle
	mockSegment := mocks_segment_handle.NewSegmentHandle(t)

	// Set up existing segment in cache
	segmentId := int64(1)
	oldTime := time.Now().Add(-1 * time.Hour)
	logHandle.SegmentHandles[segmentId] = mockSegment
	logHandle.SegmentHandleLastAccess[segmentId] = oldTime

	// Call GetExistsReadonlySegmentHandle
	result, err := logHandle.GetExistsReadonlySegmentHandle(ctx, segmentId)

	// Assertions
	assert.NoError(t, err)
	assert.Equal(t, mockSegment, result)

	// Verify access time was updated
	newTime := logHandle.SegmentHandleLastAccess[segmentId]
	assert.True(t, newTime.After(oldTime), "Access time should be updated")
	assert.True(t, time.Since(newTime) < time.Second, "Access time should be recent")

	mockSegment.AssertExpectations(t)
}

// TestCreateAndCacheNewSegmentHandle_SetsAccessTime tests that new segment handles have access time set
func TestCreateAndCacheNewSegmentHandle_SetsAccessTime(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Mock GetNextSegmentId
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*proto.SegmentMetadata{}, nil)

	// Mock StoreSegmentMetadata
	mockMeta.EXPECT().StoreSegmentMetadata(mock.Anything, "test-log", mock.AnythingOfType("*proto.SegmentMetadata")).Return(nil)

	// Call createAndCacheNewSegmentHandle
	before := time.Now()
	handle, err := logHandle.createAndCacheWritableSegmentHandle(ctx)
	after := time.Now()

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, handle)

	// Verify segment was cached with access time
	segmentId := logHandle.WritableSegmentId
	assert.Contains(t, logHandle.SegmentHandles, segmentId, "Segment should be cached")
	assert.Contains(t, logHandle.SegmentHandleLastAccess, segmentId, "Access time should be set")

	accessTime := logHandle.SegmentHandleLastAccess[segmentId]
	assert.True(t, accessTime.After(before) || accessTime.Equal(before), "Access time should be after start")
	assert.True(t, accessTime.Before(after) || accessTime.Equal(after), "Access time should be before end")

	mockMeta.AssertExpectations(t)
}

// TestGetOrCreateWritableSegmentHandle_ErrorHandling tests error handling in GetOrCreateWritableSegmentHandle
func TestGetOrCreateWritableSegmentHandle_ErrorHandling(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Mock GetOrCreateWritableSegmentHandle failure
	logHandle.WritableSegmentId = -1 // No writable segment exists

	// Mock metadata operations for createNewSegmentMeta that will fail
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*proto.SegmentMetadata{}, nil)
	mockMeta.EXPECT().StoreSegmentMetadata(mock.Anything, "test-log", mock.AnythingOfType("*proto.SegmentMetadata")).Return(errors.New("storage error"))

	// Call GetOrCreateWritableSegmentHandle and expect error
	handle, err := logHandle.GetOrCreateWritableSegmentHandle(ctx)

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, handle)
	assert.Contains(t, err.Error(), "storage error")

	// Verify all expectations were met
	mockMeta.AssertExpectations(t)
}

// TestGetOrCreateWritableSegmentHandle_TriggersCleanup tests that cleanup is triggered in write-only scenarios
func TestGetOrCreateWritableSegmentHandle_TriggersCleanup(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Mock metadata operations for creating new segment
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*proto.SegmentMetadata{}, nil)
	mockMeta.EXPECT().StoreSegmentMetadata(mock.Anything, "test-log", mock.AnythingOfType("*proto.SegmentMetadata")).Return(nil)

	// Create mock segment handles for cleanup
	mockSegment1 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegment2 := mocks_segment_handle.NewSegmentHandle(t)

	// Set up segment handles with old access times
	now := time.Now()
	oldTime := now.Add(-2 * time.Minute) // 2 minutes ago (should be cleaned up)

	// Add enough segments to trigger cleanup threshold
	for i := int64(1); i <= 6; i++ {
		logHandle.SegmentHandles[i] = mockSegment1
		logHandle.SegmentHandleLastAccess[i] = oldTime
	}

	logHandle.WritableSegmentId = -1 // No current writable segment, will create new one

	// Set lastCleanupTime to trigger cleanup
	logHandle.lastCleanupTime = now.Add(-1 * time.Minute) // 1 minute ago

	// Expect Close calls for old segments during cleanup
	mockSegment1.EXPECT().Close(mock.Anything).Return(nil).Times(6)

	// Call GetOrCreateWritableSegmentHandle - this should trigger cleanup AND create new segment
	handle, err := logHandle.GetOrCreateWritableSegmentHandle(ctx)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, handle)

	// Verify cleanup happened - old segments should be removed, new segment should be added
	assert.Len(t, logHandle.SegmentHandles, 1, "Should have 1 segment handle remaining (the new writable one)")
	assert.Len(t, logHandle.SegmentHandleLastAccess, 1, "Should have 1 access time record remaining")

	// Verify the new writable segment is present
	assert.NotEqual(t, int64(-1), logHandle.WritableSegmentId, "WritableSegmentId should be set")
	assert.Contains(t, logHandle.SegmentHandles, logHandle.WritableSegmentId, "New writable segment should be in cache")

	// Verify all expectations were met
	mockMeta.AssertExpectations(t)
	mockSegment1.AssertExpectations(t)
	mockSegment2.AssertExpectations(t)
}

// TestGetOrCreateWritableSegmentHandle_ExistingWritableTriggersCleanup tests cleanup with existing writable segment
func TestGetOrCreateWritableSegmentHandle_ExistingWritableTriggersCleanup(t *testing.T) {
	logHandle, _ := createMockLogHandle(t)
	ctx := context.Background()

	// Create mock segment handles
	mockWritableSegment := mocks_segment_handle.NewSegmentHandle(t)
	mockOldSegment := mocks_segment_handle.NewSegmentHandle(t)

	// Set up existing writable segment
	writableSegmentId := int64(10)
	logHandle.WritableSegmentId = writableSegmentId
	logHandle.SegmentHandles[writableSegmentId] = mockWritableSegment
	logHandle.SegmentHandleLastAccess[writableSegmentId] = time.Now()

	// Add old segments that should be cleaned up
	now := time.Now()
	oldTime := now.Add(-2 * time.Minute) // 2 minutes ago (should be cleaned up)

	for i := int64(1); i <= 6; i++ {
		logHandle.SegmentHandles[i] = mockOldSegment
		logHandle.SegmentHandleLastAccess[i] = oldTime
	}

	// Set lastCleanupTime to trigger cleanup
	logHandle.lastCleanupTime = now.Add(-1 * time.Minute) // 1 minute ago

	// Mock shouldCloseAndCreateNewSegment to return false (keep existing writable segment)
	mockWritableSegment.EXPECT().GetSize(mock.Anything).Return(int64(1024)) // Small size, won't trigger rollover

	// Expect Close calls for old segments during cleanup
	mockOldSegment.EXPECT().Close(mock.Anything).Return(nil).Times(6)

	// Call GetOrCreateWritableSegmentHandle - should trigger cleanup and return existing writable segment
	handle, err := logHandle.GetOrCreateWritableSegmentHandle(ctx)

	// Assertions
	assert.NoError(t, err)
	assert.Equal(t, mockWritableSegment, handle)

	// Verify cleanup happened - old segments should be removed, writable segment should remain
	assert.Len(t, logHandle.SegmentHandles, 1, "Should have 1 segment handle remaining (the writable one)")
	assert.Len(t, logHandle.SegmentHandleLastAccess, 1, "Should have 1 access time record remaining")

	// Verify the writable segment is still present
	assert.Equal(t, writableSegmentId, logHandle.WritableSegmentId, "WritableSegmentId should remain unchanged")
	assert.Contains(t, logHandle.SegmentHandles, writableSegmentId, "Writable segment should remain in cache")

	// Verify all expectations were met
	mockWritableSegment.AssertExpectations(t)
	mockOldSegment.AssertExpectations(t)
}
