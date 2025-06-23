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
					MaxInterval: 10,
					MaxSize:     64 * 1024 * 1024,
				},
			},
		},
	}

	segments := map[int64]*proto.SegmentMetadata{}
	logHandle := NewLogHandle("test-log", 1, segments, mockMeta, nil, cfg).(*logHandleImpl)

	return logHandle, mockMeta
}

func TestOpenLogReader_ReaderResourceLeak(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Mock CreateReaderTempInfo failure
	mockMeta.EXPECT().CreateReaderTempInfo(mock.Anything, mock.AnythingOfType("string"), int64(1), int64(0), int64(0)).Return(errors.New("metadata error"))

	// Mock GetTruncatedRecordId
	mockMeta.EXPECT().GetLogMeta(mock.Anything, "test-log").Return(&proto.LogMeta{
		TruncatedSegmentId: -1,
		TruncatedEntryId:   -1,
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
	mockSegment1.EXPECT().CloseWritingAndUpdateMetaIfNecessary(mock.Anything, int64(-1)).Return(nil).Times(1)

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
	mockSegment1.EXPECT().Fence(mock.Anything).Return(-1, errors.New("fence error"))
	mockSegment1.EXPECT().CloseWritingAndUpdateMetaIfNecessary(mock.Anything, int64(-1)).Return(nil)

	mockSegment2.EXPECT().Fence(mock.Anything).Return(-1, nil)
	mockSegment2.EXPECT().CloseWritingAndUpdateMetaIfNecessary(mock.Anything, int64(-1)).Return(nil)

	// Call Close
	err := logHandle.Close(ctx)

	// Should return the first error encountered (fence error)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fence error")

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
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*proto.SegmentMetadata{}, nil)

	// Mock StoreSegmentMetadata
	mockMeta.EXPECT().StoreSegmentMetadata(mock.Anything, "test-log", mock.AnythingOfType("*proto.SegmentMetadata")).Return(nil)

	// Call createAndCacheNewSegmentHandle
	handle, err := logHandle.createAndCacheWritableSegmentHandle(ctx)

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
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*proto.SegmentMetadata{}, nil)
	mockMeta.EXPECT().StoreSegmentMetadata(mock.Anything, "test-log", mock.AnythingOfType("*proto.SegmentMetadata")).Return(errors.New("storage error"))

	// Call GetOrCreateWritableSegmentHandle and expect error
	handle, err := logHandle.GetOrCreateWritableSegmentHandle(ctx)

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, handle)
	assert.Contains(t, err.Error(), "storage error")

	mockMeta.AssertExpectations(t)
}

// TestFenceLastTwoSegments_Logic tests the fencing logic for different segment counts
func TestFenceLastTwoSegments_Logic(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Test case: empty segments - should return without error
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*proto.SegmentMetadata{}, nil)

	err := logHandle.fenceLastTwoSegments(ctx)
	assert.NoError(t, err)

	mockMeta.AssertExpectations(t)
}

// TestFenceLastTwoSegments_SegmentSelection tests correct segment selection for fencing
func TestFenceLastTwoSegments_SegmentSelection(t *testing.T) {
	logHandle, mockMeta := createMockLogHandle(t)
	ctx := context.Background()

	// Test case: empty segments - should return without error
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "test-log").Return(map[int64]*proto.SegmentMetadata{}, nil)

	err := logHandle.fenceLastTwoSegments(ctx)
	assert.NoError(t, err)

	mockMeta.AssertExpectations(t)
}
