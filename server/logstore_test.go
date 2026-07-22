// Copyright (C) 2025 Zilliz. All rights reserved.
//
// This file is part of the Woodpecker project.
//
// Woodpecker is dual-licensed under the GNU Affero General Public License v3.0
// (AGPLv3) and the Server Side Public License v1 (SSPLv1). You may use this
// file under either license, at your option.
//
// AGPLv3 License: https://www.gnu.org/licenses/agpl-3.0.html
// SSPLv1 License: https://www.mongodb.com/licensing/server-side-public-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the license texts for specific language governing permissions and
// limitations under the licenses.

package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	"github.com/zilliztech/woodpecker/mocks/mocks_server/mocks_segment"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/processor"
)

const (
	testBucketName = "test-bucket"
	testRootPath   = "test-root"
	testLogId      = int64(1)
)

func createTestLogStore() *logStore {
	cfg, _ := config.NewConfiguration()
	ctx, cancel := context.WithCancel(context.Background())

	store := &logStore{
		cfg:               cfg,
		ctx:               ctx,
		cancel:            cancel,
		segmentProcessors: make(map[string]map[int64]processor.SegmentProcessor),
		deletingLogs:      make(map[string]struct{}),
		deletingInstances: make(map[string]struct{}),
		maintenance:       NewNodeMaintenanceManager(ctx),
	}
	store.stopped.Store(true) // Match production: NewLogStore starts in stopped state
	return store
}

func TestLogStore_SegmentProcessorCleanup_Stop(t *testing.T) {
	store := createTestLogStore()

	// Create mock processors
	mockProcessor1 := mocks_segment.NewSegmentProcessor(t)
	mockProcessor2 := mocks_segment.NewSegmentProcessor(t)
	mockProcessor3 := mocks_segment.NewSegmentProcessor(t)

	// Set up expectations for Close calls and GetLogId
	mockProcessor1.EXPECT().Close(mock.Anything).Return(nil).Once()
	mockProcessor1.EXPECT().GetLogId().Return(int64(1)).Times(2)
	mockProcessor2.EXPECT().Close(mock.Anything).Return(nil).Once()
	mockProcessor2.EXPECT().GetLogId().Return(int64(1)).Times(2)
	mockProcessor3.EXPECT().Close(mock.Anything).Return(nil).Once()
	mockProcessor3.EXPECT().GetLogId().Return(int64(2)).Times(2)

	// Add processors to store
	logKey1 := GetLogKey(testBucketName, testRootPath, 1)
	logKey2 := GetLogKey(testBucketName, testRootPath, 2)

	store.segmentProcessors[logKey1] = map[int64]processor.SegmentProcessor{
		10: mockProcessor1,
		20: mockProcessor2,
	}
	store.segmentProcessors[logKey2] = map[int64]processor.SegmentProcessor{
		30: mockProcessor3,
	}

	// Call Stop
	err := store.Stop()

	// Verify
	assert.NoError(t, err)
	assert.Empty(t, store.segmentProcessors)

	// Verify all expectations
	mockProcessor1.AssertExpectations(t)
	mockProcessor2.AssertExpectations(t)
	mockProcessor3.AssertExpectations(t)
}

func TestLogStore_SegmentProcessorCleanup_IdleCleanup(t *testing.T) {
	store := createTestLogStore()

	// Create 5 mock processors to test the simplified cleanup logic
	// Segments 1,2,3,4 should be cleaned if idle (not highest segment ID)
	// Segment 5 should be protected (highest segment ID)
	mockProcessors := make(map[int64]*mocks_segment.SegmentProcessor)
	for i := int64(1); i <= 5; i++ {
		mockProcessors[i] = mocks_segment.NewSegmentProcessor(t)
	}

	// Set up expectations - processors 1,2,3 should be closed (idle)
	// Processor 4 is recent, so won't be closed
	// Processor 5 is protected (highest segment ID)
	for i := int64(1); i <= 3; i++ {
		mockProcessors[i].EXPECT().Close(mock.Anything).Return(nil).Once()
		mockProcessors[i].EXPECT().GetLogId().Return(testLogId).Times(2)
	}

	// Add processors with different access times
	now := time.Now()
	oldTime := now.Add(-6 * time.Minute)    // Older than maxIdleTime (5 minutes)
	recentTime := now.Add(-1 * time.Minute) // Recent access

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = make(map[int64]processor.SegmentProcessor)

	for i := int64(1); i <= 5; i++ {
		store.segmentProcessors[logKey][i] = mockProcessors[i]

		if i == 4 { // Segment 4 is recently accessed (not idle)
			mockProcessors[i].EXPECT().GetLastAccessTime().Return(recentTime.UnixMilli()).Once()
		} else if i < 5 { // Segments 1,2,3 are idle
			mockProcessors[i].EXPECT().GetLastAccessTime().Return(oldTime.UnixMilli()).Once()
		}
		// Segment 5 (highest) won't have GetLastAccessTime called since it's protected
	}

	// Call cleanup with 5 minute max idle time
	idleProcessors := store.collectIdleSegmentProcessorsUnsafe(context.Background(), 5*time.Minute)
	for _, item := range idleProcessors {
		store.closeSegmentProcessor(context.Background(), item.logKey, item.segmentId, item.processor)
	}

	// Verify that segments 1,2,3 were removed (idle)
	for i := int64(1); i <= 3; i++ {
		assert.NotContains(t, store.segmentProcessors[logKey], i, "Segment %d should be cleaned up (idle)", i)
	}

	// Verify that segment 4 remains (not idle)
	assert.Contains(t, store.segmentProcessors[logKey], int64(4), "Segment 4 should remain (not idle)")

	// Verify that segment 5 remains (highest segment ID, protected)
	assert.Contains(t, store.segmentProcessors[logKey], int64(5), "Segment 5 should be protected (highest segment ID)")

	// Verify expectations for closed processors
	for i := int64(1); i <= 3; i++ {
		mockProcessors[i].AssertExpectations(t)
	}
}

func TestLogStore_SegmentProcessorCleanup_ThresholdConditions(t *testing.T) {
	store := createTestLogStore()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)

	// Test case 1: Not enough processors to trigger cleanup
	mockProcessor1 := mocks_segment.NewSegmentProcessor(t)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		10: mockProcessor1,
	}

	// Should trigger cleanup but processor 10 is the highest segment ID so should be protected
	store.performBackgroundCleanup(5 * time.Minute)
	assert.Contains(t, store.segmentProcessors[logKey], int64(10)) // Processor should remain (protected as highest segment ID)

	// Test case 2: Enough processors to trigger cleanup
	mockProcessors := make(map[int64]*mocks_segment.SegmentProcessor)
	recentTime := time.Now().Add(-1 * time.Minute) // Recent access time

	for i := int64(1); i <= 15; i++ {
		if store.segmentProcessors[logKey] == nil {
			store.segmentProcessors[logKey] = make(map[int64]processor.SegmentProcessor)
		}
		mockProcessors[i] = mocks_segment.NewSegmentProcessor(t)
		if i < 15 {
			// Segments 1-14 should have GetLastAccessTime called (not protected)
			mockProcessors[i].EXPECT().GetLastAccessTime().Return(recentTime.UnixMilli()).Once()
		}
		// Segment 15 is protected (highest), so GetLastAccessTime won't be called
		store.segmentProcessors[logKey][i] = mockProcessors[i]
	}

	// Should trigger cleanup but not clean anything (all processors are recent, and 15 is highest segment ID)
	store.performBackgroundCleanup(5 * time.Minute)
	assert.Len(t, store.segmentProcessors[logKey], 15) // All processors should remain
}

func TestLogStore_RemoveSegmentProcessor(t *testing.T) {
	store := createTestLogStore()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)

	// Create mock processors
	mockProcessor1 := mocks_segment.NewSegmentProcessor(t)
	mockProcessor2 := mocks_segment.NewSegmentProcessor(t)

	// Set up expectations
	mockProcessor1.EXPECT().Close(mock.Anything).Return(nil).Once()
	mockProcessor1.EXPECT().GetLogId().Return(testLogId).Times(2)

	// Add processors
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		10: mockProcessor1,
		20: mockProcessor2,
	}

	// Remove specific processor
	store.RemoveSegmentProcessor(context.Background(), testBucketName, testRootPath, testLogId, 10)

	// Verify processor is removed
	assert.NotContains(t, store.segmentProcessors[logKey], int64(10))
	assert.Contains(t, store.segmentProcessors[logKey], int64(20)) // Other processor remains

	// Verify expectations
	mockProcessor1.AssertExpectations(t)
}

func TestLogStore_RemoveSegmentProcessor_LastProcessor(t *testing.T) {
	store := createTestLogStore()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)

	// Create mock processor
	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().Close(mock.Anything).Return(nil).Once()
	mockProcessor.EXPECT().GetLogId().Return(testLogId).Times(2)

	// Add single processor
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		10: mockProcessor,
	}

	// Remove the only processor
	store.RemoveSegmentProcessor(context.Background(), testBucketName, testRootPath, testLogId, 10)

	// Verify entire log entry is removed
	assert.NotContains(t, store.segmentProcessors, logKey)

	// Verify expectations
	mockProcessor.AssertExpectations(t)
}

func TestLogStore_GetExistsSegmentProcessor_UpdatesAccessTime(t *testing.T) {
	store := createTestLogStore()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)

	// Create mock processor
	mockProcessor := mocks_segment.NewSegmentProcessor(t)

	// Add existing processor
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		10: mockProcessor,
	}

	// Get existing processor
	processor := store.getExistsSegmentProcessor(testBucketName, testRootPath, testLogId, 10)

	// Verify
	assert.Equal(t, mockProcessor, processor)
	// Note: Access time is now managed internally by the processor
}

func TestLogStore_GetExistsSegmentProcessor_NotFound(t *testing.T) {
	store := createTestLogStore()

	// Try to get non-existent processor
	processor := store.getExistsSegmentProcessor(testBucketName, testRootPath, testLogId, 10)

	// Verify
	assert.Nil(t, processor)
}

func TestLogStore_GetTotalProcessorCountUnsafe(t *testing.T) {
	store := createTestLogStore()

	logKey1 := GetLogKey(testBucketName, testRootPath, 1)
	logKey2 := GetLogKey(testBucketName, testRootPath, 2)

	// Add processors
	store.segmentProcessors[logKey1] = map[int64]processor.SegmentProcessor{
		10: mocks_segment.NewSegmentProcessor(t),
		20: mocks_segment.NewSegmentProcessor(t),
	}
	store.segmentProcessors[logKey2] = map[int64]processor.SegmentProcessor{
		30: mocks_segment.NewSegmentProcessor(t),
	}

	// Verify count
	count := store.getTotalProcessorCountUnsafe()
	assert.Equal(t, 3, count)
}

func TestLogStore_CloseSegmentProcessorUnsafe_CloseError(t *testing.T) {
	store := createTestLogStore()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)

	// Create mock processor that returns error on close
	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().Close(mock.Anything).Return(assert.AnError).Once()
	mockProcessor.EXPECT().GetLogId().Return(testLogId).Times(2)

	// Should not panic even if close fails
	store.closeSegmentProcessor(context.Background(), logKey, 10, mockProcessor)

	// Verify expectations
	mockProcessor.AssertExpectations(t)
}

func TestLogStore_SegmentProcessorCleanup_ProtectLatestSegments(t *testing.T) {
	store := createTestLogStore()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)

	// Create 5 mock processors to test protection logic
	// All have old access times, but segment 5 should be protected (highest segment ID)
	// Segments 1,2,3,4 should be cleaned (all idle and not highest)
	mockProcessors := make(map[int64]*mocks_segment.SegmentProcessor)
	for i := int64(1); i <= 5; i++ {
		mockProcessors[i] = mocks_segment.NewSegmentProcessor(t)
	}

	// Set up expectations - processors 1,2,3,4 should be closed
	// Processor 5 will be protected (highest segment ID)
	for i := int64(1); i <= 4; i++ {
		mockProcessors[i].EXPECT().Close(mock.Anything).Return(nil).Once()
		mockProcessors[i].EXPECT().GetLogId().Return(testLogId).Times(2)
	}

	// Add processors with all being idle (old access time)
	now := time.Now()
	oldTime := now.Add(-10 * time.Minute) // All are idle

	store.segmentProcessors[logKey] = make(map[int64]processor.SegmentProcessor)

	for i := int64(1); i <= 5; i++ {
		store.segmentProcessors[logKey][i] = mockProcessors[i]
		// Only processors 1-4 will have GetLastAccessTime called (segment 5 is protected)
		if i < 5 {
			mockProcessors[i].EXPECT().GetLastAccessTime().Return(oldTime.UnixMilli()).Once()
		}
	}

	// Call cleanup
	idleProcessors := store.collectIdleSegmentProcessorsUnsafe(context.Background(), 5*time.Minute)
	for _, item := range idleProcessors {
		store.closeSegmentProcessor(context.Background(), item.logKey, item.segmentId, item.processor)
	}

	// Verify that segments 1,2,3,4 were removed
	for i := int64(1); i <= 4; i++ {
		assert.NotContains(t, store.segmentProcessors[logKey], i, "Segment %d should be removed", i)
	}

	// Verify that segment 5 remains (protected: highest segment ID)
	assert.Contains(t, store.segmentProcessors[logKey], int64(5), "Segment 5 should be protected (highest segment ID)")

	// Verify expectations for closed processors
	for i := int64(1); i <= 4; i++ {
		mockProcessors[i].AssertExpectations(t)
	}
}

func TestLogStore_BackgroundCleanup_StartStop(t *testing.T) {
	store := createTestLogStore()

	// Start via the manager lifecycle (same path as production Start())
	err := store.Start()
	assert.NoError(t, err)
	assert.False(t, store.stopped.Load())

	// Stop via the manager lifecycle — must not hang or panic
	err = store.Stop()
	assert.NoError(t, err)
	assert.True(t, store.stopped.Load())
}

// === LogStore core method tests ===

func TestLogStore_NewLogStore(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	ctx := context.Background()
	store := NewLogStore(ctx, cfg, nil)
	assert.NotNil(t, store)
	ls := store.(*logStore)
	assert.NotEmpty(t, ls.address)
	assert.True(t, ls.stopped.Load())
}

func TestLogStore_StartStop(t *testing.T) {
	store := createTestLogStore()
	err := store.Start()
	assert.NoError(t, err)
	assert.False(t, store.stopped.Load())

	err = store.Stop()
	assert.NoError(t, err)
	assert.True(t, store.stopped.Load())
}

func TestLogStore_SetGetAddress(t *testing.T) {
	store := createTestLogStore()
	store.SetAddress("10.0.0.1:8080")
	assert.Equal(t, "10.0.0.1:8080", store.GetAddress())
}

func TestLogStore_AddEntry_Stopped(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(true)

	_, err := store.AddEntry(context.Background(), testBucketName, testRootPath, testLogId, &proto.LogEntry{SegId: 0, EntryId: 0}, nil)
	assert.ErrorIs(t, err, werr.ErrLogStoreShutdown)
}

func TestLogStore_AddEntry_Success(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().AddEntry(mock.Anything, mock.Anything, mock.Anything).Return(int64(0), nil).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	entry := &proto.LogEntry{SegId: 0, EntryId: 0, Values: []byte("hello")}
	entryId, err := store.AddEntry(context.Background(), testBucketName, testRootPath, testLogId, entry, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), entryId)
}

func TestLogStore_AddEntry_ProcessorError(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().AddEntry(mock.Anything, mock.Anything, mock.Anything).Return(int64(-1), assert.AnError).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	entry := &proto.LogEntry{SegId: 0, EntryId: 0}
	_, err := store.AddEntry(context.Background(), testBucketName, testRootPath, testLogId, entry, nil)
	assert.Error(t, err)
}

func TestLogStore_GetBatchEntriesAdv_Stopped(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(true)

	_, err := store.GetBatchEntriesAdv(context.Background(), testBucketName, testRootPath, testLogId, 0, 0, 10, nil)
	assert.ErrorIs(t, err, werr.ErrLogStoreShutdown)
}

func TestLogStore_GetBatchEntriesAdv_Success(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	expectedResult := &proto.BatchReadResult{
		Entries: []*proto.LogEntry{{EntryId: 0, Values: []byte("data")}},
	}
	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().ReadBatchEntriesAdv(mock.Anything, int64(0), int64(10), mock.Anything).Return(expectedResult, nil).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	result, err := store.GetBatchEntriesAdv(context.Background(), testBucketName, testRootPath, testLogId, 0, 0, 10, nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Entries, 1)
}

func TestLogStore_GetBatchEntriesAdv_Error(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().ReadBatchEntriesAdv(mock.Anything, int64(0), int64(10), mock.Anything).Return(nil, assert.AnError).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	_, err := store.GetBatchEntriesAdv(context.Background(), testBucketName, testRootPath, testLogId, 0, 0, 10, nil)
	assert.Error(t, err)
}

func TestLogStore_CompleteSegment_Stopped(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(true)

	_, err := store.CompleteSegment(context.Background(), testBucketName, testRootPath, testLogId, 0, 5)
	assert.ErrorIs(t, err, werr.ErrLogStoreShutdown)
}

func TestLogStore_CompleteSegment_Success(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().Complete(mock.Anything, int64(5)).Return(int64(5), nil).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	lac, err := store.CompleteSegment(context.Background(), testBucketName, testRootPath, testLogId, 0, 5)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), lac)
}

func TestLogStore_FenceSegment_Stopped(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(true)

	_, err := store.FenceSegment(context.Background(), testBucketName, testRootPath, testLogId, 0)
	assert.ErrorIs(t, err, werr.ErrLogStoreShutdown)
}

func TestLogStore_FenceSegment_Success(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().Fence(mock.Anything).Return(int64(10), nil).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	lastEntry, err := store.FenceSegment(context.Background(), testBucketName, testRootPath, testLogId, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), lastEntry)
}

func TestLogStore_FenceSegment_Error(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().Fence(mock.Anything).Return(int64(-1), assert.AnError).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	_, err := store.FenceSegment(context.Background(), testBucketName, testRootPath, testLogId, 0)
	assert.Error(t, err)
}

func TestLogStore_GetSegmentLastAddConfirmed_Stopped(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(true)

	_, err := store.GetSegmentLastAddConfirmed(context.Background(), testBucketName, testRootPath, testLogId, 0)
	assert.ErrorIs(t, err, werr.ErrLogStoreShutdown)
}

func TestLogStore_GetSegmentLastAddConfirmed_Success(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().GetSegmentLastAddConfirmed(mock.Anything).Return(int64(42), nil).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	lac, err := store.GetSegmentLastAddConfirmed(context.Background(), testBucketName, testRootPath, testLogId, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(42), lac)
}

func TestLogStore_GetSegmentBlockCount_Stopped(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(true)

	_, err := store.GetSegmentBlockCount(context.Background(), testBucketName, testRootPath, testLogId, 0)
	assert.ErrorIs(t, err, werr.ErrLogStoreShutdown)
}

func TestLogStore_GetSegmentBlockCount_Success(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().GetBlocksCount(mock.Anything).Return(int64(5), nil).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	count, err := store.GetSegmentBlockCount(context.Background(), testBucketName, testRootPath, testLogId, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), count)
}

func TestLogStore_GetSegmentBlockCount_NoWriter(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().GetBlocksCount(mock.Anything).Return(int64(0), werr.ErrSegmentProcessorNoWriter).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	count, err := store.GetSegmentBlockCount(context.Background(), testBucketName, testRootPath, testLogId, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestLogStore_CompactSegment_Stopped(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(true)

	_, err := store.CompactSegment(context.Background(), testBucketName, testRootPath, testLogId, 0)
	assert.ErrorIs(t, err, werr.ErrLogStoreShutdown)
}

func TestLogStore_CompactSegment_Success(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	expectedMeta := &proto.SegmentMetadata{SegNo: 0}
	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().Compact(mock.Anything).Return(expectedMeta, nil).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	meta, err := store.CompactSegment(context.Background(), testBucketName, testRootPath, testLogId, 0)
	assert.NoError(t, err)
	assert.Equal(t, expectedMeta, meta)
}

func TestLogStore_CleanSegment_Stopped(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(true)

	err := store.CleanSegment(context.Background(), testBucketName, testRootPath, testLogId, 0, 0)
	assert.ErrorIs(t, err, werr.ErrLogStoreShutdown)
}

func TestLogStore_CleanSegment_Success(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().Clean(mock.Anything, 0).Return(nil).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	err := store.CleanSegment(context.Background(), testBucketName, testRootPath, testLogId, 0, 0)
	assert.NoError(t, err)
}

func TestLogStore_NotifySegmentCompacted_Stopped(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(true)

	err := store.NotifySegmentCompacted(context.Background(), testBucketName, testRootPath, testLogId, 0)
	assert.ErrorIs(t, err, werr.ErrLogStoreShutdown)
}

func TestNotifySegmentCompacted_WritesMarkIdempotent(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	store.cfg.Woodpecker.Storage.RootPath = t.TempDir()
	store.cfg.Woodpecker.Storage.Type = "service"

	// The handler verifies the compacted footer before writing the mark ("mark ⇒ footer
	// exists" is enforced server-side, not on the caller's word). Each notify HEADs once.
	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	mockStorage.EXPECT().StatObject(mock.Anything, "b", "rp/1/2/footer.blk", "b/rp", "1").
		Return(int64(128), false, nil).Twice()
	store.storageClient = mockStorage

	ctx := context.Background()
	require.NoError(t, store.NotifySegmentCompacted(ctx, "b", "rp", 1, 2))
	seg := localSegmentDataDir(store.cfg, "b", "rp", 1, 2)
	assert.True(t, hasCompactedMark(seg))

	require.NoError(t, store.NotifySegmentCompacted(ctx, "b", "rp", 1, 2)) // idempotent
	assert.True(t, hasCompactedMark(seg))
}

// TestNotifySegmentCompacted_FooterAbsentRefusesMark pins the handler-side invariant: a
// notify for a segment whose compacted footer is NOT in object storage (stale or mis-routed
// retry, external caller of the public RPC, or a notify arriving after the truncate GC
// already removed the segment's objects) must be rejected — no mark written, no directory
// fabricated — so a bare RPC can never make decommission report data as drained, and a late
// notify can never resurrect a truncate-reaped segment dir.
func TestNotifySegmentCompacted_FooterAbsentRefusesMark(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	store.cfg.Woodpecker.Storage.RootPath = t.TempDir()
	store.cfg.Woodpecker.Storage.Type = "service"

	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	mockStorage.EXPECT().StatObject(mock.Anything, "b", "rp/1/2/footer.blk", "b/rp", "1").
		Return(int64(0), false, minio.ErrorResponse{Code: "NoSuchKey"}).Once()
	store.storageClient = mockStorage

	err := store.NotifySegmentCompacted(context.Background(), "b", "rp", 1, 2)
	assert.ErrorIs(t, err, werr.ErrSegmentNotFound)

	seg := localSegmentDataDir(store.cfg, "b", "rp", 1, 2)
	assert.False(t, hasCompactedMark(seg), "no mark may be written without a confirmed footer")
	_, statErr := os.Stat(seg)
	assert.True(t, os.IsNotExist(statErr), "no segment dir may be fabricated without a confirmed footer")
}

// TestNotifySegmentCompacted_FooterHeadTransientErrorPropagates: a transport-level StatObject
// failure (not NoSuchKey) must surface as an error so the client retries, rather than being
// treated as either "exists" or "absent".
func TestNotifySegmentCompacted_FooterHeadTransientErrorPropagates(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	store.cfg.Woodpecker.Storage.RootPath = t.TempDir()
	store.cfg.Woodpecker.Storage.Type = "service"

	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	mockStorage.EXPECT().StatObject(mock.Anything, "b", "rp/1/2/footer.blk", "b/rp", "1").
		Return(int64(0), false, fmt.Errorf("connection refused")).Once()
	store.storageClient = mockStorage

	err := store.NotifySegmentCompacted(context.Background(), "b", "rp", 1, 2)
	assert.Error(t, err)
	seg := localSegmentDataDir(store.cfg, "b", "rp", 1, 2)
	assert.False(t, hasCompactedMark(seg))
}

// TestNotifySegmentCompacted_NonCanonicalRootPathRejected pins the RPC-boundary validation:
// rootPath is caller-managed in service mode, so the local config validation cannot vouch
// for it. A non-canonical value must be rejected before anything consumes it — no footer
// HEAD (the strict mock has no StatObject expectation), no mark, no fabricated directory —
// otherwise the push path (raw value) and the pull reconcile (disk-derived, canonicalized)
// would silently split keys and metric series.
func TestNotifySegmentCompacted_NonCanonicalRootPathRejected(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	store.cfg.Woodpecker.Storage.RootPath = t.TempDir()
	store.cfg.Woodpecker.Storage.Type = "service"
	store.storageClient = mocks_objectstorage.NewObjectStorage(t)

	err := store.NotifySegmentCompacted(context.Background(), "b", "/rp//x/", 1, 2)
	assert.ErrorIs(t, err, werr.ErrLogStoreInvalidRootPath)
}

// TestNotifySegmentCompacted_NilStorageClientErrors: without an object-storage client the
// footer cannot be verified, so the handler must refuse rather than trust the caller.
func TestNotifySegmentCompacted_NilStorageClientErrors(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	store.cfg.Woodpecker.Storage.RootPath = t.TempDir()
	store.cfg.Woodpecker.Storage.Type = "service"
	store.storageClient = nil

	err := store.NotifySegmentCompacted(context.Background(), "b", "rp", 1, 2)
	assert.ErrorIs(t, err, werr.ErrInternalError)
}

func TestLogStore_NotifySegmentCompacted_NoLocalDataDir(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	store.cfg.Woodpecker.Storage.RootPath = t.TempDir()
	store.cfg.Woodpecker.Storage.Type = "minio" // pure object-storage mode: no local data dir

	err := store.NotifySegmentCompacted(context.Background(), testBucketName, testRootPath, testLogId, 0)
	assert.NoError(t, err)
}

// TestLogStore_NotifySegmentCompacted_LocalModeIsNoOp verifies the storage-mode gate: local
// storage has a node-local data dir but no object-storage footer to reclaim against, and its
// cleanup task never runs (it gates on service mode), so the handler must NOT write a mark
// (which would otherwise linger, and — in local mode — enqueue into a never-drained queue).
func TestLogStore_NotifySegmentCompacted_LocalModeIsNoOp(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	store.cfg.Woodpecker.Storage.RootPath = t.TempDir()
	store.cfg.Woodpecker.Storage.Type = "local" // local storage: localSegmentDataDir is non-empty

	require.NoError(t, store.NotifySegmentCompacted(context.Background(), "b", "rp", 1, 2))
	seg := localSegmentDataDir(store.cfg, "b", "rp", 1, 2)
	require.NotEmpty(t, seg, "precondition: local mode has a non-empty segment dir")
	assert.False(t, hasCompactedMark(seg), "local mode must not write a compacted mark")
}

func TestLogStore_UpdateLastAddConfirmed_Stopped(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(true)

	err := store.UpdateLastAddConfirmed(context.Background(), testBucketName, testRootPath, testLogId, 0, 10)
	assert.ErrorIs(t, err, werr.ErrLogStoreShutdown)
}

func TestLogStore_UpdateLastAddConfirmed_Success(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().UpdateSegmentLastAddConfirmed(mock.Anything, int64(10)).Return(nil).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	err := store.UpdateLastAddConfirmed(context.Background(), testBucketName, testRootPath, testLogId, 0, 10)
	assert.NoError(t, err)
}

func TestLogStore_GetOrCreateSegmentProcessor(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	// First call should create a new processor
	sp, err := store.getOrCreateSegmentProcessor(context.Background(), testBucketName, testRootPath, testLogId, 0)
	assert.NoError(t, err)
	assert.NotNil(t, sp)

	// Second call should return the same processor (cached)
	sp2, err := store.getOrCreateSegmentProcessor(context.Background(), testBucketName, testRootPath, testLogId, 0)
	assert.NoError(t, err)
	assert.Equal(t, sp, sp2)
}

func TestLogStore_GetLogKey(t *testing.T) {
	assert.Equal(t, "bucket/root/1", GetLogKey("bucket", "root", 1))
	assert.Equal(t, "b/r/0", GetLogKey("b", "r", 0))
}

// === Additional error path tests to improve coverage ===

func TestLogStore_CompleteSegment_Error(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().Complete(mock.Anything, int64(5)).Return(int64(-1), assert.AnError).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	_, err := store.CompleteSegment(context.Background(), testBucketName, testRootPath, testLogId, 0, 5)
	assert.Error(t, err)
}

func TestLogStore_GetSegmentLastAddConfirmed_Error(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().GetSegmentLastAddConfirmed(mock.Anything).Return(int64(-1), assert.AnError).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	_, err := store.GetSegmentLastAddConfirmed(context.Background(), testBucketName, testRootPath, testLogId, 0)
	assert.Error(t, err)
}

func TestLogStore_GetSegmentBlockCount_Error(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().GetBlocksCount(mock.Anything).Return(int64(-1), assert.AnError).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	_, err := store.GetSegmentBlockCount(context.Background(), testBucketName, testRootPath, testLogId, 0)
	assert.Error(t, err)
}

func TestLogStore_CompactSegment_Error(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().Compact(mock.Anything).Return(nil, assert.AnError).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	_, err := store.CompactSegment(context.Background(), testBucketName, testRootPath, testLogId, 0)
	assert.Error(t, err)
}

func TestLogStore_CleanSegment_Error(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().Clean(mock.Anything, 1).Return(assert.AnError).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	err := store.CleanSegment(context.Background(), testBucketName, testRootPath, testLogId, 0, 1)
	assert.Error(t, err)
}

func TestLogStore_UpdateLastAddConfirmed_Error(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().UpdateSegmentLastAddConfirmed(mock.Anything, int64(10)).Return(assert.AnError).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	err := store.UpdateLastAddConfirmed(context.Background(), testBucketName, testRootPath, testLogId, 0, 10)
	assert.Error(t, err)
}

func TestLogStore_RemoveSegmentProcessor_SegmentNotFound(t *testing.T) {
	store := createTestLogStore()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		10: mockProcessor,
	}

	// Remove a non-existent segment ID - should not panic
	store.RemoveSegmentProcessor(context.Background(), testBucketName, testRootPath, testLogId, 999)

	// Verify existing processor still there
	assert.Contains(t, store.segmentProcessors[logKey], int64(10))
}

func TestLogStore_RemoveSegmentProcessor_LogNotFound(t *testing.T) {
	store := createTestLogStore()

	// Remove from a non-existent log - should not panic
	store.RemoveSegmentProcessor(context.Background(), "nonexistent-bucket", "nonexistent-path", 999, 0)

	// Verify no crash
	assert.Empty(t, store.segmentProcessors)
}

func TestLogStore_GetBatchEntriesAdv_ErrorKinds(t *testing.T) {
	// Test with ErrEntryNotFound
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().ReadBatchEntriesAdv(mock.Anything, int64(0), int64(10), mock.Anything).Return(nil, werr.ErrEntryNotFound).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	_, err := store.GetBatchEntriesAdv(context.Background(), testBucketName, testRootPath, testLogId, 0, 0, 10, nil)
	assert.Error(t, err)
	assert.True(t, werr.ErrEntryNotFound.Is(err))
}

func TestLogStore_GetBatchEntriesAdv_ErrorEOF(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().ReadBatchEntriesAdv(mock.Anything, int64(0), int64(10), mock.Anything).Return(nil, werr.ErrFileReaderEndOfFile).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{
		0: mockProcessor,
	}

	_, err := store.GetBatchEntriesAdv(context.Background(), testBucketName, testRootPath, testLogId, 0, 0, 10, nil)
	assert.Error(t, err)
}

func TestLogStore_StopAlreadyStopped(t *testing.T) {
	store := createTestLogStore()
	// Store is already stopped by default
	assert.True(t, store.stopped.Load())

	// Start then stop normally
	store.Start()
	assert.False(t, store.stopped.Load())

	err := store.Stop()
	assert.NoError(t, err)
	assert.True(t, store.stopped.Load())

	// Calling Stop again should not panic (already stopped) — maintenance cancel is idempotent
	err = store.Stop()
	assert.NoError(t, err)
}

func TestLogStore_CollectIdleSegmentProcessors_NoIdle(t *testing.T) {
	store := createTestLogStore()

	// Empty store - should return nil
	result := store.collectIdleSegmentProcessorsUnsafe(context.Background(), 5*time.Minute)
	assert.Nil(t, result)
}

func TestLogStore_BackgroundCleanup_PerformCleanup(t *testing.T) {
	store := createTestLogStore()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)

	// Create 5 mock processors for cleanup testing
	// Segments 1,2,3,4 should be cleaned (all idle), segment 5 should be protected (highest)
	mockProcessors := make(map[int64]*mocks_segment.SegmentProcessor)
	for i := int64(1); i <= 5; i++ {
		mockProcessors[i] = mocks_segment.NewSegmentProcessor(t)
	}

	// Set up expectations - processors 1,2,3,4 should be closed
	// Processor 5 will be protected (highest segment ID)
	for i := int64(1); i <= 4; i++ {
		mockProcessors[i].EXPECT().Close(mock.Anything).Return(nil).Once()
		mockProcessors[i].EXPECT().GetLogId().Return(testLogId).Times(2)
	}

	// Add processors with old access times
	now := time.Now()
	oldTime := now.Add(-10 * time.Minute) // All are idle

	store.segmentProcessors[logKey] = make(map[int64]processor.SegmentProcessor)

	for i := int64(1); i <= 5; i++ {
		store.segmentProcessors[logKey][i] = mockProcessors[i]
		// Only processors 1-4 will have GetLastAccessTime called (segment 5 is protected)
		if i < 5 {
			mockProcessors[i].EXPECT().GetLastAccessTime().Return(oldTime.UnixMilli()).Once()
		}
	}

	// Perform background cleanup (no threshold check anymore)
	store.performBackgroundCleanup(5 * time.Minute)

	// Verify that segments 1,2,3,4 were removed
	for i := int64(1); i <= 4; i++ {
		assert.NotContains(t, store.segmentProcessors[logKey], i, "Segment %d should be removed", i)
	}

	// Verify that segment 5 remains (protected)
	assert.Contains(t, store.segmentProcessors[logKey], int64(5), "Segment 5 should be protected (highest segment ID)")

	// Verify expectations for closed processors
	for i := int64(1); i <= 4; i++ {
		mockProcessors[i].AssertExpectations(t)
	}
}

func TestLogStore_GetActiveProcessorCount(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	ctx := context.Background()
	ls := NewLogStore(ctx, cfg, nil)
	count := ls.GetActiveProcessorCount()
	assert.Equal(t, 0, count)
}

func TestLogStore_RejectNewWrites(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	ctx := context.Background()
	ls := NewLogStore(ctx, cfg, nil)
	concrete := ls.(*logStore)

	// Before reject: rejectWrites should be false
	assert.False(t, concrete.rejectWrites.Load())

	ls.RejectNewWrites()

	// After reject: rejectWrites should be true, but stopped should still be true
	// (stopped is true by default until Start() is called)
	assert.True(t, concrete.rejectWrites.Load())
}

func TestLogStore_HasLocalSegmentData_Empty(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Woodpecker.Storage.RootPath = t.TempDir()
	ctx := context.Background()
	ls := NewLogStore(ctx, cfg, nil)

	// Empty data dir — no segment data
	assert.False(t, ls.HasLocalSegmentData())
}

func TestLogStore_HasLocalSegmentData_WithDataLog(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	dir := t.TempDir()
	cfg.Woodpecker.Storage.RootPath = dir
	ctx := context.Background()
	ls := NewLogStore(ctx, cfg, nil)

	// Create a fake segment data file: <rootPath>/1/0/data.log
	segDir := filepath.Join(dir, "1", "0")
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(segDir, "data.log"), []byte("some data"), 0o644))

	assert.True(t, ls.HasLocalSegmentData())
}

func TestLogStore_HasLocalSegmentData_OnlyManagementFiles(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	dir := t.TempDir()
	cfg.Woodpecker.Storage.RootPath = dir
	ctx := context.Background()
	ls := NewLogStore(ctx, cfg, nil)

	// Only node_state.json and write.fence — no actual segment data
	require.NoError(t, os.WriteFile(filepath.Join(dir, "node_state.json"), []byte("{}"), 0o644))
	segDir := filepath.Join(dir, "1", "0")
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(segDir, "write.fence"), []byte(""), 0o644))

	assert.False(t, ls.HasLocalSegmentData())
}

func TestLogStore_HasLocalSegmentData_MarkedSegmentIgnored(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	dir := t.TempDir()
	cfg.Woodpecker.Storage.RootPath = dir
	ctx := context.Background()
	ls := NewLogStore(ctx, cfg, nil)

	// Segment has a non-empty data.log AND a data.compacted — its data.log is
	// redundant (durably compacted, awaiting physical GC) and must not block
	// decommission.
	segDir := filepath.Join(dir, "1", "0")
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(segDir, "data.log"), []byte("staged data"), 0o644))
	require.NoError(t, writeCompactedMark(ctx, segDir))

	assert.False(t, ls.HasLocalSegmentData())
}

func TestLogStore_HasLocalSegmentData_UnmarkedSegmentBlocks(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	dir := t.TempDir()
	cfg.Woodpecker.Storage.RootPath = dir
	ctx := context.Background()
	ls := NewLogStore(ctx, cfg, nil)

	// Non-empty data.log with no data.compacted still blocks decommission.
	segDir := filepath.Join(dir, "1", "0")
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(segDir, "data.log"), []byte("staged data"), 0o644))

	assert.True(t, ls.HasLocalSegmentData())
}

func TestLogStore_HasLocalSegmentData_MixedMarkedAndUnmarked(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	dir := t.TempDir()
	cfg.Woodpecker.Storage.RootPath = dir
	ctx := context.Background()
	ls := NewLogStore(ctx, cfg, nil)

	// Segment 0: marked — should be ignored.
	markedDir := filepath.Join(dir, "1", "0")
	require.NoError(t, os.MkdirAll(markedDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(markedDir, "data.log"), []byte("staged data"), 0o644))
	require.NoError(t, writeCompactedMark(ctx, markedDir))

	// Segment 1: unmarked — still blocks decommission.
	unmarkedDir := filepath.Join(dir, "1", "1")
	require.NoError(t, os.MkdirAll(unmarkedDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(unmarkedDir, "data.log"), []byte("staged data"), 0o644))

	assert.True(t, ls.HasLocalSegmentData())
}

// === EvictLog / EvictInstance tests ===

func TestLogStore_EvictLog_RejectsServing(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	ctx := context.Background()

	err := store.EvictLog(ctx, testBucketName, testRootPath, testLogId)
	assert.NoError(t, err)

	// AddEntry should now return ErrLogBeingDeleted
	_, addErr := store.AddEntry(ctx, testBucketName, testRootPath, testLogId, &proto.LogEntry{SegId: 0, EntryId: 0}, nil)
	assert.ErrorIs(t, addErr, werr.ErrLogBeingDeleted)

	// GetBatchEntriesAdv should also return ErrLogBeingDeleted
	_, getErr := store.GetBatchEntriesAdv(ctx, testBucketName, testRootPath, testLogId, 0, 0, 10, nil)
	assert.ErrorIs(t, getErr, werr.ErrLogBeingDeleted)
}

func TestLogStore_EvictLog_ClosesProcessors(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	ctx := context.Background()

	mockProc := mocks_segment.NewSegmentProcessor(t)
	mockProc.EXPECT().GetLogId().Return(testLogId).Maybe()
	mockProc.EXPECT().Close(mock.Anything).Return(nil).Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{0: mockProc}

	err := store.EvictLog(ctx, testBucketName, testRootPath, testLogId)
	assert.NoError(t, err)

	store.spMu.RLock()
	_, processorEntryPresent := store.segmentProcessors[logKey]
	_, deletingPresent := store.deletingLogs[logKey]
	store.spMu.RUnlock()

	assert.False(t, processorEntryPresent, "segmentProcessors entry should be gone after eviction")
	assert.True(t, deletingPresent, "deletingLogs should contain the logKey after eviction")
}

func TestLogStore_EvictInstance_RejectsAllLogsUnderInstance(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	ctx := context.Background()

	err := store.EvictInstance(ctx, testBucketName, testRootPath)
	assert.NoError(t, err)

	for _, logId := range []int64{1, 2, 99} {
		_, addErr := store.AddEntry(ctx, testBucketName, testRootPath, logId, &proto.LogEntry{SegId: 0, EntryId: 0}, nil)
		assert.ErrorIs(t, addErr, werr.ErrLogBeingDeleted, "logId %d should be rejected", logId)
	}
}

func TestLogStore_EvictLog_Idempotent(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mockProc := mocks_segment.NewSegmentProcessor(t)
	mockProc.EXPECT().GetLogId().Return(testLogId).Maybe()
	mockProc.EXPECT().Close(mock.Anything).Return(nil).Once() // exactly once across BOTH evicts
	store.segmentProcessors[GetLogKey(testBucketName, testRootPath, testLogId)] = map[int64]processor.SegmentProcessor{0: mockProc}

	require.NoError(t, store.EvictLog(context.Background(), testBucketName, testRootPath, testLogId))
	require.NoError(t, store.EvictLog(context.Background(), testBucketName, testRootPath, testLogId))
}

func TestLogStore_EvictInstance_ClosesProcessors(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	mock1 := mocks_segment.NewSegmentProcessor(t)
	mock1.EXPECT().GetLogId().Return(int64(1)).Maybe()
	mock1.EXPECT().Close(mock.Anything).Return(nil).Once()
	mock2 := mocks_segment.NewSegmentProcessor(t)
	mock2.EXPECT().GetLogId().Return(int64(2)).Maybe()
	mock2.EXPECT().Close(mock.Anything).Return(nil).Once()

	store.segmentProcessors[GetLogKey(testBucketName, testRootPath, 1)] = map[int64]processor.SegmentProcessor{0: mock1}
	store.segmentProcessors[GetLogKey(testBucketName, testRootPath, 2)] = map[int64]processor.SegmentProcessor{0: mock2}

	require.NoError(t, store.EvictInstance(context.Background(), testBucketName, testRootPath))

	store.spMu.RLock()
	_, r1 := store.segmentProcessors[GetLogKey(testBucketName, testRootPath, 1)]
	_, r2 := store.segmentProcessors[GetLogKey(testBucketName, testRootPath, 2)]
	store.spMu.RUnlock()
	assert.False(t, r1, "log 1 processors should be evicted")
	assert.False(t, r2, "log 2 processors should be evicted")
}

func TestLogStore_EvictInstance_DoesNotEvictSiblingInstance(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)

	siblingRoot := testRootPath + "2" // "test-root2" shares "test-root" as a string prefix
	siblingMock := mocks_segment.NewSegmentProcessor(t)
	siblingMock.EXPECT().GetLogId().Return(int64(1)).Maybe()
	// NOTE: no Close expectation — it must NOT be closed.
	siblingMock.EXPECT().AddEntry(mock.Anything, mock.Anything, mock.Anything).Return(int64(0), nil).Maybe()
	store.segmentProcessors[GetLogKey(testBucketName, siblingRoot, 1)] = map[int64]processor.SegmentProcessor{0: siblingMock}

	require.NoError(t, store.EvictInstance(context.Background(), testBucketName, testRootPath))

	// The sibling instance's processor is untouched...
	store.spMu.RLock()
	_, resident := store.segmentProcessors[GetLogKey(testBucketName, siblingRoot, 1)]
	store.spMu.RUnlock()
	assert.True(t, resident, "sibling instance test-root2 must not be evicted")

	// ...and the sibling log is still served (returns the resident processor, no ErrLogBeingDeleted).
	_, err := store.AddEntry(context.Background(), testBucketName, siblingRoot, 1,
		&proto.LogEntry{SegId: 0, EntryId: 0}, nil)
	assert.NotErrorIs(t, err, werr.ErrLogBeingDeleted, "sibling instance must not be gated")
}

func TestLogStore_HasLocalSegmentData_EmptyDataLog(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	dir := t.TempDir()
	cfg.Woodpecker.Storage.RootPath = dir
	ctx := context.Background()
	ls := NewLogStore(ctx, cfg, nil)

	// data.log exists but is empty (0 bytes) — not counted as segment data
	segDir := filepath.Join(dir, "1", "0")
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(segDir, "data.log"), []byte{}, 0o644))

	assert.False(t, ls.HasLocalSegmentData())
}

func TestLogStore_EvictLog_PersistsMarker(t *testing.T) {
	store := createTestLogStore()
	root := t.TempDir()
	store.cfg.Woodpecker.Storage.RootPath = root
	store.stopped.Store(false)

	require.NoError(t, store.EvictLog(context.Background(), testBucketName, testRootPath, testLogId))

	markers, err := scanDeleteMarkers(context.Background(), root)
	require.NoError(t, err)
	require.Len(t, markers, 1)
	assert.Equal(t, testLogId, markers[0].LogId)
	assert.False(t, markers[0].Instance)
}

func TestLogStore_RebuildDeletingSetsFromMarkers(t *testing.T) {
	root := t.TempDir()

	// Pre-seed a marker as if a previous run had evicted the log
	require.NoError(t, writeDeleteMarker(context.Background(), root, deleteMarker{
		Bucket:    testBucketName,
		RootPath:  testRootPath,
		LogId:     testLogId,
		DeletedAt: 1,
	}))

	// Create a fresh store pointing at the same root
	store := createTestLogStore()
	store.cfg.Woodpecker.Storage.RootPath = root

	// Rebuild deleting sets from on-disk markers
	require.NoError(t, store.rebuildDeletingSetsFromMarkers())
	store.stopped.Store(false)

	// The log should now be gated — AddEntry must return ErrLogBeingDeleted
	_, err := store.AddEntry(context.Background(), testBucketName, testRootPath, testLogId,
		&proto.LogEntry{SegId: 0, EntryId: 0}, nil)
	assert.ErrorIs(t, err, werr.ErrLogBeingDeleted)
}

func TestLogStore_HasLocalSegmentData_IgnoresMarkers(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	root := t.TempDir()
	cfg.Woodpecker.Storage.RootPath = root
	ctx := context.Background()
	ls := NewLogStore(ctx, cfg, nil)

	// Write a delete marker — it should NOT be counted as segment data
	require.NoError(t, writeDeleteMarker(context.Background(), root, deleteMarker{
		Bucket:    testBucketName,
		RootPath:  testRootPath,
		LogId:     testLogId,
		DeletedAt: 1,
	}))

	assert.False(t, ls.HasLocalSegmentData())
}

func TestLogStore_EvictLog_MarkerWriteFailure(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	// Point the storage root at a path that cannot be created (a regular FILE, so MkdirAll under it fails).
	badFile := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(badFile, []byte("x"), 0o644))
	store.cfg.Woodpecker.Storage.RootPath = badFile

	err := store.EvictLog(context.Background(), testBucketName, testRootPath, testLogId)
	require.Error(t, err)
	assert.False(t, werr.ErrLogBeingDeleted.Is(err), "a mark-write failure must NOT look like ErrLogBeingDeleted")
	assert.True(t, werr.ErrMarkDeleteFailed.Is(err), "a mark-write failure should be ErrMarkDeleteFailed")

	// The in-memory set must NOT have been marked (mark is durable-before-memory).
	store.spMu.RLock()
	_, marked := store.deletingLogs[GetLogKey(testBucketName, testRootPath, testLogId)]
	store.spMu.RUnlock()
	assert.False(t, marked, "log must not be marked in memory when the durable marker write failed")
}

func TestLogStore_AddEntry_DiskPressureRejected(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	ctx := context.Background()
	ls := NewLogStore(ctx, cfg, nil).(*logStore)
	ls.stopped.Store(false) // bypass Start(); the gate under test sits after the stopped check
	ls.diskRejectBps.Store(diskRejectBpsMax)

	entry := &proto.LogEntry{SegId: 1, EntryId: 0, Values: []byte("x")}
	rc := channel.NewLocalResultChannel("test/disk-pressure/0")
	id, err := ls.AddEntry(ctx, "bucket", "root", 1, entry, rc)
	assert.Equal(t, int64(-1), id)
	assert.True(t, werr.ErrLogStoreDiskPressure.Is(err), "expected disk-pressure error, got %v", err)
	assert.True(t, werr.IsRetryableErr(err), "disk-pressure rejection must be retriable")

	ids, batchErr := ls.AddEntryBatch(ctx, "bucket", "root", 1, 1,
		[]*proto.LogEntry{entry}, []channel.ResultChannel{rc})
	assert.Nil(t, ids)
	assert.True(t, werr.ErrLogStoreDiskPressure.Is(batchErr))
}

func TestLogStore_AdmitAppend_Probabilistic(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	ls := NewLogStore(context.Background(), cfg, nil).(*logStore)

	ls.diskRejectBps.Store(0)
	for i := 0; i < 100; i++ {
		assert.True(t, ls.admitAppend())
	}
	ls.diskRejectBps.Store(diskRejectBpsMax)
	for i := 0; i < 100; i++ {
		assert.False(t, ls.admitAppend())
	}
	ls.diskRejectBps.Store(5000)
	rejected := 0
	const trials = 10000
	for i := 0; i < trials; i++ {
		if !ls.admitAppend() {
			rejected++
		}
	}
	// p=0.5, n=10000: ±10σ bounds — deterministic-in-practice
	assert.Greater(t, rejected, 4000)
	assert.Less(t, rejected, 6000)
}

func hasMaintenanceTaskNamed(ls *logStore, name string) bool {
	for _, task := range ls.maintenance.tasks {
		if task.Name() == name {
			return true
		}
	}
	return false
}

func TestNewLogStore_DiskWatermarkTaskRegistration(t *testing.T) {
	ctx := context.Background()

	// service mode with a root path -> registered
	cfg, _ := config.NewConfiguration()
	cfg.Woodpecker.Storage.Type = "service"
	cfg.Woodpecker.Storage.RootPath = t.TempDir()
	ls := NewLogStore(ctx, cfg, nil).(*logStore)
	assert.True(t, hasMaintenanceTaskNamed(ls, "disk-watermark"))

	// default (minio) mode: no local WAL accumulation -> not registered
	cfg2, _ := config.NewConfiguration()
	ls2 := NewLogStore(ctx, cfg2, nil).(*logStore)
	assert.False(t, hasMaintenanceTaskNamed(ls2, "disk-watermark"))

	// explicitly disabled -> not registered
	cfg3, _ := config.NewConfiguration()
	cfg3.Woodpecker.Storage.Type = "service"
	cfg3.Woodpecker.Storage.RootPath = t.TempDir()
	cfg3.Woodpecker.Logstore.DiskWatermarkPolicy.Enabled = false
	ls3 := NewLogStore(ctx, cfg3, nil).(*logStore)
	assert.False(t, hasMaintenanceTaskNamed(ls3, "disk-watermark"))
}

// === EvictSegmentReader Tests ===

func TestLogStore_EvictSegmentReader_InvalidatesCachedProcessorReader(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	ctx := context.Background()

	mockProc := mocks_segment.NewSegmentProcessor(t)
	mockProc.EXPECT().InvalidateReader(mock.Anything).Return().Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{7: mockProc}

	err := store.EvictSegmentReader(ctx, testBucketName, testRootPath, testLogId, 7)
	assert.NoError(t, err)
}

func TestLogStore_EvictSegmentReader_AbsentKeyIsNoop(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	ctx := context.Background()

	// No processor cached for this bucket/rootPath/logId/segId at all.
	err := store.EvictSegmentReader(ctx, testBucketName, testRootPath, testLogId, 7)
	assert.NoError(t, err)

	// Must not have created a processor as a side effect.
	store.spMu.RLock()
	_, logExists := store.segmentProcessors[GetLogKey(testBucketName, testRootPath, testLogId)]
	store.spMu.RUnlock()
	assert.False(t, logExists, "EvictSegmentReader must not create a processor for an absent key")
}

func TestLogStore_EvictSegmentReader_AbsentSegmentUnderExistingLogIsNoop(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	ctx := context.Background()

	// A processor exists for a different segment under the same log; InvalidateReader
	// must NOT be called on it, and no processor should be created for segId 7.
	mockProc := mocks_segment.NewSegmentProcessor(t)
	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{3: mockProc}

	err := store.EvictSegmentReader(ctx, testBucketName, testRootPath, testLogId, 7)
	assert.NoError(t, err)

	store.spMu.RLock()
	_, segExists := store.segmentProcessors[logKey][7]
	store.spMu.RUnlock()
	assert.False(t, segExists, "EvictSegmentReader must not create a processor for an absent segment")
}

// === EvictSegmentWriter Tests ===

func TestLogStore_EvictSegmentWriter_InvalidatesCachedProcessorWriter(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	ctx := context.Background()

	mockProc := mocks_segment.NewSegmentProcessor(t)
	mockProc.EXPECT().InvalidateWriter(mock.Anything).Return().Once()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)
	store.segmentProcessors[logKey] = map[int64]processor.SegmentProcessor{7: mockProc}

	err := store.EvictSegmentWriter(ctx, testBucketName, testRootPath, testLogId, 7)
	assert.NoError(t, err)
}

func TestLogStore_EvictSegmentWriter_AbsentKeyIsNoop(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	ctx := context.Background()

	err := store.EvictSegmentWriter(ctx, testBucketName, testRootPath, testLogId, 7)
	assert.NoError(t, err)

	store.spMu.RLock()
	_, logExists := store.segmentProcessors[GetLogKey(testBucketName, testRootPath, testLogId)]
	store.spMu.RUnlock()
	assert.False(t, logExists, "EvictSegmentWriter must not create a processor for an absent key")
}
