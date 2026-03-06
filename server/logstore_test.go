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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
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
		cleanupDone:       make(chan struct{}),
	}

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
		store.closeSegmentProcessorUnsafe(context.Background(), item.logKey, item.segmentId, item.processor)
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
		// All processors are recently accessed, so none should be cleaned
		mockProcessors[i].EXPECT().GetLastAccessTime().Return(recentTime.UnixMilli()).Maybe()
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
	store.closeSegmentProcessorUnsafe(context.Background(), logKey, 10, mockProcessor)

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
		store.closeSegmentProcessorUnsafe(context.Background(), item.logKey, item.segmentId, item.processor)
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

func TestLogStore_SegmentProcessorCleanup_ProtectHighestSegment(t *testing.T) {
	store := createTestLogStore()

	logKey := GetLogKey(testBucketName, testRootPath, testLogId)

	// Create 5 processors to test highest segment protection
	mockProcessors := make(map[int64]*mocks_segment.SegmentProcessor)
	for i := int64(1); i <= 5; i++ {
		mockProcessors[i] = mocks_segment.NewSegmentProcessor(t)
	}

	// Set up expectations - processors 1,2,3,4 should be closed (all idle except highest)
	// Processor 5 will be protected (highest segment ID)
	for i := int64(1); i <= 4; i++ {
		mockProcessors[i].EXPECT().Close(mock.Anything).Return(nil).Once()
		mockProcessors[i].EXPECT().GetLogId().Return(testLogId).Times(2)
	}

	// Add processors with old access times (all idle)
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
		store.closeSegmentProcessorUnsafe(context.Background(), item.logKey, item.segmentId, item.processor)
	}

	// Verify that segments 1,2,3,4 were removed (idle and not highest)
	for i := int64(1); i <= 4; i++ {
		assert.NotContains(t, store.segmentProcessors[logKey], i, "Segment %d should be cleaned up", i)
	}

	// Verify that segment 5 remains (highest segment ID, protected)
	assert.Contains(t, store.segmentProcessors[logKey], int64(5), "Segment 5 should be protected (highest segment ID)")

	// Verify expectations for closed processors
	for i := int64(1); i <= 4; i++ {
		mockProcessors[i].AssertExpectations(t)
	}
}

func TestLogStore_BackgroundCleanup_StartStop(t *testing.T) {
	store := createTestLogStore()

	// Test starting background cleanup
	store.startBackgroundCleanup()

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	// Test stopping background cleanup
	store.stopBackgroundCleanup()

	// Verify cleanup channel is closed
	select {
	case <-store.cleanupDone:
		// Expected - channel should be closed
	default:
		t.Fatal("cleanupDone channel should be closed")
	}
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

	// Calling Stop again should not panic (already stopped)
	// Re-create cleanupDone channel since it was already closed
	store.cleanupDone = make(chan struct{})
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
