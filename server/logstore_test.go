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
	"github.com/zilliztech/woodpecker/mocks/mocks_server/mocks_segment"
	"github.com/zilliztech/woodpecker/server/processor"
)

func createTestLogStore() *logStore {
	cfg := &config.Configuration{}
	ctx, cancel := context.WithCancel(context.Background())

	store := &logStore{
		cfg:                        cfg,
		ctx:                        ctx,
		cancel:                     cancel,
		segmentProcessors:          make(map[int64]map[int64]processor.SegmentProcessor),
		segmentProcessorLastAccess: make(map[int64]map[int64]time.Time),
	}

	return store
}

func TestLogStore_SegmentProcessorCleanup_Stop(t *testing.T) {
	store := createTestLogStore()

	// Create mock processors
	mockProcessor1 := mocks_segment.NewSegmentProcessor(t)
	mockProcessor2 := mocks_segment.NewSegmentProcessor(t)
	mockProcessor3 := mocks_segment.NewSegmentProcessor(t)

	// Set up expectations for Close calls
	mockProcessor1.EXPECT().Close(mock.Anything).Return(nil).Once()
	mockProcessor2.EXPECT().Close(mock.Anything).Return(nil).Once()
	mockProcessor3.EXPECT().Close(mock.Anything).Return(nil).Once()

	// Add processors to store
	store.segmentProcessors[1] = map[int64]processor.SegmentProcessor{
		10: mockProcessor1,
		20: mockProcessor2,
	}
	store.segmentProcessors[2] = map[int64]processor.SegmentProcessor{
		30: mockProcessor3,
	}
	store.segmentProcessorLastAccess[1] = map[int64]time.Time{
		10: time.Now(),
		20: time.Now(),
	}
	store.segmentProcessorLastAccess[2] = map[int64]time.Time{
		30: time.Now(),
	}

	// Call Stop
	err := store.Stop()

	// Verify
	assert.NoError(t, err)
	assert.Empty(t, store.segmentProcessors)
	assert.Empty(t, store.segmentProcessorLastAccess)

	// Verify all expectations
	mockProcessor1.AssertExpectations(t)
	mockProcessor2.AssertExpectations(t)
	mockProcessor3.AssertExpectations(t)
}

func TestLogStore_SegmentProcessorCleanup_IdleCleanup(t *testing.T) {
	store := createTestLogStore()

	// Create 15 mock processors to ensure some will be cleaned and some protected
	mockProcessors := make(map[int64]*mocks_segment.SegmentProcessor)
	for i := int64(1); i <= 15; i++ {
		mockProcessors[i] = mocks_segment.NewSegmentProcessor(t)
	}

	// Set up expectations - processors 1-5 should be closed (oldest by segment ID)
	// Processors 6-15 (top 10) should be protected
	// But we'll make processor 15 recently accessed, so it won't be cleaned anyway
	for i := int64(1); i <= 5; i++ {
		mockProcessors[i].EXPECT().Close(mock.Anything).Return(nil).Once()
	}

	// Add processors with different access times
	now := time.Now()
	oldTime := now.Add(-5 * time.Minute)    // Older than maxIdleTime (3 minutes)
	recentTime := now.Add(-1 * time.Minute) // Recent access

	store.segmentProcessors[1] = make(map[int64]processor.SegmentProcessor)
	store.segmentProcessorLastAccess[1] = make(map[int64]time.Time)

	// Add all processors to log 1
	for i := int64(1); i <= 15; i++ {
		store.segmentProcessors[1][i] = mockProcessors[i]
		if i == 15 {
			// Make the highest segment ID recently accessed
			store.segmentProcessorLastAccess[1][i] = recentTime
		} else {
			// All others are idle
			store.segmentProcessorLastAccess[1][i] = oldTime
		}
	}

	// Set last cleanup time to trigger cleanup
	store.lastCleanupTime = now.Add(-2 * time.Minute)

	// Call cleanup
	store.cleanupIdleSegmentProcessorsUnsafe(context.Background(), 3*time.Minute)

	// Verify that segments 1-5 are removed (oldest and not protected)
	for i := int64(1); i <= 5; i++ {
		assert.NotContains(t, store.segmentProcessors[1], i, "Segment %d should be removed", i)
	}
	// Verify that segments 6-15 remain (protected as top 10)
	for i := int64(6); i <= 15; i++ {
		assert.Contains(t, store.segmentProcessors[1], i, "Segment %d should be protected", i)
	}

	// Verify access times are also cleaned up correctly
	for i := int64(1); i <= 5; i++ {
		assert.NotContains(t, store.segmentProcessorLastAccess[1], i, "Access time for segment %d should be removed", i)
	}
	for i := int64(6); i <= 15; i++ {
		assert.Contains(t, store.segmentProcessorLastAccess[1], i, "Access time for segment %d should remain", i)
	}

	// Verify expectations for closed processors
	for i := int64(1); i <= 5; i++ {
		mockProcessors[i].AssertExpectations(t)
	}
}

func TestLogStore_SegmentProcessorCleanup_TryCleanupConditions(t *testing.T) {
	store := createTestLogStore()

	// Test case 1: Not enough processors to trigger cleanup
	store.segmentProcessors[1] = map[int64]processor.SegmentProcessor{
		10: mocks_segment.NewSegmentProcessor(t),
	}
	store.segmentProcessorLastAccess[1] = map[int64]time.Time{
		10: time.Now(),
	}
	store.lastCleanupTime = time.Now().Add(-2 * time.Minute)

	// Should not trigger cleanup (less than minProcessorThreshold)
	store.tryCleanupIdleSegmentProcessorsUnsafe(context.Background())
	assert.Contains(t, store.segmentProcessors[1], int64(10)) // Processor should remain

	// Test case 2: Recent cleanup, should not trigger again
	for i := int64(1); i <= 15; i++ {
		if store.segmentProcessors[1] == nil {
			store.segmentProcessors[1] = make(map[int64]processor.SegmentProcessor)
			store.segmentProcessorLastAccess[1] = make(map[int64]time.Time)
		}
		store.segmentProcessors[1][i] = mocks_segment.NewSegmentProcessor(t)
		store.segmentProcessorLastAccess[1][i] = time.Now()
	}

	store.lastCleanupTime = time.Now() // Recent cleanup

	// Should not trigger cleanup (recent cleanup)
	store.tryCleanupIdleSegmentProcessorsUnsafe(context.Background())
	assert.Len(t, store.segmentProcessors[1], 15) // All processors should remain
}

func TestLogStore_RemoveSegmentProcessor(t *testing.T) {
	store := createTestLogStore()

	// Create mock processors
	mockProcessor1 := mocks_segment.NewSegmentProcessor(t)
	mockProcessor2 := mocks_segment.NewSegmentProcessor(t)

	// Set up expectations
	mockProcessor1.EXPECT().Close(mock.Anything).Return(nil).Once()

	// Add processors
	store.segmentProcessors[1] = map[int64]processor.SegmentProcessor{
		10: mockProcessor1,
		20: mockProcessor2,
	}
	store.segmentProcessorLastAccess[1] = map[int64]time.Time{
		10: time.Now(),
		20: time.Now(),
	}

	// Remove specific processor
	store.RemoveSegmentProcessor(context.Background(), 1, 10)

	// Verify processor is removed
	assert.NotContains(t, store.segmentProcessors[1], int64(10))
	assert.Contains(t, store.segmentProcessors[1], int64(20)) // Other processor remains
	assert.NotContains(t, store.segmentProcessorLastAccess[1], int64(10))
	assert.Contains(t, store.segmentProcessorLastAccess[1], int64(20))

	// Verify expectations
	mockProcessor1.AssertExpectations(t)
}

func TestLogStore_RemoveSegmentProcessor_LastProcessor(t *testing.T) {
	store := createTestLogStore()

	// Create mock processor
	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().Close(mock.Anything).Return(nil).Once()

	// Add single processor
	store.segmentProcessors[1] = map[int64]processor.SegmentProcessor{
		10: mockProcessor,
	}
	store.segmentProcessorLastAccess[1] = map[int64]time.Time{
		10: time.Now(),
	}

	// Remove the only processor
	store.RemoveSegmentProcessor(context.Background(), 1, 10)

	// Verify entire log entry is removed
	assert.NotContains(t, store.segmentProcessors, int64(1))
	assert.NotContains(t, store.segmentProcessorLastAccess, int64(1))

	// Verify expectations
	mockProcessor.AssertExpectations(t)
}

func TestLogStore_GetExistsSegmentProcessor_UpdatesAccessTime(t *testing.T) {
	store := createTestLogStore()

	// Create mock processor
	mockProcessor := mocks_segment.NewSegmentProcessor(t)

	// Add existing processor
	store.segmentProcessors[1] = map[int64]processor.SegmentProcessor{
		10: mockProcessor,
	}
	oldTime := time.Now().Add(-1 * time.Hour)
	store.segmentProcessorLastAccess[1] = map[int64]time.Time{
		10: oldTime,
	}

	// Get existing processor
	processor := store.getExistsSegmentProcessor(1, 10)

	// Verify
	assert.Equal(t, mockProcessor, processor)

	// Verify access time was updated
	newTime := store.segmentProcessorLastAccess[1][10]
	assert.True(t, newTime.After(oldTime))
}

func TestLogStore_GetExistsSegmentProcessor_NotFound(t *testing.T) {
	store := createTestLogStore()

	// Try to get non-existent processor
	processor := store.getExistsSegmentProcessor(1, 10)

	// Verify
	assert.Nil(t, processor)
}

func TestLogStore_GetTotalProcessorCountUnsafe(t *testing.T) {
	store := createTestLogStore()

	// Add processors
	store.segmentProcessors[1] = map[int64]processor.SegmentProcessor{
		10: mocks_segment.NewSegmentProcessor(t),
		20: mocks_segment.NewSegmentProcessor(t),
	}
	store.segmentProcessors[2] = map[int64]processor.SegmentProcessor{
		30: mocks_segment.NewSegmentProcessor(t),
	}

	// Verify count
	count := store.getTotalProcessorCountUnsafe()
	assert.Equal(t, 3, count)
}

func TestLogStore_CloseSegmentProcessorUnsafe_CloseError(t *testing.T) {
	store := createTestLogStore()

	// Create mock processor that returns error on close
	mockProcessor := mocks_segment.NewSegmentProcessor(t)
	mockProcessor.EXPECT().Close(mock.Anything).Return(assert.AnError).Once()

	// Should not panic even if close fails
	store.closeSegmentProcessorUnsafe(context.Background(), 1, 10, mockProcessor)

	// Verify expectations
	mockProcessor.AssertExpectations(t)
}

func TestLogStore_SegmentProcessorCleanup_ProtectLatestSegments(t *testing.T) {
	store := createTestLogStore()

	// Create 15 mock processors with different segment IDs
	mockProcessors := make(map[int64]*mocks_segment.SegmentProcessor)
	for i := int64(1); i <= 15; i++ {
		mockProcessors[i] = mocks_segment.NewSegmentProcessor(t)
	}

	// Only processors with smaller segment IDs should be closed (segments 1-5)
	// Segments 6-15 (top 10) should be protected
	for i := int64(1); i <= 5; i++ {
		mockProcessors[i].EXPECT().Close(mock.Anything).Return(nil).Once()
	}

	// Add processors with all being idle (old access time)
	now := time.Now()
	oldTime := now.Add(-10 * time.Minute) // All are idle

	store.segmentProcessors[1] = make(map[int64]processor.SegmentProcessor)
	store.segmentProcessorLastAccess[1] = make(map[int64]time.Time)

	for i := int64(1); i <= 15; i++ {
		store.segmentProcessors[1][i] = mockProcessors[i]
		store.segmentProcessorLastAccess[1][i] = oldTime
	}

	// Call cleanup
	store.cleanupIdleSegmentProcessorsUnsafe(context.Background(), 5*time.Minute)

	// Verify that only segments 1-5 were removed, segments 6-15 (top 10) remain
	for i := int64(1); i <= 5; i++ {
		assert.NotContains(t, store.segmentProcessors[1], i, "Segment %d should be removed", i)
	}
	for i := int64(6); i <= 15; i++ {
		assert.Contains(t, store.segmentProcessors[1], i, "Segment %d should be protected", i)
	}

	// Verify expectations for closed processors
	for i := int64(1); i <= 5; i++ {
		mockProcessors[i].AssertExpectations(t)
	}
}

func TestLogStore_SegmentProcessorCleanup_ProtectWhenLessThan10(t *testing.T) {
	store := createTestLogStore()

	// Create only 5 processors (less than protection threshold of 10)
	mockProcessors := make(map[int64]*mocks_segment.SegmentProcessor)
	for i := int64(1); i <= 5; i++ {
		mockProcessors[i] = mocks_segment.NewSegmentProcessor(t)
	}

	// None should be closed since we have less than 10 total
	// (all should be protected)

	// Add processors with all being idle
	now := time.Now()
	oldTime := now.Add(-10 * time.Minute) // All are idle

	store.segmentProcessors[1] = make(map[int64]processor.SegmentProcessor)
	store.segmentProcessorLastAccess[1] = make(map[int64]time.Time)

	for i := int64(1); i <= 5; i++ {
		store.segmentProcessors[1][i] = mockProcessors[i]
		store.segmentProcessorLastAccess[1][i] = oldTime
	}

	// Call cleanup
	store.cleanupIdleSegmentProcessorsUnsafe(context.Background(), 5*time.Minute)

	// Verify that all segments remain (all are protected since total < 10)
	for i := int64(1); i <= 5; i++ {
		assert.Contains(t, store.segmentProcessors[1], i, "Segment %d should be protected", i)
	}

	// No expectations to verify since no processors should be closed
}
