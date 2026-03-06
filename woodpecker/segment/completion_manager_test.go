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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

func newTestSegmentForCompletion(t *testing.T) (*segmentHandleImpl, *mocks_logstore_client.LogStoreClient) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{
					QueueSize:  10,
					MaxRetries: 2,
				},
			},
		},
	}

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Active,
			LastEntryId: -1,
			Quorum: &proto.QuorumInfo{
				Id:    1,
				Aq:    1,
				Es:    1,
				Wq:    1,
				Nodes: []string{"127.0.0.1"},
			},
		},
		Revision: 1,
	}

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)
	return segmentHandle.(*segmentHandleImpl), mockClient
}

// TestCompletionManager_TriggerAndWait tests that triggering completion and waiting returns success.
func TestCompletionManager_TriggerAndWait(t *testing.T) {
	seg, mockClient := newTestSegmentForCompletion(t)
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(5), nil)
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(5), nil)

	seg.completionMgr.TriggerCompletion()
	err := seg.completionMgr.WaitForCompletion()
	assert.NoError(t, err)

	// Verify segment is no longer writable
	writable, _ := seg.IsWritable(context.Background())
	assert.False(t, writable)
}

// TestCompletionManager_RetryOnFailureThenSucceed tests that completion retries on transient failure
// and eventually succeeds.
func TestCompletionManager_RetryOnFailureThenSucceed(t *testing.T) {
	seg, mockClient := newTestSegmentForCompletion(t)

	callCount := 0
	// First call fails, second succeeds
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).
		RunAndReturn(func(ctx context.Context, bucket, root string, logId, segId int64) (int64, error) {
			callCount++
			if callCount == 1 {
				return -1, assert.AnError
			}
			return int64(3), nil
		})
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(3), nil).Maybe()

	seg.completionMgr.TriggerCompletion()
	err := seg.completionMgr.WaitForCompletion()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, callCount, 2, "Should have retried at least once")

	writable, _ := seg.IsWritable(context.Background())
	assert.False(t, writable)
}

// TestCompletionManager_AllRetriesExhausted tests that when all retries fail,
// WaitForCompletion returns an error and writer invalidation is triggered.
func TestCompletionManager_AllRetriesExhausted(t *testing.T) {
	seg, mockClient := newTestSegmentForCompletion(t)

	// All fence calls fail
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).
		Return(int64(-1), assert.AnError)

	invalidated := false
	seg.SetWriterInvalidationNotifier(context.Background(), func(ctx context.Context, reason string) {
		invalidated = true
	})

	seg.completionMgr.TriggerCompletion()
	err := seg.completionMgr.WaitForCompletion()
	assert.Error(t, err)
	assert.True(t, invalidated, "Writer should be invalidated after retry exhaustion")

	// Segment should still transition to non-writable (close writing runs regardless)
	writable, _ := seg.IsWritable(context.Background())
	assert.False(t, writable)
}

// TestCompletionManager_MultipleTriggersCoalesce tests that multiple TriggerCompletion calls
// result in only one execution.
func TestCompletionManager_MultipleTriggersCoalesce(t *testing.T) {
	seg, mockClient := newTestSegmentForCompletion(t)

	fenceCount := 0
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).
		RunAndReturn(func(ctx context.Context, bucket, root string, logId, segId int64) (int64, error) {
			fenceCount++
			return int64(2), nil
		})
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()

	// Fire multiple triggers
	for i := 0; i < 10; i++ {
		seg.completionMgr.TriggerCompletion()
	}

	err := seg.completionMgr.WaitForCompletion()
	assert.NoError(t, err)
	assert.Equal(t, 1, fenceCount, "Fence should be called exactly once (coalesced)")
}

// TestCompletionManager_ConcurrentWaiters tests that multiple goroutines waiting on
// WaitForCompletion all get the same result.
func TestCompletionManager_ConcurrentWaiters(t *testing.T) {
	seg, mockClient := newTestSegmentForCompletion(t)
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(4), nil)
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(4), nil)

	numWaiters := 5
	results := make([]error, numWaiters)
	var wg sync.WaitGroup

	for i := 0; i < numWaiters; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = seg.completionMgr.WaitForCompletion()
		}(i)
	}

	// Give waiters time to block
	time.Sleep(50 * time.Millisecond)
	seg.completionMgr.TriggerCompletion()

	wg.Wait()
	for i := 0; i < numWaiters; i++ {
		assert.NoError(t, results[i], "All waiters should get the same nil error result")
	}
}

// TestCompletionManager_ContextCancellation tests that cancelling the context aborts the manager.
func TestCompletionManager_ContextCancellation(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(0), nil).Maybe()
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(0), nil).Maybe()

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{
					QueueSize:  10,
					MaxRetries: 2,
				},
			},
		},
	}

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Active,
			LastEntryId: -1,
			Quorum: &proto.QuorumInfo{
				Id:    1,
				Aq:    1,
				Es:    1,
				Wq:    1,
				Nodes: []string{"127.0.0.1"},
			},
		},
		Revision: 1,
	}

	// Create segment handle manually without starting completionMgr via constructor
	// so we can control the context.
	seg := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true).(*segmentHandleImpl)

	// Stop the auto-started completionMgr and replace with one using a cancellable context
	seg.completionMgr.Stop()
	cm := newCompletionManager(seg)
	seg.completionMgr = cm

	ctx, cancel := context.WithCancel(context.Background())
	cm.Start(ctx)

	// Cancel before triggering
	cancel()

	err := cm.WaitForCompletion()
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestCompletionManager_ForceCompleteOnReadonly tests that ForceCompleteAndClose
// on a readonly segment is a no-op.
func TestCompletionManager_ForceCompleteOnReadonly(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{
					QueueSize:  10,
					MaxRetries: 2,
				},
			},
		},
	}

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Active,
			LastEntryId: -1,
			Quorum: &proto.QuorumInfo{
				Id:    1,
				Aq:    1,
				Es:    1,
				Wq:    1,
				Nodes: []string{"127.0.0.1"},
			},
		},
		Revision: 1,
	}

	// Create readonly segment (canWrite=false)
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)

	err := segmentHandle.ForceCompleteAndClose(context.Background())
	assert.NoError(t, err)
}
