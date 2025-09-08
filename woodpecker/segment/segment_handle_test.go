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
	"container/list"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/tracer"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

func init() {
	cfg, _ := config.NewConfiguration()
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)
}

func TestNewSegmentHandle(t *testing.T) {
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
			SegNo: 1,
		},
		Revision: 1,
	}
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)
	assert.Equal(t, "testLog", segmentHandle.GetLogName())
	assert.Equal(t, int64(1), segmentHandle.GetId(context.Background()))
}

func TestAppendAsync_Success(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)
	mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), mock.Anything, mock.Anything).Return(0, nil)
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil)
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
				Aq:    1,
				Es:    1,
				Id:    1,
				Nodes: []string{"127.0.0.1"},
				Wq:    1,
			},
		},
		Revision: 1,
	}
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)
	callbackCalled := false
	callback := func(segmentId int64, entryId int64, err error) {
		callbackCalled = true
		assert.Equal(t, int64(1), segmentId)
		assert.Equal(t, int64(0), entryId)
		assert.Nil(t, err)
	}
	segmentHandle.AppendAsync(context.Background(), []byte("test"), callback)
	segImpl := segmentHandle.(*segmentHandleImpl)
	op := segImpl.appendOpsQueue.Front().Value
	appendOp := op.(*AppendOp)
	go func(op2 *AppendOp) {
		for i := 0; i < 5; i++ {
			if len(appendOp.resultChannels) > 0 {
				_ = appendOp.resultChannels[0].SendResult(context.Background(), &channel.AppendResult{
					SyncedId: 0,
					Err:      nil,
				})
				_ = appendOp.resultChannels[0].Close(context.Background())
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Errorf("Wait timeout for append")
	}(appendOp)
	time.Sleep(1000 * time.Millisecond) // Give some time for the async operation to complete
	assert.True(t, callbackCalled)
}

func TestMultiAppendAsync_AllSuccess_InSequential(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)
	for i := 0; i < 20; i++ {
		mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
			SegId:   1,
			EntryId: int64(i),
			Values:  []byte(fmt.Sprintf("test_%d", i)),
		}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(i), nil)
	}
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{
					QueueSize:  25,
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
				Aq:    1,
				Es:    1,
				Id:    1,
				Nodes: []string{"127.0.0.1"},
				Wq:    1,
			},
		},
		Revision: 1,
	}
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)

	callbackCalledNum := 0
	syncedIds := make([]int64, 0)
	for i := 0; i < 20; i++ {
		entryIndex := i
		callback := func(segmentId int64, entryId int64, err error) {
			callbackCalledNum++
			assert.Equal(t, int64(1), segmentId)
			assert.Equal(t, int64(entryIndex), entryId)
			assert.Nil(t, err)
			syncedIds = append(syncedIds, entryId)
		}
		segmentHandle.AppendAsync(context.Background(), []byte(fmt.Sprintf("test_%d", i)), callback)
	}

	// Manually trigger the async callbacks
	segImpl := segmentHandle.(*segmentHandleImpl)
	go func() {
		processedCount := 0
		maxAttempts := 50
		attempts := 0
		for processedCount < 20 && attempts < maxAttempts {
			time.Sleep(20 * time.Millisecond)
			attempts++

			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				appendOp := e.Value.(*AppendOp)
				if len(appendOp.resultChannels) > 0 {
					_ = appendOp.resultChannels[0].SendResult(context.Background(), &channel.AppendResult{
						SyncedId: appendOp.entryId,
						Err:      nil,
					})
					_ = appendOp.resultChannels[0].Close(context.Background())
					processedCount++
				}
			}
		}
	}()

	time.Sleep(500 * time.Millisecond) // Give some time for the async operation to complete
	assert.Equal(t, 20, callbackCalledNum)
	assert.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, syncedIds)
}

func TestMultiAppendAsync_PartialSuccess(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil)
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil)
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(1), nil)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)
	for i := 0; i < 5; i++ {
		ch := make(chan int64, 1)
		ch <- int64(i)
		close(ch)
		if i != 2 {
			// 0,1,3,4 success
			mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
				SegId:   1,
				EntryId: int64(i),
				Values:  []byte(fmt.Sprintf("test_%d", i)),
			}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(i), nil)
		} else {
			// 2 fail, and retry 3 times
			mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
				SegId:   1,
				EntryId: int64(i),
				Values:  []byte(fmt.Sprintf("test_%d", i)),
			}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(i), errors.New("test error"))
		}
	}
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{
					QueueSize:  10, // only 10 execute queue, so some callback will be block but finally success
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
				Aq:    1,
				Es:    1,
				Id:    1,
				Nodes: []string{"127.0.0.1"},
				Wq:    1,
			},
		},
		Revision: 1,
	}
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)

	failedAttempts := 0
	successAttempts := 0
	syncedIds := make([]int64, 0)
	for i := 0; i < 5; i++ {
		entryIndex := i
		callback := func(segmentId int64, entryId int64, err error) {
			if entryIndex >= 2 {
				failedAttempts++
				// received callback 2,3,4 fail
				assert.Error(t, err)
				assert.Equal(t, int64(1), segmentId)
				assert.Equal(t, int64(entryIndex), entryId)
			} else {
				successAttempts++
				// received callback 0,1 success
				assert.NoError(t, err)
				assert.Equal(t, int64(1), segmentId)
				assert.Equal(t, int64(entryIndex), entryId)
				syncedIds = append(syncedIds, entryId)
			}
		}
		segmentHandle.AppendAsync(context.Background(), []byte(fmt.Sprintf("test_%d", i)), callback)
	}

	// Manually trigger the async callbacks
	segImpl := segmentHandle.(*segmentHandleImpl)
	go func() {
		processedCount := 0
		maxAttempts := 50
		attempts := 0
		for processedCount < 5 && attempts < maxAttempts {
			time.Sleep(20 * time.Millisecond)
			attempts++

			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				appendOp := e.Value.(*AppendOp)
				if len(appendOp.resultChannels) > 0 {
					_ = appendOp.resultChannels[0].SendResult(context.Background(), &channel.AppendResult{
						SyncedId: appendOp.entryId,
						Err:      nil,
					})
					_ = appendOp.resultChannels[0].Close(context.Background())
					processedCount++
				}
			}
		}
	}()

	time.Sleep(1000 * time.Millisecond) // Give some time for the async operation to complete
	assert.Equal(t, 2, successAttempts) // 0,1
	assert.Equal(t, 3, failedAttempts)  // 2,3,4
	assert.Equal(t, []int64{0, 1}, syncedIds)
}

// TestAppendAsync_TimeoutBug tests the specific bug where when resultChan.ReadResult times out,
// the callback receives nil error instead of the actual timeout error.
// This test is based on TestMultiAppendAsync_PartialSuccess pattern:
// - Entries 0,1 succeed normally
// - Entry 2 times out by not sending any data to result channel
// - The bug causes the callback to receive nil error instead of timeout error
func TestAppendAsync_TimeoutBug(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(1), nil)
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil)
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(1), nil)

	// Mock for successful entries (0,1) and timeout entry (2)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()

	// Entry 0,1 will succeed with AppendEntry and get result
	for i := 0; i < 2; i++ {
		mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
			SegId:   1,
			EntryId: int64(i),
			Values:  []byte(fmt.Sprintf("test_%d", i)),
		}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(i), nil)
	}

	// Entry 2 will succeed with AppendEntry but we won't send result to channel (timeout simulation)
	// This will cause multiple retries due to timeout - expect initial attempt + 2 retries = 3 total
	mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId:   1,
		EntryId: int64(2),
		Values:  []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil).Times(1)

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{
					QueueSize:  10,
					MaxRetries: 2, // Allow 2 retries before giving up
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
				Aq:    1,
				Es:    1,
				Id:    1,
				Nodes: []string{"127.0.0.1"},
				Wq:    1,
			},
		},
		Revision: 1,
	}

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)

	// Track callback results
	callbackResults := make(map[int64]error)
	callbackCounts := make(map[int64]int)
	var mu sync.Mutex

	// Create append operations
	for i := 0; i < 3; i++ {
		callback := func(segmentId int64, entryId int64, err error) {
			mu.Lock()
			defer mu.Unlock()
			callbackResults[entryId] = err
			callbackCounts[entryId]++
			t.Logf("Callback for entry %d called (count: %d) with error: %v", entryId, callbackCounts[entryId], err)
		}
		segmentHandle.AppendAsync(context.Background(), []byte(fmt.Sprintf("test_%d", i)), callback)
	}

	// Get the segment handle implementation to access operations
	segImpl := segmentHandle.(*segmentHandleImpl)

	// Process the operations
	go func() {
		maxAttempts := 100
		attempts := 0
		processedSuccessfully := make(map[int64]bool)

		for attempts < maxAttempts {
			time.Sleep(20 * time.Millisecond)
			attempts++

			segImpl.Lock()
			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				appendOp := e.Value.(*AppendOp)
				entryId := appendOp.entryId

				if len(appendOp.resultChannels) > 0 {
					if entryId < 2 && !processedSuccessfully[entryId] {
						// Send success result for entries 0,1
						t.Logf("Sending success result for entry %d", entryId)
						_ = appendOp.resultChannels[0].SendResult(context.Background(), &channel.AppendResult{
							SyncedId: entryId,
							Err:      nil,
						})
						_ = appendOp.resultChannels[0].Close(context.Background())
						processedSuccessfully[entryId] = true
					}
					// For entry 2: DO NOT send any result - this simulates timeout
					// The receivedAckCallback will timeout when calling resultChan.ReadResult(subCtx)
					// This will trigger the bug in append_op.go lines 172-173
				}
			}
			segImpl.Unlock()

			// Check if we've processed entries 0,1 and entry 2 has started timing out
			if processedSuccessfully[0] && processedSuccessfully[1] {
				break
			}
		}
	}()

	// Wait for operations to complete and timeouts to occur
	t.Logf("=== WAITING FOR TIMEOUT BUG TO MANIFEST ===")
	t.Logf("Entries 0,1 should succeed normally")
	t.Logf("Entry 2 will timeout because we don't send result to its channel")
	t.Logf("Due to bug in lines 172-173, entry 2's callback will receive nil error instead of timeout error")

	// First, wait for all append operations to be initiated
	time.Sleep(1 * time.Second)

	// Check if all three operations have been initiated
	segImpl.Lock()
	queueSize := segImpl.appendOpsQueue.Len()
	var entry2Op *AppendOp
	for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
		appendOp := e.Value.(*AppendOp)
		if appendOp.entryId == 2 {
			entry2Op = appendOp
			break
		}
	}
	segImpl.Unlock()

	t.Logf("Queue size: %d, Entry 2 operation found: %v", queueSize, entry2Op != nil)

	if entry2Op != nil {
		t.Logf("Entry 2 operation exists, waiting longer for timeout...")
		// Since the real timeout is 30 seconds, we need to wait longer or trigger it manually
		// For now, let's wait a bit longer and then check the state
		time.Sleep(2 * time.Second)

		// Force check if entry 2 has any result channels that we can examine
		if len(entry2Op.resultChannels) > 0 {
			t.Logf("Entry 2 has %d result channels", len(entry2Op.resultChannels))
		} else {
			t.Logf("Entry 2 has no result channels yet")
		}
	}

	// Wait long enough for the complete timeout and retry cycle:
	// 1. Entries 0,1 to complete successfully (immediate)
	// 2. Entry 2 first attempt: 30s timeout
	// 3. Entry 2 retry 1: 30s timeout
	// 4. Entry 2 retry 2: 30s timeout
	// 5. Final FastFail callback with bug (nil error)
	// Total: ~90 seconds for 3 attempts, we wait 2 minutes to be safe
	t.Logf("Waiting 2 minutes for complete timeout and retry cycle...")
	t.Logf("This will demonstrate the real-world timeout behavior")
	time.Sleep(2 * time.Minute)

	mu.Lock()
	defer mu.Unlock()

	// Verify results
	t.Logf("=== RESULTS VERIFICATION ===")
	for entryId := int64(0); entryId < 3; entryId++ {
		err := callbackResults[entryId]
		count := callbackCounts[entryId]
		t.Logf("Entry %d: callback called %d times, final error: %v", entryId, count, err)
	}

	// Entries 0,1 should succeed
	assert.NoError(t, callbackResults[0], "Entry 0 should succeed")
	assert.NoError(t, callbackResults[1], "Entry 1 should succeed")

	// Entry 2 should demonstrate the FIXED behavior (bug has been fixed)
	t.Logf("- AppendEntry succeeded (err = nil)")
	t.Logf("- resultChan.ReadResult(subCtx) timed out (readChanErr = timeout)")
	t.Logf("- Fixed code correctly uses 'readChanErr' instead of 'err'")
	// Verify the fix works correctly
	assert.NotNil(t, callbackResults[2], "FIX VERIFIED: Entry 2 correctly receives timeout error")
	assert.True(t, errors.Is(callbackResults[2], context.DeadlineExceeded), "FIX VERIFIED: Error is context deadline exceeded")
}

func TestMultiAppendAsync_PartialFailButAllSuccessAfterRetry(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)
	for i := 0; i < 5; i++ {
		mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), mock.Anything, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(i), nil)
	}
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{
					QueueSize:  10, // only 10 execute queue, so some callback will be block but finally success
					MaxRetries: 3,
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
				Aq:    1,
				Es:    1,
				Id:    1,
				Nodes: []string{"127.0.0.1"},
				Wq:    1,
			},
		},
		Revision: 1,
	}
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)

	callbackCalled := 0
	successCount := 0
	syncedIds := make([]int64, 0)
	for i := 0; i < 5; i++ {
		entryIndex := i
		callback := func(segmentId int64, entryId int64, err error) {
			callbackCalled++
			assert.NoError(t, err, fmt.Sprintf("entry:%d add fail", entryId))
			assert.Equal(t, int64(1), segmentId)
			assert.Equal(t, int64(entryIndex), entryId)
			if err == nil {
				successCount++
				syncedIds = append(syncedIds, entryId)
			}
		}
		segmentHandle.AppendAsync(context.Background(), []byte(fmt.Sprintf("test_%d", i)), callback)
	}

	// Manually trigger the async callbacks
	segImpl := segmentHandle.(*segmentHandleImpl)
	go func() {
		processedCount := 0
		maxAttempts := 50
		attempts := 0
		for processedCount < 5 && attempts < maxAttempts {
			time.Sleep(20 * time.Millisecond)
			attempts++

			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				appendOp := e.Value.(*AppendOp)
				if len(appendOp.resultChannels) > 0 {
					_ = appendOp.resultChannels[0].SendResult(context.Background(), &channel.AppendResult{
						SyncedId: appendOp.entryId,
						Err:      nil,
					})
					_ = appendOp.resultChannels[0].Close(context.Background())
					processedCount++
				}
			}
		}
	}()

	time.Sleep(1000 * time.Millisecond)
	assert.Equal(t, 5, callbackCalled)
	assert.Equal(t, 5, successCount)
	assert.Equal(t, []int64{0, 1, 2, 3, 4}, syncedIds)
}

func TestDisorderMultiAppendAsync_AllSuccess_InSequential(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)
	for i := 0; i < 20; i++ {
		mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), mock.Anything, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(i), nil)
	}
	cfg, _ := config.NewConfiguration()
	cfg.Woodpecker.Client.SegmentAppend.QueueSize = 10
	cfg.Woodpecker.Client.SegmentAppend.MaxRetries = 2
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)
	_ = tracer.InitTracer(cfg, "test", 1001)

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

	callbackCalledNum := 0
	syncedIds := make([]int64, 0)
	for i := 0; i < 20; i++ {
		entryIndex := i // Capture the loop variable by value
		callback := func(segmentId int64, entryId int64, err error) {
			callbackCalledNum++
			assert.Equal(t, int64(1), segmentId)
			assert.Equal(t, int64(entryIndex), entryId)
			assert.Nil(t, err)
			t.Logf("=====exec callback segID:%d entryID:%d entryIdx:%d callNum:%d \n\n", segmentId, entryId, entryIndex, callbackCalledNum)
			syncedIds = append(syncedIds, entryId)
		}
		segmentHandle.AppendAsync(context.Background(), []byte(fmt.Sprintf("test_%d", i)), callback)
	}
	segImpl := segmentHandle.(*segmentHandleImpl)
	go func() {
		processedCount := 0
		maxAttempts := 10
		attempts := 0
		for processedCount < 20 && attempts < maxAttempts {
			time.Sleep(1000 * time.Millisecond)
			attempts++
			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				appendOp := e.Value.(*AppendOp)
				// when chan ready
				if len(appendOp.resultChannels) > 0 {
					if appendOp.entryId%2 == 0 && appendOp.channelAttempts[0]+1 <= 1 {
						// if attempt=1 and entryId is even, mock fail in this attempt
						t.Logf("start to send %d to chan %d/%d/%d , which in No.%d attempt\n", -1, appendOp.logId, appendOp.segmentId, appendOp.entryId, appendOp.channelAttempts[0])
						_ = appendOp.resultChannels[0].SendResult(context.Background(), &channel.AppendResult{
							SyncedId: -1,
							Err:      nil,
						})
						t.Logf("finish to send %d to chan %d/%d/%d , which in No.%d attempt\n\n", -1, appendOp.logId, appendOp.segmentId, appendOp.entryId, appendOp.channelAttempts[0])
					} else {
						// otherwise, mock success
						t.Logf("start to send %d to chan %d/%d/%d , which in No.%d attempt\n", appendOp.entryId, appendOp.logId, appendOp.segmentId, appendOp.entryId, appendOp.channelAttempts[0])
						_ = appendOp.resultChannels[0].SendResult(context.Background(), &channel.AppendResult{
							SyncedId: appendOp.entryId,
							Err:      nil,
						})
						t.Logf("finish to send %d to chan %d/%d/%d , which in No.%d attempt\n\n", appendOp.entryId, appendOp.logId, appendOp.segmentId, appendOp.entryId, appendOp.channelAttempts[0])
						processedCount++
					}
				}
			}
		}
	}()
	time.Sleep(5000 * time.Millisecond) // Give some time for the async operation to complete
	assert.Equal(t, 20, callbackCalledNum)
	assert.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, syncedIds)
}

func TestSegmentHandleFenceAndClosed(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(0, nil)
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(0, nil)
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
	segmentMetaAfterFenced := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Completed,
			LastEntryId: 1,
			Quorum: &proto.QuorumInfo{
				Id:    1,
				Aq:    1,
				Es:    1,
				Wq:    1,
				Nodes: []string{"127.0.0.1"},
			},
		},
		Revision: 2,
	}
	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(segmentMetaAfterFenced, nil)
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)
	lastEntryId, fenceErr := segmentHandle.FenceAndComplete(context.Background())
	assert.NoError(t, fenceErr)
	assert.Equal(t, int64(0), lastEntryId)
	callbackCalled := false
	callback := func(segmentId int64, entryId int64, err error) {
		callbackCalled = true
		assert.Equal(t, int64(1), segmentId)
		assert.Equal(t, int64(-1), entryId) // indicate the appendOp is fail before add to request queue
		assert.Error(t, err)
		assert.True(t, werr.ErrSegmentFenced.Is(err))
	}
	segmentHandle.AppendAsync(context.Background(), []byte("test"), callback)
	time.Sleep(100 * time.Millisecond) // Give some time for the async operation to complete
	assert.True(t, callbackCalled)

	closeErr := segmentHandle.ForceCompleteAndClose(context.Background())
	assert.NoError(t, closeErr)
}

func TestSendAppendSuccessCallbacks(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{
					QueueSize:  10, // only 10 execute queue, so some callback will be block but finally success
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

	testQueue := list.New()
	segmentHandle := NewSegmentHandleWithAppendOpsQueue(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, testQueue)
	callbackMap := make(map[int64]func(segmentId int64, entryId int64, err error))
	successCount := 0
	successIds := make([]int64, 0)
	ops := make([]*AppendOp, 0)
	for i := 0; i < 10; i++ {
		callback := func(segmentId int64, entryId int64, err error) {
			assert.Equal(t, int64(1), segmentId)
			assert.Equal(t, int64(i), entryId)
			assert.Nil(t, err)
			successCount++
			successIds = append(successIds, entryId)
		}
		callbackMap[int64(i)] = callback
		op := NewAppendOp(
			1,
			1,
			int64(i),
			[]byte(fmt.Sprintf("test_%d", i)),
			callback,
			mockClientPool,
			segmentHandle,
			segmentMeta.Metadata.Quorum)
		ops = append(ops, op)
		testQueue.PushBack(op)
	}

	// success 3-9, but 0-2 is not finish
	for i := 3; i < 10; i++ {
		ops[i].ackSet.Set(0)
		ops[i].completed.Store(true)
		segmentHandle.SendAppendSuccessCallbacks(context.TODO(), int64(i))
	}
	time.Sleep(1000 * time.Millisecond) // Give some time for the async operation to complete
	assert.Equal(t, 0, successCount)
	for i := 0; i < 3; i++ {
		ops[i].ackSet.Set(0)
		ops[i].completed.Store(true)
		segmentHandle.SendAppendSuccessCallbacks(context.TODO(), int64(i))
	}
	time.Sleep(100 * time.Millisecond) // Give some time for the async operation to complete
	assert.Equal(t, 10, successCount)
	assert.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, successIds)
}

func TestSendAppendErrorCallbacks(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil)
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(4, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{
					QueueSize:  10, // only 10 execute queue, so some callback will be block but finally success
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

	testQueue := list.New()
	segmentHandle := NewSegmentHandleWithAppendOpsQueue(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, testQueue)
	callbackMap := make(map[int64]func(segmentId int64, entryId int64, err error))
	successCount := 0
	failedCount := 0
	successIds := make([]int64, 0)
	ops := make([]*AppendOp, 0)
	for i := 0; i < 10; i++ {
		callback := func(segmentId int64, entryId int64, err error) {
			if err == nil {
				successCount++
				successIds = append(successIds, entryId)
			} else {
				failedCount++
			}
		}
		callbackMap[int64(i)] = callback
		op := NewAppendOp(
			1,
			1,
			int64(i),
			[]byte(fmt.Sprintf("test_%d", i)),
			callback,
			mockClientPool,
			segmentHandle,
			segmentMeta.Metadata.Quorum)
		ops = append(ops, op)
		testQueue.PushBack(op)
	}

	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(4, nil)
	// success 3-9, but 0-2 is not finish
	for i := 0; i < 10; i++ {
		if i == 5 {
			// fail
			ops[i].channelAttempts[0] = 3
			segmentHandle.HandleAppendRequestFailure(context.TODO(), int64(i), werr.ErrSegmentHandleWriteFailed, 0, "127.0.0.1")
		} else {
			// success
			ops[i].ackSet.Set(0)
			ops[i].completed.Store(true)
			segmentHandle.SendAppendSuccessCallbacks(context.TODO(), int64(i))
		}
	}
	time.Sleep(1000 * time.Millisecond) // Give some time for the async operation to complete
	assert.Equal(t, 5, successCount)
	assert.Equal(t, 5, failedCount)
	assert.Equal(t, []int64{0, 1, 2, 3, 4}, successIds)
}

// TestFence_WithPendingAppendOps_PartialSuccess tests that Fence correctly handles pending append operations
// with partial success based on lastEntryId returned from FenceSegment
func TestFence_WithPendingAppendOps_PartialSuccess(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)

	// Mock FenceSegment to return lastEntryId = 2, meaning entries 0,1,2 are flushed
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil)

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

	testQueue := list.New()
	segmentHandle := NewSegmentHandleWithAppendOpsQueue(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, testQueue)

	// Create pending append operations with entryIds 0,1,2,3,4
	successCallbacks := make([]bool, 5)
	failCallbacks := make([]bool, 5)
	ops := make([]*AppendOp, 5)

	for i := 0; i < 5; i++ {
		entryIndex := i
		callback := func(segmentId int64, entryId int64, err error) {
			if err == nil {
				successCallbacks[entryIndex] = true
			} else {
				failCallbacks[entryIndex] = true
			}
		}

		op := NewAppendOp(
			1,
			1,
			int64(i),
			[]byte(fmt.Sprintf("test_%d", i)),
			callback,
			mockClientPool,
			segmentHandle,
			segmentMeta.Metadata.Quorum)
		ops[i] = op
		testQueue.PushBack(op)
	}

	// Call Fence - should return lastEntryId = 2
	segmentMetaAfterFenced := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Completed,
			LastEntryId: 2,
		},
		Revision: 2,
	}
	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(segmentMetaAfterFenced, nil)
	lastEntryId, err := segmentHandle.FenceAndComplete(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(2), lastEntryId)

	time.Sleep(100 * time.Millisecond) // Give time for callbacks

	// Verify that entries 0,1,2 got success callbacks (entryId <= lastEntryId)
	assert.True(t, successCallbacks[0], "Entry 0 should succeed")
	assert.True(t, successCallbacks[1], "Entry 1 should succeed")
	assert.True(t, successCallbacks[2], "Entry 2 should succeed")

	// Verify that entries 3,4 got fail callbacks (entryId > lastEntryId)
	assert.True(t, failCallbacks[3], "Entry 3 should fail")
	assert.True(t, failCallbacks[4], "Entry 4 should fail")

	// Verify that success entries didn't get fail callbacks and vice versa
	assert.False(t, failCallbacks[0], "Entry 0 should not fail")
	assert.False(t, failCallbacks[1], "Entry 1 should not fail")
	assert.False(t, failCallbacks[2], "Entry 2 should not fail")
	assert.False(t, successCallbacks[3], "Entry 3 should not succeed")
	assert.False(t, successCallbacks[4], "Entry 4 should not succeed")
}

// TestFence_AlreadyFencedError_WithPendingAppendOps tests Fence when segment is already fenced
// and verifies that lastEntryId is still used correctly for partial success
func TestFence_AlreadyFencedError_WithPendingAppendOps(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(1), nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)

	// Mock FenceSegment to return ErrSegmentFenced but with lastEntryId = 1
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(1), nil)

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

	testQueue := list.New()
	segmentHandle := NewSegmentHandleWithAppendOpsQueue(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, testQueue)

	// Create pending append operations with entryIds 0,1,2
	successCallbacks := make([]bool, 3)
	failCallbacks := make([]bool, 3)

	for i := 0; i < 3; i++ {
		entryIndex := i
		callback := func(segmentId int64, entryId int64, err error) {
			if err == nil {
				successCallbacks[entryIndex] = true
			} else {
				failCallbacks[entryIndex] = true
			}
		}

		op := NewAppendOp(
			1,
			1,
			int64(i),
			[]byte(fmt.Sprintf("test_%d", i)),
			callback,
			mockClientPool,
			segmentHandle,
			segmentMeta.Metadata.Quorum)
		testQueue.PushBack(op)
	}

	// Call Fence - should return lastEntryId = 1 even with ErrSegmentFenced
	segmentMetaAfterFenced := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Completed,
			LastEntryId: 1,
		},
		Revision: 2,
	}
	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(segmentMetaAfterFenced, nil)
	lastEntryId, err := segmentHandle.FenceAndComplete(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(1), lastEntryId)

	time.Sleep(100 * time.Millisecond) // Give time for callbacks

	// Verify that entries 0,1 got success callbacks (entryId <= lastEntryId)
	assert.True(t, successCallbacks[0], "Entry 0 should succeed")
	assert.True(t, successCallbacks[1], "Entry 1 should succeed")

	// Verify that entry 2 got fail callback (entryId > lastEntryId)
	assert.True(t, failCallbacks[2], "Entry 2 should fail")

	// Verify no cross-contamination
	assert.False(t, failCallbacks[0], "Entry 0 should not fail")
	assert.False(t, failCallbacks[1], "Entry 1 should not fail")
	assert.False(t, successCallbacks[2], "Entry 2 should not succeed")
}

// TestSegmentHandle_SetRollingReady_RejectNewAppends tests that once segment is marked as rolling,
// new append operations are rejected with ErrSegmentHandleSegmentRolling
func TestSegmentHandle_SetRollingReady_RejectNewAppends(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(-1), nil).Maybe()
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(-1), nil)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)
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

	// Mark segment as rolling
	segmentHandle.SetRollingReady(context.Background())

	// Verify that IsForceRollingReady returns true
	assert.True(t, segmentHandle.IsForceRollingReady(context.Background()))

	// Try to append after rolling - should be rejected
	callbackCalled := false
	var receivedErr error
	callback := func(segmentId int64, entryId int64, err error) {
		callbackCalled = true
		receivedErr = err
	}

	segmentHandle.AppendAsync(context.Background(), []byte("test"), callback)

	// Give some time for the callback to be called
	time.Sleep(100 * time.Millisecond)

	assert.True(t, callbackCalled)
	assert.Error(t, receivedErr)
	assert.True(t, werr.ErrSegmentHandleSegmentRolling.Is(receivedErr))
}

// TestSegmentHandle_Rolling_AutoCompleteAndClose tests that when segment is marked as rolling
// and all pending appendOps are completed, the segment automatically calls doCompleteAndCloseUnsafe
func TestSegmentHandle_Rolling_AutoCompleteAndClose(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil)
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()

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

	testQueue := list.New()
	segmentHandle := NewSegmentHandleWithAppendOpsQueueWithWritable(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, testQueue, true)

	// Add some pending append operations
	callbackResults := make([]bool, 3)
	for i := 0; i < 3; i++ {
		entryIndex := i
		callback := func(segmentId int64, entryId int64, opErr error) {
			callbackResults[entryIndex] = (opErr == nil)
		}

		op := NewAppendOp(
			1,
			1,
			int64(entryIndex),
			[]byte(fmt.Sprintf("test_%d", entryIndex)),
			callback,
			mockClientPool,
			segmentHandle,
			segmentMeta.Metadata.Quorum)
		testQueue.PushBack(op)
	}

	// Verify we have pending operations
	assert.Equal(t, 3, testQueue.Len())

	// Check if segment is writable - before auto completeAndClose
	writable, _ := segmentHandle.IsWritable(context.Background())
	assert.True(t, writable)

	// Mark segment as rolling
	segmentHandle.SetRollingReady(context.Background())

	// Complete all pending operations by marking them as completed and triggering callbacks
	for i := 0; i < 3; i++ {
		entryIndex := i
		// Find the operation in the queue
		for e := testQueue.Front(); e != nil; e = e.Next() {
			op := e.Value.(*AppendOp)
			if op.entryId == int64(entryIndex) {
				op.ackSet.Set(0)
				op.completed.Store(true)
				break
			}
		}
		// Trigger success callback - this should trigger auto-close when queue becomes empty
		segmentHandle.SendAppendSuccessCallbacks(context.Background(), int64(i))
	}

	// Give some time for the segment to auto-close
	time.Sleep(500 * time.Millisecond)

	// Verify all callbacks were successful
	for i := 0; i < 3; i++ {
		assert.True(t, callbackResults[i], fmt.Sprintf("Entry %d should succeed", i))
	}

	// Verify the queue is empty (this is the key indicator that auto-close happened)
	assert.Equal(t, 0, testQueue.Len())

	// Check if segment is not writable - because it is successfully CompleteAndClose
	writable, _ = segmentHandle.IsWritable(context.Background())
	assert.False(t, writable)
}

// TestSegmentHandle_Rolling_CompleteFlow tests the complete rolling flow:
// 1. Add appendOps to queue
// 2. Mark rolling (should reject new appends)
// 3. Complete existing appendOps
// 4. Verify auto-close when queue becomes empty
func TestSegmentHandle_Rolling_CompleteFlow(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil)
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil)

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

	testQueue := list.New()
	segmentHandle := NewSegmentHandleWithAppendOpsQueue(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, testQueue)

	// Step 1: Add some pending append operations to the queue
	callbackResults := make([]error, 3)
	for i := 0; i < 3; i++ {
		entryIndex := i
		callback := func(segmentId int64, entryId int64, err error) {
			callbackResults[entryIndex] = err
		}

		op := NewAppendOp(
			1,
			1,
			int64(i),
			[]byte(fmt.Sprintf("test_%d", i)),
			callback,
			mockClientPool,
			segmentHandle,
			segmentMeta.Metadata.Quorum)
		testQueue.PushBack(op)
	}

	// Verify we have pending operations
	assert.Equal(t, 3, testQueue.Len())
	assert.False(t, segmentHandle.IsForceRollingReady(context.Background()))

	// Step 2: Mark segment as rolling (should reject new appends)
	segmentHandle.SetRollingReady(context.Background())
	assert.True(t, segmentHandle.IsForceRollingReady(context.Background()))

	// Try to add a new append - should be rejected
	newAppendRejected := false
	segmentHandle.AppendAsync(context.Background(), []byte("new_append"), func(segmentId int64, entryId int64, err error) {
		newAppendRejected = (err != nil && werr.ErrSegmentHandleSegmentRolling.Is(err))
	})
	time.Sleep(50 * time.Millisecond)
	assert.True(t, newAppendRejected, "New append after rolling should be rejected")

	// Step 3: Complete existing appendOps one by one
	for i := 0; i < 3; i++ {
		// Find the operation in the queue and mark it as completed
		for e := testQueue.Front(); e != nil; e = e.Next() {
			op := e.Value.(*AppendOp)
			if op.entryId == int64(i) {
				op.ackSet.Set(0)
				op.completed.Store(true)
				break
			}
		}
		// Trigger success callback - this should trigger auto-close when queue becomes empty
		segmentHandle.SendAppendSuccessCallbacks(context.Background(), int64(i))
	}

	// Give some time for the auto-close to happen
	time.Sleep(200 * time.Millisecond)

	// Step 4: Verify all callbacks were successful
	for i := 0; i < 3; i++ {
		assert.NoError(t, callbackResults[i], fmt.Sprintf("Entry %d should succeed", i))
	}

	// Verify the queue is empty (this is the key indicator that auto-close happened)
	assert.Equal(t, 0, testQueue.Len())
}

// TestSegmentHandle_Rolling_ErrorTriggersRolling tests that an error in appendOp can trigger rolling
func TestSegmentHandle_Rolling_ErrorTriggersRolling(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(0), nil).Maybe()
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil)
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil)

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

	testQueue := list.New()
	segmentHandle := NewSegmentHandleWithAppendOpsQueue(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, testQueue)

	// Add operations to the queue
	callbackResults := make([]error, 3)
	for i := 0; i < 3; i++ {
		entryIndex := i
		callback := func(segmentId int64, entryId int64, err error) {
			callbackResults[entryIndex] = err
		}

		// Set attempt for entry 1 to ensure it fails
		// attempt represents current retry count: 1=first try, 2=first retry, etc.
		// MaxRetries=2 means: attempt < 2 can retry, attempt >= 2 will be removed
		op := NewAppendOp(
			1,
			1,
			int64(i),
			[]byte(fmt.Sprintf("test_%d", i)),
			callback,
			mockClientPool,
			segmentHandle,
			segmentMeta.Metadata.Quorum)
		attempt := 1
		if i == 1 {
			attempt = 2 // This will be >= MaxRetries (2), so it will be removed
		}
		op.channelAttempts[0] = attempt
		testQueue.PushBack(op)
	}

	// Initially not rolling
	assert.False(t, segmentHandle.IsForceRollingReady(context.Background()))
	assert.Equal(t, 3, testQueue.Len())

	// Trigger error for entry 1 - this should trigger rolling and remove entry 1,2 but keep entry 0
	segmentHandle.HandleAppendRequestFailure(context.Background(), int64(1), werr.ErrSegmentHandleWriteFailed, 0, "127.0.0.1")

	// Give some time for processing
	time.Sleep(100 * time.Millisecond)

	// Verify rolling was triggered
	assert.True(t, segmentHandle.IsForceRollingReady(context.Background()))

	// Verify entry 0 is still in queue (should be 1 remaining)
	assert.Equal(t, 1, testQueue.Len())

	// Verify callbacks were called for entry 1 and 2 (should fail)
	for i := 1; i < 3; i++ {
		assert.Error(t, callbackResults[i], fmt.Sprintf("Entry %d should fail", i))
	}

	// Now complete entry 0 successfully - this should trigger auto-close since rolling is active
	for e := testQueue.Front(); e != nil; e = e.Next() {
		op := e.Value.(*AppendOp)
		if op.entryId == 0 {
			op.ackSet.Set(0)
			op.completed.Store(true)
			break
		}
	}
	segmentHandle.SendAppendSuccessCallbacks(context.Background(), int64(0))

	// Give some time for auto-close to happen
	time.Sleep(200 * time.Millisecond)

	// Now the queue should be empty (auto-close happened)
	assert.Equal(t, 0, testQueue.Len())

	// Entry 0 should have succeeded
	assert.NoError(t, callbackResults[0], "Entry 0 should succeed")
}

// TestSegmentHandle_ForceCompleteAndClose_WithRollingState tests ForceCompleteAndClose behavior
// when segment is in rolling state
func TestSegmentHandle_ForceCompleteAndClose_WithRollingState(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil)

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

	// Create a writable segment handle (canWrite=true is needed for ForceCompleteAndClose to work)
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)

	// Verify initial state
	writable, err := segmentHandle.IsWritable(context.Background())
	assert.NoError(t, err)
	assert.True(t, writable)

	// Mark segment as rolling
	segmentHandle.SetRollingReady(context.Background())
	assert.True(t, segmentHandle.IsForceRollingReady(context.Background()))

	// Force complete and close - this should work regardless of rolling state
	err = segmentHandle.ForceCompleteAndClose(context.Background())
	assert.NoError(t, err)

	// Verify the segment is no longer writable after ForceCompleteAndClose
	writable, err = segmentHandle.IsWritable(context.Background())
	assert.NoError(t, err)
	assert.False(t, writable)

	// Subsequent calls to ForceCompleteAndClose should be safe (no-op for readonly segments)
	err = segmentHandle.ForceCompleteAndClose(context.Background())
	assert.NoError(t, err)
}

// TestSegmentHandle_Rolling_ConcurrentAppends tests rolling behavior with concurrent append attempts
func TestSegmentHandle_Rolling_ConcurrentAppends(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)

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

	// First mark segment as rolling
	segmentHandle.SetRollingReady(context.Background())

	// Then start multiple goroutines trying to append - they should all be rejected
	numGoroutines := 5
	results := make([]error, numGoroutines)
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			callback := func(segmentId int64, entryId int64, err error) {
				results[index] = err
			}
			segmentHandle.AppendAsync(context.Background(), []byte(fmt.Sprintf("test_%d", index)), callback)
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Give some time for callbacks
	time.Sleep(100 * time.Millisecond)

	// Count how many operations were rejected due to rolling
	rollingErrors := 0
	for i := 0; i < numGoroutines; i++ {
		if results[i] != nil && werr.ErrSegmentHandleSegmentRolling.Is(results[i]) {
			rollingErrors++
		}
	}

	// All operations should be rejected due to rolling since we marked it as rolling first
	assert.Equal(t, numGoroutines, rollingErrors, "All operations should be rejected due to rolling state")
}

// TestSegmentHandle_Rolling_StateTransitions tests the state transitions during rolling
func TestSegmentHandle_Rolling_StateTransitions(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(0), nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(0), nil).Maybe()

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

	// Initial state
	assert.False(t, segmentHandle.IsForceRollingReady(context.Background()))
	writable, err := segmentHandle.IsWritable(context.Background())
	assert.NoError(t, err)
	assert.True(t, writable)

	// Mark as rolling
	segmentHandle.SetRollingReady(context.Background())

	// Verify rolling state
	assert.True(t, segmentHandle.IsForceRollingReady(context.Background()))
	// Should still be writable until ForceCompleteAndClose is called
	writable, err = segmentHandle.IsWritable(context.Background())
	assert.NoError(t, err)
	assert.False(t, writable) // because empty segmentHandle is force closing when SetRollingReady

	// Force complete and close
	err = segmentHandle.ForceCompleteAndClose(context.Background())
	assert.NoError(t, err)

	// Verify final state
	writable, err = segmentHandle.IsWritable(context.Background())
	assert.NoError(t, err)
	assert.False(t, writable)
}

// TestSegmentHandle_QuorumWrite_Case1_AllNodesSuccess tests quorum write case 1:
// es=wq=3, aq=2, all three nodes succeed for op0, op1, op2
// Expected: all 3 entries should succeed in write and read
func TestSegmentHandle_QuorumWrite_Case1_AllNodesSuccess(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	// Create 3 mock clients for 3 nodes
	mockClient1 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient2 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient3 := mocks_logstore_client.NewLogStoreClient(t)

	// Mock LAC sync calls for all nodes
	mockClient1.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient2.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient3.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()

	// Setup client pool to return appropriate clients
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient1, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient2, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node3").Return(mockClient3, nil).Maybe()

	// Mock AppendEntry calls - all succeed for all 3 entries on all 3 nodes
	for entryId := int64(0); entryId < 3; entryId++ {
		mockClient1.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
			SegId:   1,
			EntryId: entryId,
			Values:  []byte(fmt.Sprintf("test_%d", entryId)),
		}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(entryId, nil)

		mockClient2.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
			SegId:   1,
			EntryId: entryId,
			Values:  []byte(fmt.Sprintf("test_%d", entryId)),
		}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(entryId, nil)

		mockClient3.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
			SegId:   1,
			EntryId: entryId,
			Values:  []byte(fmt.Sprintf("test_%d", entryId)),
		}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(entryId, nil)
	}

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

	// Setup segment with quorum configuration: es=wq=3, aq=2
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Active,
			LastEntryId: -1,
			Quorum: &proto.QuorumInfo{
				Id:    1,
				Es:    3,
				Wq:    3,
				Aq:    2,
				Nodes: []string{"node1", "node2", "node3"},
			},
		},
		Revision: 1,
	}

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)

	// Track callback results for all 3 entries
	callbackResults := make(map[int64]error, 3)
	callbackEntryIds := make(map[int64]int64, 3)
	var mu sync.Mutex

	// Perform async append operations for entries 0, 1, 2
	for entryIndex := int64(0); entryIndex < 3; entryIndex++ {
		callback := func(segmentId int64, entryId int64, err error) {
			mu.Lock()
			defer mu.Unlock()
			callbackResults[entryId] = err
			callbackEntryIds[entryId] = entryId
			t.Logf("Callback for entry %d: entryId=%d, err=%v", entryId, entryId, err)
		}

		segmentHandle.AppendAsync(context.Background(), []byte(fmt.Sprintf("test_%d", entryIndex)), callback)
	}

	// Simulate successful responses from all 3 nodes for all 3 entries
	segImpl := segmentHandle.(*segmentHandleImpl)
	go func() {
		maxAttempts := 100
		attempts := 0
		processedEntries := make(map[int64]bool)

		for attempts < maxAttempts && len(processedEntries) < 3 {
			time.Sleep(20 * time.Millisecond)
			attempts++

			segImpl.Lock()
			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				appendOp := e.Value.(*AppendOp)
				entryId := appendOp.entryId

				if len(appendOp.resultChannels) >= 3 && !processedEntries[entryId] {
					// Send success results from all 3 nodes
					for i := 0; i < 3; i++ {
						if appendOp.resultChannels[i] != nil {
							err := appendOp.resultChannels[i].SendResult(context.Background(), &channel.AppendResult{
								SyncedId: entryId,
								Err:      nil,
							})
							if err != nil {
								t.Logf("Failed to send result to channel %d for entry %d: %v", i, entryId, err)
							}
						}
					}
					processedEntries[entryId] = true
					t.Logf("Processed entry %d with all 3 nodes success", entryId)
				}
			}
			segImpl.Unlock()
		}
	}()

	// Wait for all operations to complete
	time.Sleep(2 * time.Second)

	// Verify results
	mu.Lock()
	defer mu.Unlock()

	// All 3 entries should succeed
	for entryId := int64(0); entryId < 3; entryId++ {
		assert.NoError(t, callbackResults[entryId], "Entry %d should succeed", entryId)
		assert.Equal(t, entryId, callbackEntryIds[entryId], "Entry %d should have correct entryId", entryId)
	}

	// Verify segment is not in rolling state (no failures occurred)
	assert.False(t, segmentHandle.IsForceRollingReady(context.Background()), "Segment should not be rolling when all operations succeed")

	// Verify LAC is updated to the last entry
	assert.Equal(t, int64(2), segImpl.lastAddConfirmed.Load(), "Last add confirmed should be 2")

	t.Logf("=== CASE 1 PASSED: All 3 entries succeeded with es=wq=3, aq=2 ===")
}

// TestSegmentHandle_QuorumWrite_Case2_PartialNodeFailure tests quorum write case 2:
// es=wq=3, aq=2, op0 and op2 succeed normally, op1 has node3 failure but node1,node2 succeed
// Expected: all 3 entries should succeed but segment should be set to rolling
func TestSegmentHandle_QuorumWrite_Case2_PartialNodeFailure(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	// Create 3 mock clients for 3 nodes
	mockClient1 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient2 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient3 := mocks_logstore_client.NewLogStoreClient(t)

	// Mock LAC sync calls for all nodes
	mockClient1.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient2.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient3.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()

	// Mock FenceSegment and CompleteSegment calls (will be triggered when rolling state is triggered)
	mockClient1.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()
	mockClient2.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()
	mockClient3.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()
	mockClient1.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient2.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient3.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()

	// Mock UpdateSegmentMetadata for rolling state
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(&meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Completed, LastEntryId: 2,
		}, Revision: 2,
	}, nil).Maybe()

	// Setup client pool to return appropriate clients
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient1, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient2, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node3").Return(mockClient3, nil).Maybe()

	// Mock AppendEntry calls
	// Entry 0: all nodes succeed
	mockClient1.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)

	// Entry 1: node1,node2 succeed, node3 fails
	mockClient1.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), errors.New("node3 failure"))

	// Entry 2: all nodes succeed
	mockClient1.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil)

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

	// Setup segment with quorum configuration: es=wq=3, aq=2
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Active,
			LastEntryId: -1,
			Quorum: &proto.QuorumInfo{
				Id:    1,
				Es:    3,
				Wq:    3,
				Aq:    2,
				Nodes: []string{"node1", "node2", "node3"},
			},
		},
		Revision: 1,
	}

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)

	// Track callback results for all 3 entries
	callbackResults := make(map[int64]error, 3)
	callbackEntryIds := make(map[int64]int64, 3)
	var mu sync.Mutex

	// Perform async append operations for entries 0, 1, 2
	for entryIndex := int64(0); entryIndex < 3; entryIndex++ {
		callback := func(segmentId int64, entryId int64, err error) {
			mu.Lock()
			defer mu.Unlock()
			callbackResults[entryId] = err
			callbackEntryIds[entryId] = entryId
			t.Logf("Callback for entry %d: entryId=%d, err=%v", entryId, entryId, err)
		}

		segmentHandle.AppendAsync(context.Background(), []byte(fmt.Sprintf("test_%d", entryIndex)), callback)
	}

	// Simulate responses based on the test scenario
	segImpl := segmentHandle.(*segmentHandleImpl)
	go func() {
		maxAttempts := 100
		attempts := 0
		processedEntries := make(map[int64]bool)

		for attempts < maxAttempts && len(processedEntries) < 3 {
			time.Sleep(20 * time.Millisecond)
			attempts++

			segImpl.Lock()
			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				appendOp := e.Value.(*AppendOp)
				entryId := appendOp.entryId

				if len(appendOp.resultChannels) >= 3 && !processedEntries[entryId] {
					if entryId == 0 || entryId == 2 {
						// Entry 0 and 2: All 3 nodes succeed
						for i := 0; i < 3; i++ {
							if appendOp.resultChannels[i] != nil {
								err := appendOp.resultChannels[i].SendResult(context.Background(), &channel.AppendResult{
									SyncedId: entryId,
									Err:      nil,
								})
								if err != nil {
									t.Logf("Failed to send result to channel %d for entry %d: %v", i, entryId, err)
								}
							}
						}
						t.Logf("Processed entry %d with all 3 nodes success", entryId)
					} else if entryId == 1 {
						// Entry 1: node1,node2 succeed, node3 fails
						// Send success results for node1 and node2
						for i := 0; i < 2; i++ {
							if appendOp.resultChannels[i] != nil {
								err := appendOp.resultChannels[i].SendResult(context.Background(), &channel.AppendResult{
									SyncedId: entryId,
									Err:      nil,
								})
								if err != nil {
									t.Logf("Failed to send result to channel %d for entry %d: %v", i, entryId, err)
								}
							}
						}
						// Send failure result for node3
						if appendOp.resultChannels[2] != nil {
							err := appendOp.resultChannels[2].SendResult(context.Background(), &channel.AppendResult{
								SyncedId: -1,
								Err:      errors.New("node3 failure"),
							})
							if err != nil {
								t.Logf("Failed to send result to channel %d for entry %d: %v", 2, entryId, err)
							}
						}
						t.Logf("Processed entry %d with 2 nodes success, 1 node failure", entryId)
					}
					processedEntries[entryId] = true
				}
			}
			segImpl.Unlock()
		}
	}()

	// Wait for all operations to complete
	time.Sleep(2 * time.Second)

	// Verify results
	mu.Lock()
	defer mu.Unlock()

	// All 3 entries should succeed (quorum aq=2 was met for all)
	for entryId := int64(0); entryId < 3; entryId++ {
		assert.NoError(t, callbackResults[entryId], "Entry %d should succeed", entryId)
		assert.Equal(t, entryId, callbackEntryIds[entryId], "Entry %d should have correct entryId", entryId)
	}

	// Verify segment is in rolling state (node3 failure in op1 triggered rolling)
	assert.True(t, segmentHandle.IsForceRollingReady(context.Background()), "Segment should be rolling after node failure")

	// Verify LAC is updated to the last entry
	assert.Equal(t, int64(2), segImpl.lastAddConfirmed.Load(), "Last add confirmed should be 2")

	t.Logf("=== CASE 2 PASSED: All 3 entries succeeded but segment set to rolling due to node3 failure ===")
}

// TestSegmentHandle_QuorumWrite_Case3_QuorumFailure tests quorum write case 3:
// es=wq=3, aq=2, op0 and op2 succeed normally, op1 has only node1 succeed (node2,node3 fail)
// Expected: entry0 succeeds, entry1 and entry2 fail due to quorum not met
func TestSegmentHandle_QuorumWrite_Case3_QuorumFailure(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	// Create 3 mock clients for 3 nodes
	mockClient1 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient2 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient3 := mocks_logstore_client.NewLogStoreClient(t)

	// Mock LAC sync calls for all nodes
	mockClient1.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient2.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient3.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()

	// Mock FenceSegment and CompleteSegment calls (will be triggered when rolling state is triggered)
	mockClient1.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(0), nil).Maybe()
	mockClient2.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(0), nil).Maybe()
	mockClient3.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(0), nil).Maybe()
	mockClient1.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(0), nil).Maybe()
	mockClient2.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(0), nil).Maybe()
	mockClient3.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(0), nil).Maybe()

	// Mock UpdateSegmentMetadata for rolling state
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(&meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Completed, LastEntryId: 0,
		}, Revision: 2,
	}, nil).Maybe()

	// Setup client pool to return appropriate clients
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient1, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient2, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node3").Return(mockClient3, nil).Maybe()

	// Mock AppendEntry calls
	// Entry 0: all nodes succeed
	mockClient1.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)

	// Entry 1: only node1 succeeds, node2,node3 fail
	mockClient1.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), errors.New("node2 failure"))
	mockClient3.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), errors.New("node3 failure"))

	// Entry 2: all nodes succeed (but should fail due to entry 1 failure)
	mockClient1.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil)

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

	// Setup segment with quorum configuration: es=wq=3, aq=2
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Active,
			LastEntryId: -1,
			Quorum: &proto.QuorumInfo{
				Id:    1,
				Es:    3,
				Wq:    3,
				Aq:    2,
				Nodes: []string{"node1", "node2", "node3"},
			},
		},
		Revision: 1,
	}

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)

	// Track callback results for all 3 entries
	callbackResults := make(map[int64]error, 3)
	callbackEntryIds := make(map[int64]int64, 3)
	var mu sync.Mutex

	// Perform async append operations for entries 0, 1, 2
	for entryIndex := int64(0); entryIndex < 3; entryIndex++ {
		callback := func(segmentId int64, entryId int64, err error) {
			mu.Lock()
			defer mu.Unlock()
			callbackResults[entryId] = err
			callbackEntryIds[entryId] = entryId
			t.Logf("Callback for entry %d: entryId=%d, err=%v", entryId, entryId, err)
		}

		segmentHandle.AppendAsync(context.Background(), []byte(fmt.Sprintf("test_%d", entryIndex)), callback)
	}

	// Simulate responses based on the test scenario
	segImpl := segmentHandle.(*segmentHandleImpl)
	go func() {
		maxAttempts := 100
		attempts := 0
		processedEntries := make(map[int64]bool)

		for attempts < maxAttempts && len(processedEntries) < 3 {
			time.Sleep(20 * time.Millisecond)
			attempts++

			segImpl.Lock()
			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				appendOp := e.Value.(*AppendOp)
				entryId := appendOp.entryId

				if len(appendOp.resultChannels) >= 3 && !processedEntries[entryId] {
					if entryId == 0 {
						// Entry 0: All 3 nodes succeed
						for i := 0; i < 3; i++ {
							if appendOp.resultChannels[i] != nil {
								err := appendOp.resultChannels[i].SendResult(context.Background(), &channel.AppendResult{
									SyncedId: entryId,
									Err:      nil,
								})
								if err != nil {
									t.Logf("Failed to send result to channel %d for entry %d: %v", i, entryId, err)
								}
							}
						}
						t.Logf("Processed entry %d with all 3 nodes success", entryId)
					} else if entryId == 1 {
						// Entry 1: only node1 succeeds, node2,node3 fail
						// Send success result for node1
						if appendOp.resultChannels[0] != nil {
							err := appendOp.resultChannels[0].SendResult(context.Background(), &channel.AppendResult{
								SyncedId: entryId,
								Err:      nil,
							})
							if err != nil {
								t.Logf("Failed to send result to channel %d for entry %d: %v", 0, entryId, err)
							}
						}
						// Send failure results for node2 and node3
						for i := 1; i < 3; i++ {
							if appendOp.resultChannels[i] != nil {
								err := appendOp.resultChannels[i].SendResult(context.Background(), &channel.AppendResult{
									SyncedId: -1,
									Err:      errors.New(fmt.Sprintf("node%d failure", i+1)),
								})
								if err != nil {
									t.Logf("Failed to send result to channel %d for entry %d: %v", i, entryId, err)
								}
							}
						}
						t.Logf("Processed entry %d with 1 node success, 2 nodes failure (quorum not met)", entryId)
					} else if entryId == 2 {
						// Entry 2: All 3 nodes succeed (but should be fast-failed due to entry 1 failure)
						for i := 0; i < 3; i++ {
							if appendOp.resultChannels[i] != nil {
								err := appendOp.resultChannels[i].SendResult(context.Background(), &channel.AppendResult{
									SyncedId: entryId,
									Err:      nil,
								})
								if err != nil {
									t.Logf("Failed to send result to channel %d for entry %d: %v", i, entryId, err)
								}
							}
						}
						t.Logf("Processed entry %d with all 3 nodes success (but may be fast-failed)", entryId)
					}
					processedEntries[entryId] = true
				}
			}
			segImpl.Unlock()
		}
	}()

	// Wait for all operations to complete
	time.Sleep(2 * time.Second)

	// Verify results
	mu.Lock()
	defer mu.Unlock()

	// Entry 0 should succeed
	assert.NoError(t, callbackResults[0], "Entry 0 should succeed")
	assert.Equal(t, int64(0), callbackEntryIds[0], "Entry 0 should have correct entryId")

	// Entry 1 should fail (quorum not met: only 1 success < aq=2)
	assert.Error(t, callbackResults[1], "Entry 1 should fail due to quorum not met")

	// Entry 2 should fail (fast-failed due to entry 1 failure creating a hole)
	assert.Error(t, callbackResults[2], "Entry 2 should fail due to entry 1 failure")

	// Verify segment is in rolling state (failures triggered rolling)
	assert.True(t, segmentHandle.IsForceRollingReady(context.Background()), "Segment should be rolling after quorum failure")

	// Verify LAC is updated only to entry 0
	assert.Equal(t, int64(0), segImpl.lastAddConfirmed.Load(), "Last add confirmed should be 0")

	t.Logf("=== CASE 3 PASSED: Entry 0 succeeded, Entry 1&2 failed due to quorum failure ===")
}

// TestSegmentHandle_QuorumWrite_Case4_Op2NodeFailure tests quorum write case 4:
// es=wq=3, aq=2, op0 and op1 succeed normally, op2 has node1 failure but node2,node3 succeed
// Expected: all 3 entries should succeed but segment should be set to rolling
func TestSegmentHandle_QuorumWrite_Case4_Op2NodeFailure(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	// Create 3 mock clients for 3 nodes
	mockClient1 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient2 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient3 := mocks_logstore_client.NewLogStoreClient(t)

	// Mock LAC sync calls for all nodes
	mockClient1.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient2.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient3.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()

	// Mock FenceSegment and CompleteSegment calls (will be triggered when rolling state is triggered)
	mockClient1.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()
	mockClient2.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()
	mockClient3.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()
	mockClient1.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient2.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient3.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()

	// Mock UpdateSegmentMetadata for rolling state
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(&meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Completed, LastEntryId: 2,
		}, Revision: 2,
	}, nil).Maybe()

	// Setup client pool to return appropriate clients
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient1, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient2, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node3").Return(mockClient3, nil).Maybe()

	// Mock AppendEntry calls
	// Entry 0: all nodes succeed
	mockClient1.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)

	// Entry 1: all nodes succeed
	mockClient1.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), nil)

	// Entry 2: node1 fails, node2,node3 succeed
	mockClient1.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), errors.New("node1 failure"))
	mockClient2.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil)

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

	// Setup segment with quorum configuration: es=wq=3, aq=2
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Active,
			LastEntryId: -1,
			Quorum: &proto.QuorumInfo{
				Id:    1,
				Es:    3,
				Wq:    3,
				Aq:    2,
				Nodes: []string{"node1", "node2", "node3"},
			},
		},
		Revision: 1,
	}

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)

	// Track callback results for all 3 entries
	callbackResults := make(map[int64]error, 3)
	callbackEntryIds := make(map[int64]int64, 3)
	var mu sync.Mutex

	// Perform async append operations for entries 0, 1, 2
	for entryIndex := int64(0); entryIndex < 3; entryIndex++ {
		callback := func(segmentId int64, entryId int64, err error) {
			mu.Lock()
			defer mu.Unlock()
			callbackResults[entryId] = err
			callbackEntryIds[entryId] = entryId
			t.Logf("Callback for entry %d: entryId=%d, err=%v", entryId, entryId, err)
		}

		segmentHandle.AppendAsync(context.Background(), []byte(fmt.Sprintf("test_%d", entryIndex)), callback)
	}

	// Simulate responses based on the test scenario
	segImpl := segmentHandle.(*segmentHandleImpl)
	go func() {
		maxAttempts := 100
		attempts := 0
		processedEntries := make(map[int64]bool)

		for attempts < maxAttempts && len(processedEntries) < 3 {
			time.Sleep(20 * time.Millisecond)
			attempts++

			segImpl.Lock()
			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				appendOp := e.Value.(*AppendOp)
				entryId := appendOp.entryId

				if len(appendOp.resultChannels) >= 3 && !processedEntries[entryId] {
					if entryId == 0 || entryId == 1 {
						// Entry 0 and 1: All 3 nodes succeed
						for i := 0; i < 3; i++ {
							if appendOp.resultChannels[i] != nil {
								err := appendOp.resultChannels[i].SendResult(context.Background(), &channel.AppendResult{
									SyncedId: entryId,
									Err:      nil,
								})
								if err != nil {
									t.Logf("Failed to send result to channel %d for entry %d: %v", i, entryId, err)
								}
							}
						}
						t.Logf("Processed entry %d with all 3 nodes success", entryId)
					} else if entryId == 2 {
						// Entry 2: node1 fails, node2,node3 succeed
						// Send failure result for node1
						if appendOp.resultChannels[0] != nil {
							err := appendOp.resultChannels[0].SendResult(context.Background(), &channel.AppendResult{
								SyncedId: -1,
								Err:      errors.New("node1 failure"),
							})
							if err != nil {
								t.Logf("Failed to send result to channel %d for entry %d: %v", 0, entryId, err)
							}
						}
						// Send success results for node2 and node3
						for i := 1; i < 3; i++ {
							if appendOp.resultChannels[i] != nil {
								err := appendOp.resultChannels[i].SendResult(context.Background(), &channel.AppendResult{
									SyncedId: entryId,
									Err:      nil,
								})
								if err != nil {
									t.Logf("Failed to send result to channel %d for entry %d: %v", i, entryId, err)
								}
							}
						}
						t.Logf("Processed entry %d with 2 nodes success, 1 node failure", entryId)
					}
					processedEntries[entryId] = true
				}
			}
			segImpl.Unlock()
		}
	}()

	// Wait for all operations to complete
	time.Sleep(2 * time.Second)

	// Verify results
	mu.Lock()
	defer mu.Unlock()

	// All entries should succeed
	assert.NoError(t, callbackResults[0], "Entry 0 should succeed")
	assert.Equal(t, int64(0), callbackEntryIds[0], "Entry 0 should have correct entryId")

	assert.NoError(t, callbackResults[1], "Entry 1 should succeed")
	assert.Equal(t, int64(1), callbackEntryIds[1], "Entry 1 should have correct entryId")

	assert.NoError(t, callbackResults[2], "Entry 2 should succeed (quorum met despite node1 failure)")
	assert.Equal(t, int64(2), callbackEntryIds[2], "Entry 2 should have correct entryId")

	// Verify segment is in rolling state (node1 failure triggered rolling)
	assert.True(t, segmentHandle.IsForceRollingReady(context.Background()), "Segment should be rolling after node1 failure")

	// Verify LAC is updated to the last entry
	assert.Equal(t, int64(2), segImpl.lastAddConfirmed.Load(), "Last add confirmed should be 2")

	t.Logf("=== CASE 4 PASSED: All 3 entries succeeded but segment set to rolling due to node1 failure in entry 2 ===")
}

// TestSegmentHandle_QuorumWrite_Case5_Op0NodeFailure tests quorum write case 5:
// es=wq=3, aq=2, op1 and op2 succeed normally, op0 has node1 failure but node2,node3 succeed
// Expected: all 3 entries should succeed but segment should be set to rolling
func TestSegmentHandle_QuorumWrite_Case5_Op0NodeFailure(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	// Create 3 mock clients for 3 nodes
	mockClient1 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient2 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient3 := mocks_logstore_client.NewLogStoreClient(t)

	// Mock LAC sync calls for all nodes
	mockClient1.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient2.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient3.EXPECT().UpdateLastAddConfirmed(mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()

	// Mock FenceSegment and CompleteSegment calls (will be triggered when rolling state is triggered)
	mockClient1.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()
	mockClient2.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()
	mockClient3.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()
	mockClient1.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient2.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient3.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()

	// Mock UpdateSegmentMetadata for rolling state
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(&meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Completed, LastEntryId: 2,
		}, Revision: 2,
	}, nil).Maybe()

	// Setup client pool to return appropriate clients
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient1, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient2, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node3").Return(mockClient3, nil).Maybe()

	// Mock AppendEntry calls
	// Entry 0: node1 fails, node2,node3 succeed
	mockClient1.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), errors.New("node1 failure"))
	mockClient2.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)

	// Entry 1: all nodes succeed
	mockClient1.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), nil)

	// Entry 2: all nodes succeed
	mockClient1.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil)

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

	// Setup segment with quorum configuration: es=wq=3, aq=2
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Active,
			LastEntryId: -1,
			Quorum: &proto.QuorumInfo{
				Id:    1,
				Es:    3,
				Wq:    3,
				Aq:    2,
				Nodes: []string{"node1", "node2", "node3"},
			},
		},
		Revision: 1,
	}

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)

	// Track callback results for all 3 entries
	callbackResults := make(map[int64]error, 3)
	callbackEntryIds := make(map[int64]int64, 3)
	var mu sync.Mutex

	// Perform async append operations for entries 0, 1, 2
	for entryIndex := int64(0); entryIndex < 3; entryIndex++ {
		callback := func(segmentId int64, entryId int64, err error) {
			mu.Lock()
			defer mu.Unlock()
			callbackResults[entryId] = err
			callbackEntryIds[entryId] = entryId
			t.Logf("Callback for entry %d: entryId=%d, err=%v", entryId, entryId, err)
		}

		segmentHandle.AppendAsync(context.Background(), []byte(fmt.Sprintf("test_%d", entryIndex)), callback)
	}

	// Simulate responses based on the test scenario
	segImpl := segmentHandle.(*segmentHandleImpl)
	go func() {
		maxAttempts := 100
		attempts := 0
		processedEntries := make(map[int64]bool)

		for attempts < maxAttempts && len(processedEntries) < 3 {
			time.Sleep(20 * time.Millisecond)
			attempts++

			segImpl.Lock()
			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				appendOp := e.Value.(*AppendOp)
				entryId := appendOp.entryId

				if len(appendOp.resultChannels) >= 3 && !processedEntries[entryId] {
					if entryId == 0 {
						// Entry 0: node1 fails, node2,node3 succeed
						// Send failure result for node1
						if appendOp.resultChannels[0] != nil {
							err := appendOp.resultChannels[0].SendResult(context.Background(), &channel.AppendResult{
								SyncedId: -1,
								Err:      errors.New("node1 failure"),
							})
							if err != nil {
								t.Logf("Failed to send result to channel %d for entry %d: %v", 0, entryId, err)
							}
						}
						// Send success results for node2 and node3
						for i := 1; i < 3; i++ {
							if appendOp.resultChannels[i] != nil {
								err := appendOp.resultChannels[i].SendResult(context.Background(), &channel.AppendResult{
									SyncedId: entryId,
									Err:      nil,
								})
								if err != nil {
									t.Logf("Failed to send result to channel %d for entry %d: %v", i, entryId, err)
								}
							}
						}
						t.Logf("Processed entry %d with 2 nodes success, 1 node failure", entryId)
					} else if entryId == 1 || entryId == 2 {
						// Entry 1 and 2: All 3 nodes succeed
						for i := 0; i < 3; i++ {
							if appendOp.resultChannels[i] != nil {
								err := appendOp.resultChannels[i].SendResult(context.Background(), &channel.AppendResult{
									SyncedId: entryId,
									Err:      nil,
								})
								if err != nil {
									t.Logf("Failed to send result to channel %d for entry %d: %v", i, entryId, err)
								}
							}
						}
						t.Logf("Processed entry %d with all 3 nodes success", entryId)
					}
					processedEntries[entryId] = true
				}
			}
			segImpl.Unlock()
		}
	}()

	// Wait for all operations to complete
	time.Sleep(2 * time.Second)

	// Verify results
	mu.Lock()
	defer mu.Unlock()

	// All entries should succeed
	assert.NoError(t, callbackResults[0], "Entry 0 should succeed (quorum met despite node1 failure)")
	assert.Equal(t, int64(0), callbackEntryIds[0], "Entry 0 should have correct entryId")

	assert.NoError(t, callbackResults[1], "Entry 1 should succeed")
	assert.Equal(t, int64(1), callbackEntryIds[1], "Entry 1 should have correct entryId")

	assert.NoError(t, callbackResults[2], "Entry 2 should succeed")
	assert.Equal(t, int64(2), callbackEntryIds[2], "Entry 2 should have correct entryId")

	// Verify segment is in rolling state (node1 failure triggered rolling)
	assert.True(t, segmentHandle.IsForceRollingReady(context.Background()), "Segment should be rolling after node1 failure")

	// Verify LAC is updated to the last entry
	assert.Equal(t, int64(2), segImpl.lastAddConfirmed.Load(), "Last add confirmed should be 2")

	t.Logf("=== CASE 5 PASSED: All 3 entries succeeded but segment set to rolling due to node1 failure in entry 0 ===")
}
