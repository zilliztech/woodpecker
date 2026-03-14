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
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), mock.Anything, mock.Anything).Return(0, nil)
	mockClient.EXPECT().IsRemoteClient().Return(true).Maybe()
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
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
	callbackDone := make(chan struct{}, 1)
	callback := func(segmentId int64, entryId int64, err error) {
		assert.Equal(t, int64(1), segmentId)
		assert.Equal(t, int64(0), entryId)
		assert.Nil(t, err)
		select {
		case callbackDone <- struct{}{}:
		default:
		}
	}
	segmentHandle.AppendAsync(context.Background(), []byte("test"), callback)
	segImpl := segmentHandle.(*segmentHandleImpl)
	op := segImpl.appendOpsQueue.Front().Value
	appendOp := op.(*AppendOp)
	go func(op2 *AppendOp) {
		for i := 0; i < 50; i++ {
			op2.mu.Lock()
			if len(op2.resultChannels) > 0 && op2.resultChannels[0] != nil {
				rc := op2.resultChannels[0]
				op2.mu.Unlock()
				_ = rc.SendResult(context.Background(), &channel.AppendResult{
					SyncedId: 0,
					Err:      nil,
				})
				return
			}
			op2.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		}
		t.Errorf("Wait timeout for append")
	}(appendOp)
	select {
	case <-callbackDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for callback")
	}
}

func TestMultiAppendAsync_AllSuccess_InSequential(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient.EXPECT().IsRemoteClient().Return(true).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	for i := 0; i < 20; i++ {
		mockClient.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
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

	var mu sync.Mutex
	callbackCalledNum := 0
	syncedIds := make([]int64, 0)
	allDone := make(chan struct{}, 1)
	for i := 0; i < 20; i++ {
		entryIndex := i
		callback := func(segmentId int64, entryId int64, err error) {
			mu.Lock()
			defer mu.Unlock()
			callbackCalledNum++
			assert.Equal(t, int64(1), segmentId)
			assert.Equal(t, int64(entryIndex), entryId)
			assert.Nil(t, err)
			syncedIds = append(syncedIds, entryId)
			if callbackCalledNum == 20 {
				select {
				case allDone <- struct{}{}:
				default:
				}
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
		for processedCount < 20 && attempts < maxAttempts {
			time.Sleep(20 * time.Millisecond)
			attempts++

			// Snapshot ops under segment lock to avoid race with queue modifications
			var ops []*AppendOp
			segImpl.RLock()
			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				ops = append(ops, e.Value.(*AppendOp))
			}
			segImpl.RUnlock()

			for _, appendOp := range ops {
				appendOp.mu.Lock()
				if len(appendOp.resultChannels) > 0 && appendOp.resultChannels[0] != nil {
					rc := appendOp.resultChannels[0]
					appendOp.mu.Unlock()
					_ = rc.SendResult(context.Background(), &channel.AppendResult{
						SyncedId: appendOp.entryId,
						Err:      nil,
					})
					processedCount++
				} else {
					appendOp.mu.Unlock()
				}
			}
		}
	}()

	select {
	case <-allDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out waiting for all callbacks")
	}
	mu.Lock()
	assert.Equal(t, 20, callbackCalledNum)
	assert.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, syncedIds)
	mu.Unlock()
}

func TestMultiAppendAsync_PartialSuccess(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(1), nil).Maybe()
	mockClient.EXPECT().IsRemoteClient().Return(true).Maybe()
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	for i := 0; i < 5; i++ {
		ch := make(chan int64, 1)
		ch <- int64(i)
		close(ch)
		if i != 2 {
			// 0,1,3,4 success
			mockClient.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
				SegId:   1,
				EntryId: int64(i),
				Values:  []byte(fmt.Sprintf("test_%d", i)),
			}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(i), nil)
		} else {
			// 2 fail, and retry 3 times
			mockClient.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
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

	var mu sync.Mutex
	failedAttempts := 0
	successAttempts := 0
	syncedIds := make([]int64, 0)
	allDone := make(chan struct{}, 1)
	for i := 0; i < 5; i++ {
		entryIndex := i
		callback := func(segmentId int64, entryId int64, err error) {
			mu.Lock()
			defer mu.Unlock()
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
			if failedAttempts+successAttempts == 5 {
				select {
				case allDone <- struct{}{}:
				default:
				}
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

			// Snapshot ops under segment lock to avoid race with queue modifications
			var ops []*AppendOp
			segImpl.RLock()
			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				ops = append(ops, e.Value.(*AppendOp))
			}
			segImpl.RUnlock()

			for _, appendOp := range ops {
				appendOp.mu.Lock()
				if len(appendOp.resultChannels) > 0 && appendOp.resultChannels[0] != nil {
					rc := appendOp.resultChannels[0]
					appendOp.mu.Unlock()
					_ = rc.SendResult(context.Background(), &channel.AppendResult{
						SyncedId: appendOp.entryId,
						Err:      nil,
					})
					processedCount++
				} else {
					appendOp.mu.Unlock()
				}
			}
		}
	}()

	select {
	case <-allDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out waiting for all callbacks")
	}
	mu.Lock()
	assert.Equal(t, 2, successAttempts) // 0,1
	assert.Equal(t, 3, failedAttempts)  // 2,3,4
	assert.Equal(t, []int64{0, 1}, syncedIds)
	mu.Unlock()
}

// TestAppendAsync_TimeoutBug tests the specific bug where when resultChan.ReadResult times out,
// the callback receives nil error instead of the actual timeout error.
// This test is based on TestMultiAppendAsync_PartialSuccess pattern:
// - Entries 0,1 succeed normally
// - Entry 2 times out by not sending any data to result channel
// - The bug causes the callback to receive nil error instead of timeout error
func TestAppendAsync_TimeoutBug(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping timeout bug test in short mode (takes ~2 minutes)")
	}
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(1), nil).Maybe()
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(1), nil).Maybe()
	mockClient.EXPECT().IsRemoteClient().Return(true).Maybe()

	// Mock for successful entries (0,1) and timeout entry (2)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()

	// Entry 0,1 will succeed with AppendEntry and get result
	for i := 0; i < 2; i++ {
		mockClient.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
			SegId:   1,
			EntryId: int64(i),
			Values:  []byte(fmt.Sprintf("test_%d", i)),
		}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(i), nil)
	}

	// Entry 2 will succeed with AppendEntry but we won't send result to channel (timeout simulation)
	// This will cause multiple retries due to timeout - expect initial attempt + 2 retries = 3 total
	mockClient.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
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

			// Snapshot ops under segment lock to avoid race with queue modifications
			var ops []*AppendOp
			segImpl.RLock()
			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				ops = append(ops, e.Value.(*AppendOp))
			}
			segImpl.RUnlock()

			for _, appendOp := range ops {
				entryId := appendOp.entryId

				appendOp.mu.Lock()
				if len(appendOp.resultChannels) > 0 && appendOp.resultChannels[0] != nil {
					rc := appendOp.resultChannels[0]
					appendOp.mu.Unlock()
					if entryId < 2 && !processedSuccessfully[entryId] {
						// Send success result for entries 0,1
						t.Logf("Sending success result for entry %d", entryId)
						_ = rc.SendResult(context.Background(), &channel.AppendResult{
							SyncedId: entryId,
							Err:      nil,
						})
						processedSuccessfully[entryId] = true
					}
					// For entry 2: DO NOT send any result - this simulates timeout
				} else {
					appendOp.mu.Unlock()
				}
			}

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
		entry2Op.mu.Lock()
		rcLen := len(entry2Op.resultChannels)
		entry2Op.mu.Unlock()
		if rcLen > 0 {
			t.Logf("Entry 2 has %d result channels", rcLen)
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
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient.EXPECT().IsRemoteClient().Return(true).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	for i := 0; i < 5; i++ {
		mockClient.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), mock.Anything, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(i), nil)
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

	var mu sync.Mutex
	callbackCalled := 0
	successCount := 0
	syncedIds := make([]int64, 0)
	allDone := make(chan struct{}, 1)
	for i := 0; i < 5; i++ {
		entryIndex := i
		callback := func(segmentId int64, entryId int64, err error) {
			mu.Lock()
			defer mu.Unlock()
			callbackCalled++
			assert.NoError(t, err, fmt.Sprintf("entry:%d add fail", entryId))
			assert.Equal(t, int64(1), segmentId)
			assert.Equal(t, int64(entryIndex), entryId)
			if err == nil {
				successCount++
				syncedIds = append(syncedIds, entryId)
			}
			if callbackCalled == 5 {
				select {
				case allDone <- struct{}{}:
				default:
				}
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

			// Snapshot ops under segment lock to avoid race with queue modifications
			var ops []*AppendOp
			segImpl.RLock()
			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				ops = append(ops, e.Value.(*AppendOp))
			}
			segImpl.RUnlock()

			for _, appendOp := range ops {
				appendOp.mu.Lock()
				if len(appendOp.resultChannels) > 0 && appendOp.resultChannels[0] != nil {
					rc := appendOp.resultChannels[0]
					appendOp.mu.Unlock()
					_ = rc.SendResult(context.Background(), &channel.AppendResult{
						SyncedId: appendOp.entryId,
						Err:      nil,
					})
					processedCount++
				} else {
					appendOp.mu.Unlock()
				}
			}
		}
	}()

	select {
	case <-allDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out waiting for all callbacks")
	}
	mu.Lock()
	assert.Equal(t, 5, callbackCalled)
	assert.Equal(t, 5, successCount)
	assert.Equal(t, []int64{0, 1, 2, 3, 4}, syncedIds)
	mu.Unlock()
}

func TestDisorderMultiAppendAsync_AllSuccess_InSequential(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient.EXPECT().IsRemoteClient().Return(false).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	for i := 0; i < 20; i++ {
		mockClient.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), mock.Anything, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(i), nil)
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

	var mu sync.Mutex
	callbackCalledNum := 0
	syncedIds := make([]int64, 0)
	allDone := make(chan struct{}, 1)
	for i := 0; i < 20; i++ {
		entryIndex := i // Capture the loop variable by value
		callback := func(segmentId int64, entryId int64, err error) {
			mu.Lock()
			defer mu.Unlock()
			callbackCalledNum++
			assert.Equal(t, int64(1), segmentId)
			assert.Equal(t, int64(entryIndex), entryId)
			assert.Nil(t, err)
			t.Logf("=====exec callback segID:%d entryID:%d entryIdx:%d callNum:%d \n\n", segmentId, entryId, entryIndex, callbackCalledNum)
			syncedIds = append(syncedIds, entryId)
			if callbackCalledNum == 20 {
				select {
				case allDone <- struct{}{}:
				default:
				}
			}
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

			// Snapshot ops under segment lock to avoid race with queue modifications
			var ops []*AppendOp
			segImpl.RLock()
			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				ops = append(ops, e.Value.(*AppendOp))
			}
			segImpl.RUnlock()

			for _, appendOp := range ops {
				appendOp.mu.Lock()
				// when chan ready
				if len(appendOp.resultChannels) > 0 && appendOp.resultChannels[0] != nil {
					rc := appendOp.resultChannels[0]
					chanAttempt := appendOp.channelAttempts[0]
					entryId := appendOp.entryId
					logId := appendOp.logId
					segId := appendOp.segmentId
					appendOp.mu.Unlock()
					if entryId%2 == 0 && chanAttempt+1 <= 1 {
						// if attempt=1 and entryId is even, mock fail in this attempt
						t.Logf("start to send %d to chan %d/%d/%d , which in No.%d attempt\n", -1, logId, segId, entryId, chanAttempt)
						_ = rc.SendResult(context.Background(), &channel.AppendResult{
							SyncedId: -1,
							Err:      nil,
						})
						t.Logf("finish to send %d to chan %d/%d/%d , which in No.%d attempt\n\n", -1, logId, segId, entryId, chanAttempt)
					} else {
						// otherwise, mock success
						t.Logf("start to send %d to chan %d/%d/%d , which in No.%d attempt\n", entryId, logId, segId, entryId, chanAttempt)
						_ = rc.SendResult(context.Background(), &channel.AppendResult{
							SyncedId: entryId,
							Err:      nil,
						})
						t.Logf("finish to send %d to chan %d/%d/%d , which in No.%d attempt\n\n", entryId, logId, segId, entryId, chanAttempt)
						processedCount++
					}
				} else {
					appendOp.mu.Unlock()
				}
			}
		}
	}()
	select {
	case <-allDone:
	case <-time.After(30 * time.Second):
		t.Fatal("Timed out waiting for all callbacks")
	}
	mu.Lock()
	assert.Equal(t, 20, callbackCalledNum)
	assert.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, syncedIds)
	mu.Unlock()
}

func TestSegmentHandleFenceAndClosed(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(0, nil)
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(0, nil)
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
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil)
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
			"a-bucket",
			"files",
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
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil)
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(4, nil).Maybe()
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
			"a-bucket",
			"files",
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

	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(4, nil).Maybe()
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
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)

	// Mock FenceSegment to return lastEntryId = 2, meaning entries 0,1,2 are flushed
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(2), nil)

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
			"a-bucket",
			"files",
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
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(1), nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)

	// Mock FenceSegment to return ErrSegmentFenced but with lastEntryId = 1
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(1), nil)

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
			"a-bucket",
			"files",
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
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(-1), nil).Maybe()
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(-1), nil)
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
// and all pending appendOps are completed, the segment automatically triggers completion
func TestSegmentHandle_Rolling_AutoCompleteAndClose(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil)
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(2), nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()

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
			"a-bucket",
			"files",
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
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil)
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()

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
			"a-bucket",
			"files",
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
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(0), nil).Maybe()
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil)
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()

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
			"a-bucket",
			"files",
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
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(2), nil)

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
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(2), nil)
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
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(0), nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
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

	// Force complete and close (waits for async completion triggered by SetRollingReady)
	err = segmentHandle.ForceCompleteAndClose(context.Background())
	assert.NoError(t, err)

	// Verify final state: segment should not be writable after completion
	writable, err = segmentHandle.IsWritable(context.Background())
	assert.NoError(t, err)
	assert.False(t, writable)
}

// TestSegmentHandle_SetRollingReady_EmptyQueueCompletesSynchronously verifies
// SetRollingReady preserves historical behavior for empty queues: when there
// are no pending append ops, completion is finished before the method returns.
func TestSegmentHandle_SetRollingReady_EmptyQueueCompletesSynchronously(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(0), nil).Once()
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(0), nil).Once()

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

	segmentHandle.SetRollingReady(context.Background())

	writable, err := segmentHandle.IsWritable(context.Background())
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
	mockClient1.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient2.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient3.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()

	mockClient1.EXPECT().IsRemoteClient().Return(true).Maybe()
	mockClient2.EXPECT().IsRemoteClient().Return(true).Maybe()
	mockClient3.EXPECT().IsRemoteClient().Return(true).Maybe()

	// Setup client pool to return appropriate clients
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient1, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient2, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node3").Return(mockClient3, nil).Maybe()

	// Mock AppendEntry calls - all succeed for all 3 entries on all 3 nodes
	for entryId := int64(0); entryId < 3; entryId++ {
		mockClient1.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
			SegId:   1,
			EntryId: entryId,
			Values:  []byte(fmt.Sprintf("test_%d", entryId)),
		}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(entryId, nil)

		mockClient2.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
			SegId:   1,
			EntryId: entryId,
			Values:  []byte(fmt.Sprintf("test_%d", entryId)),
		}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(entryId, nil)

		mockClient3.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
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
	allDone := make(chan struct{}, 1)
	go func() {
		maxAttempts := 100
		attempts := 0
		processedEntries := make(map[int64]bool)

		for attempts < maxAttempts && len(processedEntries) < 3 {
			time.Sleep(20 * time.Millisecond)
			attempts++

			// Snapshot ops under segment lock to avoid race with queue modifications
			var ops []*AppendOp
			segImpl.RLock()
			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				ops = append(ops, e.Value.(*AppendOp))
			}
			segImpl.RUnlock()

			for _, appendOp := range ops {
				entryId := appendOp.entryId

				appendOp.mu.Lock()
				ready := len(appendOp.resultChannels) >= 3
				var rcs []channel.ResultChannel
				if ready {
					rcs = make([]channel.ResultChannel, len(appendOp.resultChannels))
					copy(rcs, appendOp.resultChannels)
				}
				appendOp.mu.Unlock()

				if ready && !processedEntries[entryId] {
					// Send success results from all 3 nodes
					for i := 0; i < 3; i++ {
						if rcs[i] != nil {
							err := rcs[i].SendResult(context.Background(), &channel.AppendResult{
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
		}
		select {
		case allDone <- struct{}{}:
		default:
		}
	}()

	// Wait for all operations to complete
	select {
	case <-allDone:
		time.Sleep(500 * time.Millisecond) // Allow callbacks to complete
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out waiting for operations to complete")
	}

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
	mockClient1.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient2.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient3.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()

	mockClient1.EXPECT().IsRemoteClient().Return(true).Maybe()
	mockClient2.EXPECT().IsRemoteClient().Return(true).Maybe()
	mockClient3.EXPECT().IsRemoteClient().Return(true).Maybe()

	// Mock FenceSegment and CompleteSegment calls (will be triggered when rolling state is triggered)
	mockClient1.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()
	mockClient2.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()
	mockClient3.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()
	mockClient1.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient2.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient3.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()

	// Mock UpdateSegmentMetadata for rolling state
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
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
	mockClient1.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)

	// Entry 1: node1,node2 succeed, node3 fails
	mockClient1.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), errors.New("node3 failure"))

	// Entry 2: all nodes succeed
	mockClient1.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
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
	allDone := make(chan struct{}, 1)
	go func() {
		maxAttempts := 100
		attempts := 0
		processedEntries := make(map[int64]bool)

		for attempts < maxAttempts && len(processedEntries) < 3 {
			time.Sleep(20 * time.Millisecond)
			attempts++

			var ops []*AppendOp
			segImpl.RLock()
			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				ops = append(ops, e.Value.(*AppendOp))
			}
			segImpl.RUnlock()

			for _, appendOp := range ops {
				entryId := appendOp.entryId

				appendOp.mu.Lock()
				ready := len(appendOp.resultChannels) >= 3
				var rcs []channel.ResultChannel
				if ready {
					rcs = make([]channel.ResultChannel, len(appendOp.resultChannels))
					copy(rcs, appendOp.resultChannels)
				}
				appendOp.mu.Unlock()

				if ready && !processedEntries[entryId] {
					if entryId == 0 || entryId == 2 {
						for i := 0; i < 3; i++ {
							if rcs[i] != nil {
								err := rcs[i].SendResult(context.Background(), &channel.AppendResult{
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
						for i := 0; i < 2; i++ {
							if rcs[i] != nil {
								err := rcs[i].SendResult(context.Background(), &channel.AppendResult{
									SyncedId: entryId,
									Err:      nil,
								})
								if err != nil {
									t.Logf("Failed to send result to channel %d for entry %d: %v", i, entryId, err)
								}
							}
						}
						if rcs[2] != nil {
							err := rcs[2].SendResult(context.Background(), &channel.AppendResult{
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
		}
		select {
		case allDone <- struct{}{}:
		default:
		}
	}()

	select {
	case <-allDone:
		time.Sleep(500 * time.Millisecond)
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out waiting for operations to complete")
	}

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
	mockClient1.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient2.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient3.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()

	mockClient1.EXPECT().IsRemoteClient().Return(true).Maybe()
	mockClient2.EXPECT().IsRemoteClient().Return(true).Maybe()
	mockClient3.EXPECT().IsRemoteClient().Return(true).Maybe()

	// Mock FenceSegment and CompleteSegment calls (will be triggered when rolling state is triggered)
	mockClient1.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(0), nil).Maybe()
	mockClient2.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(0), nil).Maybe()
	mockClient3.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(0), nil).Maybe()
	mockClient1.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(0), nil).Maybe()
	mockClient2.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(0), nil).Maybe()
	mockClient3.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(0), nil).Maybe()

	// Mock UpdateSegmentMetadata for rolling state
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
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
	mockClient1.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)

	// Entry 1: only node1 succeeds, node2,node3 fail
	mockClient1.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), errors.New("node2 failure"))
	mockClient3.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), errors.New("node3 failure"))

	// Entry 2: all nodes succeed (but should fail due to entry 1 failure)
	mockClient1.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
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
	allDone := make(chan struct{}, 1)
	go func() {
		maxAttempts := 100
		attempts := 0
		processedEntries := make(map[int64]bool)

		for attempts < maxAttempts && len(processedEntries) < 3 {
			time.Sleep(20 * time.Millisecond)
			attempts++

			// Snapshot ops under segment lock to avoid race with queue modifications
			var ops []*AppendOp
			segImpl.RLock()
			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				ops = append(ops, e.Value.(*AppendOp))
			}
			segImpl.RUnlock()

			for _, appendOp := range ops {
				entryId := appendOp.entryId

				appendOp.mu.Lock()
				ready := len(appendOp.resultChannels) >= 3
				var rcs []channel.ResultChannel
				if ready {
					rcs = make([]channel.ResultChannel, len(appendOp.resultChannels))
					copy(rcs, appendOp.resultChannels)
				}
				appendOp.mu.Unlock()

				if ready && !processedEntries[entryId] {
					if entryId == 0 {
						// Entry 0: All 3 nodes succeed
						for i := 0; i < 3; i++ {
							if rcs[i] != nil {
								err := rcs[i].SendResult(context.Background(), &channel.AppendResult{
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
						if rcs[0] != nil {
							err := rcs[0].SendResult(context.Background(), &channel.AppendResult{
								SyncedId: entryId,
								Err:      nil,
							})
							if err != nil {
								t.Logf("Failed to send result to channel %d for entry %d: %v", 0, entryId, err)
							}
						}
						// Send failure results for node2 and node3
						for i := 1; i < 3; i++ {
							if rcs[i] != nil {
								err := rcs[i].SendResult(context.Background(), &channel.AppendResult{
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
							if rcs[i] != nil {
								err := rcs[i].SendResult(context.Background(), &channel.AppendResult{
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
		}
		select {
		case allDone <- struct{}{}:
		default:
		}
	}()

	// Wait for all operations to complete
	select {
	case <-allDone:
		time.Sleep(500 * time.Millisecond) // Allow callbacks to complete
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out waiting for operations to complete")
	}

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
	mockClient1.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient2.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient3.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()

	mockClient1.EXPECT().IsRemoteClient().Return(true).Maybe()
	mockClient2.EXPECT().IsRemoteClient().Return(true).Maybe()
	mockClient3.EXPECT().IsRemoteClient().Return(true).Maybe()

	// Mock FenceSegment and CompleteSegment calls (will be triggered when rolling state is triggered)
	mockClient1.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()
	mockClient2.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()
	mockClient3.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()
	mockClient1.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient2.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient3.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()

	// Mock UpdateSegmentMetadata for rolling state
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
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
	mockClient1.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)

	// Entry 1: all nodes succeed
	mockClient1.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), nil)

	// Entry 2: node1 fails, node2,node3 succeed
	mockClient1.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), errors.New("node1 failure"))
	mockClient2.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
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
	allDone := make(chan struct{}, 1)
	go func() {
		maxAttempts := 100
		attempts := 0
		processedEntries := make(map[int64]bool)

		for attempts < maxAttempts && len(processedEntries) < 3 {
			time.Sleep(20 * time.Millisecond)
			attempts++

			// Snapshot ops under segment lock to avoid race with queue modifications
			var ops []*AppendOp
			segImpl.RLock()
			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				ops = append(ops, e.Value.(*AppendOp))
			}
			segImpl.RUnlock()

			for _, appendOp := range ops {
				entryId := appendOp.entryId

				appendOp.mu.Lock()
				ready := len(appendOp.resultChannels) >= 3
				var rcs []channel.ResultChannel
				if ready {
					rcs = make([]channel.ResultChannel, len(appendOp.resultChannels))
					copy(rcs, appendOp.resultChannels)
				}
				appendOp.mu.Unlock()

				if ready && !processedEntries[entryId] {
					if entryId == 0 || entryId == 1 {
						// Entry 0 and 1: All 3 nodes succeed
						for i := 0; i < 3; i++ {
							if rcs[i] != nil {
								err := rcs[i].SendResult(context.Background(), &channel.AppendResult{
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
						if rcs[0] != nil {
							err := rcs[0].SendResult(context.Background(), &channel.AppendResult{
								SyncedId: -1,
								Err:      errors.New("node1 failure"),
							})
							if err != nil {
								t.Logf("Failed to send result to channel %d for entry %d: %v", 0, entryId, err)
							}
						}
						// Send success results for node2 and node3
						for i := 1; i < 3; i++ {
							if rcs[i] != nil {
								err := rcs[i].SendResult(context.Background(), &channel.AppendResult{
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
		}
		select {
		case allDone <- struct{}{}:
		default:
		}
	}()

	// Wait for all operations to complete
	select {
	case <-allDone:
		time.Sleep(500 * time.Millisecond) // Allow callbacks to complete
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out waiting for operations to complete")
	}

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
	mockClient1.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient2.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClient3.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()

	mockClient1.EXPECT().IsRemoteClient().Return(true).Maybe()
	mockClient2.EXPECT().IsRemoteClient().Return(true).Maybe()
	mockClient3.EXPECT().IsRemoteClient().Return(true).Maybe()

	// Mock FenceSegment and CompleteSegment calls (will be triggered when rolling state is triggered)
	mockClient1.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()
	mockClient2.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()
	mockClient3.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()
	mockClient1.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient2.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()
	mockClient3.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(2), nil).Maybe()

	// Mock UpdateSegmentMetadata for rolling state
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
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
	mockClient1.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), errors.New("node1 failure"))
	mockClient2.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 0, Values: []byte("test_0"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(0), nil)

	// Entry 1: all nodes succeed
	mockClient1.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 1, Values: []byte("test_1"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(1), nil)

	// Entry 2: all nodes succeed
	mockClient1.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
		SegId: 1, EntryId: 2, Values: []byte("test_2"),
	}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(2), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), &proto.LogEntry{
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
	allDone := make(chan struct{}, 1)
	go func() {
		maxAttempts := 100
		attempts := 0
		processedEntries := make(map[int64]bool)

		for attempts < maxAttempts && len(processedEntries) < 3 {
			time.Sleep(20 * time.Millisecond)
			attempts++

			// Snapshot ops under segment lock to avoid race with queue modifications
			var ops []*AppendOp
			segImpl.RLock()
			for e := segImpl.appendOpsQueue.Front(); e != nil; e = e.Next() {
				ops = append(ops, e.Value.(*AppendOp))
			}
			segImpl.RUnlock()

			for _, appendOp := range ops {
				entryId := appendOp.entryId

				appendOp.mu.Lock()
				ready := len(appendOp.resultChannels) >= 3
				var rcs []channel.ResultChannel
				if ready {
					rcs = make([]channel.ResultChannel, len(appendOp.resultChannels))
					copy(rcs, appendOp.resultChannels)
				}
				appendOp.mu.Unlock()

				if ready && !processedEntries[entryId] {
					if entryId == 0 {
						// Entry 0: node1 fails, node2,node3 succeed
						// Send failure result for node1
						if rcs[0] != nil {
							err := rcs[0].SendResult(context.Background(), &channel.AppendResult{
								SyncedId: -1,
								Err:      errors.New("node1 failure"),
							})
							if err != nil {
								t.Logf("Failed to send result to channel %d for entry %d: %v", 0, entryId, err)
							}
						}
						// Send success results for node2 and node3
						for i := 1; i < 3; i++ {
							if rcs[i] != nil {
								err := rcs[i].SendResult(context.Background(), &channel.AppendResult{
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
							if rcs[i] != nil {
								err := rcs[i].SendResult(context.Background(), &channel.AppendResult{
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
		}
		select {
		case allDone <- struct{}{}:
		default:
		}
	}()

	// Wait for all operations to complete
	select {
	case <-allDone:
		time.Sleep(500 * time.Millisecond) // Allow callbacks to complete
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out waiting for operations to complete")
	}

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

func TestSegmentHandle_HandleAppendRequestFailure_RetrySubmitFailed(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	// Use QueueSize=1 so the executor queue fills up easily
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{
					QueueSize:  1,
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
	segmentHandle := NewSegmentHandleWithAppendOpsQueueWithWritable(
		context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, testQueue, true)
	segImpl := segmentHandle.(*segmentHandleImpl)

	// Start the executor
	segImpl.executor.Start(context.Background())
	defer segImpl.executor.Stop(context.Background())

	// Block the worker so the queue fills up
	var workerBlocked sync.WaitGroup
	workerBlocked.Add(1)
	blockingOp := createBlockingMockOperation(&workerBlocked)
	submitted := segImpl.executor.Submit(context.Background(), blockingOp)
	assert.True(t, submitted, "blocking op should be submitted")

	// Give the worker time to pick up the blocking op
	time.Sleep(20 * time.Millisecond)

	// Fill the remaining queue (size=1)
	fillerOp := createMockOperation(0)
	submitted = segImpl.executor.Submit(context.Background(), fillerOp)
	assert.True(t, submitted, "filler op should be submitted")

	// Now the executor queue is full. Add an op to the appendOpsQueue that is retryable.
	var callbackErr error
	var callbackCalled bool
	callback := func(segmentId int64, entryId int64, err error) {
		callbackErr = err
		callbackCalled = true
	}

	retryableOp := NewAppendOp(
		"a-bucket", "files", 1, 1, 0,
		[]byte("test_data"), callback,
		mockClientPool, segmentHandle,
		segmentMeta.Metadata.Quorum)
	retryableOp.channelAttempts[0] = 0 // first attempt, retryable
	testQueue.PushBack(retryableOp)

	assert.Equal(t, 1, testQueue.Len())

	// Trigger HandleAppendRequestFailure - the op is retryable but Submit will fail
	// because the queue is full and the context will be cancelled.
	// With blocking Submit, we use a short-lived context so Submit unblocks via ctx.Done().
	ctxWithTimeout, cancelTimeout := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelTimeout()
	segmentHandle.HandleAppendRequestFailure(ctxWithTimeout, 0, nil, 0, "127.0.0.1")

	// The op should have been FastFail'd and removed from the queue
	assert.Equal(t, 0, testQueue.Len(), "Op should be removed from queue after retry submit failure")
	assert.True(t, callbackCalled, "Callback should be called")
	assert.Error(t, callbackErr, "Callback should receive an error")
	assert.True(t, werr.ErrAppendOpRetrySubmitFailed.Is(callbackErr),
		"Error should be ErrAppendOpRetrySubmitFailed, got: %v", callbackErr)

	// Release the worker
	workerBlocked.Done()
}

func TestCalculateLAC(t *testing.T) {
	s := &segmentHandleImpl{}

	tests := []struct {
		name      string
		results   []int64
		ackQuorum int
		expected  int64
	}{
		{
			name:      "normal 3 nodes",
			results:   []int64{4, 6, 7},
			ackQuorum: 2,
			expected:  6,
		},
		{
			name:      "all same value",
			results:   []int64{5, 5, 5},
			ackQuorum: 2,
			expected:  5,
		},
		{
			name:      "one node empty returns -1",
			results:   []int64{0, -1},
			ackQuorum: 2,
			expected:  0,
		},
		{
			name:      "bug scenario: fence with [0, -1] ackQuorum=2",
			results:   []int64{0, -1},
			ackQuorum: 2,
			expected:  0,
		},
		{
			name:      "two valid one empty",
			results:   []int64{-1, 0, 0},
			ackQuorum: 2,
			expected:  0,
		},
		{
			name:      "multiple entries with one empty node",
			results:   []int64{2, -1},
			ackQuorum: 2,
			expected:  2,
		},
		{
			name:      "all nodes empty",
			results:   []int64{-1, -1},
			ackQuorum: 2,
			expected:  -1,
		},
		{
			name:      "single empty node",
			results:   []int64{-1},
			ackQuorum: 1,
			expected:  -1,
		},
		{
			name:      "empty results",
			results:   []int64{},
			ackQuorum: 2,
			expected:  -1,
		},
		{
			name:      "single valid node",
			results:   []int64{0},
			ackQuorum: 1,
			expected:  0,
		},
		{
			name:      "ackQuorum exceeds valid count uses index 0",
			results:   []int64{3},
			ackQuorum: 2,
			expected:  3,
		},
		{
			name:      "three valid nodes ackQuorum=2",
			results:   []int64{10, 8, 5},
			ackQuorum: 2,
			expected:  8,
		},
		{
			name:      "negative sentinel other than -1",
			results:   []int64{0, -2, 3},
			ackQuorum: 2,
			expected:  0,
		},
		{
			name:      "does not mutate input",
			results:   []int64{7, 3, 5},
			ackQuorum: 2,
			expected:  5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// copy input to verify no mutation
			original := make([]int64, len(tt.results))
			copy(original, tt.results)

			got := s.calculateLAC(tt.results, tt.ackQuorum)
			assert.Equal(t, tt.expected, got)

			// verify input slice is not mutated
			assert.Equal(t, original, tt.results, "input slice should not be mutated")
		})
	}
}

// TestGetLogName tests the GetLogName method returns the correct log name
func TestGetLogName(t *testing.T) {
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

	tests := []struct {
		name    string
		logName string
	}{
		{"simple_name", "testLog"},
		{"empty_name", ""},
		{"complex_name", "namespace/collection/partition/log-123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			segmentMeta := &meta.SegmentMeta{
				Metadata: &proto.SegmentMetadata{
					SegNo: 1,
				},
				Revision: 1,
			}
			segmentHandle := NewSegmentHandle(context.Background(), 1, tt.logName, segmentMeta, mockMetadata, mockClientPool, cfg, false)
			assert.Equal(t, tt.logName, segmentHandle.GetLogName())
		})
	}
}

// TestGetLastAddPushed tests the GetLastAddPushed method
func TestGetLastAddPushed(t *testing.T) {
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

	tests := []struct {
		name        string
		lastEntryId int64
		expected    int64
	}{
		{"initial_minus_one", -1, -1},
		{"zero", 0, 0},
		{"positive", 42, 42},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			segmentMeta := &meta.SegmentMeta{
				Metadata: &proto.SegmentMetadata{
					SegNo:       1,
					LastEntryId: tt.lastEntryId,
				},
				Revision: 1,
			}
			segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
			lastPushed, err := segmentHandle.GetLastAddPushed(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, lastPushed)
		})
	}
}

// TestGetMetadata tests the GetMetadata method returns the correct segment metadata
func TestGetMetadata(t *testing.T) {
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
			SegNo:       5,
			State:       proto.SegmentState_Active,
			LastEntryId: 100,
			Size:        2048,
		},
		Revision: 3,
	}
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	result := segmentHandle.GetMetadata(context.Background())
	assert.NotNil(t, result)
	assert.Equal(t, int64(5), result.Metadata.SegNo)
	assert.Equal(t, proto.SegmentState_Active, result.Metadata.State)
	assert.Equal(t, int64(100), result.Metadata.LastEntryId)
	assert.Equal(t, int64(2048), result.Metadata.Size)
}

// TestRefreshAndGetMetadata_Success tests refreshing metadata successfully
func TestRefreshAndGetMetadata_Success(t *testing.T) {
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
		},
		Revision: 1,
	}

	updatedMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Completed,
			LastEntryId: 50,
			Size:        4096,
		},
		Revision: 2,
	}

	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(updatedMeta, nil)

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)

	err := segmentHandle.RefreshAndGetMetadata(context.Background())
	assert.NoError(t, err)

	// Verify metadata was updated
	result := segmentHandle.GetMetadata(context.Background())
	assert.Equal(t, proto.SegmentState_Completed, result.Metadata.State)
	assert.Equal(t, int64(50), result.Metadata.LastEntryId)
	assert.Equal(t, int64(4096), result.Metadata.Size)
}

// TestRefreshAndGetMetadata_Error tests refreshing metadata when provider returns error
func TestRefreshAndGetMetadata_Error(t *testing.T) {
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
		},
		Revision: 1,
	}

	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(nil, errors.New("etcd connection failed"))

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)

	err := segmentHandle.RefreshAndGetMetadata(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "etcd connection failed")

	// Verify metadata was NOT updated (still original)
	result := segmentHandle.GetMetadata(context.Background())
	assert.Equal(t, proto.SegmentState_Active, result.Metadata.State)
	assert.Equal(t, int64(-1), result.Metadata.LastEntryId)
}

// TestGetSize tests the GetSize method returns submitted + committed size
func TestGetSize(t *testing.T) {
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

	tests := []struct {
		name         string
		initialSize  int64
		expectedSize int64
	}{
		{"zero_size", 0, 0},
		{"with_committed_size", 1024, 1024},
		{"large_size", 64 * 1024 * 1024, 64 * 1024 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			segmentMeta := &meta.SegmentMeta{
				Metadata: &proto.SegmentMetadata{
					SegNo:       1,
					State:       proto.SegmentState_Active,
					LastEntryId: -1,
					Size:        tt.initialSize,
				},
				Revision: 1,
			}
			segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
			size := segmentHandle.GetSize(context.Background())
			assert.Equal(t, tt.expectedSize, size)
		})
	}
}

// TestGetBlocksCount_CompletedSegment tests GetBlocksCount returns 0 for completed segments
func TestGetBlocksCount_CompletedSegment(t *testing.T) {
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
			State:       proto.SegmentState_Completed,
			LastEntryId: 10,
		},
		Revision: 1,
	}
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	blocksCount := segmentHandle.GetBlocksCount(context.Background())
	assert.Equal(t, int64(0), blocksCount)
}

// TestGetBlocksCount_ActiveSegment_MultipleNodes tests GetBlocksCount returns 0 for multi-node quorum
func TestGetBlocksCount_ActiveSegment_MultipleNodes(t *testing.T) {
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
				Aq:    2,
				Es:    3,
				Wq:    3,
				Nodes: []string{"node1", "node2", "node3"},
			},
		},
		Revision: 1,
	}
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	blocksCount := segmentHandle.GetBlocksCount(context.Background())
	// Multi-node quorum returns 0 (blocks count not relevant for service mode)
	assert.Equal(t, int64(0), blocksCount)
}

// TestGetBlocksCount_ActiveSegment_SingleNode tests GetBlocksCount for single-node quorum
func TestGetBlocksCount_ActiveSegment_SingleNode(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "127.0.0.1").Return(mockClient, nil)
	mockClient.EXPECT().GetBlockCount(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(7), nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	blocksCount := segmentHandle.GetBlocksCount(context.Background())
	assert.Equal(t, int64(7), blocksCount)
}

// TestGetBlocksCount_ActiveSegment_ClientError tests GetBlocksCount returns 0 on client error
func TestGetBlocksCount_ActiveSegment_ClientError(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "127.0.0.1").Return(nil, errors.New("client unavailable"))
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	blocksCount := segmentHandle.GetBlocksCount(context.Background())
	assert.Equal(t, int64(0), blocksCount)
}

// TestGetBlocksCount_ActiveSegment_GetBlockCountError tests GetBlocksCount returns 0 when GetBlockCount fails
func TestGetBlocksCount_ActiveSegment_GetBlockCountError(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "127.0.0.1").Return(mockClient, nil)
	mockClient.EXPECT().GetBlockCount(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(0), errors.New("block count error"))
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	blocksCount := segmentHandle.GetBlocksCount(context.Background())
	assert.Equal(t, int64(0), blocksCount)
}

// TestGetLastAddConfirmed_CompletedSegment tests GetLastAddConfirmed for completed segment
func TestGetLastAddConfirmed_CompletedSegment(t *testing.T) {
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
			State:       proto.SegmentState_Completed,
			LastEntryId: 42,
		},
		Revision: 1,
	}
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	lac, err := segmentHandle.GetLastAddConfirmed(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(42), lac)
}

// TestGetLastAddConfirmed_ActiveSegment_Success tests GetLastAddConfirmed for active segment via client
func TestGetLastAddConfirmed_ActiveSegment_Success(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "127.0.0.1").Return(mockClient, nil)
	mockClient.EXPECT().GetLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(99), nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	lac, err := segmentHandle.GetLastAddConfirmed(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(99), lac)
}

// TestGetLastAddConfirmed_ActiveSegment_AllNodesFail tests GetLastAddConfirmed when all nodes fail
func TestGetLastAddConfirmed_ActiveSegment_AllNodesFail(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "127.0.0.1").Return(mockClient, nil)
	mockClient.EXPECT().GetLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(-1), errors.New("node unavailable"))
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	lac, err := segmentHandle.GetLastAddConfirmed(context.Background())
	assert.Error(t, err)
	assert.Equal(t, int64(-1), lac)
}

// TestReadBatchAdv_Success tests ReadBatchAdv successfully reads from first node
func TestReadBatchAdv_Success(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "127.0.0.1").Return(mockClient, nil)

	expectedEntries := []*proto.LogEntry{
		{SegId: 1, EntryId: 0, Values: []byte("entry_0")},
		{SegId: 1, EntryId: 1, Values: []byte("entry_1")},
	}
	expectedResult := &proto.BatchReadResult{
		Entries:       expectedEntries,
		LastReadState: &proto.LastReadState{},
	}
	mockClient.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(0), int64(10), (*proto.LastReadState)(nil)).Return(expectedResult, nil)

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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	result, err := segmentHandle.ReadBatchAdv(context.Background(), 0, 10, nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 2, len(result.Entries))
	assert.Equal(t, "127.0.0.1", result.LastReadState.Node)
}

// TestReadBatchAdv_AllNodesFail tests ReadBatchAdv when all nodes fail
func TestReadBatchAdv_AllNodesFail(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "127.0.0.1").Return(mockClient, nil)
	mockClient.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(0), int64(10), (*proto.LastReadState)(nil)).Return(nil, errors.New("read failed"))

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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	result, err := segmentHandle.ReadBatchAdv(context.Background(), 0, 10, nil)
	assert.Error(t, err)
	assert.Nil(t, result)
}

// TestReadBatchAdv_WithLastReadState tests ReadBatchAdv with lastReadState directing to a specific node
func TestReadBatchAdv_WithLastReadState(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient2 := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient2, nil)

	expectedResult := &proto.BatchReadResult{
		Entries: []*proto.LogEntry{
			{SegId: 1, EntryId: 5, Values: []byte("entry_5")},
		},
		LastReadState: &proto.LastReadState{Node: "node2"},
	}
	mockClient2.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(5), int64(10), mock.Anything).Return(expectedResult, nil)

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
				Aq:    2,
				Es:    3,
				Wq:    3,
				Nodes: []string{"node1", "node2", "node3"},
			},
		},
		Revision: 1,
	}
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	lastReadState := &proto.LastReadState{Node: "node2"}
	result, err := segmentHandle.ReadBatchAdv(context.Background(), 5, 10, lastReadState)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, len(result.Entries))
	assert.Equal(t, "node2", result.LastReadState.Node)
}

// TestGetLastAccessTime tests the GetLastAccessTime method
func TestGetLastAccessTime(t *testing.T) {
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

	beforeCreate := time.Now().UnixMilli()
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	afterCreate := time.Now().UnixMilli()

	lastAccess := segmentHandle.GetLastAccessTime()
	assert.GreaterOrEqual(t, lastAccess, beforeCreate)
	assert.LessOrEqual(t, lastAccess, afterCreate)
}

// TestGetLastAccessTime_UpdatesOnAccess tests that access time is updated on method calls
func TestGetLastAccessTime_UpdatesOnAccess(t *testing.T) {
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
			State:       proto.SegmentState_Completed,
			LastEntryId: 10,
		},
		Revision: 1,
	}

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	initialAccess := segmentHandle.GetLastAccessTime()

	// Wait briefly to ensure time difference
	time.Sleep(5 * time.Millisecond)

	// Access via GetLastAddConfirmed (which calls updateAccessTime)
	_, _ = segmentHandle.GetLastAddConfirmed(context.Background())

	updatedAccess := segmentHandle.GetLastAccessTime()
	assert.Greater(t, updatedAccess, initialAccess)
}

// TestComplete_Success tests the Complete method when fencing and completing succeed
func TestComplete_Success(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "127.0.0.1").Return(mockClient, nil)
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(5), nil)
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(int64(5), nil)

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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	lastEntryId, err := segmentHandle.Complete(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(5), lastEntryId)
}

// TestComplete_FenceFails tests the Complete method when fence fails
func TestComplete_FenceFails(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "127.0.0.1").Return(mockClient, nil)
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(-1), errors.New("fence failed"))

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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	lastEntryId, err := segmentHandle.Complete(context.Background())
	assert.Error(t, err)
	assert.Equal(t, int64(-1), lastEntryId)
}

// TestCompact_LocalStorage tests that Compact skips compaction for local storage
func TestCompact_LocalStorage(t *testing.T) {
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
			Storage: config.StorageConfig{
				Type: "local",
			},
		},
	}

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Completed,
			LastEntryId: 10,
		},
		Revision: 1,
	}
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	err := segmentHandle.Compact(context.Background())
	assert.NoError(t, err)
}

// TestCompact_NotCompletedState tests that Compact fails when segment is not in Completed state
func TestCompact_NotCompletedState(t *testing.T) {
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
			Storage: config.StorageConfig{
				Type: "minio",
			},
		},
	}

	tests := []struct {
		name  string
		state proto.SegmentState
	}{
		{"active_state", proto.SegmentState_Active},
		{"sealed_state", proto.SegmentState_Sealed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			segmentMeta := &meta.SegmentMeta{
				Metadata: &proto.SegmentMetadata{
					SegNo:       1,
					State:       tt.state,
					LastEntryId: 10,
				},
				Revision: 1,
			}
			// Mock GetSegmentMetadata to return the same state (RefreshAndGetMetadata calls it)
			mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(segmentMeta, nil).Once()

			segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
			err := segmentHandle.Compact(context.Background())
			assert.Error(t, err)
			assert.True(t, werr.ErrSegmentHandleSegmentStateInvalid.Is(err))
		})
	}
}

// TestCompact_Success tests successful compaction from Completed to Sealed
func TestCompact_Success(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "127.0.0.1").Return(mockClient, nil).Maybe()

	completedMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Completed,
			LastEntryId: 10,
			Size:        2048,
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

	sealedMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Sealed,
			LastEntryId: 10,
			Size:        1024,
		},
		Revision: 2,
	}

	expectedSealedTime := time.Now().UnixMilli()
	compactedSegMetaInfo := &proto.SegmentMetadata{
		SegNo:       1,
		LastEntryId: 10,
		Size:        1024,
		SealedTime:  expectedSealedTime,
	}

	// First call for RefreshAndGetMetadata, second for refresh after compaction
	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(completedMeta, nil).Once()
	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(sealedMeta, nil).Once()
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, "testLog", int64(1), mock.MatchedBy(func(segMeta *meta.SegmentMeta) bool {
		return segMeta.Metadata.SealedTime == expectedSealedTime &&
			segMeta.Metadata.State == proto.SegmentState_Sealed &&
			segMeta.Metadata.Size == 1024
	}), proto.SegmentState_Completed).Return(nil)
	mockClient.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(compactedSegMetaInfo, nil)

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{
					QueueSize:  10,
					MaxRetries: 2,
				},
			},
			Storage: config.StorageConfig{
				Type: "minio",
			},
		},
	}

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", completedMeta, mockMetadata, mockClientPool, cfg, false)
	err := segmentHandle.Compact(context.Background())
	assert.NoError(t, err)
}

// TestCompact_SealedTimeFromCompactResponse verifies that SealedTime in metadata update
// comes from the compact response's SealedTime field, not CompletionTime.
// Regression test: previously used CompletionTime (unset, always 0) instead of SealedTime.
func TestCompact_SealedTimeFromCompactResponse(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "127.0.0.1").Return(mockClient, nil).Maybe()

	completedMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Completed,
			LastEntryId: 10,
			Size:        2048,
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

	sealedMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Sealed,
			LastEntryId: 10,
			Size:        1024,
		},
		Revision: 2,
	}

	// Compact response sets SealedTime but NOT CompletionTime (CompletionTime defaults to 0).
	// The bug was using CompletionTime (0) instead of SealedTime.
	expectedSealedTime := int64(1700000000000)
	compactedSegMetaInfo := &proto.SegmentMetadata{
		SegNo:          1,
		LastEntryId:    10,
		Size:           1024,
		SealedTime:     expectedSealedTime,
		CompletionTime: 0, // explicitly 0 to verify we don't use this field
	}

	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(completedMeta, nil).Once()
	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(sealedMeta, nil).Once()
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, "testLog", int64(1), mock.MatchedBy(func(segMeta *meta.SegmentMeta) bool {
		// Key assertion: SealedTime must equal the compact response's SealedTime, not 0
		return segMeta.Metadata.SealedTime == expectedSealedTime
	}), proto.SegmentState_Completed).Return(nil)
	mockClient.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(compactedSegMetaInfo, nil)

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{
					QueueSize:  10,
					MaxRetries: 2,
				},
			},
			Storage: config.StorageConfig{
				Type: "minio",
			},
		},
	}

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", completedMeta, mockMetadata, mockClientPool, cfg, false)
	err := segmentHandle.Compact(context.Background())
	assert.NoError(t, err)
}

// TestCompact_CompactionFails tests Compact when SegmentCompact fails on all nodes
func TestCompact_CompactionFails(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "127.0.0.1").Return(mockClient, nil).Maybe()

	completedMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Completed,
			LastEntryId: 10,
			Size:        2048,
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

	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(completedMeta, nil).Once()
	mockClient.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(nil, errors.New("compaction failed"))

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{
					QueueSize:  10,
					MaxRetries: 2,
				},
			},
			Storage: config.StorageConfig{
				Type: "minio",
			},
		},
	}

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", completedMeta, mockMetadata, mockClientPool, cfg, false)
	err := segmentHandle.Compact(context.Background())
	assert.Error(t, err)
}

// TestCompact_AlreadyCompacting tests that concurrent compact is rejected
func TestCompact_AlreadyCompacting(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "127.0.0.1").Return(mockClient, nil).Maybe()

	completedMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Completed,
			LastEntryId: 10,
			Size:        2048,
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

	sealedMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Sealed,
			LastEntryId: 10,
			Size:        1024,
		},
		Revision: 2,
	}

	compactedResult := &proto.SegmentMetadata{
		SegNo:          1,
		LastEntryId:    10,
		Size:           1024,
		CompletionTime: time.Now().UnixMilli(),
	}

	// The first compact blocks for a while; second call should see doingCompact=true and return nil
	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(completedMeta, nil).Maybe()
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, "testLog", int64(1), mock.Anything, proto.SegmentState_Completed).Return(nil).Maybe()
	// Use a channel to delay the SegmentCompact response
	blockCh := make(chan struct{})
	mockClient.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).RunAndReturn(
		func(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
			<-blockCh
			return compactedResult, nil
		},
	).Maybe()

	// For refresh after compaction
	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(sealedMeta, nil).Maybe()

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{
					QueueSize:  10,
					MaxRetries: 2,
				},
			},
			Storage: config.StorageConfig{
				Type: "minio",
			},
		},
	}

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", completedMeta, mockMetadata, mockClientPool, cfg, false)

	// Start first compact in background
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = segmentHandle.Compact(context.Background())
	}()

	// Give the first compact time to start and acquire the doingCompact flag
	time.Sleep(50 * time.Millisecond)

	// Second compact should return nil immediately (already compacting)
	err := segmentHandle.Compact(context.Background())
	assert.NoError(t, err)

	// Unblock the first compact
	close(blockCh)
	wg.Wait()
}

// TestSetWriterInvalidationNotifier tests the SetWriterInvalidationNotifier method
func TestSetWriterInvalidationNotifier(t *testing.T) {
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)

	notified := false
	var receivedReason string
	notifier := func(ctx context.Context, reason string) {
		notified = true
		receivedReason = reason
	}
	segmentHandle.SetWriterInvalidationNotifier(context.Background(), notifier)

	// Trigger the notifier through the internal method
	segImpl := segmentHandle.(*segmentHandleImpl)
	segImpl.NotifyWriterInvalidation(context.Background(), "test-reason")

	assert.True(t, notified)
	assert.Equal(t, "test-reason", receivedReason)
}

// TestNotifyWriterInvalidation_NilNotifier tests that NotifyWriterInvalidation does not panic when notifier is nil
func TestNotifyWriterInvalidation_NilNotifier(t *testing.T) {
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)

	// Should not panic even without a notifier set
	segImpl := segmentHandle.(*segmentHandleImpl)
	assert.NotPanics(t, func() {
		segImpl.NotifyWriterInvalidation(context.Background(), "test-reason")
	})
}

// TestReadBatchAdv_FailoverToSecondNode tests ReadBatchAdv failing on first node and succeeding on second
func TestReadBatchAdv_FailoverToSecondNode(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient1 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient2 := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient1, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient2, nil)

	// First node fails
	mockClient1.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(0), int64(10), (*proto.LastReadState)(nil)).Return(nil, errors.New("node1 failed"))

	// Second node succeeds
	expectedResult := &proto.BatchReadResult{
		Entries: []*proto.LogEntry{
			{SegId: 1, EntryId: 0, Values: []byte("entry_0")},
		},
		LastReadState: &proto.LastReadState{},
	}
	mockClient2.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(0), int64(10), (*proto.LastReadState)(nil)).Return(expectedResult, nil)

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
				Aq:    2,
				Es:    2,
				Wq:    2,
				Nodes: []string{"node1", "node2"},
			},
		},
		Revision: 1,
	}
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	result, err := segmentHandle.ReadBatchAdv(context.Background(), 0, 10, nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, len(result.Entries))
	assert.Equal(t, "node2", result.LastReadState.Node)
}

// TestGetLastAddConfirmed_ActiveSegment_FailoverToSecondNode tests LAC failover
func TestGetLastAddConfirmed_ActiveSegment_FailoverToSecondNode(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient1 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient2 := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient1, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient2, nil)

	// First node fails
	mockClient1.EXPECT().GetLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(-1), errors.New("node1 down"))
	// Second node succeeds
	mockClient2.EXPECT().GetLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(25), nil)

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
				Es:    2,
				Wq:    2,
				Nodes: []string{"node1", "node2"},
			},
		},
		Revision: 1,
	}
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	lac, err := segmentHandle.GetLastAddConfirmed(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(25), lac)
}

// === GetQuorumInfo tests ===

func TestGetQuorumInfo_CachedQuorum(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	quorum := &proto.QuorumInfo{Id: 1, Aq: 2, Es: 3, Wq: 2, Nodes: []string{"n1", "n2", "n3"}}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1, Quorum: quorum},
		Revision: 1,
	}
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)

	// Should return cached quorum without calling metadata
	q, err := segmentHandle.GetQuorumInfo(context.Background())
	assert.NoError(t, err)
	assert.Same(t, quorum, q)
}

func TestGetQuorumInfo_NilQuorum_ZeroId(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	// Quorum with Id=0 - should produce standalone quorum
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1, Quorum: &proto.QuorumInfo{Id: 0}},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)
	impl.quorumInfo = nil // force nil to test the zero-id path

	q, err := sh.GetQuorumInfo(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(0), q.Id)
	assert.Equal(t, []string{"127.0.0.1"}, q.Nodes)
}

func TestGetQuorumInfo_PositiveQuorumId_FetchesFromMeta(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	expectedQuorum := &proto.QuorumInfo{Id: 5, Aq: 2, Es: 3, Nodes: []string{"a", "b", "c"}}
	mockMetadata.EXPECT().GetQuorumInfo(mock.Anything, int64(5)).Return(expectedQuorum, nil)

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1, QuorumId: 5},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)
	impl.quorumInfo = nil // force nil to test metadata fetch path

	q, err := sh.GetQuorumInfo(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, expectedQuorum, q)
}

func TestGetQuorumInfo_PositiveQuorumId_MetaError(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	mockMetadata.EXPECT().GetQuorumInfo(mock.Anything, int64(5)).Return(nil, werr.ErrInternalError)

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1, QuorumId: 5},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)
	impl.quorumInfo = nil

	_, err := sh.GetQuorumInfo(context.Background())
	assert.Error(t, err)
}

// === AppendAsync edge case tests ===

func TestAppendAsync_Fenced(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)
	impl := sh.(*segmentHandleImpl)
	impl.fencedState.Store(true)

	done := make(chan struct{})
	sh.AppendAsync(context.Background(), []byte("test"), func(segmentId int64, entryId int64, err error) {
		assert.Error(t, err)
		assert.True(t, werr.ErrSegmentFenced.Is(err))
		close(done)
	})
	<-done
}

func TestAppendAsync_Rolling(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)
	impl := sh.(*segmentHandleImpl)
	impl.rollingState.Store(true)

	done := make(chan struct{})
	sh.AppendAsync(context.Background(), []byte("test"), func(segmentId int64, entryId int64, err error) {
		assert.Error(t, err)
		assert.True(t, werr.ErrSegmentHandleSegmentRolling.Is(err))
		close(done)
	})
	<-done
}

func TestAppendAsync_NotActive(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Completed, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)

	done := make(chan struct{})
	sh.AppendAsync(context.Background(), []byte("test"), func(segmentId int64, entryId int64, err error) {
		assert.Error(t, err)
		assert.True(t, werr.ErrSegmentHandleSegmentStateInvalid.Is(err))
		close(done)
	})
	<-done
}

// === syncLACToNode / completeSegmentOnNode / fenceSegmentOnNode error path tests ===

func TestSyncLACToNode_GetClientError(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(nil, errors.New("conn error"))

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)

	resultChan := make(chan nodeLACSyncResult, 1)
	impl.syncLACToNode(context.Background(), "node1", 5, resultChan)

	result := <-resultChan
	assert.Error(t, result.err)
	assert.Equal(t, "node1", result.node)
}

func TestCompleteSegmentOnNode_GetClientError(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(nil, errors.New("conn error"))

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)

	resultChan := make(chan nodeCompleteResult, 1)
	impl.completeSegmentOnNode(context.Background(), "node1", 5, resultChan)

	result := <-resultChan
	assert.Error(t, result.err)
	assert.Equal(t, "node1", result.node)
}

func TestFenceSegmentOnNode_GetClientError(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(nil, errors.New("conn error"))

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)

	resultChan := make(chan nodeFenceResult, 1)
	impl.fenceSegmentOnNode(context.Background(), "node1", resultChan)

	result := <-resultChan
	assert.Error(t, result.err)
	assert.Equal(t, "node1", result.node)
}

// === completeSegmentQuorum tests ===

func TestCompleteSegmentQuorum_InsufficientResponses(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	// Both nodes fail
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(nil, errors.New("conn error"))
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(5)).Return(int64(0), errors.New("complete failed"))

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)

	quorum := &proto.QuorumInfo{Id: 1, Aq: 2, Es: 2, Wq: 2, Nodes: []string{"node1", "node2"}}
	err := impl.completeSegmentQuorum(context.Background(), quorum, 5)
	assert.Error(t, err)
}

func TestCompleteSegmentQuorum_ContextCancel(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	// Client blocks indefinitely so context cancellation kicks in
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(nil, ctx.Err()).Maybe()

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)

	quorum := &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"node1"}}
	err := impl.completeSegmentQuorum(ctx, quorum, 5)
	assert.Error(t, err)
}

// === FenceAndComplete tests ===

func TestFenceAndComplete_AlreadyFenced(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: 10,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)
	impl.fencedState.Store(true)
	impl.lastAddConfirmed.Store(10)

	lastEntry, err := sh.FenceAndComplete(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(10), lastEntry)
}

func TestFenceAndComplete_GetQuorumInfoError(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	mockMetadata.EXPECT().GetQuorumInfo(mock.Anything, int64(5)).Return(nil, werr.ErrInternalError)

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1, QuorumId: 5},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)
	impl.quorumInfo = nil

	_, err := sh.FenceAndComplete(context.Background())
	assert.Error(t, err)
}

func TestFenceAndComplete_FenceError(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	// Fence fails - node returns error
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(nil, errors.New("conn error"))

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"node1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)

	_, err := sh.FenceAndComplete(context.Background())
	assert.Error(t, err)
}

func TestFenceAndComplete_CompleteError(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	// Fence succeeds
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(5), nil)
	// Complete fails
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(5)).Return(int64(0), errors.New("complete failed"))

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"node1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)

	_, err := sh.FenceAndComplete(context.Background())
	assert.Error(t, err)
}

func TestFenceAndComplete_MetaUpdateError(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	// Fence succeeds
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(5), nil)
	// Complete succeeds
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(5)).Return(int64(5), nil)
	// Meta update fails
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, "testLog", int64(1), mock.Anything, proto.SegmentState_Active).Return(werr.ErrInternalError)

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"node1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)

	_, err := sh.FenceAndComplete(context.Background())
	assert.Error(t, err)
}

func TestFenceAndComplete_Success(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(5), nil)
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(5)).Return(int64(5), nil)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, "testLog", int64(1), mock.Anything, proto.SegmentState_Active).Return(nil)
	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(&meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Completed, LastEntryId: 5,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"node1"}},
		},
		Revision: 2,
	}, nil)

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"node1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)

	lastEntry, err := sh.FenceAndComplete(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(5), lastEntry)

	// Verify fenced state was set
	impl := sh.(*segmentHandleImpl)
	assert.True(t, impl.fencedState.Load())
}

// === doComplete tests ===

func TestDoComplete_GetQuorumInfoError(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	mockMetadata.EXPECT().GetQuorumInfo(mock.Anything, int64(5)).Return(nil, werr.ErrInternalError)

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1, QuorumId: 5},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)
	impl.quorumInfo = nil

	_, err := impl.doComplete(context.Background())
	assert.Error(t, err)
}

func TestDoComplete_FenceError(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(nil, errors.New("conn error"))

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"node1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)

	_, err := impl.doComplete(context.Background())
	assert.Error(t, err)
}

// === compactSegmentQuorum tests ===

func TestCompactSegmentQuorum_AllNodesFail(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	// First node: get client succeeds, compact fails
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClient.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(nil, errors.New("compact failed"))
	// Second node: get client fails
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(nil, errors.New("conn error"))

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)

	quorum := &proto.QuorumInfo{Id: 1, Nodes: []string{"node1", "node2"}}
	_, err := impl.compactSegmentQuorum(context.Background(), quorum)
	assert.Error(t, err)
}

func TestCompactSegmentQuorum_SecondNodeSucceeds(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient1 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient2 := mocks_logstore_client.NewLogStoreClient(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	// First node fails
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient1, nil)
	mockClient1.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(nil, errors.New("compact failed"))
	// Second node succeeds
	expectedMeta := &proto.SegmentMetadata{SegNo: 1, Size: 100, LastEntryId: 10}
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient2, nil)
	mockClient2.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(expectedMeta, nil)

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)

	quorum := &proto.QuorumInfo{Id: 1, Nodes: []string{"node1", "node2"}}
	result, err := impl.compactSegmentQuorum(context.Background(), quorum)
	assert.NoError(t, err)
	assert.Equal(t, expectedMeta, result)
}

// === doCloseWritingAndUpdateMetaIfNecessaryUnsafe tests ===

func TestDoCloseWriting_NotWritable(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)

	// canWriteState is false, should return nil immediately
	err := impl.doCloseWritingAndUpdateMetaIfNecessaryUnsafe(context.Background(), 5)
	assert.NoError(t, err)
}

func TestDoCloseWriting_RevisionInvalid(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, "testLog", int64(1), mock.Anything, proto.SegmentState_Active).
		Return(werr.ErrMetadataRevisionInvalid)

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)
	impl := sh.(*segmentHandleImpl)

	// Set a writer invalidation notifier to capture the notification
	notified := false
	impl.SetWriterInvalidationNotifier(context.Background(), func(ctx context.Context, reason string) {
		notified = true
	})

	err := impl.doCloseWritingAndUpdateMetaIfNecessaryUnsafe(context.Background(), 5)
	assert.Error(t, err)
	assert.True(t, werr.ErrMetadataRevisionInvalid.Is(err))
	assert.True(t, notified)
}

// === fenceSegmentQuorum tests ===

func TestFenceSegmentQuorum_InsufficientResponses(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	// Both nodes fail
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(nil, errors.New("conn error"))
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(nil, errors.New("conn error"))

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)

	quorum := &proto.QuorumInfo{Id: 1, Es: 2, Aq: 2, Wq: 2, Nodes: []string{"node1", "node2"}}
	_, err := impl.fenceSegmentQuorum(context.Background(), quorum)
	assert.Error(t, err)
}

// === syncLACToQuorumAsync tests ===

func TestSyncLACToQuorumAsync_GetQuorumInfoError(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	mockMetadata.EXPECT().GetQuorumInfo(mock.Anything, int64(5)).Return(nil, werr.ErrInternalError)

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1, QuorumId: 5},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)
	impl.quorumInfo = nil

	// syncLACToQuorumAsync should return without panicking
	impl.syncLACToQuorumAsync(context.Background(), 5)
}

func TestSyncLACToQuorumAsync_NodeError(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(nil, errors.New("conn error"))

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"node1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)

	// Should complete without panicking even with node error
	impl.syncLACToQuorumAsync(context.Background(), 5)
}

// === AppendAsync double-check under lock paths ===

func TestAppendAsync_FencedUnderLock(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)
	impl := sh.(*segmentHandleImpl)

	// Hold the lock, set fenced while locked, then release and let AppendAsync proceed
	done := make(chan struct{})
	ready := make(chan struct{})
	go func() {
		impl.Lock()
		close(ready)
		// Set fenced while holding the lock — the pre-lock check will pass,
		// but the double-check under lock will catch it
		impl.fencedState.Store(true)
		time.Sleep(50 * time.Millisecond)
		impl.Unlock()
	}()
	<-ready
	time.Sleep(10 * time.Millisecond) // Let goroutine settle

	sh.AppendAsync(context.Background(), []byte("test"), func(segmentId int64, entryId int64, err error) {
		assert.Error(t, err)
		assert.True(t, werr.ErrSegmentFenced.Is(err))
		close(done)
	})
	<-done
}

func TestAppendAsync_RollingUnderLock(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)
	impl := sh.(*segmentHandleImpl)

	done := make(chan struct{})
	ready := make(chan struct{})
	go func() {
		impl.Lock()
		close(ready)
		impl.rollingState.Store(true)
		time.Sleep(50 * time.Millisecond)
		impl.Unlock()
	}()
	<-ready
	time.Sleep(10 * time.Millisecond)

	sh.AppendAsync(context.Background(), []byte("test"), func(segmentId int64, entryId int64, err error) {
		assert.Error(t, err)
		assert.True(t, werr.ErrSegmentHandleSegmentRolling.Is(err))
		close(done)
	})
	<-done
}

func TestAppendAsync_SubmitFails(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)
	impl := sh.(*segmentHandleImpl)

	// Stop the executor so Submit returns false
	impl.executor.Stop(context.Background())

	done := make(chan struct{})
	sh.AppendAsync(context.Background(), []byte("test"), func(segmentId int64, entryId int64, err error) {
		assert.Error(t, err)
		assert.True(t, werr.ErrSegmentHandleSegmentClosed.Is(err))
		close(done)
	})
	<-done
}

// === SendAppendSuccessCallbacks edge cases ===

func TestSendAppendSuccessCallbacks_EntryIdBelowLAC(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil).Maybe()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}

	testQueue := list.New()
	sh := NewSegmentHandleWithAppendOpsQueue(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, testQueue)
	impl := sh.(*segmentHandleImpl)

	// Set LAC to 5 — any op with entryId <= 5 should go through the "already confirmed" branch
	impl.lastAddConfirmed.Store(5)

	// Create ops with entryId 3, 4 (below LAC) and 7 (above, not next)
	successIds := make([]int64, 0)
	mu := sync.Mutex{}
	for _, id := range []int64{3, 4, 7} {
		entryId := id
		op := NewAppendOp("b", "r", 1, 1, entryId, []byte("data"), func(segmentId int64, eid int64, err error) {
			mu.Lock()
			successIds = append(successIds, eid)
			mu.Unlock()
		}, mockClientPool, sh, segmentMeta.Metadata.Quorum)
		op.completed.Store(true)
		testQueue.PushBack(op)
	}

	sh.SendAppendSuccessCallbacks(context.Background(), 4)
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	// entryId 3 and 4 should succeed (below LAC), entryId 7 should break (not next in sequence)
	assert.Contains(t, successIds, int64(3))
	assert.Contains(t, successIds, int64(4))
	assert.NotContains(t, successIds, int64(7))
	mu.Unlock()
}

func TestSendAppendSuccessCallbacks_NotCompleted_Break(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}

	testQueue := list.New()
	sh := NewSegmentHandleWithAppendOpsQueue(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, testQueue)

	// Add op that is NOT completed — should trigger the "not completed" break
	op := NewAppendOp("b", "r", 1, 1, 0, []byte("data"), func(segmentId int64, eid int64, err error) {}, mockClientPool, sh, segmentMeta.Metadata.Quorum)
	op.completed.Store(false)
	testQueue.PushBack(op)

	// Should not panic and should leave the queue unchanged
	sh.SendAppendSuccessCallbacks(context.Background(), 0)
	assert.Equal(t, 1, testQueue.Len()) // op still in queue since it wasn't completed
}

// === ReadBatchAdv error path tests ===

func TestReadBatchAdv_ClientError(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "127.0.0.1").Return(nil, errors.New("connection refused"))

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	result, err := sh.ReadBatchAdv(context.Background(), 0, 10, nil)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "connection refused")
}

func TestReadBatchAdv_EntryNotFound(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "127.0.0.1").Return(mockClient, nil)
	mockClient.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(0), int64(10), (*proto.LastReadState)(nil)).
		Return(nil, werr.ErrEntryNotFound)

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	result, err := sh.ReadBatchAdv(context.Background(), 0, 10, nil)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.True(t, werr.ErrEntryNotFound.Is(err))
}

func TestReadBatchAdv_EOF(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClient.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(0), int64(10), (*proto.LastReadState)(nil)).
		Return(nil, werr.ErrFileReaderEndOfFile)

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"node1", "node2"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	result, err := sh.ReadBatchAdv(context.Background(), 0, 10, nil)
	assert.Error(t, err)
	assert.Nil(t, result)
	// EOF should break immediately without trying node2
	assert.True(t, werr.ErrFileReaderEndOfFile.Is(err))
}

func TestReadBatchAdv_FailoverClientError_ThenSuccess(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient2 := mocks_logstore_client.NewLogStoreClient(t)
	// First node: client error
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(nil, errors.New("conn refused"))
	// Second node: success
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient2, nil)
	expectedResult := &proto.BatchReadResult{
		Entries:       []*proto.LogEntry{{SegId: 1, EntryId: 0, Values: []byte("data")}},
		LastReadState: &proto.LastReadState{},
	}
	mockClient2.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(0), int64(10), (*proto.LastReadState)(nil)).Return(expectedResult, nil)

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 2, Es: 2, Wq: 2, Nodes: []string{"node1", "node2"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	result, err := sh.ReadBatchAdv(context.Background(), 0, 10, nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "node2", result.LastReadState.Node)
}

// === GetLastAddConfirmed failover ===

func TestGetLastAddConfirmed_ActiveSegment_ClientError_Failover(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient2 := mocks_logstore_client.NewLogStoreClient(t)
	// First node: client error
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(nil, errors.New("conn error"))
	// Second node: success
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient2, nil)
	mockClient2.EXPECT().GetLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(10), nil)

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 2, Es: 2, Wq: 2, Nodes: []string{"node1", "node2"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	lac, err := sh.GetLastAddConfirmed(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(10), lac)
}

// === GetBlocksCount unsupported quorum ===

func TestGetBlocksCount_ActiveSegment_UnsupportedQuorum(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			// Single node in quorumInfo.Nodes but unsupported config (Wq=2)
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 2, Es: 1, Wq: 2, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	blocksCount := sh.GetBlocksCount(context.Background())
	assert.Equal(t, int64(0), blocksCount) // returns 0 for unsupported quorum config
}

func TestGetBlocksCount_ActiveSegment_QuorumInfoError(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			QuorumId: 99,  // positive quorum ID so GetQuorumInfo queries metadata
			Quorum:   nil, // nil quorum so s.quorumInfo is nil
		},
		Revision: 1,
	}
	// Mock metadata.GetQuorumInfo to return an error
	mockMetadata.EXPECT().GetQuorumInfo(mock.Anything, int64(99)).Return(nil, errors.New("quorum not found"))
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	blocksCount := sh.GetBlocksCount(context.Background())
	assert.Equal(t, int64(0), blocksCount) // returns 0 when GetQuorumInfo fails
}

// === syncLACToQuorumAsync context cancellation ===

func TestSyncLACToQuorumAsync_ContextCancelled(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	// Make UpdateLastAddConfirmed block until context is cancelled
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(5)).
		RunAndReturn(func(ctx context.Context, _ string, _ string, _ int64, _ int64, _ int64) error {
			<-ctx.Done()
			return ctx.Err()
		})

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"node1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false)
	impl := sh.(*segmentHandleImpl)

	// Cancel context quickly to trigger ctx.Done in the result collection loop
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// syncLACToQuorumAsync runs synchronously in this call — should return without panicking
	impl.syncLACToQuorumAsync(ctx, 5)
}

// === doCloseWriting edge cases ===

func TestDoCloseWriting_LastFlushedNegativeOne(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)
	impl := sh.(*segmentHandleImpl)

	// lastFlushedEntryId == -1 should return nil without updating metadata
	err := impl.doCloseWritingAndUpdateMetaIfNecessaryUnsafe(context.Background(), -1)
	assert.NoError(t, err)
	assert.False(t, impl.canWriteState.Load())
}

func TestDoCloseWriting_NonRevisionError(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, "testLog", int64(1), mock.Anything, proto.SegmentState_Active).
		Return(errors.New("etcd unavailable"))

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)
	impl := sh.(*segmentHandleImpl)

	err := impl.doCloseWritingAndUpdateMetaIfNecessaryUnsafe(context.Background(), 5)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "etcd unavailable")
}

func TestDoCloseWriting_CompletedSegment(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Completed, LastEntryId: 10,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)
	impl := sh.(*segmentHandleImpl)

	// Completed segment state should return nil without updating metadata
	err := impl.doCloseWritingAndUpdateMetaIfNecessaryUnsafe(context.Background(), 5)
	assert.NoError(t, err)
}

// === ForceCompleteAndClose nil completionMgr ===

func TestForceCompleteAndClose_NilCompletionMgr(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)
	impl := sh.(*segmentHandleImpl)
	impl.completionMgr = nil

	err := sh.ForceCompleteAndClose(context.Background())
	assert.NoError(t, err) // Returns nil when completionMgr is nil
}
