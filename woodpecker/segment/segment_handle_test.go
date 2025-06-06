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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/tracer"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/processor"
)

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

	segmentMeta := &proto.SegmentMetadata{
		SegNo: 1,
	}
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)
	assert.Equal(t, "testLog", segmentHandle.GetLogName())
	assert.Equal(t, int64(1), segmentHandle.GetId(context.Background()))
}

func TestAppendAsync_Success(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)
	mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), mock.Anything, mock.Anything).Return(0, nil)
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

	segmentMeta := &proto.SegmentMetadata{
		SegNo:       1,
		State:       proto.SegmentState_Active,
		LastEntryId: -1,
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
				appendOp.resultChannels[0] <- 0
				close(appendOp.resultChannels[0])
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
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)
	for i := 0; i < 20; i++ {
		mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &processor.SegmentEntry{
			SegmentId: 1,
			EntryId:   int64(i),
			Data:      []byte(fmt.Sprintf("test_%d", i)),
		}, mock.MatchedBy(func(ch chan<- int64) bool { return ch != nil })).Return(int64(i), nil)
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

	segmentMeta := &proto.SegmentMetadata{
		SegNo:       1,
		State:       proto.SegmentState_Active,
		LastEntryId: -1,
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
					appendOp.resultChannels[0] <- appendOp.entryId
					close(appendOp.resultChannels[0])
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
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)
	for i := 0; i < 5; i++ {
		ch := make(chan int64, 1)
		ch <- int64(i)
		close(ch)
		if i != 2 {
			// 0,1,3,4 success
			mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &processor.SegmentEntry{
				SegmentId: 1,
				EntryId:   int64(i),
				Data:      []byte(fmt.Sprintf("test_%d", i)),
			}, mock.MatchedBy(func(ch chan<- int64) bool { return ch != nil })).Return(int64(i), nil)
		} else {
			// 2 fail, and retry 3 times
			mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &processor.SegmentEntry{
				SegmentId: 1,
				EntryId:   int64(i),
				Data:      []byte(fmt.Sprintf("test_%d", i)),
			}, mock.MatchedBy(func(ch chan<- int64) bool { return ch != nil })).Return(int64(i), errors.New("test error"))
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

	segmentMeta := &proto.SegmentMetadata{
		SegNo:       1,
		State:       proto.SegmentState_Active,
		LastEntryId: -1,
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
					appendOp.resultChannels[0] <- appendOp.entryId
					close(appendOp.resultChannels[0])
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

func TestMultiAppendAsync_PartialFailButAllSuccessAfterRetry(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)
	for i := 0; i < 5; i++ {
		mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), mock.Anything, mock.MatchedBy(func(ch chan<- int64) bool { return ch != nil })).Return(int64(i), nil)
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

	segmentMeta := &proto.SegmentMetadata{
		SegNo:       1,
		State:       proto.SegmentState_Active,
		LastEntryId: -1,
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
					appendOp.resultChannels[0] <- appendOp.entryId
					close(appendOp.resultChannels[0])
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
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)
	for i := 0; i < 20; i++ {
		mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), mock.Anything, mock.MatchedBy(func(ch chan<- int64) bool { return ch != nil })).Return(int64(i), nil)
	}
	cfg, _ := config.NewConfiguration()
	cfg.Woodpecker.Client.SegmentAppend.QueueSize = 10
	cfg.Woodpecker.Client.SegmentAppend.MaxRetries = 2
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)
	_ = tracer.InitTracer(cfg, "test", 1001)

	segmentMeta := &proto.SegmentMetadata{
		SegNo:       1,
		State:       proto.SegmentState_Active,
		LastEntryId: -1,
	}
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)

	callbackCalledNum := 0
	syncedIds := make([]int64, 0)
	for i := 0; i < 20; i++ {
		callback := func(segmentId int64, entryId int64, err error) {
			callbackCalledNum++
			assert.Equal(t, int64(1), segmentId)
			assert.Equal(t, int64(i), entryId)
			assert.Nil(t, err)
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
					if appendOp.entryId == 2 {
						fmt.Sprintf("debug")
					}
					if appendOp.entryId%2 == 0 && appendOp.attempt <= 1 {
						// if attempt=1 and entryId is even, mock fail in this attempt
						t.Logf("start to send %d to chan %d/%d/%d , which in No.%d attempt\n", -1, appendOp.logId, appendOp.segmentId, appendOp.entryId, appendOp.attempt)
						appendOp.resultChannels[0] <- -1
						t.Logf("finish to send %d to chan %d/%d/%d , which in No.%d attempt\n", -1, appendOp.logId, appendOp.segmentId, appendOp.entryId, appendOp.attempt)
					} else {
						// otherwise, mock success
						t.Logf("start to send %d to chan %d/%d/%d , which in No.%d attempt\n", appendOp.entryId, appendOp.logId, appendOp.segmentId, appendOp.entryId, appendOp.attempt)
						appendOp.resultChannels[0] <- appendOp.entryId
						t.Logf("finish to send %d to chan %d/%d/%d , which in No.%d attempt\n", appendOp.entryId, appendOp.logId, appendOp.segmentId, appendOp.entryId, appendOp.attempt)
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
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), mock.Anything).Return(0, nil)
	//mockClient.EXPECT().IsSegmentFenced(mock.Anything, int64(1), mock.Anything).Return(true, nil)
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

	segmentMeta := &proto.SegmentMetadata{
		SegNo:       1,
		State:       proto.SegmentState_Active,
		LastEntryId: -1,
	}
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true)
	lastEntryId, fenceErr := segmentHandle.Fence(context.Background())
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

	closeErr := segmentHandle.CloseWritingAndUpdateMetaIfNecessary(context.Background(), lastEntryId)
	assert.NoError(t, closeErr)
}

func TestSendAppendSuccessCallbacks(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
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
	segmentMeta := &proto.SegmentMetadata{
		SegNo:       1,
		State:       proto.SegmentState_Active,
		LastEntryId: -1,
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
			nil,
			1)
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
	segmentMeta := &proto.SegmentMetadata{
		SegNo:       1,
		State:       proto.SegmentState_Active,
		LastEntryId: -1,
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
			nil,
			1)
		ops = append(ops, op)
		testQueue.PushBack(op)
	}

	// success 3-9, but 0-2 is not finish
	for i := 0; i < 10; i++ {
		if i == 5 {
			// fail
			ops[i].attempt = 3
			segmentHandle.SendAppendErrorCallbacks(context.TODO(), int64(i), werr.ErrSegmentWriteException)
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
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)

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

	segmentMeta := &proto.SegmentMetadata{
		SegNo:       1,
		State:       proto.SegmentState_Active,
		LastEntryId: -1,
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
			nil,
			1)
		ops[i] = op
		testQueue.PushBack(op)
	}

	// Call Fence - should return lastEntryId = 2
	lastEntryId, err := segmentHandle.Fence(context.Background())
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
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)

	// Mock FenceSegment to return ErrSegmentFenced but with lastEntryId = 1
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(1), werr.ErrSegmentFenced)

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

	segmentMeta := &proto.SegmentMetadata{
		SegNo:       1,
		State:       proto.SegmentState_Active,
		LastEntryId: -1,
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
			nil,
			1)
		testQueue.PushBack(op)
	}

	// Call Fence - should return lastEntryId = 1 even with ErrSegmentFenced
	lastEntryId, err := segmentHandle.Fence(context.Background())
	assert.Error(t, err)
	assert.True(t, werr.ErrSegmentFenced.Is(err))
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

// TestFence_SegmentNotFoundError_WithPendingAppendOps tests the new ErrSegmentNotFound error handling
func TestFence_SegmentNotFoundError_WithPendingAppendOps(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)

	// Mock FenceSegment to return ErrSegmentNotFound with lastEntryId = -1
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(-1), werr.ErrSegmentNotFound)

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

	segmentMeta := &proto.SegmentMetadata{
		SegNo:       1,
		State:       proto.SegmentState_Active,
		LastEntryId: -1,
	}

	testQueue := list.New()
	segmentHandle := NewSegmentHandleWithAppendOpsQueue(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, testQueue)

	// Create pending append operations
	successCallbacks := make([]bool, 2)
	failCallbacks := make([]bool, 2)

	for i := 0; i < 2; i++ {
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
			nil,
			1)
		testQueue.PushBack(op)
	}

	// Call Fence - should handle ErrSegmentNotFound correctly
	lastEntryId, err := segmentHandle.Fence(context.Background())
	assert.Error(t, err)
	assert.True(t, werr.ErrSegmentNotFound.Is(err))
	assert.Equal(t, int64(-1), lastEntryId)

	time.Sleep(100 * time.Millisecond) // Give time for callbacks

	// Verify that entry 0 got fail callback (lastEntry==-1)
	assert.False(t, successCallbacks[0], "Entry 0 should fail")

	// Verify that entry 1 got fail callback (entryId > lastEntryId)
	assert.True(t, failCallbacks[1], "Entry 1 should fail")
}

// TestFence_NoWritingFragmentError_WithPendingAppendOps tests ErrSegmentNoWritingFragment handling
func TestFence_NoWritingFragmentError_WithPendingAppendOps(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)

	// Mock FenceSegment to return ErrSegmentNoWritingFragment with lastEntryId = -1 (no entries)
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(int64(-1), werr.ErrSegmentNoWritingFragment)

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

	segmentMeta := &proto.SegmentMetadata{
		SegNo:       1,
		State:       proto.SegmentState_Active,
		LastEntryId: -1,
	}

	testQueue := list.New()
	segmentHandle := NewSegmentHandleWithAppendOpsQueue(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, testQueue)

	// Create pending append operations
	failCallbacks := make([]bool, 2)

	for i := 0; i < 2; i++ {
		entryIndex := i
		callback := func(segmentId int64, entryId int64, err error) {
			if err != nil {
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
			nil,
			1)
		testQueue.PushBack(op)
	}

	// Call Fence - should handle ErrSegmentNoWritingFragment correctly
	lastEntryId, err := segmentHandle.Fence(context.Background())
	assert.Error(t, err)
	assert.True(t, werr.ErrSegmentNoWritingFragment.Is(err))
	assert.Equal(t, int64(-1), lastEntryId)

	time.Sleep(100 * time.Millisecond) // Give time for callbacks

	// All entries should fail since lastEntryId = -1
	assert.True(t, failCallbacks[0], "Entry 0 should fail")
	assert.True(t, failCallbacks[1], "Entry 1 should fail")
}

// TestIsFence_SyncLocalStateWithRemote tests IsFence method's ability to sync local state with remote
func TestIsFence_SyncLocalStateWithRemote(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)

	// Mock IsSegmentFenced to return true (segment is fenced on server)
	mockClient.EXPECT().IsSegmentFenced(mock.Anything, int64(1), int64(1)).Return(true, nil)

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

	segmentMeta := &proto.SegmentMetadata{
		SegNo:       1,
		State:       proto.SegmentState_Active,
		LastEntryId: -1,
	}

	testQueue := list.New()
	segmentHandle := NewSegmentHandleWithAppendOpsQueue(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, testQueue)

	// Create pending append operations
	failCallbacks := make([]bool, 2)

	for i := 0; i < 2; i++ {
		entryIndex := i
		callback := func(segmentId int64, entryId int64, err error) {
			if err != nil && werr.ErrSegmentFenced.Is(err) {
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
			nil,
			1)
		testQueue.PushBack(op)
	}

	// Initially, local fenced state should be false
	segImpl := segmentHandle.(*segmentHandleImpl)
	assert.False(t, segImpl.fencedState.Load())

	// Call IsFence - should sync local state with remote and fast fail pending ops
	isFenced, err := segmentHandle.IsFence(context.Background())
	assert.NoError(t, err)
	assert.True(t, isFenced)

	// Local fenced state should now be true
	assert.True(t, segImpl.fencedState.Load())

	time.Sleep(100 * time.Millisecond) // Give time for callbacks

	// All pending operations should have been fast failed
	assert.True(t, failCallbacks[0], "Entry 0 should fail with ErrSegmentFenced")
	assert.True(t, failCallbacks[1], "Entry 1 should fail with ErrSegmentFenced")
}
