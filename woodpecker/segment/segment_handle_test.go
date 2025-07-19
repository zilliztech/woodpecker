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
	"sync"
	"testing"
	"time"

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
	"github.com/zilliztech/woodpecker/server/processor"
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

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Active,
			LastEntryId: -1,
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
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)
	for i := 0; i < 20; i++ {
		mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &processor.SegmentEntry{
			SegmentId: 1,
			EntryId:   int64(i),
			Data:      []byte(fmt.Sprintf("test_%d", i)),
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
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
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
			}, mock.MatchedBy(func(ch channel.ResultChannel) bool { return ch != nil })).Return(int64(i), nil)
		} else {
			// 2 fail, and retry 3 times
			mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &processor.SegmentEntry{
				SegmentId: 1,
				EntryId:   int64(i),
				Data:      []byte(fmt.Sprintf("test_%d", i)),
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

func TestMultiAppendAsync_PartialFailButAllSuccessAfterRetry(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)
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
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)
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
					if appendOp.entryId%2 == 0 && appendOp.attempt <= 1 {
						// if attempt=1 and entryId is even, mock fail in this attempt
						t.Logf("start to send %d to chan %d/%d/%d , which in No.%d attempt\n", -1, appendOp.logId, appendOp.segmentId, appendOp.entryId, appendOp.attempt)
						_ = appendOp.resultChannels[0].SendResult(context.Background(), &channel.AppendResult{
							SyncedId: -1,
							Err:      nil,
						})
						t.Logf("finish to send %d to chan %d/%d/%d , which in No.%d attempt\n\n", -1, appendOp.logId, appendOp.segmentId, appendOp.entryId, appendOp.attempt)
					} else {
						// otherwise, mock success
						t.Logf("start to send %d to chan %d/%d/%d , which in No.%d attempt\n", appendOp.entryId, appendOp.logId, appendOp.segmentId, appendOp.entryId, appendOp.attempt)
						_ = appendOp.resultChannels[0].SendResult(context.Background(), &channel.AppendResult{
							SyncedId: appendOp.entryId,
							Err:      nil,
						})
						t.Logf("finish to send %d to chan %d/%d/%d , which in No.%d attempt\n\n", appendOp.entryId, appendOp.logId, appendOp.segmentId, appendOp.entryId, appendOp.attempt)
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
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), int64(1)).Return(0, nil)
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1)).Return(0, nil)
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
	segmentMetaAfterFenced := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Completed,
			LastEntryId: 1,
		},
		Revision: 2,
	}
	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(segmentMetaAfterFenced, nil)
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

	closeErr := segmentHandle.ForceCompleteAndClose(context.Background())
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
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Active,
			LastEntryId: -1,
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
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)
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
			nil,
			1)
		ops = append(ops, op)
		testQueue.PushBack(op)
	}

	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1)).Return(4, nil)
	// success 3-9, but 0-2 is not finish
	for i := 0; i < 10; i++ {
		if i == 5 {
			// fail
			ops[i].attempt = 3
			segmentHandle.SendAppendErrorCallbacks(context.TODO(), int64(i), werr.ErrSegmentHandleWriteFailed)
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

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Active,
			LastEntryId: -1,
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
			nil,
			1)
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
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)

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
			nil,
			1)
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
	lastEntryId, err := segmentHandle.Fence(context.Background())
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
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()

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
			nil,
			1)
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
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()

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
			nil,
			1)
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
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1)).Return(int64(0), nil).Maybe()

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
		attempt := 1
		if i == 1 {
			attempt = 2 // This will be >= MaxRetries (2), so it will be removed
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
			attempt)
		testQueue.PushBack(op)
	}

	// Initially not rolling
	assert.False(t, segmentHandle.IsForceRollingReady(context.Background()))
	assert.Equal(t, 3, testQueue.Len())

	// Trigger error for entry 1 - this should trigger rolling and remove entry 1,2 but keep entry 0
	segmentHandle.SendAppendErrorCallbacks(context.Background(), int64(1), werr.ErrSegmentHandleWriteFailed)

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
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1)).Return(int64(2), nil).Maybe()

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
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().CompleteSegment(mock.Anything, int64(1), int64(1)).Return(int64(0), nil).Maybe()

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
	assert.True(t, writable)

	// Force complete and close
	err = segmentHandle.ForceCompleteAndClose(context.Background())
	assert.NoError(t, err)

	// Verify final state
	writable, err = segmentHandle.IsWritable(context.Background())
	assert.NoError(t, err)
	assert.False(t, writable)
}
