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
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_server/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/segment"
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg)
	assert.Equal(t, "testLog", segmentHandle.GetLogName())
	assert.Equal(t, int64(1), segmentHandle.GetId(context.Background()))
}

func TestAppendAsync_Success(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().IsSegmentFenced(mock.Anything, int64(1), mock.Anything).Return(false, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)
	// int64, <-chan int64, error
	ch := make(chan int64, 1)
	ch <- 0
	close(ch)
	mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), mock.Anything).Return(0, ch, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg)
	callbackCalled := false
	callback := func(segmentId int64, entryId int64, err error) {
		callbackCalled = true
		assert.Equal(t, int64(1), segmentId)
		assert.Equal(t, int64(0), entryId)
		assert.Nil(t, err)
	}
	segmentHandle.AppendAsync(context.Background(), []byte("test"), callback)
	time.Sleep(100 * time.Millisecond) // Give some time for the async operation to complete
	assert.True(t, callbackCalled)
}

func TestMultiAppendAsync_AllSuccess_InSequential(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().IsSegmentFenced(mock.Anything, int64(1), mock.Anything).Return(false, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)
	for i := 0; i < 20; i++ {
		ch := make(chan int64, 1)
		ch <- int64(i)
		close(ch)
		mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &segment.SegmentEntry{
			SegmentId: 1,
			EntryId:   int64(i),
			Data:      []byte(fmt.Sprintf("test_%d", i)),
		}).Return(int64(i), ch, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg)

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
	time.Sleep(100 * time.Millisecond) // Give some time for the async operation to complete
	assert.Equal(t, 20, callbackCalledNum)
	assert.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, syncedIds)
}

func TestMultiAppendAsync_PartialSuccess(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().IsSegmentFenced(mock.Anything, int64(1), mock.Anything).Return(false, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)
	for i := 0; i < 5; i++ {
		ch := make(chan int64, 1)
		ch <- int64(i)
		close(ch)
		if i != 2 {
			// 0,1,3,4 success
			mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &segment.SegmentEntry{
				SegmentId: 1,
				EntryId:   int64(i),
				Data:      []byte(fmt.Sprintf("test_%d", i)),
			}).Return(int64(i), ch, nil)
		} else {
			// 2 fail, and retry 3 times
			mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &segment.SegmentEntry{
				SegmentId: 1,
				EntryId:   int64(i),
				Data:      []byte(fmt.Sprintf("test_%d", i)),
			}).Return(int64(i), ch, errors.New("test error"))
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg)

	failedAttempts := 0
	successAttempts := 0
	syncedIds := make([]int64, 0)
	for i := 0; i < 5; i++ {
		callback := func(segmentId int64, entryId int64, err error) {
			if i >= 2 {
				failedAttempts++
				// received callback 2,3,4 fail
				assert.Error(t, err)
				assert.Equal(t, int64(1), segmentId)
				assert.Equal(t, int64(i), entryId)
			} else {
				successAttempts++
				// received callback 0,1 success
				assert.NoError(t, err)
				assert.Equal(t, int64(1), segmentId)
				assert.Equal(t, int64(i), entryId)
				syncedIds = append(syncedIds, entryId)
			}
		}
		segmentHandle.AppendAsync(context.Background(), []byte(fmt.Sprintf("test_%d", i)), callback)
	}
	time.Sleep(1000 * time.Millisecond) // Give some time for the async operation to complete
	assert.Equal(t, 2, successAttempts) // 0,1
	assert.Equal(t, 3, failedAttempts)  // 2,3,4
	assert.Equal(t, []int64{0, 1}, syncedIds)
}

func TestMultiAppendAsync_PartialFailButAllSuccessAfterRetry(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().IsSegmentFenced(mock.Anything, int64(1), mock.Anything).Return(false, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)
	for i := 0; i < 5; i++ {
		ch := make(chan int64, 1)
		ch <- int64(i)
		close(ch)
		if i != 2 {
			// 0,1,3,4 success
			mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &segment.SegmentEntry{
				SegmentId: 1,
				EntryId:   int64(i),
				Data:      []byte(fmt.Sprintf("test_%d", i)),
			}).Return(int64(i), ch, nil)
		} else {
			// two failed attempts to append entry 2
			mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &segment.SegmentEntry{
				SegmentId: 1,
				EntryId:   int64(i),
				Data:      []byte(fmt.Sprintf("test_%d", i)),
			}).Return(int64(i), ch, werr.ErrSegmentWriteException).Times(2)
			// append entry 2 success at the 3rd retry
			mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &segment.SegmentEntry{
				SegmentId: 1,
				EntryId:   int64(i),
				Data:      []byte(fmt.Sprintf("test_%d", i)),
			}).Return(int64(i), ch, nil)
		}

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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg)

	callbackCalled := 0
	successCount := 0
	syncedIds := make([]int64, 0)
	for i := 0; i < 5; i++ {
		callback := func(segmentId int64, entryId int64, err error) {
			callbackCalled++
			assert.NoError(t, err, fmt.Sprintf("entry:%d add fail", entryId))
			assert.Equal(t, int64(1), segmentId)
			assert.Equal(t, int64(i), entryId)
			if err == nil {
				successCount++
				syncedIds = append(syncedIds, entryId)
			}
		}
		segmentHandle.AppendAsync(context.Background(), []byte(fmt.Sprintf("test_%d", i)), callback)
	}
	time.Sleep(1000 * time.Millisecond) // Give some time for the async operation to complete
	assert.Equal(t, 5, callbackCalled)
	assert.Equal(t, 5, successCount)
	assert.Equal(t, []int64{0, 1, 2, 3, 4}, syncedIds)
}

func TestDisorderMultiAppendAsync_AllSuccess_InSequential(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().IsSegmentFenced(mock.Anything, int64(1), mock.Anything).Return(false, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)
	for i := 0; i < 20; i++ {
		ch := make(chan int64, 1)
		ch <- int64(i)
		close(ch)
		if i%2 == 0 {
			// 0,2,4,6,8,10,12,14,16,18,20  fail first time,
			mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &segment.SegmentEntry{
				SegmentId: 1,
				EntryId:   int64(i),
				Data:      []byte(fmt.Sprintf("test_%d", i)),
			}).Return(int64(i), ch, nil).Times(1)
			// 0,2,4,6,8,10,12,14,16,18,20  success at the second time,
			mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &segment.SegmentEntry{
				SegmentId: 1,
				EntryId:   int64(i),
				Data:      []byte(fmt.Sprintf("test_%d", i)),
			}).Return(int64(i), ch, nil)
		} else {
			// 1,3,5,7,9,11,13,15,17,19  success first time
			mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &segment.SegmentEntry{
				SegmentId: 1,
				EntryId:   int64(i),
				Data:      []byte(fmt.Sprintf("test_%d", i)),
			}).Return(int64(i), ch, nil)
		}
		mockClient.EXPECT().AppendEntry(mock.Anything, int64(1), &segment.SegmentEntry{
			SegmentId: 1,
			EntryId:   int64(i),
			Data:      []byte(fmt.Sprintf("test_%d", i)),
		}).Return(int64(i), ch, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg)

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
	time.Sleep(100 * time.Millisecond) // Give some time for the async operation to complete
	assert.Equal(t, 20, callbackCalledNum)
	assert.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, syncedIds)
}

func TestSegmentHandleFenceAndClosed(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockClient, nil)
	ch := make(chan int64, 1)
	ch <- int64(0)
	close(ch)
	mockClient.EXPECT().FenceSegment(mock.Anything, int64(1), mock.Anything).Return(nil)
	mockClient.EXPECT().IsSegmentFenced(mock.Anything, int64(1), mock.Anything).Return(true, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg)
	fenceErr := segmentHandle.Fence(context.Background())
	assert.NoError(t, fenceErr)
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

	closeErr := segmentHandle.Close(context.Background())
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
		segmentHandle.SendAppendSuccessCallbacks(int64(i))
	}
	time.Sleep(1000 * time.Millisecond) // Give some time for the async operation to complete
	assert.Equal(t, 0, successCount)
	for i := 0; i < 3; i++ {
		ops[i].ackSet.Set(0)
		ops[i].completed.Store(true)
		segmentHandle.SendAppendSuccessCallbacks(int64(i))
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
			segmentHandle.SendAppendErrorCallbacks(int64(i), werr.ErrSegmentWriteException)
		} else {
			// success
			ops[i].ackSet.Set(0)
			ops[i].completed.Store(true)
			segmentHandle.SendAppendSuccessCallbacks(int64(i))
		}
	}
	time.Sleep(1000 * time.Millisecond) // Give some time for the async operation to complete
	assert.Equal(t, 5, successCount)
	assert.Equal(t, 5, failedCount)
	assert.Equal(t, []int64{0, 1, 2, 3, 4}, successIds)
}
