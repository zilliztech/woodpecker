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
	"testing/synctest"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/topology"
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)
	assert.Equal(t, "testLog", segmentHandle.GetLogName())
	assert.Equal(t, int64(1), segmentHandle.GetId(context.Background()))
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)
	lastEntryId, fenceErr := segmentHandle.FenceAndComplete(context.Background())
	waitForAppendCallbacks(segmentHandle.(*segmentHandleImpl))
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
	cleanupInjectedAppender(t, segmentHandle)
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
			segmentMeta.Metadata.Quorum, nil,
		)
		ops = append(ops, op)
		testQueue.PushBack(op)
	}

	// success 3-9, but 0-2 is not finish
	for i := 3; i < 10; i++ {
		ops[i].ackSet.Set(0)
		ops[i].completed.Store(true)
		segmentHandle.SendAppendSuccessCallbacks(context.TODO(), int64(i))
		waitForAppendCallbacks(segmentHandle.(*segmentHandleImpl))
	}
	time.Sleep(1000 * time.Millisecond) // Give some time for the async operation to complete
	assert.Equal(t, 0, successCount)
	for i := 0; i < 3; i++ {
		ops[i].ackSet.Set(0)
		ops[i].completed.Store(true)
		segmentHandle.SendAppendSuccessCallbacks(context.TODO(), int64(i))
		waitForAppendCallbacks(segmentHandle.(*segmentHandleImpl))
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
	cleanupInjectedAppender(t, segmentHandle)
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
			segmentMeta.Metadata.Quorum, nil,
		)
		ops = append(ops, op)
		testQueue.PushBack(op)
	}

	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(4, nil).Maybe()
	// success 3-9, but 0-2 is not finish
	for i := 0; i < 10; i++ {
		if i == 5 {
			// fail
			segmentHandle.HandleAppendRequestFailure(context.TODO(), int64(i), werr.ErrSegmentHandleWriteFailed, 0, "127.0.0.1")
		} else {
			// success
			ops[i].ackSet.Set(0)
			ops[i].completed.Store(true)
			segmentHandle.SendAppendSuccessCallbacks(context.TODO(), int64(i))
			waitForAppendCallbacks(segmentHandle.(*segmentHandleImpl))
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
	cleanupInjectedAppender(t, segmentHandle)

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
			segmentMeta.Metadata.Quorum, nil,
		)
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
	waitForAppendCallbacks(segmentHandle.(*segmentHandleImpl))
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
	cleanupInjectedAppender(t, segmentHandle)

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
			segmentMeta.Metadata.Quorum, nil,
		)
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
	waitForAppendCallbacks(segmentHandle.(*segmentHandleImpl))
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)

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
	checkRecovery := trackRecoveryMetrics(t)
	defer checkRecovery(nil)
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
	cleanupInjectedAppender(t, segmentHandle)

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
			segmentMeta.Metadata.Quorum, nil,
		)
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
		waitForAppendCallbacks(segmentHandle.(*segmentHandleImpl))
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
	cleanupInjectedAppender(t, segmentHandle)

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
			segmentMeta.Metadata.Quorum, nil,
		)
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
		waitForAppendCallbacks(segmentHandle.(*segmentHandleImpl))
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
	cleanupInjectedAppender(t, segmentHandle)

	// Add operations to the queue
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
			segmentMeta.Metadata.Quorum, nil,
		)
		testQueue.PushBack(op)
	}

	// Initially not rolling
	assert.False(t, segmentHandle.IsForceRollingReady(context.Background()))
	assert.Equal(t, 3, testQueue.Len())

	// Trigger a final error for entry 1. Its callback and the suffix must wait
	// behind unfinished entry 0, while the segment enters rolling immediately.
	segmentHandle.HandleAppendRequestFailure(context.Background(), int64(1), werr.ErrSegmentHandleWriteFailed, 0, "127.0.0.1")

	// Give some time for processing
	time.Sleep(100 * time.Millisecond)

	// Verify rolling was triggered
	assert.True(t, segmentHandle.IsForceRollingReady(context.Background()))

	// Failed entries remain behind the unfinished prefix until their ordered
	// callbacks can be delivered. They do not advance LAC.
	assert.Equal(t, 3, testQueue.Len())
	assert.Equal(t, int64(-1), segmentHandle.(*segmentHandleImpl).confirmedLAC())

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
	waitForAppendCallbacks(segmentHandle.(*segmentHandleImpl))

	// Give some time for auto-close to happen
	time.Sleep(200 * time.Millisecond)

	// Now the queue should be empty (auto-close happened)
	assert.Equal(t, 0, testQueue.Len())

	waitForAppendCallbacks(segmentHandle.(*segmentHandleImpl))
	for i := 1; i < 3; i++ {
		assert.Error(t, callbackResults[i])
	}
	// Entry 0 should have succeeded
	assert.NoError(t, callbackResults[0], "Entry 0 should succeed")
}

// TestSegmentHandle_ForceCompleteAndClose_WithRollingState tests ForceCompleteAndClose behavior
// when segment is in rolling state
func TestSegmentHandle_ForceCompleteAndClose_WithRollingState(t *testing.T) {
	checkRecovery := trackRecoveryMetrics(t)
	defer checkRecovery(nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)

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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)

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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)

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

// An empty queue must not make rollover wait on a slow fence. While fencing
// is blocked, old appends are rejected and Completed metadata is not published.
func TestSegmentHandle_SetRollingReady_EmptyQueueCompletesAsynchronously(t *testing.T) {
	seg, mockClient := newTestSegmentForCompletion(t)
	fenceStarted := make(chan struct{})
	releaseFence := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFence) }) }
	t.Cleanup(func() {
		release()
		assert.NoError(t, seg.completionMgr.WaitForCompletion())
	})
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).
		Run(func(context.Context, string, string, int64, int64) {
			close(fenceStarted)
			<-releaseFence
		}).Return(int64(0), nil).Once()
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(0)).
		Return(int64(0), nil).Once()
	returned := make(chan error, 1)
	go func() { returned <- seg.SetRollingReady(context.Background()) }()
	select {
	case err := <-returned:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("SetRollingReady waited for the blocked fence")
	}
	select {
	case <-fenceStarted:
	case <-time.After(time.Second):
		t.Fatal("background fence did not start")
	}
	select {
	case <-seg.completionMgr.doneCh:
		t.Fatal("completion finished before the fence was released")
	default:
	}
	require.Equal(t, proto.SegmentState_Active, seg.segmentMetaCache.Load().Metadata.State)
	var appendErr error
	seg.AppendAsync(context.Background(), []byte("rejected after rolling"), func(_, _ int64, err error) {
		appendErr = err
	})
	require.True(t, werr.ErrSegmentHandleSegmentRolling.Is(appendErr), "got %v", appendErr)
	release()
	require.NoError(t, seg.completionMgr.WaitForCompletion())
	require.Equal(t, proto.SegmentState_Completed, seg.segmentMetaCache.Load().Metadata.State)
	writable, err := seg.IsWritable(context.Background())
	require.NoError(t, err)
	require.False(t, writable)
}

func TestResolveFenceLAC(t *testing.T) {
	s := &segmentHandleImpl{}

	tests := []struct {
		name        string
		results     []int64
		writeQuorum int
		ackQuorum   int
		expected    int64
		wantErr     bool
	}{
		{
			name:        "normal 3 nodes",
			results:     []int64{4, 6, 7},
			writeQuorum: 3,
			ackQuorum:   2,
			expected:    6,
		},
		{
			name:        "all same value",
			results:     []int64{5, 5, 5},
			writeQuorum: 3,
			ackQuorum:   2,
			expected:    5,
		},
		{
			// Two of three replicas answered. Entry 0 may also be on the replica that did
			// not answer, in which case it reached ack quorum and the client was told so.
			// A missing reply is not evidence that the entry is absent, so the target may
			// not be lowered past it. This is the history in #320: selecting -1 here
			// published an empty segment over an acknowledged write.
			name:        "missing reply may not lower the target",
			results:     []int64{0, -1},
			writeQuorum: 3,
			ackQuorum:   2,
			expected:    0,
		},
		{
			// All three replicas answered, so the evidence is complete: entry 0 really is
			// on one replica only and was never acked. Lowering to -1 is correct, and this
			// is the case an earlier fix was written for -- a target of 0 could only ever
			// be finalized by one replica, so completion never reached quorum.
			name:        "complete evidence may lower the target",
			results:     []int64{0, -1, -1},
			writeQuorum: 3,
			ackQuorum:   2,
			expected:    -1,
		},
		{
			name:        "two valid one empty",
			results:     []int64{-1, 0, 0},
			writeQuorum: 3,
			ackQuorum:   2,
			expected:    0,
		},
		{
			// Same shape as the #320 case: only two replies, so entry 2 may be on the
			// silent replica as well.
			name:        "multiple entries with one silent replica",
			results:     []int64{2, -1},
			writeQuorum: 3,
			ackQuorum:   2,
			expected:    2,
		},
		{
			name:        "all nodes empty",
			results:     []int64{-1, -1},
			writeQuorum: 3,
			ackQuorum:   2,
			expected:    -1,
		},
		{
			name:        "single empty node",
			results:     []int64{-1},
			writeQuorum: 1,
			ackQuorum:   1,
			expected:    -1,
		},
		{
			name:        "empty results cannot prove coverage",
			results:     []int64{},
			writeQuorum: 3,
			ackQuorum:   2,
			wantErr:     true,
		},
		{
			name:        "single valid node",
			results:     []int64{0},
			writeQuorum: 1,
			ackQuorum:   1,
			expected:    0,
		},
		{
			// One reply out of three leaves two silent replicas, more than the one that may
			// legitimately be missing an acknowledged entry, so nothing can be concluded.
			name:        "too few replies to skip the tolerated missing replicas",
			results:     []int64{3},
			writeQuorum: 3,
			ackQuorum:   2,
			wantErr:     true,
		},
		{
			name:        "three valid nodes ackQuorum=2",
			results:     []int64{10, 8, 5},
			writeQuorum: 3,
			ackQuorum:   2,
			expected:    8,
		},
		{
			name:        "negative sentinel other than -1",
			results:     []int64{0, -2, 3},
			writeQuorum: 3,
			ackQuorum:   2,
			expected:    0,
		},
		{
			name:        "does not mutate input",
			results:     []int64{7, 3, 5},
			writeQuorum: 3,
			ackQuorum:   2,
			expected:    5,
		},
		{
			// Five replicas tolerate two missing an acknowledged entry.
			name:        "five replicas all answered",
			results:     []int64{9, 7, 5, 3, 1},
			writeQuorum: 5,
			ackQuorum:   3,
			expected:    5,
		},
		{
			name:        "five replicas two silent",
			results:     []int64{9, 7, 5},
			writeQuorum: 5,
			ackQuorum:   3,
			expected:    9,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// copy input to verify no mutation
			original := make([]int64, len(tt.results))
			copy(original, tt.results)

			got, err := s.resolveFenceLAC(tt.results, tt.writeQuorum, tt.ackQuorum)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Equal(t, int64(-1), got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}

			// verify input slice is not mutated
			assert.Equal(t, original, tt.results, "input slice should not be mutated")
		})
	}
}

// TestResolveFenceLACNeverDropsAcknowledgedData exhaustively checks the property recovery exists
// to uphold: whatever subset of replicas answers the fence, the selected target is never below the
// boundary the client was told was durable.
//
// The acknowledged boundary is a fact about the replicas, not about the replies: an entry is
// acknowledged once at least Aq replicas hold it. A replica that does not answer is unknown, not
// empty, so at most Wq-Aq of the answers may legitimately be missing an acknowledged entry -- the
// same tolerance the append path spends before it gives up. Indexing by that count holds for every
// quorum shape; the previous len(results)-ackQuorum held only when every replica answered, which
// is how #320 published an empty segment over an acknowledged write.
//
// Odd and even write quorums are both covered even though the config currently offers only 3 and 5,
// because the two differ: at Wq=4 the tolerance is 1 while ackQuorum-1 would be 2.
func TestResolveFenceLACNeverDropsAcknowledgedData(t *testing.T) {
	s := &segmentHandleImpl{}
	domain := []int64{-1, 0, 1}

	// acknowledgedBoundary is the highest entry id held by at least ackQuorum replicas.
	acknowledgedBoundary := func(tails []int64, ackQuorum int) int64 {
		best := int64(-1)
		for _, candidate := range tails {
			held := 0
			for _, tail := range tails {
				if tail >= candidate {
					held++
				}
			}
			if held >= ackQuorum && candidate > best {
				best = candidate
			}
		}
		return best
	}

	for writeQuorum := 3; writeQuorum <= 7; writeQuorum++ {
		ackQuorum := writeQuorum/2 + 1
		missingTolerated := writeQuorum - ackQuorum

		t.Run(fmt.Sprintf("wq_%d_aq_%d", writeQuorum, ackQuorum), func(t *testing.T) {
			tails := make([]int64, writeQuorum)

			var walk func(depth int)
			walk = func(depth int) {
				if depth < writeQuorum {
					for _, v := range domain {
						tails[depth] = v
						walk(depth + 1)
					}
					return
				}

				acked := acknowledgedBoundary(tails, ackQuorum)

				// Every subset of replicas that could have answered the fence.
				for mask := 0; mask < 1<<writeQuorum; mask++ {
					replies := make([]int64, 0, writeQuorum)
					for i := 0; i < writeQuorum; i++ {
						if mask&(1<<i) != 0 {
							replies = append(replies, tails[i])
						}
					}

					lac, err := s.resolveFenceLAC(replies, writeQuorum, ackQuorum)
					if len(replies) <= missingTolerated {
						assert.Error(t, err, "tails=%v replies=%v: too few replies must not yield a target", tails, replies)
						continue
					}
					if !assert.NoError(t, err, "tails=%v replies=%v", tails, replies) {
						continue
					}

					assert.GreaterOrEqual(t, lac, acked,
						"tails=%v replies=%v: selected %d, below the acknowledged boundary %d", tails, replies, lac, acked)

					// With every replica answering the evidence is complete, so the target is
					// exactly the acknowledged boundary -- no lower (data loss) and no higher
					// (a target too few replicas can finalize, which wedges completion).
					if len(replies) == writeQuorum {
						assert.Equal(t, acked, lac, "tails=%v: complete evidence should select the acknowledged boundary", tails)
					}
				}
			}
			walk(0)
		})
	}
}

// TestResolveFenceLACIssue320 is the reported history: three replicas, tails [0,0,-1] so entry 0
// reached ack quorum and the client was told it was durable, then the fence RPC to the second
// replica fails. The surviving replies are [0,-1].
func TestResolveFenceLACIssue320(t *testing.T) {
	s := &segmentHandleImpl{}

	lac, err := s.resolveFenceLAC([]int64{0, -1}, 3, 2)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), lac, "the acknowledged entry must survive a failed fence reply")

	// Rotating which replica is empty and which fence reply is lost must not change that.
	for _, replies := range [][]int64{{-1, 0}, {0, -1}} {
		got, err := s.resolveFenceLAC(replies, 3, 2)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), got, "replies=%v", replies)
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
			segmentHandle := NewSegmentHandle(context.Background(), 1, tt.logName, segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
			segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)

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

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)

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
			segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
		{"truncated_state", proto.SegmentState_Truncated},
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

			segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	mockClient.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(compactedSegMetaInfo, nil)
	// No NotifySegmentCompacted expectation: mark distribution is auditor-driven
	// (SegmentCompactedNotifyManager), not inline in Compact; the strict mock fails
	// this test if Compact ever notifies.

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

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", completedMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	mockClient.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(compactedSegMetaInfo, nil)
	// No NotifySegmentCompacted expectation: mark distribution is auditor-driven
	// (SegmentCompactedNotifyManager), not inline in Compact; the strict mock fails
	// this test if Compact ever notifies.

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

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", completedMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	mockClient.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil, errors.New("compaction failed"))

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

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", completedMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	mockClient.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).RunAndReturn(
		func(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, expectedLastEntryId int64) (*proto.SegmentMetadata, error) {
			<-blockCh
			return compactedResult, nil
		},
	).Maybe()
	mockClient.EXPECT().NotifySegmentCompacted(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(nil).Maybe()

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

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", completedMeta, mockMetadata, mockClientPool, cfg, false, nil)

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

// TestCompact_DoesNotNotifyInline verifies the async-marking contract on the Compact side:
// a successful compaction updates the metadata to Sealed and returns WITHOUT sending any
// NotifySegmentCompacted RPC and without dialing the other quorum nodes — mark distribution
// belongs to the auditor-driven SegmentCompactedNotifyManager (covered by its own tests).
// The strict mocks enforce the negative: any notify or extra dial fails this test.
func TestCompact_DoesNotNotifyInline(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient0 := mocks_logstore_client.NewLogStoreClient(t)

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node0").Return(mockClient0, nil)

	completedMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Completed,
			LastEntryId: 10,
			Size:        2048,
			Quorum: &proto.QuorumInfo{
				Id:    1,
				Aq:    2,
				Es:    3,
				Wq:    2,
				Nodes: []string{"node0", "node1", "node2"},
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

	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(completedMeta, nil).Once()
	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, "testLog", int64(1)).Return(sealedMeta, nil).Once()
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, "testLog", int64(1), mock.Anything, proto.SegmentState_Completed).Return(nil)

	// compactSegmentQuorum tries nodes in order and stops at the first success. No other
	// node is dialed and no NotifySegmentCompacted expectation exists anywhere.
	mockClient0.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(compactedSegMetaInfo, nil)

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

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", completedMeta, mockMetadata, mockClientPool, cfg, false, nil)
	err := segmentHandle.Compact(context.Background())
	assert.NoError(t, err)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)

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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)

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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	result, err := segmentHandle.ReadBatchAdv(context.Background(), 0, 10, nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, len(result.Entries))
	assert.Equal(t, "node2", result.LastReadState.Node)
}

func TestReadBatchAdv_PrefersLocalRegionAZReplica(t *testing.T) {
	t.Setenv("REGION", "region-local")
	t.Setenv("AVAILABILITY_ZONE", "az-local")

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node-local").Return(mockClient, nil).Once()

	expectedResult := &proto.BatchReadResult{
		Entries: []*proto.LogEntry{
			{SegId: 1, EntryId: 0, Values: []byte("entry_0")},
		},
		LastReadState: &proto.LastReadState{},
	}
	mockClient.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(0), int64(10), (*proto.LastReadState)(nil)).Return(expectedResult, nil).Once()

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
				Nodes: []string{"node-remote-a", "node-local", "node-remote-b"},
				Replicas: []*proto.QuorumNode{
					{Endpoint: "node-remote-a", Region: "region-remote", Az: "az-remote-a"},
					{Endpoint: "node-local", Region: "region-local", Az: "az-local"},
					{Endpoint: "node-remote-b", Region: "region-local", Az: "az-other"},
				},
			},
		},
		Revision: 1,
	}

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	result, err := segmentHandle.ReadBatchAdv(context.Background(), 0, 10, nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "node-local", result.LastReadState.Node)
}

func TestReadBatchAdv_NoLocalReplicaFallsBackToOriginalOrder(t *testing.T) {
	t.Setenv("REGION", "region-local")
	t.Setenv("AVAILABILITY_ZONE", "az-local")

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node-first").Return(mockClient, nil).Once()

	expectedResult := &proto.BatchReadResult{
		Entries:       []*proto.LogEntry{{SegId: 1, EntryId: 0, Values: []byte("entry_0")}},
		LastReadState: &proto.LastReadState{},
	}
	mockClient.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(0), int64(10), (*proto.LastReadState)(nil)).Return(expectedResult, nil).Once()

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
				Nodes: []string{"node-first", "node-second"},
				Replicas: []*proto.QuorumNode{
					{Endpoint: "node-first", Region: "region-remote", Az: "az-remote-a"},
					{Endpoint: "node-second", Region: "region-remote", Az: "az-remote-b"},
				},
			},
		},
		Revision: 1,
	}

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	result, err := segmentHandle.ReadBatchAdv(context.Background(), 0, 10, nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "node-first", result.LastReadState.Node)
}

func TestOrderedQuorumReadCandidates_MissingLocalTopologyKeepsOriginalOrder(t *testing.T) {
	t.Setenv("REGION", "region-local")
	t.Setenv("AVAILABILITY_ZONE", "")

	candidates := orderedQuorumReadCandidates(&proto.QuorumInfo{
		Nodes: []string{"node-first", "node-local"},
		Replicas: []*proto.QuorumNode{
			{Endpoint: "node-first", Region: "region-remote", Az: "az-remote"},
			{Endpoint: "node-local", Region: "region-local", Az: "az-local"},
		},
	}, nil)

	// A client that does not know its own zone cannot classify any replica.
	assert.Equal(t, []quorumReadCandidate{
		{node: "node-first", originalIndex: 0, azScope: topology.ScopeUnknown},
		{node: "node-local", originalIndex: 1, azScope: topology.ScopeUnknown},
	}, candidates)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)

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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)

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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	impl := sh.(*segmentHandleImpl)

	_, err := impl.completeSegmentOnNode(context.Background(), "node1", 5)
	assert.EqualError(t, err, "conn error")
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	impl := sh.(*segmentHandleImpl)

	_, err := impl.fenceSegmentOnNode(context.Background(), "node1")
	assert.EqualError(t, err, "conn error")
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	impl := sh.(*segmentHandleImpl)

	quorum := &proto.QuorumInfo{Id: 1, Aq: 2, Es: 2, Wq: 2, Nodes: []string{"node1", "node2"}}
	err := impl.completeSegmentQuorum(context.Background(), quorum, 5, waitForAllReplicas)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	impl := sh.(*segmentHandleImpl)

	quorum := &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"node1"}}
	err := impl.completeSegmentQuorum(ctx, quorum, 5, waitForAllReplicas)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	impl := sh.(*segmentHandleImpl)
	impl.fencedState.Store(true)
	impl.observeFinalLAC(10)

	lastEntry, err := sh.FenceAndComplete(context.Background())
	waitForAppendCallbacks(sh.(*segmentHandleImpl))
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	impl := sh.(*segmentHandleImpl)
	impl.quorumInfo = nil

	_, err := sh.FenceAndComplete(context.Background())
	waitForAppendCallbacks(sh.(*segmentHandleImpl))
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)

	_, err := sh.FenceAndComplete(context.Background())
	waitForAppendCallbacks(sh.(*segmentHandleImpl))
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)

	_, err := sh.FenceAndComplete(context.Background())
	waitForAppendCallbacks(sh.(*segmentHandleImpl))
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)

	_, err := sh.FenceAndComplete(context.Background())
	waitForAppendCallbacks(sh.(*segmentHandleImpl))
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)

	lastEntry, err := sh.FenceAndComplete(context.Background())
	waitForAppendCallbacks(sh.(*segmentHandleImpl))
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	mockClient.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil, errors.New("compact failed"))
	// Second node: get client fails
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(nil, errors.New("conn error"))

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	impl := sh.(*segmentHandleImpl)

	quorum := &proto.QuorumInfo{Id: 1, Nodes: []string{"node1", "node2"}}
	_, err := impl.compactSegmentQuorum(context.Background(), quorum, int64(-1))
	assert.Error(t, err)
}

// TestCompactSegmentQuorum_AllNodesBehindFails: when every quorum node refuses as data-behind, the
// segment is NOT sealed — compaction fails (and is retried later), never accepting a short seal.
func TestCompactSegmentQuorum_AllNodesBehindFails(t *testing.T) {
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

	const expectedLAC = int64(5)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient1, nil)
	mockClient1.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), expectedLAC).
		Return(nil, werr.ErrSegmentCompactionDataBehind)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient2, nil)
	mockClient2.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), expectedLAC).
		Return(nil, werr.ErrSegmentCompactionDataBehind)

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	impl := sh.(*segmentHandleImpl)
	failuresBefore := testutil.ToFloat64(metrics.WpSegmentCompactionFailuresTotal.WithLabelValues(impl.logNs, "1", compactionFailureReasonDataBehind))

	quorum := &proto.QuorumInfo{Id: 1, Nodes: []string{"node1", "node2"}}
	result, err := impl.compactSegmentQuorum(context.Background(), quorum, expectedLAC)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.True(t, werr.ErrSegmentCompactionDataBehind.Is(err), "surfaced error should be data-behind, got %v", err)
	assert.Equal(t, failuresBefore+1,
		testutil.ToFloat64(metrics.WpSegmentCompactionFailuresTotal.WithLabelValues(impl.logNs, "1", compactionFailureReasonDataBehind)))
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
	mockClient1.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(nil, errors.New("compact failed"))
	// Second node succeeds
	expectedMeta := &proto.SegmentMetadata{SegNo: 1, Size: 100, LastEntryId: 10}
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient2, nil)
	mockClient2.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), mock.Anything).Return(expectedMeta, nil)

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	impl := sh.(*segmentHandleImpl)
	failuresBefore := testutil.ToFloat64(metrics.WpSegmentCompactionFailuresTotal.WithLabelValues(impl.logNs, "1", compactionFailureReasonOther))

	quorum := &proto.QuorumInfo{Id: 1, Nodes: []string{"node1", "node2"}}
	result, err := impl.compactSegmentQuorum(context.Background(), quorum, int64(-1))
	assert.NoError(t, err)
	assert.Equal(t, expectedMeta, result)
	assert.Equal(t, failuresBefore,
		testutil.ToFloat64(metrics.WpSegmentCompactionFailuresTotal.WithLabelValues(impl.logNs, "1", compactionFailureReasonOther)))
}

// TestCompactSegmentQuorum_SkipsLaggingNode is the end-to-end guard for the silent data-loss
// bug: the first node is behind and refuses (ErrSegmentCompactionDataBehind), so compaction
// must skip it and seal from the caught-up second node — never accepting the lagging node's
// short result. It also pins that the confirmed LastEntryId is forwarded to every node.
func TestCompactSegmentQuorum_SkipsLaggingNode(t *testing.T) {
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

	const expectedLAC = int64(1)
	// node1 is behind and refuses. The 6th matcher pins that it receives the confirmed LAC.
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient1, nil)
	mockClient1.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), expectedLAC).
		Return(nil, werr.ErrSegmentCompactionDataBehind)
	// node2 is caught up and seals the full segment (LastEntryId == confirmed LAC).
	fullMeta := &proto.SegmentMetadata{SegNo: 1, Size: 100, LastEntryId: expectedLAC}
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient2, nil)
	mockClient2.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), expectedLAC).
		Return(fullMeta, nil)

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	impl := sh.(*segmentHandleImpl)

	quorum := &proto.QuorumInfo{Id: 1, Nodes: []string{"node1", "node2"}}
	result, err := impl.compactSegmentQuorum(context.Background(), quorum, expectedLAC)
	assert.NoError(t, err)
	assert.Equal(t, fullMeta, result)
	assert.Equal(t, expectedLAC, result.LastEntryId)
}

func TestClassifyCompactionFailureReason(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "data behind",
			err:  werr.ErrSegmentCompactionDataBehind.WithCauseErrMsg("behind"),
			want: "data_behind",
		},
		{
			name: "invalid lac alignment",
			err:  werr.ErrInvalidLACAlignment.WithCauseErrMsg("bad lac"),
			want: "invalid_lac_alignment",
		},
		{
			name: "retryable",
			err:  werr.ErrMinioOperationErr.WithCauseErrMsg("timeout"),
			want: "transient",
		},
		{
			name: "deadline",
			err:  context.DeadlineExceeded,
			want: "transient",
		},
		{
			name: "other",
			err:  errors.New("plain failure"),
			want: "other",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, classifyCompactionFailureReason(tt.err))
		})
	}
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	impl := sh.(*segmentHandleImpl)

	quorum := &proto.QuorumInfo{Id: 1, Es: 2, Aq: 2, Wq: 2, Nodes: []string{"node1", "node2"}}
	_, err := impl.fenceSegmentQuorum(context.Background(), quorum, waitForAllReplicas)
	assert.Error(t, err)
}

// TestFenceSegmentQuorum_PartialRepliesBelowTolerance covers the shape that is not "everything
// failed": a valid three-replica quorum where one node answers and two do not. An acknowledged
// entry can be missing from at most Wq-Aq = 1 replica, so a single reply leaves two unknowns --
// more than the tolerance -- and even that reply could be the stale one. Fence must refuse rather
// than hand completion a target derived from insufficient evidence.
//
// The surfaced error is the node failure, not the resolve error: with a valid quorum every result
// is either an error or a success, so too few successes always implies at least one node error,
// and fence prefers it as the root cause. That also means the resolve error is only reachable
// through a malformed quorum or a cancelled collection, which is why the tolerance guard itself is
// covered by unit tests on resolveFenceLAC rather than from here.
func TestFenceSegmentQuorum_PartialRepliesBelowTolerance(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	// node1 answers; node2 and node3 do not.
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(int64(5), nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(nil, errors.New("conn error"))
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node3").Return(nil, errors.New("conn error"))

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	impl := sh.(*segmentHandleImpl)

	quorum := &proto.QuorumInfo{Id: 1, Es: 3, Aq: 2, Wq: 3, Nodes: []string{"node1", "node2", "node3"}}
	lastEntryId, err := impl.fenceSegmentQuorum(context.Background(), quorum, waitForAllReplicas)
	assert.Error(t, err)
	assert.Equal(t, int64(-1), lastEntryId)
	assert.Contains(t, err.Error(), "conn error", "the node failure should surface as the root cause")
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)
	impl := sh.(*segmentHandleImpl)

	// Stop the executor so Submit returns false
	impl.appender.executor.Stop(context.Background())

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
	cleanupInjectedAppender(t, sh)
	impl := sh.(*segmentHandleImpl)

	// Set LAC to 5 — any op with entryId <= 5 should go through the "already confirmed" branch
	impl.observeFinalLAC(5)

	// Create ops with entryId 3, 4 (below LAC) and 7 (above, not next)
	successIds := make([]int64, 0)
	mu := sync.Mutex{}
	for _, id := range []int64{3, 4, 7} {
		entryId := id
		op := NewAppendOp("b", "r", 1, 1, entryId, []byte("data"), func(segmentId int64, eid int64, err error) {
			mu.Lock()
			successIds = append(successIds, eid)
			mu.Unlock()
		}, segmentMeta.Metadata.Quorum, nil)
		op.completed.Store(true)
		testQueue.PushBack(op)
	}

	sh.SendAppendSuccessCallbacks(context.Background(), 4)
	waitForAppendCallbacks(sh.(*segmentHandleImpl))
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
	cleanupInjectedAppender(t, sh)

	// Add op that is NOT completed — should trigger the "not completed" break
	op := NewAppendOp("b", "r", 1, 1, 0, []byte("data"), func(segmentId int64, eid int64, err error) {}, segmentMeta.Metadata.Quorum, nil)
	op.completed.Store(false)
	testQueue.PushBack(op)

	// Should not panic and should leave the queue unchanged
	sh.SendAppendSuccessCallbacks(context.Background(), 0)
	waitForAppendCallbacks(sh.(*segmentHandleImpl))
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)
	impl := sh.(*segmentHandleImpl)
	impl.completionMgr = nil

	err := sh.ForceCompleteAndClose(context.Background())
	assert.NoError(t, err) // Returns nil when completionMgr is nil
}

// TestAppendAsync_FencedAfterPreCheck_DetectedUnderLock verifies the double-check
// fenced path under lock (L245-L248): fenced state is set after the pre-lock check
// passes but before the under-lock check runs.
func TestAppendAsync_FencedAfterPreCheck_DetectedUnderLock(t *testing.T) {
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)
	impl := sh.(*segmentHandleImpl)

	done := make(chan struct{})
	ready := make(chan struct{})
	go func() {
		impl.Lock()
		close(ready)
		// Wait for AppendAsync to pass the pre-lock check and block on Lock()
		time.Sleep(50 * time.Millisecond)
		// Set fenced AFTER AppendAsync passed the pre-lock check
		impl.fencedState.Store(true)
		impl.Unlock()
	}()
	<-ready
	time.Sleep(10 * time.Millisecond) // Let goroutine settle past Lock()

	sh.AppendAsync(context.Background(), []byte("test"), func(segmentId int64, entryId int64, err error) {
		assert.Error(t, err)
		assert.True(t, werr.ErrSegmentFenced.Is(err))
		close(done)
	})
	<-done
}

// TestAppendAsync_RollingAfterPreCheck_DetectedUnderLock verifies the under-lock
// rolling check (L252-L255): rolling state is set after the pre-lock check passes
// but before the under-lock check runs.
func TestAppendAsync_RollingAfterPreCheck_DetectedUnderLock(t *testing.T) {
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
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil)
	impl := sh.(*segmentHandleImpl)

	done := make(chan struct{})
	ready := make(chan struct{})
	go func() {
		impl.Lock()
		close(ready)
		// Wait for AppendAsync to pass the pre-lock check and block on Lock()
		time.Sleep(50 * time.Millisecond)
		// Set rolling AFTER AppendAsync passed the pre-lock check
		impl.rollingState.Store(true)
		impl.Unlock()
	}()
	<-ready
	time.Sleep(10 * time.Millisecond) // Let goroutine settle past Lock()

	sh.AppendAsync(context.Background(), []byte("test"), func(segmentId int64, entryId int64, err error) {
		assert.Error(t, err)
		assert.True(t, werr.ErrSegmentHandleSegmentRolling.Is(err))
		close(done)
	})
	<-done
}

// TestHandleAppendRequestFailure_OpAlreadyRemovedFromQueue tests the scenario where
// HandleAppendRequestFailure is called for an entryId that's no longer in the queue.
// This simulates a 3-node quorum (Es=3, Wq=3, Aq=2): 2 nodes succeed and the op is
// removed via SendAppendSuccessCallbacks, then the 3rd node reports failure — the op
// is already gone so HandleAppendRequestFailure should return early without panic.
func TestHandleAppendRequestFailure_OpAlreadyRemovedFromQueue(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	// Allow background syncLACToQuorumAsync goroutines to call GetLogStoreClient after test completes
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).
		Return(nil, errors.New("test ending")).Maybe()
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 3},
			},
		},
	}

	// 3-node quorum: Es=3, Wq=3, Aq=2
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{
				Id: 1, Aq: 2, Es: 3, Wq: 3,
				Nodes: []string{"node1:8080", "node2:8080", "node3:8080"},
			},
		},
		Revision: 1,
	}

	testQueue := list.New()
	sh := NewSegmentHandleWithAppendOpsQueueWithWritable(
		context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, testQueue, false,
	)
	cleanupInjectedAppender(t, sh)
	impl := sh.(*segmentHandleImpl)

	// Simulate: an AppendOp for entryId=0 was added and already completed by 2 nodes
	callbackCount := 0
	callback := func(segmentId int64, entryId int64, err error) {
		callbackCount++
	}
	appendOp := NewAppendOp("bucket", "root", 1, 1, 0, []byte("data"), callback,
		segmentMeta.Metadata.Quorum, nil)

	// Mark the op as completed (2 ack quorum nodes succeeded)
	appendOp.completed.Store(true)
	testQueue.PushBack(appendOp)

	// SendAppendSuccessCallbacks removes the op from queue and triggers the callback once
	impl.observeFinalLAC(-1)
	sh.SendAppendSuccessCallbacks(context.Background(), 0)
	waitForAppendCallbacks(impl)

	// Verify op was removed from queue and callback was called exactly once
	assert.Equal(t, 0, testQueue.Len(), "op should have been removed by SendAppendSuccessCallbacks")
	assert.Equal(t, 1, callbackCount, "callback should have been called once by SendAppendSuccessCallbacks")

	// Now the 3rd node reports failure — the op is no longer in the queue
	// A final failure still marks rolling, even though the application already succeeded.
	sh.HandleAppendRequestFailure(context.Background(), 0, errors.New("node3 timeout"), 2, "node3:8080")

	assert.True(t, impl.rollingState.Load())
	assert.Equal(t, 0, testQueue.Len(), "queue should still be empty")
	assert.Equal(t, 1, callbackCount, "callback should NOT have been called again by HandleAppendRequestFailure")
}

// TestFenceSegmentQuorum_CtxTimeout tests that fenceSegmentQuorum returns ctx.Err()
// when the context times out before all quorum nodes respond.
// This covers the case <-ctx.Done() branch (L1205-L1206).
func TestFenceSegmentQuorum_CtxTimeout(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	// All nodes return a mock client that blocks on FenceSegment until ctx is cancelled
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (int64, error) {
			// Block until context is cancelled, simulating a slow/unresponsive node
			<-ctx.Done()
			return -1, ctx.Err()
		}).Maybe()

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	impl := sh.(*segmentHandleImpl)

	quorum := &proto.QuorumInfo{Id: 1, Es: 3, Aq: 2, Wq: 3, Nodes: []string{"node1", "node2", "node3"}}

	// Use a short timeout so the ctx expires before any node responds
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	lastEntryId, err := impl.fenceSegmentQuorum(ctx, quorum, waitForAllReplicas)
	assert.Error(t, err)
	assert.Equal(t, int64(-1), lastEntryId)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestSyncLACToQuorumAsync_BoundedWhenNodeHangs is a regression test for issue
// #225 (#2): the background LAC syncer calls syncLACToQuorumAsync with a
// deadline-less context, so a node that is connected but never responds
// (UpdateLastAddConfirmed hangs) permanently wedges the syncer and LAC stops
// advancing for the whole segment. syncLACToQuorumAsync must bound its fan-out
// and return even when a node never replies.
func TestSyncLACToQuorumAsync_BoundedWhenNodeHangs(t *testing.T) {
	// Shrink the fan-out timeout so the test doesn't wait the production default.
	oldTimeout := lacSyncTimeout
	lacSyncTimeout = 200 * time.Millisecond
	defer func() { lacSyncTimeout = oldTimeout }()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	// A node that is connected but never responds: block until the request
	// context is cancelled, which the fan-out timeout is responsible for doing.
	mockClient.EXPECT().
		UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(rpcCtx context.Context, _ string, _ string, _ int64, _ int64, _ int64) error {
			<-rpcCtx.Done()
			return rpcCtx.Err()
		}).Maybe()

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Active,
			LastEntryId: -1,
			Quorum:      &proto.QuorumInfo{Id: 1, Es: 1, Wq: 1, Aq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}
	// canWrite=false: don't start the background loop/executor; call the fan-out directly.
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil).(*segmentHandleImpl)

	done := make(chan struct{})
	go func() {
		sh.syncLACToQuorumAsync(context.Background(), 5)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("syncLACToQuorumAsync did not return: a hung node wedged the LAC syncer")
	}
}

// TestResolveFenceLACRejectsInconsistentQuorum pins the guard directly: an ack quorum above the
// write quorum makes the tolerance Wq-Aq negative, which would index before the start of the
// sorted replies. Recovery must return an error rather than panic on a malformed quorum.
func TestResolveFenceLACRejectsInconsistentQuorum(t *testing.T) {
	s := &segmentHandleImpl{}

	for _, tc := range []struct {
		name        string
		writeQuorum int
		ackQuorum   int
	}{
		{"ack quorum above write quorum", 1, 2},
		{"ack quorum far above write quorum", 3, 9},
		{"zero ack quorum", 3, 0},
		{"negative ack quorum", 3, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lac, err := s.resolveFenceLAC([]int64{5, 5, 5}, tc.writeQuorum, tc.ackQuorum)
			assert.Error(t, err)
			assert.Equal(t, int64(-1), lac)
			assert.Contains(t, err.Error(), "inconsistent quorum")
		})
	}
}

// TestCompleteSegmentQuorum_TargetHeldByOneReplica covers the shape recovery now produces when a
// fence reply is missing: the target is the highest tail that cannot be shown to be below the
// acknowledged boundary, and only one replica holds data up to it.
//
// Requiring an ack quorum of covering replicas here would fail the completion -- after every
// replica had already frozen its footer at that target. No later attempt could then succeed: the
// target itself can never gather more coverage, and a lower one is rejected by the frozen footers.
// Since OpenLogWriter fences and completes every active segment before it returns, that would
// leave the whole log unable to accept a writer, to avoid carrying entries that were never
// acknowledged in the first place.
func TestCompleteSegmentQuorum_TargetHeldByOneReplica(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	// Replica tails [1,0,0]: entry 1 reached a single replica and was never acknowledged.
	// Recovery targets 1 after a fence reply went missing.
	for node, tail := range map[string]int64{"node1": 1, "node2": 0, "node3": 0} {
		cli := mocks_logstore_client.NewLogStoreClient(t)
		cli.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(tail, nil)
		mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, node).Return(cli, nil)
	}

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	impl := sh.(*segmentHandleImpl)

	quorum := &proto.QuorumInfo{Id: 1, Es: 3, Aq: 2, Wq: 3, Nodes: []string{"node1", "node2", "node3"}}
	err := impl.completeSegmentQuorum(t.Context(), quorum, 1, waitForAllReplicas)
	assert.NoError(t, err, "a finalized quorum with one covering replica must close the segment")
}

// TestFenceAndComplete_PartialFenceDoesNotWedgeFinalizedReplicas walks the whole
// recovery path -- fence, then complete -- against replicas that behave like
// StagedFileWriter: Finalize burns the target LAC into a durable footer, and a
// later Finalize at a different LAC is refused with ErrInvalidLACAlignment.
//
// Tails are [1,0,0] and one tail-0 replica is unreachable, so the fence sees
// [1,0] and targets 1 -- a boundary a single replica covers. Completion has to
// succeed on this pass. If it did not, the replicas finalized at 1 would outlive
// it, the next fence would see every reply and target 0, and every one of those
// footers would reject 0: the segment could never close, on this process or any
// later one. The second half of this test runs exactly that pass to show it.
func TestFenceAndComplete_PartialFenceDoesNotWedgeFinalizedReplicas(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	const notFinalized = int64(-2)
	type replica struct {
		tail      int64
		footer    int64
		reachable bool
	}
	replicas := map[string]*replica{
		"node1": {tail: 1, footer: notFinalized, reachable: true},
		"node2": {tail: 0, footer: notFinalized, reachable: false}, // missed the fence
		"node3": {tail: 0, footer: notFinalized, reachable: true},
	}

	for node, r := range replicas {
		r := r
		cli := mocks_logstore_client.NewLogStoreClient(t)
		cli.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, bucket, root string, logId, segId int64) (int64, error) {
				if !r.reachable {
					return -1, errors.New("node unreachable")
				}
				return r.tail, nil
			})
		cli.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, bucket, root string, logId, segId, lac int64) (int64, error) {
				if !r.reachable {
					return -1, errors.New("node unreachable")
				}
				if r.footer != notFinalized && r.footer != lac {
					return -1, werr.ErrInvalidLACAlignment.WithCauseErrMsg(
						fmt.Sprintf("footer already finalized at %d, refusing %d", r.footer, lac),
					)
				}
				r.footer = lac
				return r.tail, nil
			})
		mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, node).Return(cli, nil)
	}

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	impl := sh.(*segmentHandleImpl)
	quorum := &proto.QuorumInfo{Id: 1, Es: 3, Aq: 2, Wq: 3, Nodes: []string{"node1", "node2", "node3"}}
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()

		// Pass 1: one replica silent at fence.
		lac, err := impl.fenceSegmentQuorum(ctx, quorum, returnOnSafeQuorum)
		require.NoError(t, err)
		assert.Equal(t, int64(1), lac, "a silent replica is unknown, not empty, so the target stays at 1")
		require.NoError(t, impl.completeSegmentQuorum(ctx, quorum, lac, returnOnSafeQuorum),
			"two replicas recorded the boundary, which is what completion needs")
		synctest.Wait()
		assert.Equal(t, int64(1), replicas["node1"].footer)
		assert.Equal(t, int64(1), replicas["node3"].footer)

		// Pass 2, the counterfactual: the missing replica is back and every reply is in,
		// so the target drops to 0 -- and the footers already burned at 1 refuse it.
		replicas["node2"].reachable = true
		lac, err = impl.fenceSegmentQuorum(ctx, quorum, returnOnSafeQuorum)
		require.NoError(t, err)
		assert.Equal(t, int64(0), lac)
		assert.Error(t, impl.completeSegmentQuorum(ctx, quorum, lac, returnOnSafeQuorum),
			"only the replica that never finalized can accept 0, which is below the ack quorum")
	})
}

// TestCompactSegmentQuorum_AttemptDeadlineFailsOverToNextNode verifies a node that hangs rather
// than failing costs one attempt rather than the caller's whole budget, and that the sequential
// walk then reaches the remaining replicas -- which is the reason the walk is sequential.
func TestCompactSegmentQuorum_AttemptDeadlineFailsOverToNextNode(t *testing.T) {
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Woodpecker.Client.Auditor.CompactionAttemptTimeout = config.NewDurationSecondsFromInt(1)

	hung := mocks_logstore_client.NewLogStoreClient(t)
	hung.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, _ string, _ string, _ int64, _ int64, _ int64) (*proto.SegmentMetadata, error) {
			// The attempt must carry a deadline of its own, not merely inherit the caller's.
			_, ok := ctx.Deadline()
			assert.True(t, ok, "the compaction attempt should carry its own deadline")
			<-ctx.Done()
			return nil, ctx.Err()
		})

	healthy := mocks_logstore_client.NewLogStoreClient(t)
	healthy.EXPECT().SegmentCompact(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.SegmentMetadata{State: proto.SegmentState_Sealed}, nil)

	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "hung-node").Return(hung, nil)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "healthy-node").Return(healthy, nil)

	s := &segmentHandleImpl{
		logId:      1,
		logName:    "test-log",
		segmentId:  7,
		ClientPool: pool,
		cfg:        cfg,
	}

	// The caller's context has no deadline at all, exactly as the auditor's does not: the bound
	// under test has to come from the attempt itself.
	quorum := &proto.QuorumInfo{Nodes: []string{"hung-node", "healthy-node"}}
	segMeta, err := s.compactSegmentQuorum(context.Background(), quorum, 9)
	assert.NoError(t, err)
	assert.NotNil(t, segMeta)
}

// TestCompactSegmentQuorum_CancelledCallerDoesNotBlameUntriedNodes verifies a cancelled caller
// stops the walk and is reported as such, rather than being recorded as every replica failing.
//
// GetLogStoreClient returns a cached client without consulting the context, and WithTimeout on a
// context that is already done yields an immediately-expired child, so without the check every
// remaining node would "fail" instantly, land in nodeFailures, and be counted into
// WpSegmentCompactionFailuresTotal and the all-nodes-failed warning -- corrupting the signal
// operators use to identify a genuinely broken replica.
func TestCompactSegmentQuorum_CancelledCallerDoesNotBlameUntriedNodes(t *testing.T) {
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)

	// No client expectations at all: reaching the pool would fail the test.
	pool := mocks_logstore_client.NewLogStoreClientPool(t)

	s := &segmentHandleImpl{
		logId:      1,
		logName:    "test-log",
		segmentId:  7,
		ClientPool: pool,
		cfg:        cfg,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	quorum := &proto.QuorumInfo{Nodes: []string{"node-a", "node-b", "node-c"}}
	segMeta, err := s.compactSegmentQuorum(ctx, quorum, 9)
	assert.Nil(t, segMeta)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestClassifyCompactionFailureReason_GrpcDeadlineIsTransient covers the remote-mode shape of an
// attempt deadline. gRPC surfaces it as status.Error(codes.DeadlineExceeded, ...), which does not
// wrap context.DeadlineExceeded, is not a woodpeckerError and is not codes.Unavailable, so none of
// errors.Is, IsRetryableErr or IsTransportError match it. Only embedded mode returns the native
// error. Without the status check a node that stops responding is labelled "other" in exactly the
// deployment where the per-attempt deadline is what detects it.
func TestClassifyCompactionFailureReason_GrpcDeadlineIsTransient(t *testing.T) {
	grpcDeadline := status.Error(codes.DeadlineExceeded, "context deadline exceeded")

	// Guard the premise: if this ever starts matching, the extra check is redundant rather than
	// load-bearing, and this test says so.
	assert.False(t, errors.Is(grpcDeadline, context.DeadlineExceeded),
		"a gRPC status error does not wrap the context sentinel")

	assert.Equal(t, compactionFailureReasonTransient, classifyCompactionFailureReason(grpcDeadline))
	assert.Equal(t, compactionFailureReasonTransient, classifyCompactionFailureReason(context.DeadlineExceeded))
}

func cleanupInjectedAppender(t *testing.T, handle SegmentHandle) {
	t.Helper()
	t.Cleanup(func() {
		g := handle.(*segmentHandleImpl).appender
		g.stop()
		g.wait()
	})
}
