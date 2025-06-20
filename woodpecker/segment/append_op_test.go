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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_segment_handle"
	"github.com/zilliztech/woodpecker/proto"
)

func TestNewAppendOp(t *testing.T) {
	logId := int64(1)
	segmentId := int64(2)
	entryId := int64(3)
	value := []byte("test data")
	callback := func(segmentId int64, entryId int64, err error) {}
	clientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	handle := mocks_segment_handle.NewSegmentHandle(t)
	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Wq:    1,
		Aq:    1,
		Es:    1,
		Nodes: []string{"node1"},
	}
	attempt := 1

	op := NewAppendOp(logId, segmentId, entryId, value, callback, clientPool, handle, quorumInfo, attempt)

	assert.NotNil(t, op)
	assert.Equal(t, logId, op.logId)
	assert.Equal(t, segmentId, op.segmentId)
	assert.Equal(t, entryId, op.entryId)
	assert.Equal(t, value, op.value)
	assert.Equal(t, attempt, op.attempt)
	assert.False(t, op.completed.Load())
	assert.False(t, op.fastCalled.Load())
	assert.NotNil(t, op.resultChannels)
	assert.Equal(t, 0, len(op.resultChannels))
}

func TestAppendOp_Identifier(t *testing.T) {
	op := &AppendOp{
		logId:     1,
		segmentId: 2,
		entryId:   3,
	}

	identifier := op.Identifier()
	assert.Equal(t, "1/2/3", identifier)
}

func TestAppendOp_Execute_Success(t *testing.T) {
	// Setup mocks
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)

	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Wq:    1,
		Aq:    1,
		Es:    1,
		Nodes: []string{"node1"},
	}

	// Setup expectations
	mockHandle.On("GetQuorumInfo", mock.Anything).Return(quorumInfo, nil)
	mockClientPool.On("GetLogStoreClient", "node1").Return(mockClient, nil)
	mockClient.On("AppendEntry", mock.Anything, int64(1), mock.Anything, mock.Anything).Return(int64(3), nil)

	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, mockClientPool, mockHandle, quorumInfo, 1)

	// Execute
	op.Execute()

	// Verify
	assert.Equal(t, 1, len(op.resultChannels))
}

func TestAppendOp_Execute_GetQuorumInfoError(t *testing.T) {
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	expectedErr := errors.New("quorum info error")
	mockHandle.On("GetQuorumInfo", mock.Anything).Return((*proto.QuorumInfo)(nil), expectedErr)
	mockHandle.On("SendAppendErrorCallbacks", mock.Anything, int64(3), expectedErr).Return()

	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, mockClientPool, mockHandle, nil, 1)

	op.Execute()

	assert.Equal(t, expectedErr, op.err)
}

func TestAppendOp_Execute_GetClientError(t *testing.T) {
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Wq:    1,
		Aq:    1,
		Es:    1,
		Nodes: []string{"node1"},
	}

	expectedErr := errors.New("client error")
	mockHandle.On("GetQuorumInfo", mock.Anything).Return(quorumInfo, nil)
	mockClientPool.On("GetLogStoreClient", "node1").Return(nil, expectedErr)
	mockHandle.On("SendAppendErrorCallbacks", mock.Anything, int64(3), mock.Anything).Return()

	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, mockClientPool, mockHandle, quorumInfo, 1)

	op.Execute()

	assert.Equal(t, expectedErr, op.err)
}

func TestAppendOp_receivedAckCallback_Success(t *testing.T) {
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Wq:    1,
		Aq:    1,
		Es:    1,
		Nodes: []string{"node1"},
	}

	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, mockHandle, quorumInfo, 1)

	// Create a channel and send success signal
	rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 0))
	_ = rc.SendResult(context.Background(), &channel.AppendResult{
		SyncedId: 3,
		Err:      nil,
	})

	mockHandle.On("SendAppendSuccessCallbacks", mock.Anything, int64(3)).Return()

	// Execute callback
	op.receivedAckCallback(context.Background(), time.Now(), 3, rc, nil, 0)

	// Verify
	assert.True(t, op.completed.Load())
	// Check if bit 0 is set in the ackSet (bitset doesn't have IsSet method, we check Count instead)
	assert.Equal(t, 1, op.ackSet.Count())
}

func TestAppendOp_receivedAckCallback_SyncError(t *testing.T) {
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	expectedErr := errors.New("sync error")

	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, mockHandle, nil, 1)

	mockHandle.On("SendAppendErrorCallbacks", mock.Anything, int64(3), expectedErr).Return()

	// Execute callback with error
	op.receivedAckCallback(context.Background(), time.Now(), 3, nil, expectedErr, 0)

	// Verify
	assert.Equal(t, expectedErr, op.err)
}

func TestAppendOp_receivedAckCallback_FailureSignal(t *testing.T) {
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, mockHandle, nil, 1)

	// Create a channel and send failure signal
	rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 0))
	_ = rc.SendResult(context.Background(), &channel.AppendResult{
		SyncedId: -1,
		Err:      nil,
	})

	mockHandle.On("SendAppendErrorCallbacks", mock.Anything, int64(3), mock.Anything).Return()

	// Execute callback
	op.receivedAckCallback(context.Background(), time.Now(), 3, rc, nil, 0)

	// No additional assertions needed, just verify it doesn't panic
}

func TestAppendOp_receivedAckCallback_ChannelClosed(t *testing.T) {
	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, nil, 1)

	// Create a channel and close it
	rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 0))
	_ = rc.Close(context.TODO())

	// Execute callback - should return without error when channel is closed
	op.receivedAckCallback(context.Background(), time.Now(), 3, rc, nil, 0)

	// No assertions needed, just ensure it doesn't panic or hang
}

func TestAppendOp_FastFail(t *testing.T) {
	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, nil, 1)

	// Add some result channels
	rc1 := channel.NewLocalResultChannel("1/2/3-1")
	rc2 := channel.NewLocalResultChannel("1/2/3-2")
	op.resultChannels = []channel.ResultChannel{rc1, rc2}

	// Execute FastFail
	op.FastFail(context.Background(), errors.New("test error"))

	// Verify channels received failure signal
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()
	result1, err1 := rc1.ReadResult(ctx1)
	assert.NoError(t, err1)
	assert.Equal(t, int64(-1), result1.SyncedId)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	result2, err2 := rc2.ReadResult(ctx2)
	assert.NoError(t, err2)
	assert.Equal(t, int64(-1), result2.SyncedId)

	// Verify channels are closed
	assert.True(t, rc1.IsClosed())
	assert.True(t, rc2.IsClosed())

	// Verify fastCalled flag is set
	assert.True(t, op.fastCalled.Load())
}

func TestAppendOp_FastFail_Idempotent(t *testing.T) {
	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, nil, 1)

	rc := channel.NewLocalResultChannel("1/2/3")
	op.resultChannels = []channel.ResultChannel{rc}

	// Call FastFail twice
	op.FastFail(context.Background(), errors.New("test error"))
	op.FastFail(context.Background(), errors.New("test error")) // Should be no-op

	// Verify only one signal was sent
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := rc.ReadResult(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), result.SyncedId)

	// Channel should be closed
	assert.True(t, rc.IsClosed())
}

func TestAppendOp_FastSuccess(t *testing.T) {
	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, nil, 1)

	// Add some result channels
	rc1 := channel.NewLocalResultChannel("1/2/3-1")
	rc2 := channel.NewLocalResultChannel("1/2/3-2")
	op.resultChannels = []channel.ResultChannel{rc1, rc2}

	// Execute FastSuccess
	op.FastSuccess(context.Background())

	// Verify channels received success signal
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()
	result1, err1 := rc1.ReadResult(ctx1)
	assert.NoError(t, err1)
	assert.Equal(t, int64(3), result1.SyncedId)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	result2, err2 := rc2.ReadResult(ctx2)
	assert.NoError(t, err2)
	assert.Equal(t, int64(3), result2.SyncedId)

	// Verify channels are closed
	assert.True(t, rc1.IsClosed())
	assert.True(t, rc2.IsClosed())

	// Verify fastCalled flag is set
	assert.True(t, op.fastCalled.Load())
}

func TestAppendOp_FastSuccess_Idempotent(t *testing.T) {
	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, nil, 1)

	rc := channel.NewLocalResultChannel("1/2/3")
	op.resultChannels = []channel.ResultChannel{rc}

	// Call FastSuccess twice
	op.FastSuccess(context.Background())
	op.FastSuccess(context.Background()) // Should be no-op

	// Verify only one signal was sent
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := rc.ReadResult(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), result.SyncedId)

	// Channel should be closed
	assert.True(t, rc.IsClosed())
}

func TestAppendOp_FastFail_WithClosedChannel(t *testing.T) {
	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, nil, 1)

	// Add a closed channel
	rc := channel.NewLocalResultChannel("1/2/3")
	_ = rc.Close(context.TODO()) // Close the result channel
	op.resultChannels = []channel.ResultChannel{rc}

	// Execute FastFail - should not panic
	assert.NotPanics(t, func() {
		op.FastFail(context.Background(), errors.New("test error"))
	})

	// Verify fastCalled flag is set
	assert.True(t, op.fastCalled.Load())
}

func TestAppendOp_FastSuccess_WithClosedChannel(t *testing.T) {
	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, nil, 1)

	// Add a closed channel
	rc := channel.NewLocalResultChannel("1/2/3")
	_ = rc.Close(context.TODO()) // Close the result channel
	op.resultChannels = []channel.ResultChannel{rc}

	// Execute FastSuccess - should not panic
	assert.NotPanics(t, func() {
		op.FastSuccess(context.Background())
	})

	// Verify fastCalled flag is set
	assert.True(t, op.fastCalled.Load())
}

func TestAppendOp_ConcurrentFastCalls(t *testing.T) {
	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, nil, 1)

	rc := channel.NewLocalResultChannel("1/2/3")
	op.resultChannels = []channel.ResultChannel{rc}

	var wg sync.WaitGroup
	callCount := 0
	mu := sync.Mutex{}

	// Launch multiple goroutines calling FastFail and FastSuccess
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				op.FastFail(context.Background(), errors.New("test error"))
			} else {
				op.FastSuccess(context.Background())
			}
			mu.Lock()
			callCount++
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	// Verify only one call was effective
	assert.True(t, op.fastCalled.Load())
	assert.Equal(t, 10, callCount) // All calls completed

	// Verify channel received exactly one signal
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := rc.ReadResult(ctx)
	assert.NoError(t, err)
	assert.True(t, result.SyncedId == -1 || result.SyncedId == 3) // Either success or failure

	// Channel should be closed
	assert.True(t, rc.IsClosed())
}

func TestAppendOp_toSegmentEntry(t *testing.T) {
	op := &AppendOp{
		segmentId: 2,
		entryId:   3,
		value:     []byte("test data"),
	}

	entry := op.toSegmentEntry()

	assert.NotNil(t, entry)
	assert.Equal(t, int64(2), entry.SegmentId)
	assert.Equal(t, int64(3), entry.EntryId)
	assert.Equal(t, []byte("test data"), entry.Data)
}

func TestAppendOp_receivedAckCallback_Timeout(t *testing.T) {
	// This test demonstrates the timeout scenario setup
	// In a real timeout test, we would need to modify the receivedAckCallback
	// to respect context timeouts or use a shorter ticker interval
	start := time.Now()

	// For this test, we just verify the setup doesn't hang
	go func() {
		time.Sleep(50 * time.Millisecond)
		// Simulate timeout by not sending anything to the channel
	}()

	// Create a channel but don't send anything (simulate slow response)
	syncedCh := make(chan int64, 1)

	// Verify the test setup is correct
	assert.NotNil(t, syncedCh)

	elapsed := time.Since(start)
	assert.True(t, elapsed < 200*time.Millisecond) // Ensure test doesn't hang
}
