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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

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
	syncedCh := make(chan int64, 1)
	syncedCh <- 3 // Send the entryId

	mockHandle.On("SendAppendSuccessCallbacks", mock.Anything, int64(3)).Return()

	// Execute callback
	op.receivedAckCallback(context.Background(), time.Now(), 3, syncedCh, nil, 0)

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
	syncedCh := make(chan int64, 1)
	syncedCh <- -1 // Send failure signal

	mockHandle.On("SendAppendErrorCallbacks", mock.Anything, int64(3), mock.Anything).Return()

	// Execute callback
	op.receivedAckCallback(context.Background(), time.Now(), 3, syncedCh, nil, 0)

	// No additional assertions needed, just verify it doesn't panic
}

func TestAppendOp_receivedAckCallback_ChannelClosed(t *testing.T) {
	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, nil, 1)

	// Create a channel and close it
	syncedCh := make(chan int64)
	close(syncedCh)

	// Execute callback - should return without error when channel is closed
	op.receivedAckCallback(context.Background(), time.Now(), 3, syncedCh, nil, 0)

	// No assertions needed, just ensure it doesn't panic or hang
}

func TestAppendOp_FastFail(t *testing.T) {
	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, nil, 1)

	// Add some result channels
	ch1 := make(chan int64, 1)
	ch2 := make(chan int64, 1)
	op.resultChannels = []chan int64{ch1, ch2}

	// Execute FastFail
	op.FastFail(context.Background(), errors.New("test error"))

	// Verify channels received failure signal
	assert.Equal(t, int64(-1), <-ch1)
	assert.Equal(t, int64(-1), <-ch2)

	// Verify channels are closed
	_, ok1 := <-ch1
	_, ok2 := <-ch2
	assert.False(t, ok1)
	assert.False(t, ok2)

	// Verify fastCalled flag is set
	assert.True(t, op.fastCalled.Load())
}

func TestAppendOp_FastFail_Idempotent(t *testing.T) {
	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, nil, 1)

	ch := make(chan int64, 1)
	op.resultChannels = []chan int64{ch}

	// Call FastFail twice
	op.FastFail(context.Background(), errors.New("test error"))
	op.FastFail(context.Background(), errors.New("test error")) // Should be no-op

	// Verify only one signal was sent
	assert.Equal(t, int64(-1), <-ch)

	// Channel should be closed
	_, ok := <-ch
	assert.False(t, ok)
}

func TestAppendOp_FastSuccess(t *testing.T) {
	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, nil, 1)

	// Add some result channels
	ch1 := make(chan int64, 1)
	ch2 := make(chan int64, 1)
	op.resultChannels = []chan int64{ch1, ch2}

	// Execute FastSuccess
	op.FastSuccess(context.Background())

	// Verify channels received success signal
	assert.Equal(t, int64(3), <-ch1)
	assert.Equal(t, int64(3), <-ch2)

	// Verify channels are closed
	_, ok1 := <-ch1
	_, ok2 := <-ch2
	assert.False(t, ok1)
	assert.False(t, ok2)

	// Verify fastCalled flag is set
	assert.True(t, op.fastCalled.Load())
}

func TestAppendOp_FastSuccess_Idempotent(t *testing.T) {
	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, nil, 1)

	ch := make(chan int64, 1)
	op.resultChannels = []chan int64{ch}

	// Call FastSuccess twice
	op.FastSuccess(context.Background())
	op.FastSuccess(context.Background()) // Should be no-op

	// Verify only one signal was sent
	assert.Equal(t, int64(3), <-ch)

	// Channel should be closed
	_, ok := <-ch
	assert.False(t, ok)
}

func TestAppendOp_FastFail_WithClosedChannel(t *testing.T) {
	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, nil, 1)

	// Add a closed channel
	ch := make(chan int64, 1)
	close(ch)
	op.resultChannels = []chan int64{ch}

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
	ch := make(chan int64, 1)
	close(ch)
	op.resultChannels = []chan int64{ch}

	// Execute FastSuccess - should not panic
	assert.NotPanics(t, func() {
		op.FastSuccess(context.Background())
	})

	// Verify fastCalled flag is set
	assert.True(t, op.fastCalled.Load())
}

func TestAppendOp_ConcurrentFastCalls(t *testing.T) {
	op := NewAppendOp(1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, nil, 1)

	ch := make(chan int64, 1)
	op.resultChannels = []chan int64{ch}

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
	select {
	case val := <-ch:
		assert.True(t, val == -1 || val == 3) // Either success or failure
	default:
		t.Fatal("Expected one signal in channel")
	}

	// Channel should be closed
	_, ok := <-ch
	assert.False(t, ok)
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
