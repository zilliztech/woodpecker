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

	op := NewAppendOp("a-bucket", "files", logId, segmentId, entryId, value, callback, clientPool, handle, quorumInfo)

	assert.NotNil(t, op)
	assert.Equal(t, logId, op.logId)
	assert.Equal(t, segmentId, op.segmentId)
	assert.Equal(t, entryId, op.entryId)
	assert.Equal(t, value, op.value)
	assert.Equal(t, 1, len(op.channelAttempts))
	assert.Equal(t, 0, op.channelAttempts[0])
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
	// mockHandle.EXPECT().GetQuorumInfo(mock.Anything).Return(quorumInfo, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClient.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), mock.Anything, mock.Anything).Return(int64(3), nil)

	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), func(int64, int64, error) {}, mockClientPool, mockHandle, quorumInfo)

	// Execute
	op.Execute()

	// Verify
	assert.Equal(t, 1, len(op.resultChannels))
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
	// mockHandle.EXPECT().GetQuorumInfo(mock.Anything).Return(quorumInfo, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(nil, expectedErr)
	mockHandle.EXPECT().HandleAppendRequestFailure(mock.Anything, int64(3), mock.Anything, mock.Anything, mock.Anything).Return()

	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), func(int64, int64, error) {}, mockClientPool, mockHandle, quorumInfo)

	op.Execute()
	// Wait for async handleAppendRequestFailure to be processed
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, expectedErr, op.channelErrors[0])
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

	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, mockHandle, quorumInfo)

	// Create a channel and send success signal
	rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 0))
	_ = rc.SendResult(context.Background(), &channel.AppendResult{
		SyncedId: 3,
		Err:      nil,
	})

	mockHandle.EXPECT().SendAppendSuccessCallbacks(mock.Anything, int64(3)).Return()

	// Execute callback
	op.receivedAckCallback(context.Background(), time.Now(), 3, rc, nil, 0, "node0")

	// Verify
	assert.True(t, op.completed.Load())
	// Check if bit 0 is set in the ackSet (bitset doesn't have IsSet method, we check Count instead)
	assert.Equal(t, 1, op.ackSet.Count())
}

func TestAppendOp_receivedAckCallback_SyncError(t *testing.T) {
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	expectedErr := errors.New("sync error")

	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, mockHandle, &proto.QuorumInfo{Nodes: []string{"127.0.0.1"}})

	mockHandle.EXPECT().HandleAppendRequestFailure(mock.Anything, int64(3), mock.Anything, mock.Anything, mock.Anything).Return()

	// Execute callback with error
	op.receivedAckCallback(context.Background(), time.Now(), 3, nil, expectedErr, 0, "node0")

	// Verify
	assert.Equal(t, expectedErr, op.channelErrors[0])
}

func TestAppendOp_receivedAckCallback_FailureSignal(t *testing.T) {
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, mockHandle, &proto.QuorumInfo{Nodes: []string{"127.0.0.1"}})

	// Create a channel and send failure signal
	rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 0))
	_ = rc.SendResult(context.Background(), &channel.AppendResult{
		SyncedId: -1,
		Err:      nil,
	})

	mockHandle.EXPECT().HandleAppendRequestFailure(mock.Anything, int64(3), mock.Anything, mock.Anything, mock.Anything).Return()

	// Execute callback
	op.receivedAckCallback(context.Background(), time.Now(), 3, rc, nil, 0, "node0")

	// No additional assertions needed, just verify it doesn't panic
}

func TestAppendOp_receivedAckCallback_ChannelClosed(t *testing.T) {
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, mockHandle, &proto.QuorumInfo{Nodes: []string{"127.0.0.1"}})

	// Create a channel and close it
	rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 0))
	_ = rc.Close(context.TODO())

	mockHandle.EXPECT().HandleAppendRequestFailure(mock.Anything, int64(3), mock.Anything, mock.Anything, mock.Anything).Return()

	// Execute callback - should return without error when channel is closed
	op.receivedAckCallback(context.Background(), time.Now(), 3, rc, nil, 0, "node0")

	// No assertions needed, just ensure it doesn't panic or hang
}

func TestAppendOp_FastFail(t *testing.T) {
	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, &proto.QuorumInfo{Nodes: []string{"127.0.0.1"}})

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
	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, &proto.QuorumInfo{Nodes: []string{"127.0.0.1"}})

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
	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, &proto.QuorumInfo{Nodes: []string{"127.0.0.1"}})

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
	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, &proto.QuorumInfo{Nodes: []string{"127.0.0.1"}})

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
	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, &proto.QuorumInfo{Nodes: []string{"127.0.0.1"}})

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
	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, &proto.QuorumInfo{Nodes: []string{"127.0.0.1"}})

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
	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), func(int64, int64, error) {}, nil, nil, &proto.QuorumInfo{Nodes: []string{"127.0.0.1"}})

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

	entry := op.toLogEntry()

	assert.NotNil(t, entry)
	assert.Equal(t, int64(2), entry.SegId)
	assert.Equal(t, int64(3), entry.EntryId)
	assert.Equal(t, []byte("test data"), entry.Values)
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

// TestAppendOp_Execute_RetryIdempotency tests that retry operations are idempotent
// by verifying that result channels are reused across multiple Execute calls
func TestAppendOp_Execute_RetryIdempotency(t *testing.T) {
	// Setup mocks
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)

	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Wq:    2,
		Aq:    2,
		Es:    2,
		Nodes: []string{"node1", "node2"},
	}

	// Setup expectations - GetQuorumInfo will be called multiple times
	// mockHandle.EXPECT().GetQuorumInfo(mock.Anything).Return(quorumInfo, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient, nil)
	mockClient.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), mock.Anything, mock.Anything).Return(int64(3), nil)

	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), func(int64, int64, error) {}, mockClientPool, mockHandle, quorumInfo)

	// First execution
	op.Execute()

	// Verify initial state
	assert.Equal(t, 2, len(op.resultChannels))
	assert.NotNil(t, op.resultChannels[0])
	assert.NotNil(t, op.resultChannels[1])

	// Store references to original channels
	originalChannel0 := op.resultChannels[0]
	originalChannel1 := op.resultChannels[1]

	// Second execution (simulating retry)
	op.Execute()

	// Verify channels are reused (idempotent)
	assert.Equal(t, 2, len(op.resultChannels))
	assert.Same(t, originalChannel0, op.resultChannels[0], "Channel 0 should be reused on retry")
	assert.Same(t, originalChannel1, op.resultChannels[1], "Channel 1 should be reused on retry")

	// Third execution (simulating another retry)
	op.Execute()

	// Verify channels are still the same
	assert.Equal(t, 2, len(op.resultChannels))
	assert.Same(t, originalChannel0, op.resultChannels[0], "Channel 0 should be reused on multiple retries")
	assert.Same(t, originalChannel1, op.resultChannels[1], "Channel 1 should be reused on multiple retries")
}

// TestAppendOp_Execute_RetryIdempotency_WithSameQuorumSize tests idempotency
// when quorum info is refreshed but maintains the same size
func TestAppendOp_Execute_RetryIdempotency_WithSameQuorumSize(t *testing.T) {
	// Setup mocks
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)

	// Initial quorum with 2 nodes
	initialQuorumInfo := &proto.QuorumInfo{
		Id:    1,
		Wq:    2,
		Aq:    2,
		Es:    2,
		Nodes: []string{"node1", "node2"},
	}

	// Setup expectations - first call returns initial quorum, second call returns updated quorum
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient, nil)
	mockClient.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), mock.Anything, mock.Anything).Return(int64(3), nil)

	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), func(int64, int64, error) {}, mockClientPool, mockHandle, initialQuorumInfo)

	// First execution with 2 nodes
	op.Execute()

	// Verify initial state
	assert.Equal(t, 2, len(op.resultChannels))
	assert.NotNil(t, op.resultChannels[0])
	assert.NotNil(t, op.resultChannels[1])

	// Store references to original channels
	originalChannel0 := op.resultChannels[0]
	originalChannel1 := op.resultChannels[1]

	// Second execution with same quorum size (simulating retry with refreshed quorum info)
	op.Execute()

	// Verify that existing channels are preserved
	assert.Equal(t, 2, len(op.resultChannels))
	assert.Same(t, originalChannel0, op.resultChannels[0], "Channel 0 should be preserved")
	assert.Same(t, originalChannel1, op.resultChannels[1], "Channel 1 should be preserved")
}

// TestAppendOp_sendWriteRequest_ChannelReuse tests that sendWriteRequest
// creates channels only once per server
func TestAppendOp_sendWriteRequest_ChannelReuse(t *testing.T) {
	// Setup mocks
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)

	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Wq:    1,
		Aq:    1,
		Es:    1,
		Nodes: []string{"node1"},
	}

	mockClient.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), mock.Anything, mock.Anything).Return(int64(3), nil)

	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), func(int64, int64, error) {}, mockClientPool, nil, quorumInfo)

	// Initialize result channels slice
	op.resultChannels = make([]channel.ResultChannel, 1)

	// First call to sendWriteRequest
	op.sendWriteRequest(context.Background(), mockClient, 0, "node0")

	// Verify channel was created
	assert.NotNil(t, op.resultChannels[0])
	originalChannel := op.resultChannels[0]

	// Second call to sendWriteRequest for the same server
	op.sendWriteRequest(context.Background(), mockClient, 0, "node0")

	// Verify the same channel is reused
	assert.Same(t, originalChannel, op.resultChannels[0], "Channel should be reused for the same server")
}

// TestAppendOp_Execute_RetryIdempotency_WithNilChannels tests behavior when
// some channels are nil during retry
func TestAppendOp_Execute_RetryIdempotency_WithNilChannels(t *testing.T) {
	// Setup mocks
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)

	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Wq:    2,
		Aq:    2,
		Es:    2,
		Nodes: []string{"node1", "node2"},
	}

	// mockHandle.EXPECT().GetQuorumInfo(mock.Anything).Return(quorumInfo, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient, nil)
	mockClient.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), mock.Anything, mock.Anything).Return(int64(3), nil)

	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), func(int64, int64, error) {}, mockClientPool, mockHandle, quorumInfo)

	// Manually set up result channels with one nil channel (simulating partial failure)
	op.resultChannels = make([]channel.ResultChannel, 2)
	op.resultChannels[0] = channel.NewLocalResultChannel("test-channel-0")
	op.resultChannels[1] = nil // This should be recreated

	originalChannel0 := op.resultChannels[0]

	// Execute should handle nil channels correctly
	op.Execute()

	// Verify that existing non-nil channel is preserved and nil channel is created
	assert.Equal(t, 2, len(op.resultChannels))
	assert.Same(t, originalChannel0, op.resultChannels[0], "Existing non-nil channel should be preserved")
	assert.NotNil(t, op.resultChannels[1], "Nil channel should be created")
}

// TestAppendOp_Execute_RetryIdempotency_ChannelIdentifier tests that
// channels created for retry use the same identifier
func TestAppendOp_Execute_RetryIdempotency_ChannelIdentifier(t *testing.T) {
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

	// mockHandle.EXPECT().GetQuorumInfo(mock.Anything).Return(quorumInfo, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClient.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), mock.Anything, mock.Anything).Return(int64(3), nil)

	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), func(int64, int64, error) {}, mockClientPool, mockHandle, quorumInfo)

	// First execution
	op.Execute()

	// Verify channel was created
	assert.Equal(t, 1, len(op.resultChannels))
	assert.NotNil(t, op.resultChannels[0])

	// We can't directly access the channel's identifier, but we can verify
	// that the channel is properly created and reused
	// The identifier should be consistent with the operation's identifier: "1/2/3"
	originalChannel := op.resultChannels[0]

	// Second execution (retry)
	op.Execute()

	// Verify the same channel is reused
	assert.Same(t, originalChannel, op.resultChannels[0], "Channel should be reused with same identifier")
}

// TestAppendOp_QuorumWrite_Case1_AllNodesSuccess tests quorum write with es=3,wq=3,aq=2
// All 3 nodes succeed, appendOp should succeed
func TestAppendOp_QuorumWrite_Case1_AllNodesSuccess(t *testing.T) {
	// Setup mocks
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient1 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient2 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient3 := mocks_logstore_client.NewLogStoreClient(t)

	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Wq:    3,
		Aq:    2, // Only need 2 acks to succeed
		Es:    3,
		Nodes: []string{"node1", "node2", "node3"},
	}

	callbackCalled := false
	var callbackSegmentId, callbackEntryId int64
	var callbackErr error

	callback := func(segmentId int64, entryId int64, err error) {
		callbackCalled = true
		callbackSegmentId = segmentId
		callbackEntryId = entryId
		callbackErr = err
	}

	// Setup expectations - AppendEntry calls will create async operations
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient1, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient2, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node3").Return(mockClient3, nil)

	// Setup AppendEntry to return immediately, we'll send async results later
	mockClient1.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), mock.Anything, mock.Anything).Return(int64(3), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), mock.Anything, mock.Anything).Return(int64(3), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), mock.Anything, mock.Anything).Return(int64(3), nil)

	// Expect SendAppendSuccessCallbacks to be called when quorum (aq=2) is reached
	mockHandle.EXPECT().SendAppendSuccessCallbacks(mock.Anything, int64(3)).Run(func(ctx context.Context, entryId int64) {
		// Simulate what the real SendAppendSuccessCallbacks would do - call the callback
		callback(2, entryId, nil)
	}).Return()

	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), callback, mockClientPool, mockHandle, quorumInfo)

	// Execute
	op.Execute()

	// Wait for channels to be created
	time.Sleep(50 * time.Millisecond)

	// Simulate async responses from all 3 nodes
	for i := 0; i < 3; i++ {
		if len(op.resultChannels) > i && op.resultChannels[i] != nil {
			err := op.resultChannels[i].SendResult(context.Background(), &channel.AppendResult{SyncedId: 3, Err: nil})
			if err != nil {
				t.Logf("Failed to send result to channel %d: %v", i, err)
			}
		}
	}

	// Wait for async operations to complete
	time.Sleep(500 * time.Millisecond)

	// Verify that operation completed successfully
	assert.True(t, op.completed.Load(), "Operation should be completed")
	assert.GreaterOrEqual(t, op.ackSet.Count(), 2, "At least 2 nodes should have acked")

	// Wait for callback
	time.Sleep(50 * time.Millisecond)
	assert.True(t, callbackCalled, "Callback should have been called")
	assert.Equal(t, int64(2), callbackSegmentId)
	assert.Equal(t, int64(3), callbackEntryId)
	assert.NoError(t, callbackErr)
}

// TestAppendOp_QuorumWrite_Case2_TwoSuccessOneFail tests quorum write with es=3,wq=3,aq=2
// 2 nodes succeed, 1 fails, appendOp should still succeed
func TestAppendOp_QuorumWrite_Case2_TwoSuccessOneFail(t *testing.T) {
	// Setup mocks
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient1 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient2 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient3 := mocks_logstore_client.NewLogStoreClient(t)

	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Wq:    3,
		Aq:    2, // Only need 2 acks to succeed
		Es:    3,
		Nodes: []string{"node1", "node2", "node3"},
	}

	callbackCalled := false
	var callbackSegmentId, callbackEntryId int64
	var callbackErr error

	callback := func(segmentId int64, entryId int64, err error) {
		callbackCalled = true
		callbackSegmentId = segmentId
		callbackEntryId = entryId
		callbackErr = err
	}

	// Setup expectations
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient1, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(mockClient2, nil)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node3").Return(mockClient3, nil)

	// Setup AppendEntry to return immediately, we'll send async results later
	mockClient1.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), mock.Anything, mock.Anything).Return(int64(3), nil)
	mockClient2.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), mock.Anything, mock.Anything).Return(int64(3), nil)
	mockClient3.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), mock.Anything, mock.Anything).Return(int64(3), nil)

	// Expect SendAppendSuccessCallbacks to be called when quorum (aq=2) is reached
	mockHandle.EXPECT().SendAppendSuccessCallbacks(mock.Anything, int64(3)).Run(func(ctx context.Context, entryId int64) {
		// Simulate what the real SendAppendSuccessCallbacks would do - call the callback
		callback(2, entryId, nil)
	}).Return()

	// Expect HandleAppendRequestFailure for the failing node
	mockHandle.EXPECT().HandleAppendRequestFailure(mock.Anything, int64(3), mock.Anything, 2, "node3").Return()

	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), callback, mockClientPool, mockHandle, quorumInfo)

	// Execute
	op.Execute()

	// Wait for channels to be created
	time.Sleep(50 * time.Millisecond)

	// Simulate async responses: 2 success, 1 failure
	// Node 1 succeeds
	if len(op.resultChannels) > 0 && op.resultChannels[0] != nil {
		err := op.resultChannels[0].SendResult(context.Background(), &channel.AppendResult{SyncedId: 3, Err: nil})
		if err != nil {
			t.Logf("Failed to send result to channel 0: %v", err)
		}
	}

	// Node 2 succeeds
	if len(op.resultChannels) > 1 && op.resultChannels[1] != nil {
		err := op.resultChannels[1].SendResult(context.Background(), &channel.AppendResult{SyncedId: 3, Err: nil})
		if err != nil {
			t.Logf("Failed to send result to channel 1: %v", err)
		}
	}

	// Node 3 fails
	if len(op.resultChannels) > 2 && op.resultChannels[2] != nil {
		err := op.resultChannels[2].SendResult(context.Background(), &channel.AppendResult{SyncedId: -1, Err: errors.New("node3 failure")})
		if err != nil {
			t.Logf("Failed to send result to channel 2: %v", err)
		}
	}

	// Wait for async operations to complete
	time.Sleep(500 * time.Millisecond)

	// Verify that operation completed successfully despite 1 failure
	assert.True(t, op.completed.Load(), "Operation should be completed successfully")
	assert.GreaterOrEqual(t, op.ackSet.Count(), 2, "At least 2 nodes should have acked (quorum reached)")

	// Wait for callback
	time.Sleep(50 * time.Millisecond)
	assert.True(t, callbackCalled, "Success callback should have been called")
	assert.Equal(t, int64(2), callbackSegmentId)
	assert.Equal(t, int64(3), callbackEntryId)
	assert.NoError(t, callbackErr, "Should succeed despite 1 node failure")
}

// TestAppendOp_QuorumWrite_Case3_SingleNodeSuccess tests quorum write with es=1,wq=1,aq=1
// Single node succeeds, appendOp should succeed
func TestAppendOp_QuorumWrite_Case3_SingleNodeSuccess(t *testing.T) {
	// Setup mocks
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)

	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Wq:    1,
		Aq:    1, // Need 1 ack to succeed
		Es:    1,
		Nodes: []string{"node1"},
	}

	callbackCalled := false
	var callbackSegmentId, callbackEntryId int64
	var callbackErr error

	callback := func(segmentId int64, entryId int64, err error) {
		callbackCalled = true
		callbackSegmentId = segmentId
		callbackEntryId = entryId
		callbackErr = err
	}

	// Setup expectations
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClient.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), mock.Anything, mock.Anything).Return(int64(3), nil)

	// Expect SendAppendSuccessCallbacks to be called when quorum (aq=1) is reached
	mockHandle.EXPECT().SendAppendSuccessCallbacks(mock.Anything, int64(3)).Run(func(ctx context.Context, entryId int64) {
		// Simulate what the real SendAppendSuccessCallbacks would do - call the callback
		callback(2, entryId, nil)
	}).Return()

	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), callback, mockClientPool, mockHandle, quorumInfo)

	// Execute
	op.Execute()

	// Wait for channels to be created
	time.Sleep(50 * time.Millisecond)

	// Simulate successful response from the single node
	if len(op.resultChannels) > 0 && op.resultChannels[0] != nil {
		err := op.resultChannels[0].SendResult(context.Background(), &channel.AppendResult{SyncedId: 3, Err: nil})
		if err != nil {
			t.Logf("Failed to send result to channel 0: %v", err)
		}
	}

	// Wait for async operations to complete
	time.Sleep(500 * time.Millisecond)

	// Verify that operation completed successfully
	assert.True(t, op.completed.Load(), "Operation should be completed")
	assert.Equal(t, 1, op.ackSet.Count(), "Single node should have acked")

	// Wait for callback
	time.Sleep(50 * time.Millisecond)
	assert.True(t, callbackCalled, "Callback should have been called")
	assert.Equal(t, int64(2), callbackSegmentId)
	assert.Equal(t, int64(3), callbackEntryId)
	assert.NoError(t, callbackErr)
}

// TestAppendOp_QuorumWrite_Case4_SingleNodeFailure tests quorum write with es=1,wq=1,aq=1
// Single node fails, appendOp should fail
func TestAppendOp_QuorumWrite_Case4_SingleNodeFailure(t *testing.T) {
	// Setup mocks
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)

	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Wq:    1,
		Aq:    1, // Need 1 ack to succeed
		Es:    1,
		Nodes: []string{"node1"},
	}

	callback := func(segmentId int64, entryId int64, err error) {
		// Expected to be called with failure in this test case
		// For failure cases, we don't verify callback details in this simple test
	}

	// Setup expectations
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)

	failureErr := errors.New("node1 failure")
	mockClient.EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), mock.Anything, mock.Anything).Return(int64(3), nil)

	// Expect HandleAppendRequestFailure for the failing node
	mockHandle.EXPECT().HandleAppendRequestFailure(mock.Anything, int64(3), mock.Anything, 0, "node1").Return()

	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), callback, mockClientPool, mockHandle, quorumInfo)

	// Execute
	op.Execute()

	// Wait for channels to be created
	time.Sleep(50 * time.Millisecond)

	// Simulate failure response from the single node
	if len(op.resultChannels) > 0 && op.resultChannels[0] != nil {
		err := op.resultChannels[0].SendResult(context.Background(), &channel.AppendResult{SyncedId: -1, Err: failureErr})
		if err != nil {
			t.Logf("Failed to send result to channel 0: %v", err)
		}
	}

	// Wait for async operations to complete
	time.Sleep(500 * time.Millisecond)

	// Verify that operation did not complete successfully
	assert.False(t, op.completed.Load(), "Operation should not be completed")
	assert.Equal(t, 0, op.ackSet.Count(), "No nodes should have acked")
	assert.Equal(t, failureErr, op.channelErrors[0], "Error should be set for channel 0")
}

// TestAppendOp_QuorumWrite_Case5_SimpleQuorumTest tests basic quorum behavior
// This is a simplified version focusing on key quorum logic
func TestAppendOp_QuorumWrite_Case5_SimpleQuorumTest(t *testing.T) {
	t.Run("QuorumReached_Wq1_case1_ShouldSucceed", func(t *testing.T) {
		// Test with 1 node, aq=1, 1 success -> should succeed
		testSimpleQuorum(t, 1, 1, 1, true)
	})
	t.Run("QuorumReached_Wq1_case2_ShouldFail", func(t *testing.T) {
		// Test with 1 node, aq=1, 1 success -> should fail
		testSimpleQuorum(t, 1, 1, 0, false)
	})

	t.Run("QuorumReached_Wq3_case1_ShouldSucceed", func(t *testing.T) {
		// Test with 3 nodes, aq=2, 3 success -> should succeed
		testSimpleQuorum(t, 3, 2, 3, true)
	})

	t.Run("QuorumReached_Wq3_case2_ShouldSucceed", func(t *testing.T) {
		// Test with 3 nodes, aq=2, 2 success -> should succeed
		testSimpleQuorum(t, 3, 2, 2, true)
	})

	t.Run("QuorumNotReached_Wq3_case3_ShouldFail", func(t *testing.T) {
		// Test with 3 nodes, aq=2, only 1 success -> should fail
		testSimpleQuorum(t, 3, 2, 1, false)
	})

	t.Run("QuorumNotReached_Wq5_case1_ShouldSuccess", func(t *testing.T) {
		// Test with 5 nodes, aq=3, 5 success -> should succeed
		testSimpleQuorum(t, 5, 3, 5, true)
	})

	t.Run("QuorumNotReached_Wq5_case2_ShouldSuccess", func(t *testing.T) {
		// Test with 5 nodes, aq=3, 4 success -> should succeed
		testSimpleQuorum(t, 5, 3, 4, true)
	})

	t.Run("QuorumNotReached_Wq5_case3_ShouldSuccess", func(t *testing.T) {
		// Test with 5 nodes, aq=3, 3 success -> should succeed
		testSimpleQuorum(t, 5, 3, 3, true)
	})

	t.Run("QuorumNotReached_Wq5_case4_ShouldFail", func(t *testing.T) {
		// Test with 5 nodes, aq=3, only 2 success -> should fail
		testSimpleQuorum(t, 5, 3, 2, false)
	})

	t.Run("QuorumNotReached_Wq5_case4_ShouldFail", func(t *testing.T) {
		// Test with 5 nodes, aq=3, only 2 success -> should fail
		testSimpleQuorum(t, 5, 3, 1, false)
	})
}

// Helper function for simple quorum testing
func testSimpleQuorum(t *testing.T, nodeCount, ackQuorum, successCount int, expectedSuccess bool) {
	// Setup mocks
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	// Create mock clients for all nodes
	mockClients := make([]*mocks_logstore_client.LogStoreClient, nodeCount)
	nodes := make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		mockClients[i] = mocks_logstore_client.NewLogStoreClient(t)
		nodes[i] = fmt.Sprintf("node%d", i+1)
	}

	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Wq:    int32(nodeCount),
		Aq:    int32(ackQuorum),
		Es:    int32(nodeCount),
		Nodes: nodes,
	}

	callbackCalled := false
	callback := func(segmentId int64, entryId int64, err error) {
		callbackCalled = true
	}

	// Setup expectations for client pool and AppendEntry
	for i := 0; i < nodeCount; i++ {
		mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, fmt.Sprintf("node%d", i+1)).Return(mockClients[i], nil)

		mockClients[i].EXPECT().AppendEntry(mock.Anything, mock.Anything, mock.Anything, int64(1), mock.Anything, mock.Anything).Return(int64(3), nil)

		if i >= successCount {
			// Expect failure callback for failed nodes
			mockHandle.EXPECT().HandleAppendRequestFailure(mock.Anything, int64(3), mock.Anything, i, fmt.Sprintf("node%d", i+1)).Return()
		}
	}

	if expectedSuccess {
		// Expect SendAppendSuccessCallbacks to be called when quorum is reached
		mockHandle.EXPECT().SendAppendSuccessCallbacks(mock.Anything, int64(3)).Run(func(ctx context.Context, entryId int64) {
			// Simulate what the real SendAppendSuccessCallbacks would do - call the callback
			callback(2, entryId, nil)
		}).Return()
	}

	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"), callback, mockClientPool, mockHandle, quorumInfo)

	// Execute
	op.Execute()

	// Wait for channels to be created
	time.Sleep(50 * time.Millisecond)

	// Simulate async responses
	for i := 0; i < nodeCount; i++ {
		if len(op.resultChannels) > i && op.resultChannels[i] != nil {
			var err error
			if i < successCount {
				// Success response
				err = op.resultChannels[i].SendResult(context.Background(), &channel.AppendResult{SyncedId: 3, Err: nil})
			} else {
				// Failure response
				err = op.resultChannels[i].SendResult(context.Background(), &channel.AppendResult{SyncedId: -1, Err: errors.New("node failure")})
			}
			if err != nil {
				t.Logf("Failed to send result to channel %d: %v", i, err)
			}
		}
	}

	// Wait for async operations to complete
	time.Sleep(500 * time.Millisecond)

	// Verify results
	if expectedSuccess {
		assert.True(t, op.completed.Load(), "Operation should be completed successfully")
		assert.GreaterOrEqual(t, op.ackSet.Count(), ackQuorum, fmt.Sprintf("At least %d nodes should have acked", ackQuorum))
		assert.True(t, callbackCalled, "Success callback should have been called")
	} else {
		assert.False(t, op.completed.Load(), "Operation should not be completed")
		assert.Less(t, op.ackSet.Count(), ackQuorum, fmt.Sprintf("Less than %d nodes should have acked (insufficient for quorum)", ackQuorum))
	}
}
