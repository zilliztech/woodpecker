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

package channel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"github.com/zilliztech/woodpecker/proto"
)

// mockStreamClient is a mock implementation of grpc.ServerStreamingClient[proto.AddEntryResponse].
type mockStreamClient struct {
	recvFunc func() (*proto.AddEntryResponse, error)
	closed   bool
}

func (m *mockStreamClient) Recv() (*proto.AddEntryResponse, error) {
	return m.recvFunc()
}

func (m *mockStreamClient) CloseSend() error {
	m.closed = true
	return nil
}

func (m *mockStreamClient) Header() (metadata.MD, error) { return nil, nil }
func (m *mockStreamClient) Trailer() metadata.MD         { return nil }
func (m *mockStreamClient) Context() context.Context     { return context.Background() }
func (m *mockStreamClient) SendMsg(any) error            { return nil }
func (m *mockStreamClient) RecvMsg(any) error            { return nil }

// mockStreamClientWithCloseError returns an error on CloseSend.
type mockStreamClientWithCloseError struct {
	mockStreamClient
}

func (m *mockStreamClientWithCloseError) CloseSend() error {
	return errors.New("close error")
}

func TestNewRemoteResultChannel(t *testing.T) {
	identifier := "test-channel-1"
	channel := NewRemoteResultChannel(identifier)

	assert.NotNil(t, channel)
	assert.Equal(t, identifier, channel.GetIdentifier())
	assert.False(t, channel.IsClosed())
	assert.Nil(t, channel.result)
	assert.Nil(t, channel.ch)
}

func TestRemoteResultChannel_SendResult_Success(t *testing.T) {
	channel := NewRemoteResultChannel("test-channel")
	ctx := context.Background()

	result := &AppendResult{
		SyncedId: 123,
		Err:      nil,
	}

	err := channel.SendResult(ctx, result)
	assert.NoError(t, err)
	assert.Equal(t, result, channel.result)
}

func TestRemoteResultChannel_SendResult_Closed(t *testing.T) {
	channel := NewRemoteResultChannel("test-channel")
	ctx := context.Background()

	// Close the channel first
	err := channel.Close(ctx)
	assert.NoError(t, err)

	result := &AppendResult{
		SyncedId: 123,
		Err:      nil,
	}

	err = channel.SendResult(ctx, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "is closed")
}

func TestRemoteResultChannel_ReadResult_DirectResult(t *testing.T) {
	channel := NewRemoteResultChannel("test-channel")
	ctx := context.Background()

	// Set result directly via SendResult
	expectedResult := &AppendResult{
		SyncedId: 456,
		Err:      nil,
	}

	err := channel.SendResult(ctx, expectedResult)
	assert.NoError(t, err)

	// ReadResult should return the direct result immediately
	result, err := channel.ReadResult(ctx)
	assert.NoError(t, err)
	assert.Equal(t, expectedResult, result)
	assert.Equal(t, int64(456), result.SyncedId)
}

func TestRemoteResultChannel_ReadResult_DirectResult_WithError(t *testing.T) {
	channel := NewRemoteResultChannel("test-channel")
	ctx := context.Background()

	// Set result with error directly via SendResult
	expectedErr := errors.New("test error")
	expectedResult := &AppendResult{
		SyncedId: -1,
		Err:      expectedErr,
	}

	err := channel.SendResult(ctx, expectedResult)
	assert.NoError(t, err)

	// ReadResult should return the direct result immediately
	result, err := channel.ReadResult(ctx)
	assert.NoError(t, err) // SendResult itself doesn't return error
	assert.Equal(t, expectedResult, result)
	assert.Equal(t, int64(-1), result.SyncedId)
	assert.Equal(t, expectedErr, result.Err)
}

func TestRemoteResultChannel_ReadResult_NotInitialized(t *testing.T) {
	channel := NewRemoteResultChannel("test-channel")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Try to read without setting result or initializing gRPC stream
	// Should timeout since we're now polling for direct results
	result, err := channel.ReadResult(ctx)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestRemoteResultChannel_ReadResult_Closed(t *testing.T) {
	channel := NewRemoteResultChannel("test-channel")
	ctx := context.Background()

	// Close the channel first
	err := channel.Close(ctx)
	assert.NoError(t, err)

	// Try to read from closed channel
	result, err := channel.ReadResult(ctx)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "is closed")
}

func TestRemoteResultChannel_Close_Multiple(t *testing.T) {
	channel := NewRemoteResultChannel("test-channel")
	ctx := context.Background()

	// First close should succeed
	err := channel.Close(ctx)
	assert.NoError(t, err)
	assert.True(t, channel.IsClosed())

	// Second close should be no-op (no error)
	err = channel.Close(ctx)
	assert.NoError(t, err)
	assert.True(t, channel.IsClosed())
}

func TestRemoteResultChannel_ConcurrentOperations(t *testing.T) {
	channel := NewRemoteResultChannel("test-channel")
	ctx := context.Background()

	// Test concurrent SendResult and ReadResult
	result := &AppendResult{
		SyncedId: 999,
		Err:      nil,
	}

	// Send result in goroutine
	go func() {
		time.Sleep(10 * time.Millisecond)
		err := channel.SendResult(ctx, result)
		assert.NoError(t, err)
	}()

	// Read result should eventually get the result
	// Note: Since we're not using gRPC stream, ReadResult will fail until SendResult is called
	time.Sleep(20 * time.Millisecond) // Wait for SendResult to complete

	readResult, err := channel.ReadResult(ctx)
	assert.NoError(t, err)
	assert.Equal(t, result, readResult)
}

func TestRemoteResultChannel_GetIdentifier(t *testing.T) {
	identifier := "unique-test-identifier-12345"
	channel := NewRemoteResultChannel(identifier)

	assert.Equal(t, identifier, channel.GetIdentifier())
}

func TestRemoteResultChannel_ReadResult_WaitsForDirectResult(t *testing.T) {
	channel := NewRemoteResultChannel("test-channel")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start reading in a goroutine - should wait for result
	resultChan := make(chan *AppendResult, 1)
	errorChan := make(chan error, 1)

	go func() {
		result, err := channel.ReadResult(ctx)
		if err != nil {
			errorChan <- err
		} else {
			resultChan <- result
		}
	}()

	// Wait a bit to ensure ReadResult is polling
	time.Sleep(50 * time.Millisecond)

	// Now send the result
	expectedResult := &AppendResult{
		SyncedId: 999,
		Err:      nil,
	}

	err := channel.SendResult(context.Background(), expectedResult)
	assert.NoError(t, err)

	// ReadResult should now return the result
	select {
	case result := <-resultChan:
		assert.Equal(t, expectedResult, result)
	case err := <-errorChan:
		t.Fatalf("ReadResult returned error: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("ReadResult did not return within timeout")
	}
}

func TestRemoteResultChannel_ReadResult_ContextCancellation(t *testing.T) {
	channel := NewRemoteResultChannel("test-channel")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// ReadResult should respect context cancellation
	result, err := channel.ReadResult(ctx)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, context.DeadlineExceeded, err)
}

// Test the workflow that simulates real usage in append operations
func TestRemoteResultChannel_MockUsageWorkflow(t *testing.T) {
	t.Run("SuccessWorkflow", func(t *testing.T) {
		channel := NewRemoteResultChannel("mock-workflow-success")
		ctx := context.Background()

		// Step 1: Create channel (done)
		assert.False(t, channel.IsClosed())

		// Step 2: Simulate async operation completing with success
		go func() {
			time.Sleep(5 * time.Millisecond)
			err := channel.SendResult(ctx, &AppendResult{
				SyncedId: 12345,
				Err:      nil,
			})
			assert.NoError(t, err)
		}()

		// Step 3: Read result (should get the success result)
		time.Sleep(10 * time.Millisecond) // Wait for async operation
		result, err := channel.ReadResult(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, int64(12345), result.SyncedId)
		assert.NoError(t, result.Err)

		// Step 4: Close channel
		err = channel.Close(ctx)
		assert.NoError(t, err)
		assert.True(t, channel.IsClosed())
	})

	t.Run("FailureWorkflow", func(t *testing.T) {
		channel := NewRemoteResultChannel("mock-workflow-failure")
		ctx := context.Background()

		// Step 1: Create channel (done)
		assert.False(t, channel.IsClosed())

		// Step 2: Simulate async operation completing with failure
		expectedErr := errors.New("operation failed")
		go func() {
			time.Sleep(5 * time.Millisecond)
			err := channel.SendResult(ctx, &AppendResult{
				SyncedId: -1,
				Err:      expectedErr,
			})
			assert.NoError(t, err)
		}()

		// Step 3: Read result (should get the failure result)
		time.Sleep(10 * time.Millisecond) // Wait for async operation
		result, err := channel.ReadResult(ctx)
		assert.NoError(t, err) // ReadResult itself succeeds, but result contains error
		assert.NotNil(t, result)
		assert.Equal(t, int64(-1), result.SyncedId)
		assert.Equal(t, expectedErr, result.Err)

		// Step 4: Close channel
		err = channel.Close(ctx)
		assert.NoError(t, err)
		assert.True(t, channel.IsClosed())
	})
}

func TestRemoteResultChannel_InitResponseStream(t *testing.T) {
	channel := NewRemoteResultChannel("test-init-stream")
	ctx, cancel := context.WithCancel(context.Background())

	stream := &mockStreamClient{
		recvFunc: func() (*proto.AddEntryResponse, error) {
			return &proto.AddEntryResponse{
				EntryId: 42,
				State:   proto.AddEntryState_Synced,
			}, nil
		},
	}

	channel.InitResponseStream(stream, ctx, cancel)
	assert.Equal(t, stream, channel.ch)
	assert.NotNil(t, channel.cancel)
	cancel()
}

func TestRemoteResultChannel_InitResponseStream_AlreadyClosed(t *testing.T) {
	channel := NewRemoteResultChannel("test-init-closed")
	ctx := context.Background()

	// Close the channel first
	err := channel.Close(ctx)
	assert.NoError(t, err)

	// InitResponseStream on closed channel should cancel the context
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &mockStreamClient{}

	channel.InitResponseStream(stream, streamCtx, cancel)
	// Stream should NOT be set
	assert.Nil(t, channel.ch)
	// Context should be cancelled
	assert.Error(t, streamCtx.Err())
}

func TestRemoteResultChannel_Close_WithCancelAndStream(t *testing.T) {
	channel := NewRemoteResultChannel("test-close-stream")
	ctx, cancel := context.WithCancel(context.Background())

	stream := &mockStreamClient{}
	channel.InitResponseStream(stream, ctx, cancel)

	// Close should cancel context and close stream
	err := channel.Close(context.Background())
	assert.NoError(t, err)
	assert.True(t, channel.IsClosed())
	assert.True(t, stream.closed)
	assert.Error(t, ctx.Err()) // context should be cancelled
}

func TestRemoteResultChannel_Close_WithStreamCloseError(t *testing.T) {
	channel := NewRemoteResultChannel("test-close-err")
	ctx, cancel := context.WithCancel(context.Background())

	stream := &mockStreamClientWithCloseError{}
	channel.InitResponseStream(stream, ctx, cancel)

	// Close should succeed even if CloseSend fails (logs warning)
	err := channel.Close(context.Background())
	assert.NoError(t, err)
	assert.True(t, channel.IsClosed())
}

func TestRemoteResultChannel_ReadResult_ViaStream_Synced(t *testing.T) {
	channel := NewRemoteResultChannel("test-read-stream-synced")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := &mockStreamClient{
		recvFunc: func() (*proto.AddEntryResponse, error) {
			return &proto.AddEntryResponse{
				EntryId: 100,
				State:   proto.AddEntryState_Synced,
			}, nil
		},
	}
	channel.InitResponseStream(stream, ctx, cancel)

	result, err := channel.ReadResult(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(100), result.SyncedId)
	assert.NoError(t, result.Err)
}

func TestRemoteResultChannel_ReadResult_ViaStream_Failed(t *testing.T) {
	channel := NewRemoteResultChannel("test-read-stream-fail")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := &mockStreamClient{
		recvFunc: func() (*proto.AddEntryResponse, error) {
			return &proto.AddEntryResponse{
				EntryId: 200,
				State:   proto.AddEntryState_Failed, // not Synced
				Status:  &proto.Status{Code: 500},
			}, nil
		},
	}
	channel.InitResponseStream(stream, ctx, cancel)

	result, err := channel.ReadResult(context.Background())
	assert.Error(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(200), result.SyncedId)
	assert.Error(t, result.Err)
}

func TestRemoteResultChannel_ReadResult_ViaStream_RecvError(t *testing.T) {
	channel := NewRemoteResultChannel("test-read-stream-recv-err")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := &mockStreamClient{
		recvFunc: func() (*proto.AddEntryResponse, error) {
			return nil, errors.New("stream broken")
		},
	}
	channel.InitResponseStream(stream, ctx, cancel)

	result, err := channel.ReadResult(context.Background())
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "stream broken")
}

func TestRemoteResultChannel_ReadResult_ClosedDuringPoll(t *testing.T) {
	channel := NewRemoteResultChannel("test-closed-during-poll")

	// Start reading in goroutine (will enter polling loop since ch == nil)
	resultCh := make(chan error, 1)
	go func() {
		_, err := channel.ReadResult(context.Background())
		resultCh <- err
	}()

	// Wait for polling to start, then close
	time.Sleep(30 * time.Millisecond)
	channel.Close(context.Background())

	// Should detect closed state during poll
	select {
	case err := <-resultCh:
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is closed")
	case <-time.After(1 * time.Second):
		t.Fatal("ReadResult did not return after close")
	}
}

func TestRemoteResultChannel_ReadResult_DirectResultAlreadySet(t *testing.T) {
	channel := NewRemoteResultChannel("test-direct-already-set")
	ctx := context.Background()

	// Set result directly, then also init a stream (result should be returned without touching stream)
	channel.SendResult(ctx, &AppendResult{SyncedId: 77, Err: nil})

	streamCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &mockStreamClient{
		recvFunc: func() (*proto.AddEntryResponse, error) {
			t.Fatal("stream Recv should not be called when direct result exists")
			return nil, nil
		},
	}
	channel.InitResponseStream(stream, streamCtx, cancel)

	result, err := channel.ReadResult(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(77), result.SyncedId)
}
