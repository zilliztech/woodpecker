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
)

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
