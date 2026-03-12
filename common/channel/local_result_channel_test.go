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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/werr"
)

func TestLocalResultChannel_Basic(t *testing.T) {
	rc := NewLocalResultChannel("test-1")

	assert.Equal(t, "test-1", rc.GetIdentifier())
	assert.False(t, rc.IsClosed())
}

func TestLocalResultChannel_SendAndRead(t *testing.T) {
	rc := NewLocalResultChannel("test-2")

	ctx := context.Background()
	result := &AppendResult{SyncedId: 123, Err: nil}

	// Test send
	err := rc.SendResult(ctx, result)
	assert.NoError(t, err)

	// Test read
	readResult, err := rc.ReadResult(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(123), readResult.SyncedId)
}

func TestLocalResultChannel_SendAfterClose(t *testing.T) {
	rc := NewLocalResultChannel("test-3")

	ctx := context.Background()

	// Close first
	err := rc.Close(ctx)
	assert.NoError(t, err)
	assert.True(t, rc.IsClosed())

	// Try to send after close
	result := &AppendResult{SyncedId: 123, Err: nil}
	err = rc.SendResult(ctx, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "is closed")
}

func TestLocalResultChannel_ReadAfterClose(t *testing.T) {
	rc := NewLocalResultChannel("test-4")

	ctx := context.Background()
	result := &AppendResult{SyncedId: 123, Err: nil}

	// Send first
	err := rc.SendResult(ctx, result)
	assert.NoError(t, err)

	// Close
	err = rc.Close(ctx)
	assert.NoError(t, err)

	// Read after close should still return buffered data (Go channel semantics)
	readResult, err := rc.ReadResult(ctx)
	assert.NoError(t, err, "should be able to read buffered data after close")
	assert.Equal(t, int64(123), readResult.SyncedId)
}

func TestLocalResultChannel_ReadTimeout(t *testing.T) {
	rc := NewLocalResultChannel("test-5")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Try to read from empty channel with timeout
	_, err := rc.ReadResult(ctx)
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestLocalResultChannel_SendTimeout(t *testing.T) {
	rc := NewLocalResultChannel("test-6")

	ctx := context.Background()
	result1 := &AppendResult{SyncedId: 123, Err: nil}
	result2 := &AppendResult{SyncedId: 456, Err: nil}

	// Send first result (should succeed since buffer size is 1)
	err := rc.SendResult(ctx, result1)
	assert.NoError(t, err)

	// Try to send second result (should fail because buffer is full)
	err = rc.SendResult(ctx, result2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "full or closed")
}

func TestLocalResultChannel_ConcurrentSendAndClose(t *testing.T) {
	rc := NewLocalResultChannel("test-7")

	var wg sync.WaitGroup
	var sendErrors []error
	var sendMutex sync.Mutex

	// Start multiple senders
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			result := &AppendResult{SyncedId: int64(id), Err: nil}

			err := rc.SendResult(ctx, result)
			sendMutex.Lock()
			if err != nil {
				sendErrors = append(sendErrors, err)
			}
			sendMutex.Unlock()
		}(i)
	}

	// Close after a short delay
	go func() {
		time.Sleep(10 * time.Millisecond)
		rc.Close(context.Background())
	}()

	wg.Wait()

	// Some sends might succeed, some might fail due to race condition
	t.Logf("Send errors: %v", sendErrors)
}

func TestLocalResultChannel_ConcurrentReadAndClose(t *testing.T) {
	rc := NewLocalResultChannel("test-8")

	ctx := context.Background()
	result := &AppendResult{SyncedId: 123, Err: nil}
	rc.SendResult(ctx, result)

	var wg sync.WaitGroup
	var readResult *AppendResult
	var readError error

	// Start reader
	wg.Add(1)
	go func() {
		defer wg.Done()
		readCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		readResult, readError = rc.ReadResult(readCtx)
	}()

	// Close immediately
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		rc.Close(context.Background())
	}()

	wg.Wait()

	// Either the reader got the buffered data, or got a closed error (both valid)
	if readError == nil {
		assert.Equal(t, int64(123), readResult.SyncedId)
	} else {
		assert.Contains(t, readError.Error(), "closed")
	}
}

func TestLocalResultChannel_ReadFromEmptyClosedChannel(t *testing.T) {
	rc := NewLocalResultChannel("test-9")

	ctx := context.Background()

	// Close without sending anything
	err := rc.Close(ctx)
	assert.NoError(t, err)

	// Try to read from empty closed channel
	readCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = rc.ReadResult(readCtx)
	assert.Error(t, err)
	// Should return closed error, not timeout
	assert.Contains(t, err.Error(), "closed")
}

func TestLocalResultChannel_MultipleClose(t *testing.T) {
	rc := NewLocalResultChannel("test-10")

	ctx := context.Background()

	// Multiple closes should be safe
	err1 := rc.Close(ctx)
	err2 := rc.Close(ctx)
	err3 := rc.Close(ctx)

	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.NoError(t, err3)
	assert.True(t, rc.IsClosed())
}

// Test that ReadResult + Close don't deadlock
func TestLocalResultChannel_ReadResultDeadlock(t *testing.T) {
	rc := NewLocalResultChannel("test-deadlock")

	var wg sync.WaitGroup
	var readError error

	// Start a reader that will block on empty channel
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		_, readError = rc.ReadResult(ctx)
	}()

	// Give reader time to start blocking
	time.Sleep(50 * time.Millisecond)

	// Close should not deadlock
	closeErr := rc.Close(context.Background())
	assert.NoError(t, closeErr)

	wg.Wait()

	// Reader should get closed error (channel was closed while it was blocking)
	assert.Error(t, readError)
	assert.True(t, werr.ErrAppendOpResultChannelClosed.Is(readError))
}

// Test that concurrent Send and Close don't panic (race safety)
func TestLocalResultChannel_SendRaceCondition(t *testing.T) {
	rc := NewLocalResultChannel("test-race")

	var wg sync.WaitGroup
	var sendError error
	var closeError error

	// Start sender
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx := context.Background()
		result := &AppendResult{SyncedId: 123, Err: nil}
		time.Sleep(10 * time.Millisecond)
		sendError = rc.SendResult(ctx, result)
	}()

	// Start closer
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		closeError = rc.Close(context.Background())
	}()

	// Must not panic
	assert.NotPanics(t, func() { wg.Wait() })
	assert.NoError(t, closeError)
	// sendError may be nil or "closed" depending on timing - both are valid
	if sendError != nil {
		assert.Contains(t, sendError.Error(), "closed")
	}
}

// Test that ReadResult properly respects context timeout on empty channel
func TestLocalResultChannel_ReadResultTimeout(t *testing.T) {
	rc := NewLocalResultChannel("test-timeout")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := rc.ReadResult(ctx)
	duration := time.Since(start)

	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
	// Should timeout around 100ms, not much longer
	assert.True(t, duration < 200*time.Millisecond, "ReadResult took too long: %v", duration)
}

// Test buffered data after close
func TestLocalResultChannel_BufferedDataAfterClose(t *testing.T) {
	rc := NewLocalResultChannel("test-buffered")

	ctx := context.Background()

	// Send one result (buffer size is 1)
	result1 := &AppendResult{SyncedId: 111, Err: nil}

	err := rc.SendResult(ctx, result1)
	assert.NoError(t, err)

	// Close the channel
	err = rc.Close(ctx)
	assert.NoError(t, err)

	// Should still be able to read buffered data (Go channel semantics)
	readCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	read1, err := rc.ReadResult(readCtx)
	assert.NoError(t, err, "should be able to read buffered data after close")
	assert.Equal(t, int64(111), read1.SyncedId)

	// Second read should fail because no more data
	_, err = rc.ReadResult(readCtx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

// Test that ReadResult returns buffered data even after Close
func TestLocalResultChannel_ReadResultBufferedAfterClose(t *testing.T) {
	rc := NewLocalResultChannel("test-buffered-read")

	ctx := context.Background()
	result := &AppendResult{SyncedId: 999, Err: nil}

	// Send data to the channel
	err := rc.SendResult(ctx, result)
	assert.NoError(t, err)

	// Close the channel
	err = rc.Close(ctx)
	assert.NoError(t, err)

	// Read should return buffered data (Go closed channel returns buffered data first)
	readCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	readResult, err := rc.ReadResult(readCtx)
	assert.NoError(t, err, "ReadResult should return buffered data even after close")
	assert.Equal(t, int64(999), readResult.SyncedId)
}

// Test a potential issue with concurrent reads
func TestLocalResultChannel_ConcurrentReads(t *testing.T) {
	rc := NewLocalResultChannel("test-concurrent")

	ctx := context.Background()

	// Send one result (buffer size is 1)
	result1 := &AppendResult{SyncedId: 111, Err: nil}

	err := rc.SendResult(ctx, result1)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	var results []*AppendResult
	var errors []error
	var mu sync.Mutex

	// Start two concurrent readers
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			readCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			result, err := rc.ReadResult(readCtx)

			mu.Lock()
			if err != nil {
				errors = append(errors, err)
			} else {
				results = append(results, result)
			}
			mu.Unlock()
		}()
	}

	wg.Wait()

	// Only one reader should succeed, the other should get error
	totalOperations := len(results) + len(errors)
	assert.Equal(t, 2, totalOperations, "Should have 2 total operations")
	assert.Equal(t, 1, len(results), "Only one reader should get the message")
	if len(results) > 0 {
		assert.Equal(t, int64(111), results[0].SyncedId)
	}

	// The other reader should timeout
	assert.Equal(t, 1, len(errors), "One reader should get error")

	for _, err := range errors {
		// The other reader should get a context timeout (channel empty, no more data)
		assert.Equal(t, context.DeadlineExceeded, err, "non-winning reader should timeout")
	}
}

// Test to check if there's a subtle timing issue
func TestLocalResultChannel_TimingIssue(t *testing.T) {
	// This test tries to expose a potential race condition where
	// ReadResult might not behave correctly under specific timing conditions

	for i := 0; i < 100; i++ { // Run multiple times to increase chance of race
		rc := NewLocalResultChannel(fmt.Sprintf("test-timing-%d", i))

		var wg sync.WaitGroup
		var readErr error
		var readResult *AppendResult

		// Start reader first (it will block)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			readResult, readErr = rc.ReadResult(ctx)
		}()

		// Give reader time to start and potentially acquire lock
		time.Sleep(1 * time.Millisecond)

		// Send data
		result := &AppendResult{SyncedId: int64(i), Err: nil}
		sendErr := rc.SendResult(context.Background(), result)
		if sendErr != nil {
			t.Errorf("Iteration %d: Send failed: %v", i, sendErr)
		}

		wg.Wait()

		if readErr != nil {
			t.Errorf("Iteration %d: Read failed: %v", i, readErr)
		} else if readResult.SyncedId != int64(i) {
			t.Errorf("Iteration %d: Expected %d, got %d", i, i, readResult.SyncedId)
		}
	}
}

// Test the typical one-time use pattern: send once, read once, then close
func TestLocalResultChannel_OneTimeUsePattern(t *testing.T) {
	rc := NewLocalResultChannel("test-onetime")

	ctx := context.Background()
	result := &AppendResult{SyncedId: 123, Err: nil}

	// 1. Send once
	err := rc.SendResult(ctx, result)
	assert.NoError(t, err)

	// 2. Close the ResultChannel (typical pattern)
	err = rc.Close(ctx)
	assert.NoError(t, err)

	// 3. Read once - should still work
	readResult, err := rc.ReadResult(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(123), readResult.SyncedId)

	// 4. Try to read again - should fail
	readCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = rc.ReadResult(readCtx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

// Test what happens after closing the ResultChannel
func TestLocalResultChannel_CloseAndRead(t *testing.T) {
	rc := NewLocalResultChannel("test-close-and-read")

	ctx := context.Background()
	result := &AppendResult{SyncedId: 456, Err: nil}

	// Send data
	err := rc.SendResult(ctx, result)
	assert.NoError(t, err)

	// Read data
	readResult, err := rc.ReadResult(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(456), readResult.SyncedId)

	// Close the ResultChannel (this now closes the underlying channel too)
	err = rc.Close(ctx)
	assert.NoError(t, err)

	// Try to read again - should fail gracefully (not panic)
	readCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// This should not panic, but should return an error
	assert.NotPanics(t, func() {
		_, err = rc.ReadResult(readCtx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "underlying channel is closed")
	})
}

// Test multiple readers on the same channel (should only one succeed)
func TestLocalResultChannel_MultipleReadersOneMessage(t *testing.T) {
	rc := NewLocalResultChannel("test-multiple-readers")

	ctx := context.Background()
	result := &AppendResult{SyncedId: 789, Err: nil}

	// Send one message
	err := rc.SendResult(ctx, result)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	var results []*AppendResult
	var errors []error
	var mu sync.Mutex

	// Start multiple readers
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			readCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			readResult, readErr := rc.ReadResult(readCtx)

			mu.Lock()
			if readErr != nil {
				errors = append(errors, readErr)
			} else {
				results = append(results, readResult)
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	// Only one reader should succeed
	assert.Len(t, results, 1, "Only one reader should get the message")
	assert.Equal(t, int64(789), results[0].SyncedId)

	// The other readers should timeout or get closed error
	assert.Len(t, errors, 2, "Two readers should get errors")

	for _, err := range errors {
		// The other readers should get a context timeout (channel empty, no more data)
		assert.Equal(t, context.DeadlineExceeded, err, "non-winning reader should timeout")
	}
}

// Test the resource cleanup pattern
func TestLocalResultChannel_ResourceCleanupPattern(t *testing.T) {
	// This test demonstrates the typical lifecycle
	rc := NewLocalResultChannel("test-cleanup")

	ctx := context.Background()
	result := &AppendResult{SyncedId: 999, Err: nil}

	// Phase 1: Send and close (sender's responsibility)
	err := rc.SendResult(ctx, result)
	assert.NoError(t, err)

	err = rc.Close(ctx)
	assert.NoError(t, err)
	assert.True(t, rc.IsClosed())

	// Phase 2: Read (receiver's responsibility)
	readResult, err := rc.ReadResult(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(999), readResult.SyncedId)

	// Phase 3: Verify no more data can be read
	readCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = rc.ReadResult(readCtx)
	assert.Error(t, err)

	// Further operations should still be safe
	assert.True(t, rc.IsClosed())

	// Sending should still fail gracefully
	err = rc.SendResult(ctx, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestLocalResultChannel_SendResult_ContextCancelled(t *testing.T) {
	rc := NewLocalResultChannel("test-ctx-cancel")

	ctx := context.Background()
	// Fill the buffer first
	err := rc.SendResult(ctx, &AppendResult{SyncedId: 1, Err: nil})
	assert.NoError(t, err)

	// Now use a cancelled context to send when buffer is full
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err = rc.SendResult(cancelledCtx, &AppendResult{SyncedId: 2, Err: nil})
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

// Test the recommended usage pattern - simplified
func TestLocalResultChannel_RecommendedUsagePattern(t *testing.T) {
	rc := NewLocalResultChannel("test-recommended")

	ctx := context.Background()
	result := &AppendResult{SyncedId: 456, Err: nil}

	// Phase 1: Sender - send once and close ResultChannel
	err := rc.SendResult(ctx, result)
	assert.NoError(t, err)

	err = rc.Close(ctx)
	assert.NoError(t, err)

	// Phase 2: Receiver - read once (should still work with buffered data)
	readResult, err := rc.ReadResult(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(456), readResult.SyncedId)

	// Verify complete cleanup
	assert.True(t, rc.IsClosed())

	// No further operations should be possible
	_, err = rc.ReadResult(ctx)
	assert.Error(t, err)

	err = rc.SendResult(ctx, result)
	assert.Error(t, err)
}
