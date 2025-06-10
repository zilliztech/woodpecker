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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockOperation is a mock implementation of Operation interface for testing
type MockOperation struct {
	executed atomic.Bool
	delay    time.Duration
}

func (m *MockOperation) Execute() {
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	m.executed.Store(true)
}

func (m *MockOperation) IsExecuted() bool {
	return m.executed.Load()
}

func (m *MockOperation) Identifier() string {
	return ""
}

// OrderedMockOperation is a mock operation that records execution order
type OrderedMockOperation struct {
	executed atomic.Bool
	delay    time.Duration
	index    int
	recorder func(int)
}

func (m *OrderedMockOperation) Execute() {
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	m.executed.Store(true)
	if m.recorder != nil {
		m.recorder(m.index)
	}
}

func (m *OrderedMockOperation) IsExecuted() bool {
	return m.executed.Load()
}

func (m *OrderedMockOperation) Identifier() string {
	return fmt.Sprintf("%d", m.index)
}

// BlockingMockOperation is a mock operation that can be controlled to block
type BlockingMockOperation struct {
	executed atomic.Bool
	blocker  *sync.WaitGroup
}

func (m *BlockingMockOperation) Execute() {
	if m.blocker != nil {
		m.blocker.Wait() // Block until released
	}
	m.executed.Store(true)
}

func (m *BlockingMockOperation) IsExecuted() bool {
	return m.executed.Load()
}

func (m *BlockingMockOperation) Identifier() string {
	return ""
}

// createMockOperation creates a mock operation for testing
func createMockOperation(delay time.Duration) *MockOperation {
	return &MockOperation{delay: delay}
}

// createOrderedMockOperation creates an ordered mock operation for testing
func createOrderedMockOperation(index int, delay time.Duration, recorder func(int)) *OrderedMockOperation {
	return &OrderedMockOperation{
		index:    index,
		delay:    delay,
		recorder: recorder,
	}
}

func createBlockingMockOperation(blocker *sync.WaitGroup) *BlockingMockOperation {
	return &BlockingMockOperation{blocker: blocker}
}

func TestNewSequentialExecutor(t *testing.T) {
	bufferSize := 10
	executor := NewSequentialExecutor(bufferSize)

	assert.NotNil(t, executor)
	assert.NotNil(t, executor.operationQueue)
	assert.Equal(t, bufferSize, cap(executor.operationQueue))
	assert.False(t, executor.closed)
}

func TestSequentialExecutor_BasicExecution(t *testing.T) {
	executor := NewSequentialExecutor(10)
	executor.Start(context.TODO())
	defer executor.Stop(context.TODO())

	// Create a mock operation
	mockOp := createMockOperation(0)

	// Submit the operation
	success := executor.Submit(context.TODO(), mockOp)
	assert.True(t, success)

	// Wait for execution
	time.Sleep(100 * time.Millisecond)

	// Verify the operation was executed
	assert.True(t, mockOp.IsExecuted())
}

func TestSequentialExecutor_MultipleOperations(t *testing.T) {
	executor := NewSequentialExecutor(10)
	executor.Start(context.TODO())
	defer executor.Stop(context.TODO())

	numOps := 5
	mockOps := make([]*MockOperation, numOps)

	// Submit multiple operations
	for i := 0; i < numOps; i++ {
		mockOps[i] = createMockOperation(0)
		success := executor.Submit(context.TODO(), mockOps[i])
		assert.True(t, success)
	}

	// Wait for all executions
	time.Sleep(200 * time.Millisecond)

	// Verify all operations were executed
	for i := 0; i < numOps; i++ {
		assert.True(t, mockOps[i].IsExecuted(), "Operation %d should be executed", i)
	}
}

func TestSequentialExecutor_SequentialOrder(t *testing.T) {
	executor := NewSequentialExecutor(10)
	executor.Start(context.TODO())
	defer executor.Stop(context.TODO())

	var executionOrder []int
	var mu sync.Mutex

	// Create a recorder function that safely appends to the execution order
	recorder := func(index int) {
		mu.Lock()
		executionOrder = append(executionOrder, index)
		mu.Unlock()
	}

	numOps := 5

	for i := 0; i < numOps; i++ {
		mockOp := createOrderedMockOperation(i, 10*time.Millisecond, recorder)
		success := executor.Submit(context.TODO(), mockOp)
		assert.True(t, success)
	}

	// Wait for all executions
	time.Sleep(500 * time.Millisecond)

	// Verify sequential execution order
	mu.Lock()
	assert.Equal(t, numOps, len(executionOrder))
	for i := 0; i < numOps; i++ {
		assert.Equal(t, i, executionOrder[i], "Operations should execute in order")
	}
	mu.Unlock()
}

func TestSequentialExecutor_SubmitAfterStop(t *testing.T) {
	executor := NewSequentialExecutor(10)
	executor.Start(context.TODO())

	// Stop the executor
	executor.Stop(context.TODO())

	// Try to submit after stop
	mockOp := createMockOperation(0)
	success := executor.Submit(context.TODO(), mockOp)

	// Should return false
	assert.False(t, success)

	// Operation should not be executed
	time.Sleep(100 * time.Millisecond)
	assert.False(t, mockOp.IsExecuted())
}

func TestSequentialExecutor_ConcurrentSubmitAndStop(t *testing.T) {
	// This test verifies the race condition fix
	for i := 0; i < 100; i++ { // Run multiple times to increase chance of hitting race condition
		executor := NewSequentialExecutor(10)
		executor.Start(context.TODO())

		var wg sync.WaitGroup
		var successCount atomic.Int32

		// Start multiple goroutines submitting operations
		numSubmitters := 10
		for j := 0; j < numSubmitters; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				mockOp := createMockOperation(0)
				if executor.Submit(context.TODO(), mockOp) {
					successCount.Add(1)
				}
			}()
		}

		// Stop the executor concurrently
		go func() {
			time.Sleep(1 * time.Millisecond) // Small delay to let some submits happen
			executor.Stop(context.TODO())
		}()

		wg.Wait()

		// Should not panic and some operations might succeed
		// The exact count depends on timing, but it should be >= 0 and <= numSubmitters
		count := successCount.Load()
		assert.GreaterOrEqual(t, int(count), 0)
		assert.LessOrEqual(t, int(count), numSubmitters)
	}
}

func TestSequentialExecutor_FullBufferBlocking(t *testing.T) {
	bufferSize := 2
	executor := NewSequentialExecutor(bufferSize)
	executor.Start(context.TODO())
	defer executor.Stop(context.TODO())

	// Create a blocker to control when the first operation completes
	var workerBlocked sync.WaitGroup
	workerBlocked.Add(1)

	// Submit the blocking operation first - this will block the worker
	blockingOp := createBlockingMockOperation(&workerBlocked)
	success := executor.Submit(context.TODO(), blockingOp)
	assert.True(t, success)

	// Now submit operations to fill the buffer
	slowOps := make([]*MockOperation, bufferSize)
	for i := 0; i < bufferSize; i++ {
		slowOps[i] = createMockOperation(0)
		success := executor.Submit(context.TODO(), slowOps[i])
		assert.True(t, success)
	}

	// Now the buffer should be full (blocking op + 2 buffered ops)
	// The next submit should block
	start := time.Now()
	var submitDone atomic.Bool
	extraOp := createMockOperation(0)

	go func() {
		success := executor.Submit(context.TODO(), extraOp)
		assert.True(t, success)
		submitDone.Store(true)
	}()

	// Wait a bit and verify submit is still blocking
	time.Sleep(50 * time.Millisecond)
	assert.False(t, submitDone.Load(), "Submit should be blocking")

	// Release the worker to process operations
	workerBlocked.Done()

	// Wait for submit to complete
	time.Sleep(100 * time.Millisecond)
	assert.True(t, submitDone.Load(), "Submit should complete after buffer has space")

	duration := time.Since(start)
	assert.Greater(t, duration, 40*time.Millisecond, "Submit should have been blocked")
}

func TestSequentialExecutor_StopWaitsForCompletion(t *testing.T) {
	executor := NewSequentialExecutor(10)
	executor.Start(context.TODO())

	// Submit a slow operation
	slowOp := createMockOperation(200 * time.Millisecond)
	success := executor.Submit(context.TODO(), slowOp)
	assert.True(t, success)

	// Stop should wait for the operation to complete
	start := time.Now()
	executor.Stop(context.TODO())
	duration := time.Since(start)

	// Stop should have waited for the slow operation
	assert.Greater(t, duration, 150*time.Millisecond)
	assert.True(t, slowOp.IsExecuted())
}

func TestSequentialExecutor_MultipleStopCalls(t *testing.T) {
	executor := NewSequentialExecutor(10)
	executor.Start(context.TODO())

	// Multiple stop calls should not panic or cause issues
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			executor.Stop(context.TODO())
		}()
	}

	// Should not panic
	wg.Wait()
}

func TestSequentialExecutor_StopWithoutStart(t *testing.T) {
	executor := NewSequentialExecutor(10)

	// Stop without start should not panic
	require.NotPanics(t, func() {
		executor.Stop(context.TODO())
	})
}

func TestSequentialExecutor_SubmitWithoutStart(t *testing.T) {
	executor := NewSequentialExecutor(10)

	mockOp := createMockOperation(0)

	// Submit without start should work (but operation won't be processed)
	success := executor.Submit(context.TODO(), mockOp)
	assert.True(t, success)

	// Operation should not be executed since worker is not running
	time.Sleep(100 * time.Millisecond)
	assert.False(t, mockOp.IsExecuted())

	// Note: We don't call executor.Stop() here because there's no worker running
	// to process the submitted operation, which would cause WaitGroup to hang
}

func TestSequentialExecutor_ConcurrentSubmits(t *testing.T) {
	executor := NewSequentialExecutor(100)
	executor.Start(context.TODO())
	defer executor.Stop(context.TODO())

	numGoroutines := 50
	numOpsPerGoroutine := 10
	totalOps := numGoroutines * numOpsPerGoroutine

	var wg sync.WaitGroup
	var successCount atomic.Int32
	allOps := make([]*MockOperation, totalOps)

	// Start multiple goroutines submitting operations concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(startIdx int) {
			defer wg.Done()
			for j := 0; j < numOpsPerGoroutine; j++ {
				opIdx := startIdx*numOpsPerGoroutine + j
				allOps[opIdx] = createMockOperation(0)
				if executor.Submit(context.TODO(), allOps[opIdx]) {
					successCount.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()

	// All submits should succeed
	assert.Equal(t, int32(totalOps), successCount.Load())

	// Wait for all operations to complete
	time.Sleep(500 * time.Millisecond)

	// All operations should be executed
	for i, op := range allOps {
		assert.True(t, op.IsExecuted(), "Operation %d should be executed", i)
	}
}
