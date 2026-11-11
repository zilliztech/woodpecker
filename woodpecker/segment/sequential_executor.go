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

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
)

// SequentialExecutor is a sequential append executor
type SequentialExecutor struct {
	operationQueue chan Operation
	done           chan struct{}
	wg             sync.WaitGroup
	mu             sync.RWMutex
	closed         bool
	started        bool

	// maxBatchEntries / maxBatchBytes control client-side group commit: when
	// maxBatchEntries > 1 the worker opportunistically coalesces consecutive
	// AppendOps already waiting in the queue into a single BatchAppendOp. <= 1
	// disables batching (the worker runs one op per iteration, as before).
	maxBatchEntries int
	maxBatchBytes   int64
}

// NewSequentialExecutor initializes a SequentialExecutor with batching disabled.
func NewSequentialExecutor(bufferSize int) *SequentialExecutor {
	return NewBatchingSequentialExecutor(bufferSize, 1, 0)
}

// NewBatchingSequentialExecutor initializes a SequentialExecutor that
// opportunistically coalesces up to maxBatchEntries consecutive AppendOps
// (bounded by maxBatchBytes, 0 = unbounded) into a single batched request.
func NewBatchingSequentialExecutor(bufferSize int, maxBatchEntries int, maxBatchBytes int64) *SequentialExecutor {
	if maxBatchEntries < 1 {
		maxBatchEntries = 1
	}
	return &SequentialExecutor{
		operationQueue:  make(chan Operation, bufferSize),
		done:            make(chan struct{}),
		closed:          false,
		started:         false,
		maxBatchEntries: maxBatchEntries,
		maxBatchBytes:   maxBatchBytes,
	}
}

// Start starts the sequential append executor
func (se *SequentialExecutor) Start(ctx context.Context) {
	se.mu.Lock()
	defer se.mu.Unlock()

	if se.started {
		logger.Ctx(ctx).Debug("sequential executor already started, skip", zap.String("inst", fmt.Sprintf("%p", se)))
		return
	}

	se.started = true
	logger.Ctx(ctx).Info("start sequential executor", zap.String("inst", fmt.Sprintf("%p", se)))
	go se.worker()
}

// worker executes the logic for each order
func (se *SequentialExecutor) worker() {
	for {
		select {
		case op := <-se.operationQueue:
			se.executeMaybeBatched(op)
		case <-se.done:
			se.drainQueue()
			return
		}
	}
}

// executeOp runs a single operation with panic recovery.
func (se *SequentialExecutor) executeOp(op Operation) {
	defer se.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			logger.Ctx(context.Background()).Warn("Operation execution panicked",
				zap.String("OpId", op.Identifier()),
				zap.Any("panic", r))
		}
	}()
	op.Execute()
}

// executeMaybeBatched runs first, opportunistically coalescing it with other
// AppendOps already waiting in the queue into a single BatchAppendOp when
// batching is enabled. It only drains ops that are already queued (non-blocking),
// so it never adds latency: at low load every batch has size 1 and behaves
// exactly like executeOp. Non-AppendOp operations (e.g. retries) are never
// batched.
func (se *SequentialExecutor) executeMaybeBatched(first Operation) {
	firstAppend, ok := first.(*AppendOp)
	if !ok || se.maxBatchEntries <= 1 {
		se.executeOp(first)
		return
	}

	batch := []*AppendOp{firstAppend}
	batchBytes := int64(len(firstAppend.value))
	for len(batch) < se.maxBatchEntries {
		if se.maxBatchBytes > 0 && batchBytes >= se.maxBatchBytes {
			break
		}
		select {
		case next := <-se.operationQueue:
			nextAppend, ok := next.(*AppendOp)
			if !ok {
				// A non-append op breaks the batch: flush what we have, then run it.
				se.executeAppendBatch(batch)
				se.executeOp(next)
				return
			}
			batch = append(batch, nextAppend)
			batchBytes += int64(len(nextAppend.value))
		default:
			// Nothing else queued right now; flush the current batch.
			se.executeAppendBatch(batch)
			return
		}
	}
	se.executeAppendBatch(batch)
}

// executeAppendBatch executes a coalesced batch of AppendOps with panic recovery,
// calling wg.Done once per op. A batch of one falls back to the single-op path.
func (se *SequentialExecutor) executeAppendBatch(batch []*AppendOp) {
	if len(batch) == 1 {
		se.executeOp(batch[0])
		return
	}
	defer func() {
		for range batch {
			se.wg.Done()
		}
		if r := recover(); r != nil {
			logger.Ctx(context.Background()).Warn("Batch append execution panicked",
				zap.Int("batchSize", len(batch)),
				zap.Any("panic", r))
		}
	}()
	NewBatchAppendOp(batch).Execute()
}

// drainQueue processes any remaining operations in the queue after done is closed.
func (se *SequentialExecutor) drainQueue() {
	for {
		select {
		case op := <-se.operationQueue:
			se.executeOp(op)
		default:
			return
		}
	}
}

// Submit an op to the queue. Blocks until the operation is enqueued.
// Returns false if the executor is closed or the context is cancelled.
func (se *SequentialExecutor) Submit(ctx context.Context, op Operation) bool {
	logger.Ctx(ctx).Debug("try to submit", zap.String("OpId", op.Identifier()), zap.String("inst", fmt.Sprintf("%p", se)))

	// Use RLock to allow concurrent submits while preventing Stop() from proceeding.
	// Stop() acquires a write Lock before closing done, so holding RLock here guarantees
	// done is not closed while we are in the select, preventing any race between
	// the channel send and shutdown.
	se.mu.RLock()
	defer se.mu.RUnlock()
	if se.closed {
		logger.Ctx(ctx).Debug("submit failed, executor already closed", zap.String("OpId", op.Identifier()), zap.String("inst", fmt.Sprintf("%p", se)))
		return false
	}

	// wg.Add must happen before the channel send, because once the op is in the channel
	// the worker may immediately pick it up and call wg.Done(). If wg.Add hadn't been
	// called yet, wg would go negative and panic.
	se.wg.Add(1)

	select {
	case se.operationQueue <- op:
		logger.Ctx(ctx).Debug("finish to submit", zap.String("OpId", op.Identifier()), zap.String("inst", fmt.Sprintf("%p", se)))
		return true
	case <-se.done:
		se.wg.Done()
		logger.Ctx(ctx).Debug("submit failed, executor stopping", zap.String("OpId", op.Identifier()), zap.String("inst", fmt.Sprintf("%p", se)))
		return false
	case <-ctx.Done():
		se.wg.Done()
		logger.Ctx(ctx).Debug("submit failed, context cancelled", zap.String("OpId", op.Identifier()), zap.String("inst", fmt.Sprintf("%p", se)))
		return false
	}
}

// Stop stops the sequential append executor
func (se *SequentialExecutor) Stop(ctx context.Context) {
	logger.Ctx(ctx).Info("try to stop sequential executor", zap.String("inst", fmt.Sprintf("%p", se)))

	se.mu.Lock()
	if se.closed {
		logger.Ctx(ctx).Info("sequential executor already stopped, skip", zap.String("inst", fmt.Sprintf("%p", se)))
		se.mu.Unlock()
		return
	}
	se.closed = true
	close(se.done)
	se.mu.Unlock()

	// Wait for all operations to complete
	se.wg.Wait()
	logger.Ctx(ctx).Info("finish to stop sequential executor", zap.String("inst", fmt.Sprintf("%p", se)))
}
