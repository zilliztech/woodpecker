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
	wg             sync.WaitGroup
	mu             sync.RWMutex
	closed         bool
	started        bool
}

// NewSequentialExecutor initializes a SequentialExecutor
func NewSequentialExecutor(bufferSize int) *SequentialExecutor {
	return &SequentialExecutor{
		operationQueue: make(chan Operation, bufferSize),
		closed:         false,
		started:        false,
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
	for op := range se.operationQueue {
		// Use defer to ensure Done() is called even if Execute() panics
		func() {
			defer se.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					// Use context.Background() since we don't have access to the original context here
					logger.Ctx(context.Background()).Warn("Operation execution panicked",
						zap.String("OpId", op.Identifier()),
						zap.Any("panic", r))
				}
			}()
			op.Execute()
		}()
	}
}

// Submit an op to the queue. Returns false if the executor is closed or the queue is full.
func (se *SequentialExecutor) Submit(ctx context.Context, op Operation) bool {
	logger.Ctx(ctx).Debug("try to submit", zap.String("OpId", op.Identifier()), zap.String("inst", fmt.Sprintf("%p", se)))

	// Use RLock to allow concurrent submits while preventing Stop() from closing the channel.
	// Stop() acquires a write Lock before closing the channel, so holding RLock here guarantees
	// the channel remains open through the non-blocking send attempt, preventing panic
	// from sending on a closed channel.
	se.mu.RLock()
	if se.closed {
		logger.Ctx(ctx).Debug("submit failed, executor already closed", zap.String("OpId", op.Identifier()), zap.String("inst", fmt.Sprintf("%p", se)))
		se.mu.RUnlock()
		return false
	}

	// wg.Add must happen before the channel send, because once the op is in the channel
	// the worker may immediately pick it up and call wg.Done(). If wg.Add hadn't been
	// called yet, wg would go negative and panic.
	se.wg.Add(1)

	// Non-blocking send: return false immediately if queue is full to avoid
	// blocking the caller (who may hold other locks such as segmentHandle lock).
	select {
	case se.operationQueue <- op:
		// success
	default:
		// Undo wg.Add since the op was not enqueued
		se.wg.Done()
		logger.Ctx(ctx).Warn("submit failed, operation queue is full", zap.String("OpId", op.Identifier()), zap.String("inst", fmt.Sprintf("%p", se)))
		se.mu.RUnlock()
		return false
	}
	se.mu.RUnlock()

	logger.Ctx(ctx).Debug("finish to submit", zap.String("OpId", op.Identifier()), zap.String("inst", fmt.Sprintf("%p", se)))
	return true
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
	close(se.operationQueue)
	se.mu.Unlock()

	// Wait for all operations to complete
	se.wg.Wait()
	logger.Ctx(ctx).Info("finish to stop sequential executor", zap.String("inst", fmt.Sprintf("%p", se)))
}
