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
	logger.Ctx(ctx).Debug("start sequential executor", zap.String("inst", fmt.Sprintf("%p", se)))
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

// Submit an op to the queue
func (se *SequentialExecutor) Submit(ctx context.Context, op Operation) bool {
	logger.Ctx(ctx).Debug("try to submit", zap.String("OpId", op.Identifier()), zap.String("inst", fmt.Sprintf("%p", se)))

	se.mu.Lock()
	if se.closed {
		logger.Ctx(ctx).Debug("submit failed, executor already closed", zap.String("OpId", op.Identifier()), zap.String("inst", fmt.Sprintf("%p", se)))
		se.mu.Unlock()
		return false
	}

	se.wg.Add(1)

	// Get a reference to the channel while holding the lock
	queue := se.operationQueue
	se.mu.Unlock()

	// Now send to the channel outside the lock to avoid deadlock
	// This is safe because once we've incremented wg, the Stop method will wait for us
	queue <- op
	logger.Ctx(ctx).Debug("finish to submit", zap.String("OpId", op.Identifier()), zap.String("inst", fmt.Sprintf("%p", se)))
	return true
}

// Stop stops the sequential append executor
func (se *SequentialExecutor) Stop(ctx context.Context) {
	logger.Ctx(ctx).Debug("try to stop sequential executor", zap.String("inst", fmt.Sprintf("%p", se)))

	se.mu.Lock()
	if se.closed {
		logger.Ctx(ctx).Debug("sequential executor already stopped, skip", zap.String("inst", fmt.Sprintf("%p", se)))
		se.mu.Unlock()
		return
	}
	se.closed = true
	close(se.operationQueue)
	se.mu.Unlock()

	// Wait for all operations to complete
	se.wg.Wait()
	logger.Ctx(ctx).Debug("finish to stop sequential executor", zap.String("inst", fmt.Sprintf("%p", se)))
}
