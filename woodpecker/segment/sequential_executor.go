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
	"sync"
)

// SequentialExecutor is a sequential append executor
type SequentialExecutor struct {
	operationQueue chan Operation
	wg             sync.WaitGroup
	mu             sync.RWMutex
	closed         bool
}

// NewSequentialExecutor initializes a SequentialExecutor
func NewSequentialExecutor(bufferSize int) *SequentialExecutor {
	return &SequentialExecutor{
		operationQueue: make(chan Operation, bufferSize),
		closed:         false,
	}
}

// Start starts the sequential append executor
func (se *SequentialExecutor) Start() {
	go se.worker()
}

// worker executes the logic for each order
func (se *SequentialExecutor) worker() {
	for op := range se.operationQueue {
		op.Execute()
		se.wg.Done()
	}
}

// Submit an op to the queue
func (se *SequentialExecutor) Submit(op Operation) bool {
	se.mu.RLock()
	defer se.mu.RUnlock()

	if se.closed {
		return false
	}

	se.wg.Add(1)

	// Block and wait
	se.operationQueue <- op
	return true
}

// Stop stops the sequential append executor
func (se *SequentialExecutor) Stop() {
	se.mu.Lock()
	if se.closed {
		se.mu.Unlock()
		return
	}
	se.closed = true
	close(se.operationQueue)
	se.mu.Unlock()
	se.wg.Wait()
}
