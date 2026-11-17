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
	"sync"
)

// SequentialExecutor preserves submission order and opportunistically batches
// appends. It owns no transport, retry, quorum or storage-mode policy.
type SequentialExecutor struct {
	operationQueue  chan *AppendOp
	dispatch        func([]*AppendOp)
	done            chan struct{}
	finished        chan struct{}
	startOnce       sync.Once
	stopOnce        sync.Once
	mu              sync.Mutex
	closed          bool
	maxBatchEntries int
	maxBatchBytes   int64
}

func NewSequentialExecutor(bufferSize int, dispatch func([]*AppendOp)) *SequentialExecutor {
	return NewBatchingSequentialExecutor(bufferSize, 1, 0, dispatch)
}

func NewBatchingSequentialExecutor(bufferSize, maxBatchEntries int, maxBatchBytes int64, dispatch func([]*AppendOp)) *SequentialExecutor {
	return &SequentialExecutor{
		operationQueue: make(chan *AppendOp, max(bufferSize, 1)),
		dispatch:       dispatch, done: make(chan struct{}), finished: make(chan struct{}),
		maxBatchEntries: max(maxBatchEntries, 1), maxBatchBytes: maxBatchBytes,
	}
}

func (se *SequentialExecutor) Start(context.Context) {
	se.startOnce.Do(func() { go se.worker() })
}

// Submit publishes an append whose capacity was reserved by segmentAppender.
// The lock serializes publication with stop. A full queue is rejected rather
// than waited on; all capacity waiting belongs to segmentAppender.reserve.
func (se *SequentialExecutor) Submit(ctx context.Context, op *AppendOp) bool {
	se.mu.Lock()
	defer se.mu.Unlock()
	if se.closed || ctx.Err() != nil {
		return false
	}
	select {
	case se.operationQueue <- op:
		return true
	default:
		return false
	}
}

func (se *SequentialExecutor) worker() {
	defer close(se.finished)
	for {
		select {
		case first := <-se.operationQueue:
			se.executeBatch(first)
		case <-se.done:
			for {
				select {
				case first := <-se.operationQueue:
					se.executeBatch(first)
				default:
					return
				}
			}
		}
	}
}

func (se *SequentialExecutor) executeBatch(first *AppendOp) {
	ops := []*AppendOp{first}
	size := first.Size()
	// Check the byte threshold before taking the next entry. That final entry
	// can take the batch over the threshold; entries are never split or rejected.
	for len(ops) < se.maxBatchEntries && (se.maxBatchBytes <= 0 || size < se.maxBatchBytes) {
		select {
		case next := <-se.operationQueue:
			ops = append(ops, next)
			size += next.Size()
		default:
			se.dispatch(ops)
			return
		}
	}
	se.dispatch(ops)
}

func (se *SequentialExecutor) stop() {
	se.stopOnce.Do(func() {
		se.mu.Lock()
		se.closed = true
		close(se.done)
		se.mu.Unlock()
		se.Start(context.Background()) // drain even if stopped before Start
	})
}

func (se *SequentialExecutor) wait()                { <-se.finished }
func (se *SequentialExecutor) Stop(context.Context) { se.stop(); se.wait() }
