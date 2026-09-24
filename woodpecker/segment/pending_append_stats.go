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

import "time"

// refreshPendingSnapshot republishes the queue's depth and the timestamp of its head.
//
// Always called under the handle's lock, from the places that mutate appendOpsQueue. It exists
// so PendingAppendStats can answer without taking that lock at all: AppendAsync holds the write
// lock across executor.Submit, Submit blocks on a bounded queue, and that queue stops draining
// while its worker waits on a hung node -- so a reader that took the lock would block precisely
// during the stall it was added to describe, and would take the auditor's cleanup and compaction
// down with it.
func (s *segmentHandleImpl) refreshPendingSnapshot() {
	if s.appendOpsQueue == nil {
		s.pendingCount.Store(0)
		s.oldestPendingMs.Store(0)
		return
	}
	s.pendingCount.Store(int64(s.appendOpsQueue.Len()))
	var oldestMs int64
	if front := s.appendOpsQueue.Front(); front != nil {
		if op, ok := front.Value.(*AppendOp); ok && !op.queuedAt.IsZero() {
			oldestMs = op.queuedAt.UnixMilli()
		}
	}
	s.oldestPendingMs.Store(oldestMs)
}

// PendingAppendStats reads the submit queue without taking any lock.
//
// lastPushed is the highest entry id submitted; lastAddConfirmed is the highest the quorum has
// confirmed durable. Note that lastPushed never goes back: when an append fails finally its ops
// are drained from the queue but the submitted position stays where it was, so the distance
// between the two positions is not a count of outstanding appends. pending is.
//
// An empty queue reports an age of zero, which is also what a queue whose head was just enqueued
// reports; pending distinguishes them.
func (s *segmentHandleImpl) PendingAppendStats() (pending int, oldestPendingAge time.Duration, lastPushed, lastAddConfirmed int64) {
	pending = int(s.pendingCount.Load())
	if oldestMs := s.oldestPendingMs.Load(); oldestMs > 0 {
		oldestPendingAge = time.Since(time.UnixMilli(oldestMs))
	}
	return pending, oldestPendingAge, s.lastPushed.Load(), s.lastAddConfirmed.Load()
}
