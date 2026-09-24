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
	"container/list"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func handleWithQueue(ages ...time.Duration) *segmentHandleImpl {
	q := list.New()
	now := time.Now()
	for i, age := range ages {
		op := &AppendOp{entryId: int64(i)}
		op.queuedAt = now.Add(-age)
		q.PushBack(op)
	}
	s := &segmentHandleImpl{appendOpsQueue: q}
	s.refreshPendingSnapshot()
	return s
}

// TestPendingAppendStats_ReportsDepthAndOldestAge covers the pair of numbers that say whether a
// writer is idle or wedged. Depth alone cannot: a deep queue draining quickly and a shallow one
// that has not moved in minutes are different situations, and only the age of the oldest entry
// separates them.
func TestPendingAppendStats_ReportsDepthAndOldestAge(t *testing.T) {
	s := handleWithQueue(90*time.Second, 30*time.Second, time.Second)
	s.lastPushed.Store(4900)
	s.lastAddConfirmed.Store(4821)

	pending, oldest, lastPushed, lac := s.PendingAppendStats()

	require.Equal(t, 3, pending)
	require.Equal(t, int64(4900), lastPushed)
	require.Equal(t, int64(4821), lac)
	require.InDelta(t, 90.0, oldest.Seconds(), 2.0,
		"the age reported must be the oldest entry's, not the newest or the mean")
}

// TestPendingAppendStats_EmptyQueue keeps a drained queue from reporting a stale age.
func TestPendingAppendStats_EmptyQueue(t *testing.T) {
	s := handleWithQueue()
	s.lastPushed.Store(4821)
	s.lastAddConfirmed.Store(4821)

	pending, oldest, _, _ := s.PendingAppendStats()

	require.Equal(t, 0, pending)
	require.Zero(t, oldest)
}

// TestPendingAppendStats_ReadableWhileTheWriteLockIsHeld is the point of the whole design.
//
// AppendAsync holds this handle's write lock across executor.Submit, Submit blocks on a bounded
// queue, and that queue stops draining while its worker waits on a hung node. A reader that took
// the lock would therefore block exactly during the stall it exists to describe — and, because
// the auditor is the caller, would stall truncated-segment cleanup and compaction with it.
func TestPendingAppendStats_ReadableWhileTheWriteLockIsHeld(t *testing.T) {
	s := handleWithQueue(45 * time.Second)
	s.lastPushed.Store(99)

	s.Lock() // stand in for an AppendAsync blocked in Submit
	defer s.Unlock()

	done := make(chan struct{})
	var pending int
	var oldest time.Duration
	go func() {
		pending, oldest, _, _ = s.PendingAppendStats()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("PendingAppendStats blocked behind the write lock; a stalled writer would stop reporting")
	}
	require.Equal(t, 1, pending)
	require.InDelta(t, 45.0, oldest.Seconds(), 2.0)
}

// TestRefreshPendingSnapshot_TracksTheHeadAsItDrains pins that the published head follows the
// queue rather than sticking to the first op ever enqueued.
func TestRefreshPendingSnapshot_TracksTheHeadAsItDrains(t *testing.T) {
	s := handleWithQueue(90*time.Second, 10*time.Second)

	s.appendOpsQueue.Remove(s.appendOpsQueue.Front())
	s.refreshPendingSnapshot()

	pending, oldest, _, _ := s.PendingAppendStats()
	require.Equal(t, 1, pending)
	require.InDelta(t, 10.0, oldest.Seconds(), 2.0,
		"after the head leaves, the age is the new head's")
}
