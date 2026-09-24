package segment

import (
	"container/list"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPendingAppendStats_ReportsDepthAndOldestAge covers the pair of numbers that say whether a
// writer is idle or wedged. Depth alone cannot: a deep queue draining quickly and a shallow one
// that has not moved in minutes are different situations, and only the age of the oldest entry
// separates them.
func TestPendingAppendStats_ReportsDepthAndOldestAge(t *testing.T) {
	q := list.New()
	now := time.Now()
	for i, age := range []time.Duration{90 * time.Second, 30 * time.Second, time.Second} {
		op := &AppendOp{entryId: int64(i)}
		op.queuedAt = now.Add(-age)
		q.PushBack(op)
	}

	s := &segmentHandleImpl{appendOpsQueue: q}
	s.lastPushed.Store(4900)
	s.lastAddConfirmed.Store(4821)

	pending, oldest, lastPushed, lac := s.PendingAppendStats()

	require.Equal(t, 3, pending)
	require.Equal(t, int64(4900), lastPushed)
	require.Equal(t, int64(4821), lac)
	require.InDelta(t, 90.0, oldest.Seconds(), 2.0,
		"the age reported must be the oldest entry's, not the newest or the mean")
}

// TestPendingAppendStats_EmptyQueueHasNoAge keeps an idle writer from reporting an age at all.
// Zero would read as "nothing has been waiting", which is true, but a stale non-zero value would
// read as a stall that is not happening.
func TestPendingAppendStats_EmptyQueueHasNoAge(t *testing.T) {
	s := &segmentHandleImpl{appendOpsQueue: list.New()}
	s.lastPushed.Store(4821)
	s.lastAddConfirmed.Store(4821)

	pending, oldest, _, _ := s.PendingAppendStats()

	require.Equal(t, 0, pending)
	require.Zero(t, oldest)
}
