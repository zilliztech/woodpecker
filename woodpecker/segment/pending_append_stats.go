package segment

import "time"

// PendingAppendStats reads the submit queue without disturbing it.
//
// lastPushed is the highest entry id submitted; lastAddConfirmed is the highest the quorum has
// confirmed durable. The distance between them, together with how long the oldest queued append
// has waited, is what tells an idle writer from a wedged one: idle has nothing pending and the
// two positions equal, wedged has lastPushed ahead of a lastAddConfirmed that has stopped moving
// and an oldest age that keeps growing.
//
// An empty queue reports no age rather than a zero one: "nothing is waiting" is not "something
// has waited 0s".
func (s *segmentHandleImpl) PendingAppendStats() (pending int, oldestPendingAge time.Duration, lastPushed, lastAddConfirmed int64) {
	s.RLock()
	defer s.RUnlock()

	lastPushed = s.lastPushed.Load()
	lastAddConfirmed = s.lastAddConfirmed.Load()
	if s.appendOpsQueue == nil {
		return 0, 0, lastPushed, lastAddConfirmed
	}
	pending = s.appendOpsQueue.Len()
	// The queue is append-ordered, so the front is the oldest.
	if front := s.appendOpsQueue.Front(); front != nil {
		if op, ok := front.Value.(*AppendOp); ok && !op.queuedAt.IsZero() {
			oldestPendingAge = time.Since(op.queuedAt)
		}
	}
	return pending, oldestPendingAge, lastPushed, lastAddConfirmed
}
