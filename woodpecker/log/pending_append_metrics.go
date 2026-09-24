package log

import (
	"context"

	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/woodpecker/segment"
)

// publishPendingAppends reports a writable segment's submit queue on the auditor's tick.
//
// It runs here rather than on the append path for two reasons. The append path is per-entry hot,
// and — the reason that matters — the acknowledgement path stops running exactly when a writer
// wedges, so anything published from there freezes at the moment it starts to matter. The
// auditor keeps ticking regardless, so the submitted position keeps advancing against a
// confirmed position that has stopped, which is the shape a stall has from outside.
//
// A nil handle publishes nothing: a log between segments has no queue to describe, and writing
// zeroes would read as a drained queue rather than an absent one.
func publishPendingAppends(ctx context.Context, logNs, logIdStr string, handle segment.SegmentHandle) {
	if handle == nil {
		return
	}
	pending, oldest, lastPushed, _ := handle.PendingAppendStats()
	metrics.SetPendingAppends(logNs, logIdStr, pending, oldest)
	metrics.SetSubmittedFrontier(logNs, logIdStr, handle.GetId(ctx), lastPushed)
}
