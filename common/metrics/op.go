package metrics

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Op represents an in-flight operation being tracked.
type Op struct {
	OpType    string
	Labels    prometheus.Labels
	LogID     int64
	SegmentID int64
	TraceID   string
	SpanID    string

	histo   prometheus.Observer // may be nil
	start   time.Time
	ended   atomic.Bool
	handles []uint64 // one per observer
}

// OpOption configures an Op at creation time.
type OpOption func(*Op)

// WithLogSegment sets the log and segment IDs for the op.
func WithLogSegment(logID, segmentID int64) OpOption {
	return func(op *Op) {
		op.LogID = logID
		op.SegmentID = segmentID
	}
}

// StartOp begins tracking an operation. The histogram observer is optional (may be nil).
func StartOp(opType string, hist prometheus.Observer, labels prometheus.Labels, opts ...OpOption) *Op {
	op := &Op{
		OpType: opType,
		Labels: labels,
		histo:  hist,
		start:  time.Now(),
	}
	for _, opt := range opts {
		opt(op)
	}
	if len(observers) > 0 {
		op.handles = make([]uint64, len(observers))
		for i, obs := range observers {
			op.handles[i] = obs.OnOpStart(op)
		}
	}
	return op
}

// End completes the operation. Records the histogram observation and notifies observers.
// Safe to call multiple times — only the first call takes effect.
func (o *Op) End(status string) {
	if !o.ended.CompareAndSwap(false, true) {
		return
	}
	elapsed := time.Since(o.start)
	if o.histo != nil {
		o.histo.Observe(float64(elapsed.Milliseconds()))
	}
	for i, obs := range observers {
		obs.OnOpEnd(o, o.handles[i], elapsed, status)
	}
}

// StartedAt returns when the op started.
func (o *Op) StartedAt() time.Time {
	return o.start
}
