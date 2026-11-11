package opregistry

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/zilliztech/woodpecker/common/metrics"
)

// Compile-time assertion that Registry implements OpObserver.
var _ metrics.OpObserver = (*Registry)(nil)

var opCounter atomic.Uint64

// OnOpStart registers the op in the pool and returns the handle.
func (r *Registry) OnOpStart(op *metrics.Op) uint64 {
	rec := OpRecord{
		OpID:      generateOpID(op.OpType),
		OpType:    OpType(op.OpType),
		TraceID:   op.TraceID,
		SpanID:    op.SpanID,
		StartedAt: op.StartedAt(),
		LogID:     op.LogID,
		SegmentID: op.SegmentID,
	}
	return r.Register(rec)
}

// OnOpEnd deregisters the op from the pool.
func (r *Registry) OnOpEnd(op *metrics.Op, handle uint64, elapsed time.Duration, status string) {
	r.Deregister(handle)
}

func generateOpID(opType string) string {
	seq := opCounter.Add(1)
	return fmt.Sprintf("%s-%06d", opType, seq)
}
