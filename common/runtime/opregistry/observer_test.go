package opregistry

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/metrics"
)

func TestRegistry_AsOpObserver(t *testing.T) {
	metrics.ResetObservers()
	defer metrics.ResetObservers()

	reg := NewWithClock(100, 30*time.Second, clockwork.NewFakeClock())
	metrics.RegisterOpObserver(reg)

	op := metrics.StartOp("file.flush", nil, nil, metrics.WithLogSegment(42, 7))

	// Should be registered in the pool.
	ops := reg.List(Filter{})
	require.Len(t, ops, 1)
	assert.Equal(t, OpType("file.flush"), ops[0].OpType)
	assert.Equal(t, int64(42), ops[0].LogID)
	assert.Equal(t, int64(7), ops[0].SegmentID)

	// End should deregister.
	op.End("success")
	require.Len(t, reg.List(Filter{}), 0)
}

func TestRegistry_OpObserver_MultipleOps(t *testing.T) {
	metrics.ResetObservers()
	defer metrics.ResetObservers()

	reg := NewWithClock(100, 30*time.Second, clockwork.NewFakeClock())
	metrics.RegisterOpObserver(reg)

	op1 := metrics.StartOp("file.flush", nil, nil)
	op2 := metrics.StartOp("file.compact", nil, nil)
	op3 := metrics.StartOp("logstore.add_entry", nil, nil)

	require.Len(t, reg.List(Filter{}), 3)

	op2.End("success")
	require.Len(t, reg.List(Filter{}), 2)

	op1.End("error")
	op3.End("success")
	require.Len(t, reg.List(Filter{}), 0)
}

func TestGenerateOpID(t *testing.T) {
	id1 := generateOpID("file.flush")
	id2 := generateOpID("file.flush")
	assert.NotEqual(t, id1, id2, "op IDs should be unique")
	assert.Contains(t, id1, "file.flush-")
}
