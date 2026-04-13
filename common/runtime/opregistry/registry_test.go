package opregistry

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegistry_RegisterDeregister(t *testing.T) {
	reg := NewWithClock(100, 30*time.Second, clockwork.NewFakeClock())

	h1 := reg.Register(OpRecord{OpID: "op-1", OpType: OpTypeFileFlush})
	h2 := reg.Register(OpRecord{OpID: "op-2", OpType: OpTypeFileCompact})
	h3 := reg.Register(OpRecord{OpID: "op-3", OpType: OpTypeFileSync})

	require.Len(t, reg.List(Filter{}), 3)

	reg.Deregister(h2)
	ops := reg.List(Filter{})
	require.Len(t, ops, 2)

	// Verify op-2 is gone.
	require.Nil(t, reg.Get("op-2"))
	require.NotNil(t, reg.Get("op-1"))
	require.NotNil(t, reg.Get("op-3"))

	reg.Deregister(h1)
	reg.Deregister(h3)
	require.Len(t, reg.List(Filter{}), 0)
}

func TestRegistry_DeregisterReturnsElapsed(t *testing.T) {
	fc := clockwork.NewFakeClock()
	reg := NewWithClock(100, 30*time.Second, fc)

	h := reg.Register(OpRecord{OpID: "op-1", StartedAt: fc.Now()})
	fc.Advance(5 * time.Second)
	elapsed := reg.Deregister(h)
	assert.Equal(t, 5*time.Second, elapsed)
}

func TestRegistry_DeregisterUnknownHandle(t *testing.T) {
	reg := NewWithClock(100, 30*time.Second, clockwork.NewFakeClock())
	elapsed := reg.Deregister(99999)
	assert.Equal(t, time.Duration(0), elapsed)
}

func TestRegistry_CapacityEviction(t *testing.T) {
	fc := clockwork.NewFakeClock()
	reg := NewWithClock(3, 30*time.Second, fc)

	reg.Register(OpRecord{OpID: "op-1", StartedAt: fc.Now()})
	fc.Advance(1 * time.Second)
	reg.Register(OpRecord{OpID: "op-2", StartedAt: fc.Now()})
	fc.Advance(1 * time.Second)
	reg.Register(OpRecord{OpID: "op-3", StartedAt: fc.Now()})
	fc.Advance(1 * time.Second)

	// Pool is full. Adding op-4 should evict op-1 (oldest).
	reg.Register(OpRecord{OpID: "op-4", StartedAt: fc.Now()})

	require.Len(t, reg.List(Filter{}), 3)
	require.Nil(t, reg.Get("op-1"), "op-1 should have been evicted")
	require.NotNil(t, reg.Get("op-2"))
	require.NotNil(t, reg.Get("op-3"))
	require.NotNil(t, reg.Get("op-4"))
}

func TestRegistry_EvictionYoungSignal(t *testing.T) {
	fc := clockwork.NewFakeClock()
	reg := NewWithClock(2, 30*time.Second, fc)

	reg.Register(OpRecord{OpID: "op-1", StartedAt: fc.Now()})
	fc.Advance(5 * time.Second) // age=5s < warnAge=30s → young
	reg.Register(OpRecord{OpID: "op-2", StartedAt: fc.Now()})
	fc.Advance(1 * time.Second)

	// Force eviction of op-1.
	reg.Register(OpRecord{OpID: "op-3", StartedAt: fc.Now()})

	stats := reg.Stats()
	assert.Equal(t, int64(1), stats.EvictedYoung)
	assert.Equal(t, int64(0), stats.EvictedOld)
}

func TestRegistry_EvictionOldSignal(t *testing.T) {
	fc := clockwork.NewFakeClock()
	reg := NewWithClock(2, 30*time.Second, fc)

	reg.Register(OpRecord{OpID: "op-1", StartedAt: fc.Now()})
	fc.Advance(60 * time.Second) // age=60s >= warnAge=30s → old (STALL!)
	reg.Register(OpRecord{OpID: "op-2", StartedAt: fc.Now()})
	fc.Advance(1 * time.Second)

	// Force eviction of op-1.
	reg.Register(OpRecord{OpID: "op-3", StartedAt: fc.Now()})

	stats := reg.Stats()
	assert.Equal(t, int64(0), stats.EvictedYoung)
	assert.Equal(t, int64(1), stats.EvictedOld)
}

func TestRegistry_EvictionCallback(t *testing.T) {
	fc := clockwork.NewFakeClock()
	reg := NewWithClock(1, 10*time.Second, fc)

	var callbackAge time.Duration
	var callbackOld bool
	reg.SetOnEvict(func(age time.Duration, isOld bool) {
		callbackAge = age
		callbackOld = isOld
	})

	reg.Register(OpRecord{OpID: "op-1", StartedAt: fc.Now()})
	fc.Advance(15 * time.Second)
	reg.Register(OpRecord{OpID: "op-2", StartedAt: fc.Now()})

	assert.Equal(t, 15*time.Second, callbackAge)
	assert.True(t, callbackOld)
}

func TestRegistry_Get(t *testing.T) {
	reg := NewWithClock(100, 30*time.Second, clockwork.NewFakeClock())

	reg.Register(OpRecord{OpID: "op-42", OpType: OpTypeFileFlush, LogID: 42})

	rec := reg.Get("op-42")
	require.NotNil(t, rec)
	assert.Equal(t, OpTypeFileFlush, rec.OpType)
	assert.Equal(t, int64(42), rec.LogID)

	require.Nil(t, reg.Get("nonexistent"))
}

func TestRegistry_ListFilter(t *testing.T) {
	fc := clockwork.NewFakeClock()
	reg := NewWithClock(100, 30*time.Second, fc)

	reg.Register(OpRecord{OpID: "flush-1", OpType: OpTypeFileFlush, LogID: 42, StartedAt: fc.Now()})
	fc.Advance(2 * time.Second)
	reg.Register(OpRecord{OpID: "compact-1", OpType: OpTypeFileCompact, LogID: 42, StartedAt: fc.Now()})
	fc.Advance(2 * time.Second)
	reg.Register(OpRecord{OpID: "flush-2", OpType: OpTypeFileFlush, LogID: 99, StartedAt: fc.Now()})
	fc.Advance(1 * time.Second)

	// Filter by type.
	ops := reg.List(Filter{Types: []OpType{OpTypeFileFlush}})
	require.Len(t, ops, 2)

	// Filter by logID.
	ops = reg.List(Filter{LogID: ptr(int64(42))})
	require.Len(t, ops, 2)

	// Filter by type + logID.
	ops = reg.List(Filter{Types: []OpType{OpTypeFileFlush}, LogID: ptr(int64(42))})
	require.Len(t, ops, 1)
	assert.Equal(t, "flush-1", ops[0].OpID)

	// Filter by longerThan.
	ops = reg.List(Filter{LongerThan: 3 * time.Second})
	require.Len(t, ops, 2) // flush-1 (5s), compact-1 (3s)
}

func TestRegistry_ListSortedByElapsedDesc(t *testing.T) {
	fc := clockwork.NewFakeClock()
	reg := NewWithClock(100, 30*time.Second, fc)

	reg.Register(OpRecord{OpID: "newest", StartedAt: fc.Now()})
	fc.Advance(1 * time.Second)
	reg.Register(OpRecord{OpID: "middle", StartedAt: fc.Now()})
	fc.Advance(1 * time.Second)

	// Register oldest last to verify sort works (not just insertion order).
	t0 := fc.Now().Add(-10 * time.Second)
	reg.Register(OpRecord{OpID: "oldest", StartedAt: t0})

	ops := reg.List(Filter{})
	require.Len(t, ops, 3)
	assert.Equal(t, "oldest", ops[0].OpID)  // longest elapsed
	assert.Equal(t, "newest", ops[1].OpID)  // 2s
	assert.Equal(t, "middle", ops[2].OpID)  // 1s (swap-remove doesn't preserve order)
}

func TestRegistry_ListLimit(t *testing.T) {
	fc := clockwork.NewFakeClock()
	reg := NewWithClock(100, 30*time.Second, fc)

	for i := 0; i < 10; i++ {
		reg.Register(OpRecord{OpID: fmt.Sprintf("op-%d", i), StartedAt: fc.Now()})
		fc.Advance(100 * time.Millisecond)
	}

	ops := reg.List(Filter{Limit: 3})
	require.Len(t, ops, 3)
}

func TestRegistry_Stats(t *testing.T) {
	fc := clockwork.NewFakeClock()
	reg := NewWithClock(5, 10*time.Second, fc)

	reg.Register(OpRecord{OpID: "op-1", StartedAt: fc.Now()})
	reg.Register(OpRecord{OpID: "op-2", StartedAt: fc.Now()})

	stats := reg.Stats()
	assert.Equal(t, 5, stats.Capacity)
	assert.Equal(t, 2, stats.InUse)
	assert.Equal(t, int64(10000), stats.WarnAgeMS)
	assert.Equal(t, int64(0), stats.EvictedTotal)
}

func TestRegistry_DefaultCapacity(t *testing.T) {
	reg := NewWithClock(0, 30*time.Second, clockwork.NewFakeClock())
	assert.Equal(t, 1024, reg.Stats().Capacity)

	reg = NewWithClock(-1, 30*time.Second, clockwork.NewFakeClock())
	assert.Equal(t, 1024, reg.Stats().Capacity)
}

func TestRegistry_ConcurrentAccess(t *testing.T) {
	reg := New(50, 30*time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			h := reg.Register(OpRecord{
				OpID:      fmt.Sprintf("op-%d", id),
				OpType:    OpTypeFileFlush,
				StartedAt: time.Now(),
			})
			_ = reg.List(Filter{})
			_ = reg.Get(fmt.Sprintf("op-%d", id))
			_ = reg.Stats()
			reg.Deregister(h)
		}(i)
	}
	wg.Wait()

	// All ops deregistered — pool should be empty (some may have been evicted).
	stats := reg.Stats()
	assert.Equal(t, 0, stats.InUse)
}
