package opregistry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpType_String(t *testing.T) {
	assert.Equal(t, "logstore.add_entry", string(OpTypeLogstoreAddEntry))
	assert.Equal(t, "file.flush", string(OpTypeFileFlush))
	assert.Equal(t, "file.compact", string(OpTypeFileCompact))
	assert.Equal(t, "grpc", string(OpTypeGRPC))
}

func TestOpRecord_Elapsed(t *testing.T) {
	r := OpRecord{
		StartedAt: time.Now().Add(-5 * time.Second),
	}
	elapsed := r.Elapsed()
	assert.InDelta(t, 5.0, elapsed.Seconds(), 0.5)
}

func TestOpRecord_ElapsedAt(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r := OpRecord{StartedAt: start}
	now := start.Add(10 * time.Second)
	assert.Equal(t, 10*time.Second, r.ElapsedAt(now))
}

func TestFilter_EmptyMatchesAll(t *testing.T) {
	r := OpRecord{
		OpType:    OpTypeFileFlush,
		LogID:     42,
		SegmentID: 7,
		StartedAt: time.Now().Add(-2 * time.Minute),
	}
	require.True(t, Filter{}.Matches(r, time.Now()))
}

func TestFilter_TypeFilter(t *testing.T) {
	r := OpRecord{OpType: OpTypeFileFlush, StartedAt: time.Now()}
	now := time.Now()

	require.True(t, Filter{Types: []OpType{OpTypeFileFlush}}.Matches(r, now))
	require.True(t, Filter{Types: []OpType{OpTypeFileCompact, OpTypeFileFlush}}.Matches(r, now))
	require.False(t, Filter{Types: []OpType{OpTypeFileCompact}}.Matches(r, now))
}

func TestFilter_LogIDFilter(t *testing.T) {
	r := OpRecord{LogID: 42, StartedAt: time.Now()}
	now := time.Now()

	require.True(t, Filter{LogID: ptr(int64(42))}.Matches(r, now))
	require.False(t, Filter{LogID: ptr(int64(99))}.Matches(r, now))
}

func TestFilter_SegmentIDFilter(t *testing.T) {
	r := OpRecord{SegmentID: 7, StartedAt: time.Now()}
	now := time.Now()

	require.True(t, Filter{SegmentID: ptr(int64(7))}.Matches(r, now))
	require.False(t, Filter{SegmentID: ptr(int64(99))}.Matches(r, now))
}

func TestFilter_LongerThan(t *testing.T) {
	r := OpRecord{StartedAt: time.Now().Add(-2 * time.Minute)}
	now := time.Now()

	require.True(t, Filter{LongerThan: 30 * time.Second}.Matches(r, now))
	require.False(t, Filter{LongerThan: 5 * time.Minute}.Matches(r, now))
}

func TestFilter_Combined(t *testing.T) {
	r := OpRecord{
		OpType:    OpTypeFileFlush,
		LogID:     42,
		SegmentID: 7,
		StartedAt: time.Now().Add(-1 * time.Minute),
	}
	now := time.Now()

	// All match
	f := Filter{
		Types:      []OpType{OpTypeFileFlush},
		LogID:      ptr(int64(42)),
		LongerThan: 30 * time.Second,
	}
	require.True(t, f.Matches(r, now))

	// Type mismatch
	f.Types = []OpType{OpTypeFileCompact}
	require.False(t, f.Matches(r, now))
}

func TestStats_Fields(t *testing.T) {
	s := Stats{
		Capacity:     1024,
		InUse:        500,
		WarnAgeMS:    30000,
		EvictedTotal: 100,
		EvictedYoung: 95,
		EvictedOld:   5,
	}
	assert.Equal(t, 1024, s.Capacity)
	assert.Equal(t, 500, s.InUse)
	assert.Equal(t, int64(30000), s.WarnAgeMS)
	assert.Equal(t, int64(100), s.EvictedTotal)
	assert.Equal(t, int64(95), s.EvictedYoung)
	assert.Equal(t, int64(5), s.EvictedOld)
}

func ptr[T any](v T) *T { return &v }
