package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockObserver struct {
	startCalled bool
	endCalled   bool
	endStatus   string
	endElapsed  time.Duration
}

func (m *mockObserver) OnOpStart(op *Op) uint64 {
	m.startCalled = true
	return 42
}

func (m *mockObserver) OnOpEnd(op *Op, handle uint64, elapsed time.Duration, status string) {
	m.endCalled = true
	m.endStatus = status
	m.endElapsed = elapsed
}

func TestOp_EndObservesHistogram(t *testing.T) {
	ResetObservers()
	defer ResetObservers()

	hist := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "test_op_histogram",
		Buckets: prometheus.DefBuckets,
	})

	op := StartOp("test.op", hist, nil)
	time.Sleep(5 * time.Millisecond)
	op.End("success")

	var m dto.Metric
	require.NoError(t, hist.Write(&m))
	require.NotNil(t, m.Histogram)
	assert.Equal(t, uint64(1), *m.Histogram.SampleCount)
	assert.Greater(t, *m.Histogram.SampleSum, float64(0))
}

func TestOp_EndNotifiesObservers(t *testing.T) {
	ResetObservers()
	defer ResetObservers()

	mock := &mockObserver{}
	RegisterOpObserver(mock)

	op := StartOp("test.op", nil, nil)
	require.True(t, mock.startCalled)

	op.End("success")
	require.True(t, mock.endCalled)
	assert.Equal(t, "success", mock.endStatus)
}

func TestOp_EndCalledOnce(t *testing.T) {
	ResetObservers()
	defer ResetObservers()

	callCount := 0
	mock := &mockObserver{}
	mock2 := &countingObserver{count: &callCount}
	RegisterOpObserver(mock)
	RegisterOpObserver(mock2)

	op := StartOp("test.op", nil, nil)
	op.End("success")
	op.End("error") // should be no-op
	assert.Equal(t, 1, callCount)
}

type countingObserver struct {
	count *int
}

func (c *countingObserver) OnOpStart(op *Op) uint64 { return 0 }
func (c *countingObserver) OnOpEnd(op *Op, handle uint64, elapsed time.Duration, status string) {
	*c.count++
}

func TestOp_WithNilHistogram(t *testing.T) {
	ResetObservers()
	defer ResetObservers()

	op := StartOp("test.op", nil, nil)
	// Should not panic.
	op.End("success")
}

func TestOp_StartedAt(t *testing.T) {
	ResetObservers()
	defer ResetObservers()

	before := time.Now()
	op := StartOp("test.op", nil, nil)
	after := time.Now()

	assert.False(t, op.StartedAt().Before(before))
	assert.False(t, op.StartedAt().After(after))
}

func TestWithInstance_SetsBucketAndRoot(t *testing.T) {
	ResetObservers()
	defer ResetObservers()
	op := StartOp("test.op", nil, nil, WithInstance("bucket-a", "root-a"), WithLogSegment(7, 3))
	if op.BucketName != "bucket-a" || op.RootPath != "root-a" {
		t.Fatalf("got bucket=%q root=%q", op.BucketName, op.RootPath)
	}
	if op.LogID != 7 || op.SegmentID != 3 {
		t.Fatalf("WithLogSegment regressed: log=%d seg=%d", op.LogID, op.SegmentID)
	}
}

func TestOp_NoObservers(t *testing.T) {
	ResetObservers()
	defer ResetObservers()

	// With no observers, should work fine.
	op := StartOp("test.op", nil, nil)
	op.End("success")
}
