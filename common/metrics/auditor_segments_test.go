package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestAddAuditorSegments_OnlyCreatesSeriesThatHappened keeps a quiet auditor from publishing a
// zero for every outcome on every tick. A counter that only ever reads zero is indistinguishable
// from one nobody increments, and three of them per log per cycle is series for nothing.
func TestAddAuditorSegments_OnlyCreatesSeriesThatHappened(t *testing.T) {
	WpClientAuditorSegmentsTotal.Reset()

	AddAuditorSegments("b/r", "42", 3, 0, 0)

	require.Equal(t, 1, testutil.CollectAndCount(WpClientAuditorSegmentsTotal),
		"an outcome that did not happen creates no series")
	require.Equal(t, 3.0, testutil.ToFloat64(WpClientAuditorSegmentsTotal.WithLabelValues("b/r", "42", "compacted")))
}

// TestAddAuditorSegments_AccumulatesAcrossCycles pins counter semantics: each cycle adds to the
// running total rather than replacing it, so a rate over failures is meaningful.
func TestAddAuditorSegments_AccumulatesAcrossCycles(t *testing.T) {
	WpClientAuditorSegmentsTotal.Reset()

	AddAuditorSegments("b/r", "42", 2, 1, 4)
	AddAuditorSegments("b/r", "42", 5, 2, 0)

	require.Equal(t, 7.0, testutil.ToFloat64(WpClientAuditorSegmentsTotal.WithLabelValues("b/r", "42", "compacted")))
	require.Equal(t, 3.0, testutil.ToFloat64(WpClientAuditorSegmentsTotal.WithLabelValues("b/r", "42", "failed")))
	require.Equal(t, 4.0, testutil.ToFloat64(WpClientAuditorSegmentsTotal.WithLabelValues("b/r", "42", "deferred")))
}
