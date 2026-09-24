package log

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/metrics"
	mocks_segment_handle "github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_segment_handle"
)

// TestPublishPendingAppends_ReportsTheQueue covers the tick that keeps working when the
// acknowledgement path has stopped: the submitted position and the queue behind it are what say
// a writer is wedged rather than idle.
func TestPublishPendingAppends_ReportsTheQueue(t *testing.T) {
	metrics.WpClientPendingAppendOps.Reset()
	metrics.WpClientOldestPendingAppendSeconds.Reset()
	metrics.WpClientSubmittedFrontierEntry.Reset()

	seg := mocks_segment_handle.NewSegmentHandle(t)
	seg.EXPECT().PendingAppendStats().Return(7, 95*time.Second, int64(4900), int64(4821))
	seg.EXPECT().GetId(context.Background()).Return(int64(12))

	publishPendingAppends(context.Background(), "b/r", "42", seg)

	require.Equal(t, 7.0, testutil.ToFloat64(metrics.WpClientPendingAppendOps.WithLabelValues("b/r", "42")))
	require.Equal(t, 95.0, testutil.ToFloat64(metrics.WpClientOldestPendingAppendSeconds.WithLabelValues("b/r", "42")))
	require.Equal(t, 4900.0, testutil.ToFloat64(metrics.WpClientSubmittedFrontierEntry.WithLabelValues("b/r", "42")),
		"the submitted position must be published, not the confirmed one")
}

// TestPublishPendingAppends_NoWritableSegmentPublishesNothing keeps an absent queue from being
// reported as a drained one. A log between segments has nothing to describe, and a zero would
// read as "everything is acknowledged".
func TestPublishPendingAppends_NoWritableSegmentPublishesNothing(t *testing.T) {
	metrics.WpClientPendingAppendOps.Reset()

	publishPendingAppends(context.Background(), "b/r", "43", nil)

	require.Equal(t, 0, testutil.CollectAndCount(metrics.WpClientPendingAppendOps),
		"an absent queue publishes no series at all")
}
