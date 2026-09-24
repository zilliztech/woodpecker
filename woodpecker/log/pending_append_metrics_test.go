// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

// TestPublishPendingAppends_NoWritableSegmentDropsTheSeries covers what a stall leaves behind.
// A tick samples a queue mid-stall, then the segment fails, rolls or the writer closes and there
// is no writable segment. Leaving the last values in place keeps an age threshold firing against
// a queue that no longer exists, so the series are dropped rather than held or zeroed.
func TestPublishPendingAppends_NoWritableSegmentDropsTheSeries(t *testing.T) {
	metrics.WpClientPendingAppendOps.Reset()
	metrics.WpClientOldestPendingAppendSeconds.Reset()

	stalled := mocks_segment_handle.NewSegmentHandle(t)
	stalled.EXPECT().PendingAppendStats().Return(7, 95*time.Second, int64(4900), int64(4821))
	stalled.EXPECT().GetId(context.Background()).Return(int64(12))
	publishPendingAppends(context.Background(), "b/r", "43", stalled)
	require.Equal(t, 95.0, testutil.ToFloat64(metrics.WpClientOldestPendingAppendSeconds.WithLabelValues("b/r", "43")))

	publishPendingAppends(context.Background(), "b/r", "43", nil)

	require.Equal(t, 0, testutil.CollectAndCount(metrics.WpClientPendingAppendOps),
		"the queue series must go, not keep the value sampled during the stall")
	require.Equal(t, 0, testutil.CollectAndCount(metrics.WpClientOldestPendingAppendSeconds))
}
