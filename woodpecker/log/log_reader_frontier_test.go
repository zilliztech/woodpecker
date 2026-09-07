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
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/metrics"
)

// readFrontierSeriesCount counts the live read-frontier children. Other tests in
// this package deliver entries and leave children behind, so these tests assert
// deltas rather than absolutes.
func readFrontierSeriesCount() int {
	return testutil.CollectAndCount(metrics.WpClientReadFrontierSegment)
}

// TestLogReader_PublishesItsOpeningPosition covers the reader that never
// delivers a first entry: waiting out ErrSegmentNotFound, or opened at the tail
// of an idle log. Without a series at open, the lag panel drops the row and
// "stuck where it started" is indistinguishable from "no reader running".
func TestLogReader_PublishesItsOpeningPosition(t *testing.T) {
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logHandle := &testLogHandleMock{}
	logHandle.Test(t)
	logHandle.On("GetName").Return("frontier-open-log").Maybe()
	logHandle.On("GetId").Return(int64(77)).Maybe()

	readerName := fmt.Sprintf("opening-position-reader-%d", time.Now().UnixNano())
	from := &LogMessageId{SegmentId: 4, EntryId: 12}

	before := readFrontierSeriesCount()
	reader, err := NewLogBatchReader(context.Background(), logHandle, nil, from, readerName,
		&fakeReaderTempSession{logId: 77, readerName: readerName}, cfg)
	require.NoError(t, err)
	require.NotNil(t, reader)

	assert.Equal(t, before+1, readFrontierSeriesCount(),
		"a reader must have a position as soon as it opens, before it delivers anything")

	logNs := metrics.BuildLogNs(cfg.Minio.BucketName, cfg.Minio.RootPath)
	assert.Equal(t, float64(4),
		testutil.ToFloat64(metrics.WpClientReadFrontierSegment.WithLabelValues(logNs, "77", readerName)))
	assert.Equal(t, float64(12),
		testutil.ToFloat64(metrics.WpClientReadFrontierEntry.WithLabelValues(logNs, "77", readerName)))

	reader.(*logBatchReaderImpl).retireReadFrontierMetric()
	assert.Equal(t, before, readFrontierSeriesCount())
}

// TestLogReader_RetiredFrontierIsNotRepublished covers an application closing a
// reader from one goroutine while another is still inside ReadNext. The two have
// no mutual exclusion of their own, so without the reader's own ordering a
// delivery landing after the clear re-creates the series and its guard entry -
// and nothing removes them again, because there is no second Close. reader_name
// is unique per open, so each occurrence would leave one more frozen series.
func TestLogReader_RetiredFrontierIsNotRepublished(t *testing.T) {
	readerName := fmt.Sprintf("retired-reader-%d", time.Now().UnixNano())
	reader := &logBatchReaderImpl{
		logNs:      "bucket/root",
		logIdStr:   "88",
		readerName: readerName,
	}

	before := readFrontierSeriesCount()
	reader.publishReadFrontierMetric(2, 5)
	require.Equal(t, before+1, readFrontierSeriesCount())

	reader.retireReadFrontierMetric()
	require.Equal(t, before, readFrontierSeriesCount(), "Close must drop the reader's series")

	// A delivery that was already in flight when Close ran.
	reader.publishReadFrontierMetric(3, 9)
	assert.Equal(t, before, readFrontierSeriesCount(),
		"a delivery racing Close must not resurrect a series nobody will clean up")

	// Retiring twice is harmless.
	reader.retireReadFrontierMetric()
	assert.Equal(t, before, readFrontierSeriesCount())
}

// TestLogReader_OpeningAtLatestDoesNotPoisonTheFrontier covers a tail reader.
//
// LatestLogMessageID is math.MaxInt64 in both halves - a request to start at the
// tail, not a position. Seeding the monotonic guard with it makes every real
// position afterwards look like it goes backwards, so the guard rejects them all
// and the gauge stays pinned at ~9.2e18 for the reader's whole life, with the lag
// panel reading about -9.2e18. That is the opposite of what publishing the
// opening position is for.
func TestLogReader_OpeningAtLatestDoesNotPoisonTheFrontier(t *testing.T) {
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logHandle := &testLogHandleMock{}
	logHandle.Test(t)
	logHandle.On("GetName").Return("frontier-tail-log").Maybe()
	logHandle.On("GetId").Return(int64(78)).Maybe()

	readerName := fmt.Sprintf("tail-reader-%d", time.Now().UnixNano())
	from := LatestLogMessageID()

	before := readFrontierSeriesCount()
	reader, err := NewLogBatchReader(context.Background(), logHandle, nil, &from, readerName,
		&fakeReaderTempSession{logId: 78, readerName: readerName}, cfg)
	require.NoError(t, err)
	tailReader := reader.(*logBatchReaderImpl)
	t.Cleanup(tailReader.retireReadFrontierMetric)

	assert.Equal(t, before, readFrontierSeriesCount(),
		"the Latest sentinel is not a position and must not create a series")

	// What handleTailRead publishes once it has resolved the sentinel against the
	// real tail. Before the fix the guard already held MaxInt64 and dropped this.
	tailReader.publishReadFrontierMetric(9, 41)

	logNs := metrics.BuildLogNs(cfg.Minio.BucketName, cfg.Minio.RootPath)
	require.Equal(t, before+1, readFrontierSeriesCount(), "the resolved tail position must be published")
	assert.Equal(t, float64(9),
		testutil.ToFloat64(metrics.WpClientReadFrontierSegment.WithLabelValues(logNs, "78", readerName)))
	assert.Equal(t, float64(41),
		testutil.ToFloat64(metrics.WpClientReadFrontierEntry.WithLabelValues(logNs, "78", readerName)))

	// And delivery keeps advancing it, rather than being rejected as a step back.
	tailReader.publishReadFrontierMetric(9, 42)
	assert.Equal(t, float64(42),
		testutil.ToFloat64(metrics.WpClientReadFrontierEntry.WithLabelValues(logNs, "78", readerName)))
}
