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

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func readFrontier(t *testing.T, logNs, logId, reader string) (float64, float64) {
	t.Helper()
	return testutil.ToFloat64(WpClientReadFrontierSegment.WithLabelValues(logNs, logId, reader)),
		testutil.ToFloat64(WpClientReadFrontierEntry.WithLabelValues(logNs, logId, reader))
}

func resetReadFrontier() {
	WpClientReadFrontierSegment.Reset()
	WpClientReadFrontierEntry.Reset()
	frontierMu.Lock()
	readFrontiers = make(map[string]progressFrontier)
	frontierMu.Unlock()
}

// A log can have several readers at different positions — a recovery reader and
// a normal one, or two consumers. They must not share a monotonic guard: with a
// single per-log key the reader that is further ahead would silently clamp every
// update from the one behind it, and the lagging reader would look caught up.
func TestSetReadFrontier_ReadersAreIndependent(t *testing.T) {
	resetReadFrontier()
	reg := prometheus.NewRegistry()
	reg.MustRegister(WpClientReadFrontierSegment, WpClientReadFrontierEntry)

	ns, logId := "bucket/root", "42"
	SetReadFrontier(ns, logId, "reader-ahead", 7, 100)
	SetReadFrontier(ns, logId, "reader-behind", 3, 5)

	seg, entry := readFrontier(t, ns, logId, "reader-ahead")
	assert.Equal(t, float64(7), seg)
	assert.Equal(t, float64(100), entry)

	seg, entry = readFrontier(t, ns, logId, "reader-behind")
	assert.Equal(t, float64(3), seg, "the leading reader must not clamp the lagging one")
	assert.Equal(t, float64(5), entry)

	count, err := testutil.GatherAndCount(reg, "woodpecker_client_read_frontier_segment")
	assert.NoError(t, err)
	assert.Equal(t, 2, count, "one series per reader")
}

func TestSetReadFrontier_DoesNotGoBackwards(t *testing.T) {
	resetReadFrontier()
	ns, logId, reader := "bucket/root", "42", "reader-a"

	SetReadFrontier(ns, logId, reader, 7, 100)
	SetReadFrontier(ns, logId, reader, 7, 99) // stale update from a slower path
	seg, entry := readFrontier(t, ns, logId, reader)
	assert.Equal(t, float64(7), seg)
	assert.Equal(t, float64(100), entry)

	SetReadFrontier(ns, logId, reader, 8, 0) // next segment starts entry ids over
	seg, entry = readFrontier(t, ns, logId, reader)
	assert.Equal(t, float64(8), seg)
	assert.Equal(t, float64(0), entry)
}

// Clearing must drop the guard entry as well as the series. Leaving it behind
// would be invisible until a reader name recurred, at which point the new
// reader's genuinely lower position would be clamped to the dead one's and its
// frontier would look stuck.
func TestClearReadFrontier_DropsSeriesAndGuard(t *testing.T) {
	resetReadFrontier()
	reg := prometheus.NewRegistry()
	reg.MustRegister(WpClientReadFrontierSegment, WpClientReadFrontierEntry)

	ns, logId, reader := "bucket/root", "42", "reader-a"
	SetReadFrontier(ns, logId, reader, 7, 100)

	count, err := testutil.GatherAndCount(reg, "woodpecker_client_read_frontier_segment")
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	ClearReadFrontier(ns, logId, reader)

	count, err = testutil.GatherAndCount(reg, "woodpecker_client_read_frontier_segment")
	assert.NoError(t, err)
	assert.Equal(t, 0, count, "series removed on close")
	count, err = testutil.GatherAndCount(reg, "woodpecker_client_read_frontier_entry")
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	// A reader reusing the name starts from wherever it actually is.
	SetReadFrontier(ns, logId, reader, 1, 1)
	seg, entry := readFrontier(t, ns, logId, reader)
	assert.Equal(t, float64(1), seg, "a cleared reader is not clamped by the dead one's position")
	assert.Equal(t, float64(1), entry)
}

// Open and close many readers, as a scanner backoff loop does: the metric must
// not accumulate a series per reader.
func TestReadFrontier_NoSeriesLeakAcrossReaderChurn(t *testing.T) {
	resetReadFrontier()
	reg := prometheus.NewRegistry()
	reg.MustRegister(WpClientReadFrontierSegment)

	ns, logId := "bucket/root", "42"
	for i := 0; i < 50; i++ {
		reader := "reader-recovery-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		SetReadFrontier(ns, logId, reader, int64(i), int64(i*10))
		ClearReadFrontier(ns, logId, reader)
	}

	count, err := testutil.GatherAndCount(reg, "woodpecker_client_read_frontier_segment")
	assert.NoError(t, err)
	assert.Equal(t, 0, count, "no series survives a closed reader")

	frontierMu.Lock()
	guards := len(readFrontiers)
	frontierMu.Unlock()
	assert.Equal(t, 0, guards, "no monotonic guard entry survives either")
}
