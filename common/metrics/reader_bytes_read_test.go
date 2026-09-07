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
	"github.com/stretchr/testify/require"
)

// Readers are short-lived and are recreated constantly — a recovery reader is
// rebuilt on every scanner restart — so the bytes counter must not carry the
// reader's identity. Doing so created one permanent series per reader ever
// opened, and it also lost the bytes of any reader that finished within a
// single scrape interval. Aggregating per log fixes both.
func TestReaderBytesRead_AggregatesAcrossReaders(t *testing.T) {
	WpLogReaderBytesRead.Reset()

	reg := prometheus.NewRegistry()
	reg.MustRegister(WpLogReaderBytesRead)

	ns, logId := "bucket/root", "42"

	// Three readers over the life of one log, as scanner restarts would produce.
	for _, n := range []float64{100, 250, 30} {
		WpLogReaderBytesRead.WithLabelValues(ns, logId).Add(n)
	}

	count, err := testutil.GatherAndCount(reg, "woodpecker_client_reader_bytes_read")
	assert.NoError(t, err)
	assert.Equal(t, 1, count, "one series per log regardless of how many readers ran")
	assert.Equal(t, float64(380), testutil.ToFloat64(WpLogReaderBytesRead.WithLabelValues(ns, logId)))

	// The label set is the actual regression guard: re-adding a per-reader label
	// compiles fine and silently reintroduces the unbounded series growth.
	families, err := reg.Gather()
	require.NoError(t, err)
	var labelNames []string
	for _, f := range families {
		if f.GetName() != "woodpecker_client_reader_bytes_read" {
			continue
		}
		require.NotEmpty(t, f.GetMetric())
		for _, l := range f.GetMetric()[0].GetLabel() {
			labelNames = append(labelNames, l.GetName())
		}
	}
	assert.ElementsMatch(t, []string{"log_ns", "log_id"}, labelNames, "label set must stay per-log")
}
