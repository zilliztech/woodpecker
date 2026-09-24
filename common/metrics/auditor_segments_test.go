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
