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

func TestActiveSegmentNodes_SetAndClear(t *testing.T) {
	WpActiveSegmentNode.Reset()

	reg := prometheus.NewRegistry()
	reg.MustRegister(WpActiveSegmentNode)

	ns, logId, segId := "bucket/root", "42", "7"
	nodes := []string{"10.0.0.1:8000", "10.0.0.2:8000", "10.0.0.3:8000"}

	SetActiveSegmentNodes(ns, logId, segId, nodes)

	count, err := testutil.GatherAndCount(reg, "woodpecker_client_active_segment_node")
	assert.NoError(t, err)
	assert.Equal(t, len(nodes), count, "one series per node after set")
	for _, n := range nodes {
		v := testutil.ToFloat64(WpActiveSegmentNode.WithLabelValues(ns, logId, segId, n))
		assert.Equal(t, float64(1), v)
	}

	ClearActiveSegmentNodes(ns, logId, segId, nodes)
	count, err = testutil.GatherAndCount(reg, "woodpecker_client_active_segment_node")
	assert.NoError(t, err)
	assert.Equal(t, 0, count, "series removed after clear")
}
