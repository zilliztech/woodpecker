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
	"sort"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/topology"
)

func TestActiveSegmentNodes_SetAndClear(t *testing.T) {
	WpActiveSegmentNode.Reset()

	reg := prometheus.NewRegistry()
	reg.MustRegister(WpActiveSegmentNode)

	ns, logId, segId := "bucket/root", "42", "7"
	nodes := []ActiveSegmentNode{
		{Node: "10.0.0.1:8000", AZ: "us-west-2a"},
		{Node: "10.0.0.2:8000", AZ: "us-west-2b"},
		{Node: "10.0.0.3:8000", AZ: ""}, // placement unknown for this replica
	}

	SetActiveSegmentNodes(ns, logId, segId, nodes)

	count, err := testutil.GatherAndCount(reg, "woodpecker_client_active_segment_node")
	assert.NoError(t, err)
	assert.Equal(t, len(nodes), count, "one series per node after set")
	for _, n := range nodes {
		// An unset AZ is rendered "unknown" at the metric boundary (issue #292),
		// so that is the label the series is filed under.
		v := testutil.ToFloat64(WpActiveSegmentNode.WithLabelValues(ns, logId, segId, n.Node, topology.LabelOrUnknown(n.AZ)))
		assert.Equal(t, float64(1), v)
	}

	ClearActiveSegmentNodes(ns, logId, segId)
	count, err = testutil.GatherAndCount(reg, "woodpecker_client_active_segment_node")
	assert.NoError(t, err)
	assert.Equal(t, 0, count, "series removed after clear")
}

// A quorum whose placement changed since it was published must still be
// removed in full: the clear matches on the segment, not on the exact labels.
func TestActiveSegmentNodes_ClearAfterPlacementChange(t *testing.T) {
	WpActiveSegmentNode.Reset()

	reg := prometheus.NewRegistry()
	reg.MustRegister(WpActiveSegmentNode)

	ns, logId, segId := "bucket/root", "42", "7"
	SetActiveSegmentNodes(ns, logId, segId, []ActiveSegmentNode{{Node: "10.0.0.1:8000", AZ: "us-west-2a"}})
	SetActiveSegmentNodes(ns, logId, segId, []ActiveSegmentNode{{Node: "10.0.0.1:8000", AZ: "us-west-2c"}})

	count, err := testutil.GatherAndCount(reg, "woodpecker_client_active_segment_node")
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

	ClearActiveSegmentNodes(ns, logId, segId)
	count, err = testutil.GatherAndCount(reg, "woodpecker_client_active_segment_node")
	assert.NoError(t, err)
	assert.Equal(t, 0, count, "all series for the segment removed")
}

// Issue #292: an unset AZ reached the metric as az="", which is structurally
// indistinguishable from a recorded placement on a dashboard — a misconfigured
// cluster looked fine and said nothing for 49 days. The blank label must not be
// emitted at all; "unknown" is the same word Scope already uses for it.
func TestActiveSegmentNodes_UnsetAZIsUnknownNeverBlank(t *testing.T) {
	WpActiveSegmentNode.Reset()

	reg := prometheus.NewRegistry()
	reg.MustRegister(WpActiveSegmentNode)

	ns, logId, segId := "bucket/root", "42", "7"
	SetActiveSegmentNodes(ns, logId, segId, []ActiveSegmentNode{
		{Node: "10.0.0.1:8000", AZ: ""},
		{Node: "10.0.0.2:8000", AZ: "us-west-2a"},
	})

	// The gathered label values are asserted rather than the collector's keys,
	// because the exposed label is what a dashboard groups by, and that is what
	// was wrong.
	families, err := reg.Gather()
	assert.NoError(t, err)

	var azValues []string
	for _, family := range families {
		if family.GetName() != "woodpecker_client_active_segment_node" {
			continue
		}
		for _, m := range family.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == "az" {
					azValues = append(azValues, label.GetValue())
				}
			}
		}
	}
	sort.Strings(azValues)

	assert.Equal(t, []string{"unknown", "us-west-2a"}, azValues,
		"an unset AZ is reported as unknown, a real one passes through, and neither is blank")
	assert.NotContains(t, azValues, "", "a blank az label is never emitted")

	// And the series is reachable under "unknown" rather than under "".
	assert.Equal(t, float64(1),
		testutil.ToFloat64(WpActiveSegmentNode.WithLabelValues(ns, logId, segId, "10.0.0.1:8000", "unknown")))

	count, err := testutil.GatherAndCount(reg, "woodpecker_client_active_segment_node")
	assert.NoError(t, err)
	assert.Equal(t, 2, count, "one series per node, and normalising did not split one in two")
}
