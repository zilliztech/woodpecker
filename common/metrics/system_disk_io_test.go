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
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiskIOCollector_Describe(t *testing.T) {
	c := newDiskIOCollector("/var/lib/woodpecker")
	ch := make(chan *prometheus.Desc, 8)
	c.Describe(ch)
	close(ch)

	var got []string
	for d := range ch {
		got = append(got, d.String())
	}
	require.Len(t, got, 4)

	for _, want := range []string{
		"woodpecker_server_system_disk_read_ops_total",
		"woodpecker_server_system_disk_write_ops_total",
		"woodpecker_server_system_disk_read_bytes_total",
		"woodpecker_server_system_disk_write_bytes_total",
	} {
		assert.True(t, strings.Contains(strings.Join(got, "\n"), want), "missing %s", want)
	}
}

// The collector reads the host's mount table and diskstats, so what it can emit depends on
// where the test runs. What must hold everywhere: it never panics, and it emits either
// nothing or a complete set of four correctly labelled series -- never a partial one.
func TestDiskIOCollector_CollectIsAllOrNothing(t *testing.T) {
	c := newDiskIOCollector("/")

	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(c))

	families, err := reg.Gather()
	require.NoError(t, err)

	if len(families) == 0 {
		t.Skip("no block device resolvable here; nothing to assert beyond not panicking")
	}
	assert.Len(t, families, 4, "a device that reports at all reports all four counters")
	for _, f := range families {
		require.Len(t, f.GetMetric(), 1)
		var names []string
		for _, l := range f.GetMetric()[0].GetLabel() {
			names = append(names, l.GetName())
		}
		assert.ElementsMatch(t, []string{"node_id", "path", "device"}, names)
		assert.NotNil(t, f.GetMetric()[0].GetCounter(), "must be a counter, not a gauge")
	}
}

func TestDiskIOCollector_UnresolvableDeviceEmitsNothing(t *testing.T) {
	c := &diskIOCollector{path: "/var/lib/woodpecker", device: "definitely-not-a-device"}
	ch := make(chan prometheus.Metric, 8)
	assert.NotPanics(t, func() { c.Collect(ch) })
	close(ch)
	assert.Empty(t, ch, "a device that reports no counters yields no series, not zeros")

	c.mu.Lock()
	dev := c.device
	c.mu.Unlock()
	assert.Empty(t, dev, "a failed read must clear the cache so a remount can recover")
}

func TestRegisterDiskIOMetrics_NoDataPath(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	RegisterDiskIOMetrics(reg, "")
	families, err := reg.Gather()
	require.NoError(t, err)
	assert.Empty(t, families, "a node with no local WAL directory has no device to report")
}
