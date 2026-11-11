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
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/zilliztech/woodpecker/common/hardware"
)

var wpDiskIORegisterOnce sync.Once

// diskIOCollector reports the cumulative I/O counters of the block device backing the WAL
// directory.
//
// The values are read at scrape time and handed over as-is rather than being sampled on a
// ticker and accumulated. The device counter is already monotonic, so reporting it verbatim
// means a missed scrape loses no increments, and a device counter reset reads as the counter
// reset it is instead of arriving as a negative delta that Counter.Add would reject.
//
// Reading /proc/diskstats costs microseconds, which is why node_exporter does the same thing.
type diskIOCollector struct {
	path string

	mu     sync.Mutex
	device string // resolved lazily, re-resolved if the device stops reporting

	readOps    *prometheus.Desc
	writeOps   *prometheus.Desc
	readBytes  *prometheus.Desc
	writeBytes *prometheus.Desc
}

func newDiskIOCollector(path string) *diskIOCollector {
	labels := []string{"node_id", "path", "device"}
	d := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc(
			prometheus.BuildFQName(woodpeckerNamespace, serverRole, name), help, labels, nil)
	}
	return &diskIOCollector{
		path:       path,
		readOps:    d("system_disk_read_ops_total", "Completed reads on the device backing the WAL directory, since boot"),
		writeOps:   d("system_disk_write_ops_total", "Completed writes on the device backing the WAL directory, since boot"),
		readBytes:  d("system_disk_read_bytes_total", "Bytes read from the device backing the WAL directory, since boot"),
		writeBytes: d("system_disk_write_bytes_total", "Bytes written to the device backing the WAL directory, since boot"),
	}
}

func (c *diskIOCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.readOps
	ch <- c.writeOps
	ch <- c.readBytes
	ch <- c.writeBytes
}

// resolve returns the cached device name, resolving it on first use. Resolution walks the
// mount table, which is more expensive than reading the counters, so it is not repeated per
// scrape -- only after a read fails, which is how a remount onto a different volume recovers.
func (c *diskIOCollector) resolve() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.device != "" {
		return c.device, nil
	}
	dev, err := hardware.DeviceForPath(c.path)
	if err != nil {
		return "", err
	}
	c.device = dev
	return dev, nil
}

func (c *diskIOCollector) forget() {
	c.mu.Lock()
	c.device = ""
	c.mu.Unlock()
}

func (c *diskIOCollector) Collect(ch chan<- prometheus.Metric) {
	device, err := c.resolve()
	if err != nil {
		return // no device to attribute to; emitting nothing beats emitting the whole host
	}
	counters, err := hardware.GetDeviceIOCounters(device)
	if err != nil {
		c.forget()
		return
	}
	emit := func(desc *prometheus.Desc, v uint64) {
		ch <- prometheus.MustNewConstMetric(
			desc, prometheus.CounterValue, float64(v), NodeID, c.path, counters.Device)
	}
	emit(c.readOps, counters.ReadOps)
	emit(c.writeOps, counters.WriteOps)
	emit(c.readBytes, counters.ReadBytes)
	emit(c.writeBytes, counters.WriteBytes)
}

// RegisterDiskIOMetrics registers the WAL device's I/O counters. A node with no local WAL
// directory has no device to report and registers nothing.
func RegisterDiskIOMetrics(registerer prometheus.Registerer, dataPath string) {
	if dataPath == "" {
		return
	}
	wpDiskIORegisterOnce.Do(func() {
		registerer.MustRegister(newDiskIOCollector(dataPath))
	})
}
