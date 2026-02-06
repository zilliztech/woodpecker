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
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/zilliztech/woodpecker/common/hardware"
)

// System metrics are initialized at package level so they are always safe to use.
// node_id is a regular label whose value comes from the NodeID package variable.
var (
	WpSystemRegisterOnce sync.Once

	WpSystemCPUUsage = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "system_cpu_usage",
		Help:      "Current CPU usage percentage",
	}, []string{"node_id"})
	WpSystemCPUNum = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "system_cpu_num",
		Help:      "Number of CPU cores available",
	}, []string{"node_id"})
	WpSystemMemoryTotalBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "system_memory_total_bytes",
		Help:      "Total system memory in bytes",
	}, []string{"node_id"})
	WpSystemMemoryUsedBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "system_memory_used_bytes",
		Help:      "Used system memory in bytes",
	}, []string{"node_id"})
	WpSystemMemoryUsageRatio = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "system_memory_usage_ratio",
		Help:      "Memory usage ratio (used/total)",
	}, []string{"node_id"})
	WpSystemDiskTotalBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "system_disk_total_bytes",
		Help:      "Total disk space in bytes",
	}, []string{"node_id", "path"})
	WpSystemDiskUsedBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "system_disk_used_bytes",
		Help:      "Used disk space in bytes",
	}, []string{"node_id", "path"})
	WpSystemIOWait = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "system_io_wait",
		Help:      "IO wait percentage",
	}, []string{"node_id"})
)

// RegisterSystemMetrics registers all system metrics with the given registerer.
func RegisterSystemMetrics(registerer prometheus.Registerer) {
	WpSystemRegisterOnce.Do(func() {
		registerer.MustRegister(WpSystemCPUUsage)
		registerer.MustRegister(WpSystemCPUNum)
		registerer.MustRegister(WpSystemMemoryTotalBytes)
		registerer.MustRegister(WpSystemMemoryUsedBytes)
		registerer.MustRegister(WpSystemMemoryUsageRatio)
		registerer.MustRegister(WpSystemDiskTotalBytes)
		registerer.MustRegister(WpSystemDiskUsedBytes)
		registerer.MustRegister(WpSystemIOWait)
	})
}

// StartSystemMetricsCollector starts a background goroutine that periodically collects
// system metrics (CPU, memory, disk, IO wait) and updates the corresponding gauges.
func StartSystemMetricsCollector(ctx context.Context, dataPath string, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Collect once immediately
		collectSystemMetrics(dataPath)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				collectSystemMetrics(dataPath)
			}
		}
	}()
}

func collectSystemMetrics(dataPath string) {
	// CPU
	WpSystemCPUUsage.WithLabelValues(NodeID).Set(hardware.GetCPUUsage())
	WpSystemCPUNum.WithLabelValues(NodeID).Set(float64(hardware.GetCPUNum()))

	// Memory
	WpSystemMemoryTotalBytes.WithLabelValues(NodeID).Set(float64(hardware.GetMemoryCount()))
	WpSystemMemoryUsedBytes.WithLabelValues(NodeID).Set(float64(hardware.GetUsedMemoryCount()))
	WpSystemMemoryUsageRatio.WithLabelValues(NodeID).Set(hardware.GetMemoryUseRatio())

	// Disk
	if dataPath != "" {
		usedGB, totalGB, err := hardware.GetDiskUsage(dataPath)
		if err == nil {
			WpSystemDiskTotalBytes.WithLabelValues(NodeID, dataPath).Set(totalGB * 1e9)
			WpSystemDiskUsedBytes.WithLabelValues(NodeID, dataPath).Set(usedGB * 1e9)
		}
	}

	// IO Wait
	ioWait, err := hardware.GetIOWait()
	if err == nil {
		WpSystemIOWait.WithLabelValues(NodeID).Set(ioWait)
	}
}
