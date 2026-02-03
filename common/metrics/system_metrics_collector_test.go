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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestCollectSystemMetrics(t *testing.T) {
	// Initialize system metrics so the gauges are non-nil
	initSystemMetrics()
	// Just verify collectSystemMetrics doesn't panic
	collectSystemMetrics("/tmp")
	collectSystemMetrics("")
}

func TestStartSystemMetricsCollector(t *testing.T) {
	initSystemMetrics()
	ctx, cancel := context.WithCancel(context.Background())
	StartSystemMetricsCollector(ctx, "/tmp", 100*time.Millisecond)

	// Let it run for a couple of collection cycles
	time.Sleep(350 * time.Millisecond)
	cancel()

	// Give goroutine time to exit
	time.Sleep(50 * time.Millisecond)
}

func TestRegisterSystemMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	// Should not panic
	assert.NotPanics(t, func() {
		RegisterSystemMetrics(registry)
	})
}
