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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

func TestBuildMetricsNamespace(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		assert.Equal(t, "mybucket/myroot", BuildMetricsNamespace("mybucket", "myroot"))
	})
	t.Run("empty", func(t *testing.T) {
		assert.Equal(t, "/", BuildMetricsNamespace("", ""))
	})
	t.Run("single_empty", func(t *testing.T) {
		assert.Equal(t, "bucket/", BuildMetricsNamespace("bucket", ""))
		assert.Equal(t, "/root", BuildMetricsNamespace("", "root"))
	})
}

func TestNodeID_Default(t *testing.T) {
	// NodeID should default to "embed" when NODE_NAME env is not set
	// or be set to the env value. Just verify it's non-empty.
	assert.NotEmpty(t, NodeID)
}

func TestRegisterClientMetricsWithRegisterer(t *testing.T) {
	// Reset the sync.Once so we can test registration
	WpClientRegisterOnce = sync.Once{}

	registry := prometheus.NewRegistry()
	assert.NotPanics(t, func() {
		RegisterClientMetricsWithRegisterer(registry)
	})

	// Trigger an observation so metrics show up in Gather()
	WpClientAppendRequestsTotal.WithLabelValues("test-ns", "1").Inc()

	// Verify metrics are registered by gathering them
	metricFamilies, err := registry.Gather()
	assert.NoError(t, err)

	// Build set of metric names
	names := make(map[string]bool)
	for _, mf := range metricFamilies {
		names[mf.GetName()] = true
	}

	// Verify at least the observed metric appears
	assert.True(t, names["woodpecker_client_append_requests_total"],
		"expected metric woodpecker_client_append_requests_total to be registered, got: %v", names)
}

func TestRegisterServerMetricsWithRegisterer(t *testing.T) {
	// Reset the sync.Once so we can test registration
	WpServerRegisterOnce = sync.Once{}

	registry := prometheus.NewRegistry()
	assert.NotPanics(t, func() {
		RegisterServerMetricsWithRegisterer(registry)
	})

	// Verify metrics are registered by gathering them
	_, err := registry.Gather()
	assert.NoError(t, err)
}

func TestRegisterClientMetrics_OnlyOnce(t *testing.T) {
	// Reset for clean test
	WpClientRegisterOnce = sync.Once{}

	registry := prometheus.NewRegistry()
	RegisterClientMetricsWithRegisterer(registry)

	// Second call with different registry should be a no-op due to sync.Once
	registry2 := prometheus.NewRegistry()
	assert.NotPanics(t, func() {
		RegisterClientMetricsWithRegisterer(registry2)
	})
}

func TestRegisterServerMetrics_OnlyOnce(t *testing.T) {
	// Reset for clean test
	WpServerRegisterOnce = sync.Once{}

	registry := prometheus.NewRegistry()
	RegisterServerMetricsWithRegisterer(registry)

	// Second call should be a no-op
	registry2 := prometheus.NewRegistry()
	assert.NotPanics(t, func() {
		RegisterServerMetricsWithRegisterer(registry2)
	})
}

func TestUpdateSegmentState(t *testing.T) {
	// Initialize gauge values first
	namespace := "test-ns"
	logId := "1"
	oldState := "Active"
	newState := "Completed"

	// Set initial state
	WpClientSegmentState.WithLabelValues(namespace, logId, oldState).Set(5)
	WpClientSegmentState.WithLabelValues(namespace, logId, newState).Set(0)

	// Call UpdateSegmentState
	UpdateSegmentState(namespace, logId, oldState, newState)

	// Read old state gauge - should be 4 (5 - 1)
	oldMetric := &dto.Metric{}
	WpClientSegmentState.WithLabelValues(namespace, logId, oldState).Write(oldMetric)
	assert.Equal(t, float64(4), oldMetric.GetGauge().GetValue())

	// Read new state gauge - should be 1 (0 + 1)
	newMetric := &dto.Metric{}
	WpClientSegmentState.WithLabelValues(namespace, logId, newState).Write(newMetric)
	assert.Equal(t, float64(1), newMetric.GetGauge().GetValue())
}

func TestUpdateSegmentState_MultipleTransitions(t *testing.T) {
	namespace := "test-multi"
	logId := "2"

	WpClientSegmentState.WithLabelValues(namespace, logId, "Active").Set(10)
	WpClientSegmentState.WithLabelValues(namespace, logId, "Completed").Set(0)
	WpClientSegmentState.WithLabelValues(namespace, logId, "Sealed").Set(0)

	// Active -> Completed
	UpdateSegmentState(namespace, logId, "Active", "Completed")
	// Active -> Completed again
	UpdateSegmentState(namespace, logId, "Active", "Completed")
	// Completed -> Sealed
	UpdateSegmentState(namespace, logId, "Completed", "Sealed")

	activeMetric := &dto.Metric{}
	WpClientSegmentState.WithLabelValues(namespace, logId, "Active").Write(activeMetric)
	assert.Equal(t, float64(8), activeMetric.GetGauge().GetValue())

	completedMetric := &dto.Metric{}
	WpClientSegmentState.WithLabelValues(namespace, logId, "Completed").Write(completedMetric)
	assert.Equal(t, float64(1), completedMetric.GetGauge().GetValue())

	sealedMetric := &dto.Metric{}
	WpClientSegmentState.WithLabelValues(namespace, logId, "Sealed").Write(sealedMetric)
	assert.Equal(t, float64(1), sealedMetric.GetGauge().GetValue())
}
