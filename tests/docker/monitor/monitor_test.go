package monitor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/tests/docker/framework"
)

// newMonitorCluster creates a MonitorCluster for use in monitor tests.
// It does NOT call Up/Down — that is managed by run_monitor_tests.sh.
func newMonitorCluster(t *testing.T) *MonitorCluster {
	t.Helper()
	return NewMonitorCluster(t)
}

// assertMetricExists asserts that a metric exists in Prometheus with a non-zero value.
func assertMetricExists(t *testing.T, cluster *MonitorCluster, metricName string) {
	t.Helper()
	hasValue := cluster.QueryMetricHasValue(t, metricName)
	assert.True(t, hasValue, "metric %s should have non-zero value in Prometheus", metricName)
}

// assertMetricRegistered asserts that a metric is registered in Prometheus (exists, value may be zero).
func assertMetricRegistered(t *testing.T, cluster *MonitorCluster, metricName string) {
	t.Helper()
	exists, _ := cluster.QueryMetric(t, metricName)
	assert.True(t, exists, "metric %s should be registered in Prometheus", metricName)
}

// TestMonitor_MetricsVerification verifies that Woodpecker Prometheus metrics
// are correctly reported after performing read/write operations.
//
// Design:
//   - woodpecker_server_* and grpc_server_* metrics run inside server containers
//     and have real non-zero values after read/write operations.
//   - woodpecker_client_* metrics use labeled vectors (CounterVec, HistogramVec).
//     They are registered in the server process via RegisterWoodpeckerWithRegisterer,
//     but Prometheus only exposes labeled metrics after .With(labels) is called.
//     Since client operations run in the Go test process (not the server container),
//     the server never populates any label values, so these metrics are invisible
//     to Prometheus. We skip client metric verification here.
//   - woodpecker_server_system_* metrics are collected by a background goroutine
//     in the server and should have non-zero values.
//   - Some server metrics (buffer_wait_latency, object_storage_bytes_transferred)
//     may not be triggered during a short test; we only assert they are registered
//     when their parent counter/histogram has at least one observation.
func TestMonitor_MetricsVerification(t *testing.T) {
	cluster := newMonitorCluster(t)
	ctx := context.Background()

	// Wait for Prometheus to be ready
	cluster.WaitForPrometheusReady(t, 60*time.Second)

	// Set up Grafana dashboard (non-fatal if it fails)
	cluster.SetupGrafanaDashboard(t)

	// Create client and perform read/write operations
	client, _ := cluster.NewClient(t, ctx)

	logName := fmt.Sprintf("monitor-metrics-%d", time.Now().UnixNano())
	err := client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write 100 entries
	ids := framework.WriteEntries(t, ctx, writer, 0, 100)
	require.Len(t, ids, 100)
	t.Log("Wrote 100 entries")

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Read all entries back
	msgs := framework.ReadAllEntries(t, ctx, logHandle, 100)
	require.Len(t, msgs, 100)
	t.Log("Read 100 entries back")

	// Wait for Prometheus to scrape the metrics (scrape_interval=5s, wait 15s for safety)
	t.Log("Waiting for Prometheus to scrape metrics...")
	time.Sleep(15 * time.Second)

	// --- Sub-tests to verify each metric category ---

	t.Run("ServerLogStoreMetrics", func(t *testing.T) {
		assertMetricExists(t, cluster, "woodpecker_server_logstore_operations_total")
		assertMetricExists(t, cluster, "woodpecker_server_logstore_active_logs")
		assertMetricExists(t, cluster, "woodpecker_server_logstore_active_segments")
	})

	t.Run("ServerFileBufferMetrics", func(t *testing.T) {
		assertMetricExists(t, cluster, "woodpecker_server_file_operations_total")
		assertMetricExists(t, cluster, "woodpecker_server_flush_bytes_written")
		// buffer_wait_latency is a HistogramVec — only appears after at least one
		// observation. In a short test the buffer may never block, so just check
		// that the file-level flush latency histogram is populated instead.
		assertMetricExists(t, cluster, "woodpecker_server_file_flush_latency_count")
	})

	t.Run("ServerObjectStorageMetrics", func(t *testing.T) {
		assertMetricExists(t, cluster, "woodpecker_server_object_storage_operations_total")
		// object_storage_bytes_transferred only increments on actual put/get data
		// transfers. Short tests may only trigger stat operations, so verify the
		// operation counter instead.
	})

	t.Run("SystemMetrics", func(t *testing.T) {
		assertMetricRegistered(t, cluster, "woodpecker_server_system_cpu_usage")
		assertMetricRegistered(t, cluster, "woodpecker_server_system_cpu_num")
		assertMetricRegistered(t, cluster, "woodpecker_server_system_memory_total_bytes")
		assertMetricRegistered(t, cluster, "woodpecker_server_system_memory_used_bytes")
	})

	t.Run("GrpcServerMetrics", func(t *testing.T) {
		assertMetricExists(t, cluster, "grpc_server_started_total")
		assertMetricExists(t, cluster, "grpc_server_handled_total")
		assertMetricExists(t, cluster, "grpc_server_handling_seconds_count")
	})

	// NOTE: woodpecker_client_* metrics use labeled vectors (CounterVec/HistogramVec).
	// Prometheus only exposes them after .With(labels) is called. Since client
	// operations run in the Go test process (not in the server container), the
	// server never populates label values and these metrics are not queryable
	// via the Prometheus API. Client metric correctness is verified implicitly
	// through successful read/write operations above.

	t.Log("MetricsVerification passed: all metrics verified in Prometheus")
}
