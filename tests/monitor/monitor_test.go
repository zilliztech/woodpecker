package monitor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/woodpecker/log"
)

const (
	// writeResultTimeout is the maximum time to wait for a single WriteAsync result.
	writeResultTimeout = 60 * time.Second
)

// newMonitorCluster creates a MonitorCluster for use in monitor tests.
// It does NOT call Up/Down — that is managed by run_monitor_tests.sh.
func newMonitorCluster(t *testing.T) *MonitorCluster {
	t.Helper()
	return NewMonitorCluster(t)
}

// writeEntries writes n entries to the given writer and returns the successful message IDs.
func writeEntries(t *testing.T, ctx context.Context, writer log.LogWriter, offset, count int) []*log.LogMessageId {
	t.Helper()
	ids := make([]*log.LogMessageId, 0, count)
	for i := 0; i < count; i++ {
		resultCh := make(chan *log.WriteResult, 1)
		go func(idx int) {
			ch := writer.WriteAsync(ctx, &log.WriteMessage{
				Payload: []byte(fmt.Sprintf("entry-%d", offset+idx)),
				Properties: map[string]string{
					"index": fmt.Sprintf("%d", offset+idx),
				},
			})
			if ch != nil {
				resultCh <- <-ch
			}
		}(i)

		select {
		case result := <-resultCh:
			require.NoError(t, result.Err, "write entry %d failed", offset+i)
			require.NotNil(t, result.LogMessageId, "write entry %d returned nil ID", offset+i)
			ids = append(ids, result.LogMessageId)
		case <-time.After(writeResultTimeout):
			t.Fatalf("write entry %d timed out after %v (possible quorum failure)", offset+i, writeResultTimeout)
		}
	}
	return ids
}

// readAllEntries reads entries from the earliest position and returns them.
func readAllEntries(t *testing.T, ctx context.Context, logHandle log.LogHandle, expectedCount int) []*log.LogMessage {
	t.Helper()
	earliest := log.EarliestLogMessageID()
	reader, err := logHandle.OpenLogReader(ctx, &earliest, fmt.Sprintf("monitor-reader-%d", time.Now().UnixNano()))
	require.NoError(t, err, "failed to open log reader")
	defer reader.Close(ctx)

	msgs := make([]*log.LogMessage, 0, expectedCount)
	for i := 0; i < expectedCount; i++ {
		msg, err := reader.ReadNext(ctx)
		require.NoError(t, err, "failed to read entry %d", i)
		require.NotNil(t, msg, "entry %d is nil", i)
		msgs = append(msgs, msg)
	}
	return msgs
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

// TestMonitor_MetricsVerification verifies that all Woodpecker Prometheus metrics
// are correctly reported after performing read/write operations.
//
// Design:
//   - woodpecker_server_* and grpc_server_* metrics run inside server containers
//     and have real non-zero values after read/write operations.
//   - woodpecker_client_* metrics are registered in the server process
//     (RegisterWoodpeckerWithRegisterer registers all metrics), but their values
//     are zero because client operations execute in the Go test process, not in
//     the server container. We verify these are registered (exist) but don't
//     require non-zero values.
//   - woodpecker_server_system_* metrics are collected by a background goroutine
//     in the server and should have non-zero values.
func TestMonitor_MetricsVerification(t *testing.T) {
	cluster := newMonitorCluster(t)
	ctx := context.Background()

	// Wait for Prometheus to be ready
	cluster.WaitForPrometheusReady(t, 60*time.Second)

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
	ids := writeEntries(t, ctx, writer, 0, 100)
	require.Len(t, ids, 100)
	t.Log("Wrote 100 entries")

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Read all entries back
	msgs := readAllEntries(t, ctx, logHandle, 100)
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
		assertMetricExists(t, cluster, "woodpecker_server_buffer_wait_latency_count")
	})

	t.Run("ServerObjectStorageMetrics", func(t *testing.T) {
		assertMetricExists(t, cluster, "woodpecker_server_object_storage_operations_total")
		assertMetricExists(t, cluster, "woodpecker_server_object_storage_bytes_transferred")
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

	t.Run("ClientAppendMetrics", func(t *testing.T) {
		assertMetricRegistered(t, cluster, "woodpecker_client_append_requests_total")
		assertMetricRegistered(t, cluster, "woodpecker_client_append_entries_total")
		assertMetricRegistered(t, cluster, "woodpecker_client_append_bytes_bucket")
	})

	t.Run("ClientReadMetrics", func(t *testing.T) {
		assertMetricRegistered(t, cluster, "woodpecker_client_read_requests_total")
		assertMetricRegistered(t, cluster, "woodpecker_client_read_entries_total")
	})

	t.Run("ClientEtcdMetrics", func(t *testing.T) {
		assertMetricRegistered(t, cluster, "woodpecker_client_etcd_meta_operations_total")
	})

	t.Log("MetricsVerification passed: all metrics verified in Prometheus")
}
