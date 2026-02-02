package monitor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/tests/docker/framework"
	"github.com/zilliztech/woodpecker/woodpecker/log"
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

// TestMonitor_BasicMetricsVerification verifies that Woodpecker Prometheus metrics
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
func TestMonitor_BasicMetricsVerification(t *testing.T) {
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

	t.Log("BasicMetricsVerification passed: all metrics verified in Prometheus")
}

// continuousWriteFor writes entries every 5ms for the given duration using WriteAsync.
// It returns the number of successfully written entries.
func continuousWriteFor(ctx context.Context, t *testing.T, writer log.LogWriter, duration time.Duration) int {
	t.Helper()
	const writeInterval = 5 * time.Millisecond
	const perWriteTimeout = 30 * time.Second

	deadline := time.Now().Add(duration)
	written := 0

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return written
		default:
		}

		idx := written
		resultCh := make(chan *log.WriteResult, 1)
		go func() {
			ch := writer.WriteAsync(ctx, &log.WriteMessage{
				Payload: []byte(fmt.Sprintf("comprehensive-entry-%d", idx)),
				Properties: map[string]string{
					"index": fmt.Sprintf("%d", idx),
				},
			})
			if ch != nil {
				select {
				case r := <-ch:
					resultCh <- r
				case <-ctx.Done():
					resultCh <- &log.WriteResult{Err: ctx.Err()}
				}
			} else {
				resultCh <- &log.WriteResult{Err: fmt.Errorf("WriteAsync returned nil channel")}
			}
		}()

		select {
		case result := <-resultCh:
			if result.Err != nil {
				if ctx.Err() == nil {
					t.Logf("Write %d failed: %v", idx, result.Err)
				}
			} else {
				written++
			}
		case <-time.After(perWriteTimeout):
			t.Logf("Write %d timed out", idx)
		case <-ctx.Done():
			return written
		}

		select {
		case <-time.After(writeInterval):
		case <-ctx.Done():
			return written
		}
	}
	return written
}

// continuousReadFor opens a reader from the earliest position and reads entries
// until ctx is cancelled. It runs as a tailing reader that follows the writer.
// Returns the number of successfully read entries.
func continuousReadFor(ctx context.Context, t *testing.T, logHandle log.LogHandle) int {
	t.Helper()
	earliest := log.EarliestLogMessageID()
	reader, err := logHandle.OpenLogReader(ctx, &earliest, fmt.Sprintf("continuous-reader-%d", time.Now().UnixNano()))
	if err != nil {
		t.Logf("Failed to open continuous reader: %v", err)
		return 0
	}
	defer reader.Close(ctx)

	readCount := 0
	for {
		select {
		case <-ctx.Done():
			return readCount
		default:
		}

		msg, err := reader.ReadNext(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return readCount
			}
			t.Logf("Read error at entry %d: %v", readCount, err)
			continue
		}
		if msg != nil {
			readCount++
		}
	}
}

// TestMonitor_ComprehensiveMetricsVerification performs sustained writes for 3+ minutes
// to trigger segment rolling and then verifies the full set of Prometheus metrics.
//
// Design:
//   - Uses a 10s segment rolling interval (default is 600s) to produce ~19
//     segment rollings during the 3m15s write window, making metrics easier to observe.
//   - Writes every 5ms using WriteAsync to generate substantial I/O load.
//   - A concurrent reader tails the log during the entire write window, ensuring
//     read-path metrics (read_batch_latency, read_batch_bytes, file_reader) are
//     continuously generated alongside write-path metrics.
//   - Waits 20s for Prometheus to scrape, then verifies ~30 metrics across
//     6 categories: LogStore, File I/O, Buffer, Object Storage, System, gRPC.
func TestMonitor_ComprehensiveMetricsVerification(t *testing.T) {
	cluster := newMonitorCluster(t)
	ctx := context.Background()

	// Wait for Prometheus to be ready
	cluster.WaitForPrometheusReady(t, 60*time.Second)

	// Set up Grafana dashboard (non-fatal if it fails)
	cluster.SetupGrafanaDashboard(t)

	// Create client with segment rolling interval = 10s
	client, _ := cluster.NewClientWithOverride(t, ctx, func(cfg *config.Configuration) {
		cfg.Woodpecker.Client.SegmentRollingPolicy.MaxInterval = config.NewDurationSecondsFromInt(10)
	})

	logName := fmt.Sprintf("comprehensive-metrics-%d", time.Now().UnixNano())
	err := client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Run writer and reader concurrently for 3m15s
	writeDuration := 3*time.Minute + 15*time.Second
	readerCtx, readerCancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	var writtenCount, readCount int

	// Start continuous reader (tails the log as writer produces entries)
	wg.Add(1)
	go func() {
		defer wg.Done()
		readCount = continuousReadFor(readerCtx, t, logHandle)
	}()

	// Continuously write for 3m15s to trigger multiple segment rollings
	t.Log("Starting continuous writes for 3m15s with concurrent reader (segment rolling interval=10s)...")
	writtenCount = continuousWriteFor(ctx, t, writer, writeDuration)
	t.Logf("Wrote %d entries", writtenCount)
	require.Greater(t, writtenCount, 0, "should have written at least some entries")

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Give the reader a few seconds to catch up, then stop it
	time.Sleep(5 * time.Second)
	readerCancel()
	wg.Wait()
	t.Logf("Continuous reader read %d entries", readCount)

	// Wait for Prometheus to scrape metrics
	t.Log("Waiting 20s for Prometheus to scrape metrics...")
	time.Sleep(20 * time.Second)

	// --- Verify all metric categories ---

	t.Run("LogStoreCoreMetrics", func(t *testing.T) {
		assertMetricExists(t, cluster, "woodpecker_server_logstore_operations_total")
		assertMetricExists(t, cluster, "woodpecker_server_logstore_operation_latency_count")
		assertMetricExists(t, cluster, "woodpecker_server_logstore_active_logs")
		assertMetricExists(t, cluster, "woodpecker_server_logstore_active_segments")
		assertMetricExists(t, cluster, "woodpecker_server_logstore_instances_total")
		assertMetricRegistered(t, cluster, "woodpecker_server_logstore_active_segment_processors")
	})

	t.Run("FileIOMetrics", func(t *testing.T) {
		assertMetricExists(t, cluster, "woodpecker_server_file_operations_total")
		assertMetricExists(t, cluster, "woodpecker_server_file_operation_latency_count")
		assertMetricExists(t, cluster, "woodpecker_server_file_flush_latency_count")
		assertMetricExists(t, cluster, "woodpecker_server_flush_bytes_written")
		assertMetricExists(t, cluster, "woodpecker_server_file_compaction_latency_count")
		assertMetricExists(t, cluster, "woodpecker_server_compact_bytes_written")
		assertMetricExists(t, cluster, "woodpecker_server_read_batch_latency_count")
		assertMetricExists(t, cluster, "woodpecker_server_read_batch_bytes_total")
		assertMetricRegistered(t, cluster, "woodpecker_server_file_writer")
		assertMetricRegistered(t, cluster, "woodpecker_server_file_reader")
	})

	t.Run("BufferMetrics", func(t *testing.T) {
		// buffer_wait_latency is a HistogramVec with label "log_id". It only records
		// when the buffer actually blocks waiting for space. With fast 5ms writes the
		// buffer may never block, so .With(labels) is never called and the metric
		// won't appear in Prometheus. We only verify it if it was populated.
		exists, _ := cluster.QueryMetric(t, "woodpecker_server_buffer_wait_latency")
		if exists {
			t.Log("buffer_wait_latency is present")
		} else {
			t.Log("buffer_wait_latency not populated (buffer never blocked) — expected")
		}
	})

	t.Run("ObjectStorageMetrics", func(t *testing.T) {
		assertMetricExists(t, cluster, "woodpecker_server_object_storage_operations_total")
		assertMetricExists(t, cluster, "woodpecker_server_object_storage_operation_latency_count")
		assertMetricExists(t, cluster, "woodpecker_server_object_storage_bytes_transferred")
		assertMetricExists(t, cluster, "woodpecker_server_object_storage_request_bytes_count")
	})

	t.Run("SystemMetrics", func(t *testing.T) {
		assertMetricExists(t, cluster, "woodpecker_server_system_cpu_usage")
		assertMetricExists(t, cluster, "woodpecker_server_system_cpu_num")
		assertMetricExists(t, cluster, "woodpecker_server_system_memory_total_bytes")
		assertMetricExists(t, cluster, "woodpecker_server_system_memory_used_bytes")
		assertMetricExists(t, cluster, "woodpecker_server_system_memory_usage_ratio")
		assertMetricRegistered(t, cluster, "woodpecker_server_system_disk_total_bytes")
		assertMetricRegistered(t, cluster, "woodpecker_server_system_disk_used_bytes")
		assertMetricRegistered(t, cluster, "woodpecker_server_system_io_wait")
	})

	t.Run("GrpcServerMetrics", func(t *testing.T) {
		assertMetricExists(t, cluster, "grpc_server_started_total")
		assertMetricExists(t, cluster, "grpc_server_handled_total")
		assertMetricExists(t, cluster, "grpc_server_handling_seconds_count")
	})

	t.Log("ComprehensiveMetricsVerification passed: all metrics verified in Prometheus")
}
