package monitor

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// --- Latency Tracking ---

// timedSample records a single operation's latency with a timestamp.
type timedSample struct {
	timestamp time.Time
	latency   time.Duration
	failed    bool
}

// latencyTracker collects timestamped latency samples thread-safely.
type latencyTracker struct {
	mu      sync.Mutex
	samples []timedSample
}

func newLatencyTracker() *latencyTracker {
	return &latencyTracker{samples: make([]timedSample, 0, 10000)}
}

func (lt *latencyTracker) record(latency time.Duration, failed bool) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.samples = append(lt.samples, timedSample{
		timestamp: time.Now(),
		latency:   latency,
		failed:    failed,
	})
}

// samplesInRange returns successful latencies and failure count for a time range.
func (lt *latencyTracker) samplesInRange(start, end time.Time) ([]time.Duration, int) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	var latencies []time.Duration
	failures := 0
	for _, s := range lt.samples {
		if s.timestamp.Before(start) || s.timestamp.After(end) {
			continue
		}
		if s.failed {
			failures++
		} else {
			latencies = append(latencies, s.latency)
		}
	}
	return latencies, failures
}

// --- Stats Computation ---

type phaseStats struct {
	Name     string
	Duration time.Duration
	Total    int
	Success  int
	Failures int
	P50      time.Duration
	P99      time.Duration
	Max      time.Duration
	Avg      time.Duration
}

func computePhaseStats(name string, latencies []time.Duration, failures int, phaseDuration time.Duration) phaseStats {
	stats := phaseStats{
		Name:     name,
		Duration: phaseDuration,
		Success:  len(latencies),
		Failures: failures,
		Total:    len(latencies) + failures,
	}
	if len(latencies) == 0 {
		return stats
	}

	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	stats.P50 = sortedPercentile(sorted, 0.50)
	stats.P99 = sortedPercentile(sorted, 0.99)
	stats.Max = sorted[len(sorted)-1]

	var total time.Duration
	for _, d := range sorted {
		total += d
	}
	stats.Avg = total / time.Duration(len(sorted))

	return stats
}

func sortedPercentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(len(sorted))*p)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func formatLatency(d time.Duration) string {
	if d == 0 {
		return "      -"
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%5.0fus", float64(d.Microseconds()))
	}
	if d < time.Second {
		return fmt.Sprintf("%5.1fms", float64(d.Microseconds())/1000.0)
	}
	return fmt.Sprintf("%5.2fs ", d.Seconds())
}

// --- Continuous Writer ---

const (
	rollingWriteTimeout  = 30 * time.Second
	rollingWriteInterval = 5 * time.Millisecond
)

// continuousWrite writes entries in a loop until ctx is cancelled, recording per-write latency.
// Returns the number of successfully written entries.
func continuousWrite(ctx context.Context, t *testing.T, writer log.LogWriter, tracker *latencyTracker) int {
	t.Helper()
	written := 0
	for {
		select {
		case <-ctx.Done():
			return written
		default:
		}

		idx := written
		resultCh := make(chan *log.WriteResult, 1)
		start := time.Now()
		go func() {
			ch := writer.WriteAsync(ctx, &log.WriteMessage{
				Payload: []byte(fmt.Sprintf("rolling-entry-%d", idx)),
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
			elapsed := time.Since(start)
			if result.Err != nil {
				tracker.record(elapsed, true)
				if ctx.Err() == nil {
					t.Logf("Write %d failed (latency: %v): %v", idx, elapsed, result.Err)
				}
			} else {
				tracker.record(elapsed, false)
				written++
			}
		case <-time.After(rollingWriteTimeout):
			elapsed := time.Since(start)
			tracker.record(elapsed, true)
			t.Logf("Write %d timed out (latency: %v)", idx, elapsed)
		case <-ctx.Done():
			return written
		}

		select {
		case <-time.After(rollingWriteInterval):
		case <-ctx.Done():
			return written
		}
	}
}

// --- Continuous Reader ---

// continuousRead reads entries from earliest in a loop until ctx is cancelled.
// Each ReadNext that returns data records a latency sample.
// Note: when the reader catches up to the writer head, ReadNext blocks until
// a new entry is available. The measured latency therefore includes both the
// "wait for data" time and the actual read time, reflecting end-to-end pipeline latency.
func continuousRead(ctx context.Context, t *testing.T, logHandle log.LogHandle, tracker *latencyTracker) {
	t.Helper()
	earliest := log.EarliestLogMessageID()
	readerName := fmt.Sprintf("rolling-reader-%d", time.Now().UnixNano())
	reader, err := logHandle.OpenLogReader(ctx, &earliest, readerName)
	if err != nil {
		if ctx.Err() == nil {
			t.Logf("Failed to open reader: %v", err)
		}
		return
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		reader.Close(closeCtx)
	}()

	for {
		start := time.Now()
		msg, err := reader.ReadNext(ctx)
		elapsed := time.Since(start)

		if ctx.Err() != nil {
			return
		}

		if err != nil || msg == nil {
			tracker.record(elapsed, true)
			continue
		}
		tracker.record(elapsed, false)
	}
}

// --- Main Test ---

// TestMonitor_RollingRestart_LatencyProfile measures write and read latency across
// a rolling restart of all Woodpecker nodes. The test provides a latency profile
// to evaluate the impact of rolling upgrades on service quality.
//
// Phases:
//  1. Baseline: 15s of writes+reads with all 4 nodes healthy
//  2. Rolling restart: stop and restart each node one at a time (4 cycles)
//     At most 1 node is down at any point → 3 alive >= ensemble_size=3 → writes continue
//  3. Recovery: 15s of writes+reads with all nodes restored
//
// Output:
//   - Per-phase write/read latency (P50, P99, Max, Avg, failure count)
//   - Latency ratio vs baseline for each restart phase
//   - Server-side Prometheus metrics (gRPC latency, buffer wait, flush latency)
func TestMonitor_RollingRestart_LatencyProfile(t *testing.T) {
	cluster := newMonitorCluster(t)
	ctx := context.Background()

	cluster.WaitForPrometheusReady(t, 60*time.Second)

	// Create client and log
	client, _ := cluster.NewClient(t, ctx)

	logName := fmt.Sprintf("monitor-rolling-%d", time.Now().UnixNano())
	err := client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Trackers
	writeTracker := newLatencyTracker()
	readTracker := newLatencyTracker()

	// Start continuous writer and reader
	writeCtx, writeCancel := context.WithCancel(ctx)
	readCtx, readCancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	var totalWritten int

	wg.Add(2)
	go func() {
		defer wg.Done()
		totalWritten = continuousWrite(writeCtx, t, writer, writeTracker)
	}()
	go func() {
		defer wg.Done()
		continuousRead(readCtx, t, logHandle, readTracker)
	}()

	// Phase tracking
	type phase struct {
		name  string
		start time.Time
		end   time.Time
	}
	var phases []phase

	// --- Phase 1: Baseline ---
	baselineStart := time.Now()
	t.Log("=== Phase: Baseline (15s) ===")
	time.Sleep(15 * time.Second)
	baselineEnd := time.Now()
	phases = append(phases, phase{"Baseline", baselineStart, baselineEnd})
	t.Log("Baseline phase complete")

	// --- Phase 2: Rolling Restart ---
	for _, node := range cluster.Nodes {
		phaseName := fmt.Sprintf("Restart-%s", node.ContainerName)
		t.Logf("=== Phase: %s ===", phaseName)
		phaseStart := time.Now()

		// Graceful stop (simulates rolling upgrade)
		cluster.StopNode(t, node.ContainerName)
		t.Logf("Stopped %s, waiting for gossip detection...", node.ContainerName)
		time.Sleep(5 * time.Second)

		// Restart
		cluster.StartNode(t, node.ContainerName)
		t.Logf("Started %s, waiting for healthy...", node.ContainerName)
		cluster.WaitForHealthy(t, node.ContainerName, 60*time.Second)
		t.Logf("%s is healthy, waiting for gossip convergence...", node.ContainerName)
		time.Sleep(10 * time.Second)

		phaseEnd := time.Now()
		phases = append(phases, phase{phaseName, phaseStart, phaseEnd})
		t.Logf("Phase %s complete (duration: %v)", phaseName, phaseEnd.Sub(phaseStart))
	}

	// --- Phase 3: Recovery ---
	recoveryStart := time.Now()
	t.Log("=== Phase: Recovery (15s) ===")
	time.Sleep(15 * time.Second)
	recoveryEnd := time.Now()
	phases = append(phases, phase{"Recovery", recoveryStart, recoveryEnd})
	t.Log("Recovery phase complete")

	// Stop writer, give reader a moment to drain, then stop reader
	writeCancel()
	time.Sleep(2 * time.Second)
	readCancel()
	wg.Wait()

	t.Logf("Total entries written: %d", totalWritten)

	// Close writer
	closeCtx, closeCancel := context.WithTimeout(ctx, 30*time.Second)
	err = writer.Close(closeCtx)
	closeCancel()
	if err != nil {
		t.Logf("Writer close error (may be expected after restart): %v", err)
	}

	// --- Verify data integrity with batch read ---
	if totalWritten > 0 {
		logHandle2, err := client.OpenLog(ctx, logName)
		require.NoError(t, err)
		msgs := readAllEntries(t, ctx, logHandle2, totalWritten)
		require.Len(t, msgs, totalWritten, "all written entries should be readable")
		t.Logf("Data integrity verified: %d entries read back successfully", totalWritten)
	}

	// --- Compute and Report Latency Profile ---
	t.Log("")
	t.Log("========================================================================")
	t.Log("              ROLLING RESTART LATENCY PROFILE")
	t.Log("========================================================================")

	// Write latency
	t.Log("")
	t.Log("WRITE LATENCY:")
	t.Logf("  %-30s %6s %5s %8s %8s %8s %8s",
		"Phase", "Total", "Fail", "P50", "P99", "Max", "Avg")
	t.Logf("  %-30s %6s %5s %8s %8s %8s %8s",
		"------------------------------", "------", "-----", "--------", "--------", "--------", "--------")

	var writePhases []phaseStats
	for _, p := range phases {
		latencies, failures := writeTracker.samplesInRange(p.start, p.end)
		stats := computePhaseStats(p.name, latencies, failures, p.end.Sub(p.start))
		writePhases = append(writePhases, stats)
		t.Logf("  %-30s %6d %5d %8s %8s %8s %8s",
			stats.Name, stats.Total, stats.Failures,
			formatLatency(stats.P50), formatLatency(stats.P99),
			formatLatency(stats.Max), formatLatency(stats.Avg))
	}

	// Read latency
	t.Log("")
	t.Log("READ LATENCY:")
	t.Logf("  %-30s %6s %5s %8s %8s %8s %8s",
		"Phase", "Total", "Fail", "P50", "P99", "Max", "Avg")
	t.Logf("  %-30s %6s %5s %8s %8s %8s %8s",
		"------------------------------", "------", "-----", "--------", "--------", "--------", "--------")

	for _, p := range phases {
		latencies, failures := readTracker.samplesInRange(p.start, p.end)
		stats := computePhaseStats(p.name, latencies, failures, p.end.Sub(p.start))
		t.Logf("  %-30s %6d %5d %8s %8s %8s %8s",
			stats.Name, stats.Total, stats.Failures,
			formatLatency(stats.P50), formatLatency(stats.P99),
			formatLatency(stats.Max), formatLatency(stats.Avg))
	}

	// --- Latency Impact Summary ---
	t.Log("")
	t.Log("LATENCY IMPACT SUMMARY (Write P99 vs Baseline):")
	if len(writePhases) >= 2 && writePhases[0].P99 > 0 {
		baselineP99 := writePhases[0].P99
		t.Logf("  %-30s %8s (reference)", writePhases[0].Name, formatLatency(baselineP99))
		for _, ws := range writePhases[1:] {
			if ws.P99 > 0 {
				ratio := float64(ws.P99) / float64(baselineP99)
				delta := float64(ws.P99-baselineP99) / float64(baselineP99) * 100
				t.Logf("  %-30s %8s (%+.1f%%, %.2fx)", ws.Name, formatLatency(ws.P99), delta, ratio)
			} else {
				t.Logf("  %-30s %8s (no data)", ws.Name, formatLatency(ws.P99))
			}
		}
	}

	// --- Prometheus Server-Side Metrics ---
	t.Log("")
	t.Log("Waiting for Prometheus to scrape final metrics...")
	time.Sleep(15 * time.Second)

	t.Log("")
	t.Log("SERVER-SIDE METRICS (from Prometheus):")
	queries := []struct {
		label string
		query string
	}{
		{"gRPC P50 latency", `histogram_quantile(0.5, sum(rate(grpc_server_handling_seconds_bucket[10m])) by (le))`},
		{"gRPC P99 latency", `histogram_quantile(0.99, sum(rate(grpc_server_handling_seconds_bucket[10m])) by (le))`},
		{"Buffer Wait P99", `histogram_quantile(0.99, sum(rate(woodpecker_server_buffer_wait_latency_bucket[10m])) by (le))`},
		{"File Flush P99", `histogram_quantile(0.99, sum(rate(woodpecker_server_file_flush_latency_bucket[10m])) by (le))`},
		{"LogStore Op P99", `histogram_quantile(0.99, sum(rate(woodpecker_server_logstore_operation_latency_bucket[10m])) by (le))`},
		{"ObjStore Op P99", `histogram_quantile(0.99, sum(rate(woodpecker_server_object_storage_operation_latency_bucket[10m])) by (le))`},
	}
	for _, q := range queries {
		value, ok := cluster.QueryPromQL(t, q.query)
		if ok {
			t.Logf("  %-25s = %s", q.label, value)
		} else {
			t.Logf("  %-25s = (no data)", q.label)
		}
	}

	t.Log("")
	t.Log("========================================================================")
	t.Log("RollingRestart_LatencyProfile completed")
}
