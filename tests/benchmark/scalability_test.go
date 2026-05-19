// Copyright (C) 2025 Zilliz. All rights reserved.
//
// This file is part of the Woodpecker project.
//
// Scalability benchmark for issue #103: 100K log support on single server node.
// Measures server-side resource consumption (goroutines, memory, FDs, latency)
// across different log counts and active ratios.
//
// Usage:
//   go test -v -timeout 30m -run TestScalability ./tests/benchmark/
//   go test -v -timeout 30m -run TestScalability/IdleResources ./tests/benchmark/
//   go test -v -timeout 30m -run TestScalability/WriteLatency ./tests/benchmark/

package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server"
)

// ============================================================================
// Helpers
// ============================================================================

type resourceSnapshot struct {
	Goroutines   int
	HeapAllocMB  float64
	HeapSysMB    float64
	SysMB        float64
	HeapObjects  uint64
	OpenFDs      int
	StackInUseMB float64
}

func captureResources() resourceSnapshot {
	debug.FreeOSMemory()
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fds := countOpenFDs()

	return resourceSnapshot{
		Goroutines:   runtime.NumGoroutine(),
		HeapAllocMB:  float64(m.HeapAlloc) / (1024 * 1024),
		HeapSysMB:    float64(m.HeapSys) / (1024 * 1024),
		SysMB:        float64(m.Sys) / (1024 * 1024),
		HeapObjects:  m.HeapObjects,
		OpenFDs:      fds,
		StackInUseMB: float64(m.StackInuse) / (1024 * 1024),
	}
}

func countOpenFDs() int {
	entries, err := os.ReadDir("/dev/fd")
	if err != nil {
		// macOS fallback
		entries, err = os.ReadDir(fmt.Sprintf("/proc/%d/fd", os.Getpid()))
		if err != nil {
			return -1
		}
	}
	return len(entries)
}

func (s resourceSnapshot) String() string {
	return fmt.Sprintf(
		"Goroutines=%d  HeapAlloc=%.1fMB  HeapSys=%.1fMB  Sys=%.1fMB  StackInUse=%.1fMB  HeapObjects=%d  OpenFDs=%d",
		s.Goroutines, s.HeapAllocMB, s.HeapSysMB, s.SysMB, s.StackInUseMB, s.HeapObjects, s.OpenFDs,
	)
}

func diffResources(before, after resourceSnapshot) string {
	return fmt.Sprintf(
		"Δ Goroutines=%+d  Δ HeapAlloc=%+.1fMB  Δ StackInUse=%+.1fMB  Δ OpenFDs=%+d",
		after.Goroutines-before.Goroutines,
		after.HeapAllocMB-before.HeapAllocMB,
		after.StackInUseMB-before.StackInUseMB,
		after.OpenFDs-before.OpenFDs,
	)
}

type latencyStats struct {
	Count int
	P50   time.Duration
	P99   time.Duration
	P999  time.Duration
	Max   time.Duration
	Avg   time.Duration
}

func computeLatencyStats(latencies []time.Duration) latencyStats {
	if len(latencies) == 0 {
		return latencyStats{}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	n := len(latencies)
	var total time.Duration
	for _, l := range latencies {
		total += l
	}
	return latencyStats{
		Count: n,
		P50:   latencies[n*50/100],
		P99:   latencies[n*99/100],
		P999:  latencies[n*999/1000],
		Max:   latencies[n-1],
		Avg:   total / time.Duration(n),
	}
}

func (s latencyStats) String() string {
	return fmt.Sprintf(
		"count=%d  avg=%v  p50=%v  p99=%v  p999=%v  max=%v",
		s.Count, s.Avg, s.P50, s.P99, s.P999, s.Max,
	)
}

func createTestLogStore(t *testing.T) (server.LogStore, *config.Configuration, string) {
	t.Helper()
	tmpDir := t.TempDir()
	cfg, err := config.NewConfiguration()
	if err != nil {
		t.Fatalf("failed to create config: %v", err)
	}
	cfg.Woodpecker.Storage.Type = "service" // use service mode (StagedFileWriter) for multi-tenant benchmarks
	cfg.Woodpecker.Storage.RootPath = tmpDir
	cfg.Log.Level = "warn" // reduce log noise during benchmarks

	ctx := context.Background()
	store := server.NewLogStore(ctx, cfg, nil)
	if err := store.Start(); err != nil {
		t.Fatalf("failed to start logstore: %v", err)
	}
	metrics.NodeID = "bench-node"
	return store, cfg, tmpDir
}

// ============================================================================
// Test 1: Idle Resource Consumption
// Measures goroutines, memory, FDs when creating N segments with no writes.
// ============================================================================

func TestScalability_IdleResources(t *testing.T) {
	scenarios := []struct {
		name      string
		numLogs   int
		segPerLog int
	}{
		{"100_logs", 100, 1},
		{"1K_logs", 1000, 1},
		{"10K_logs", 10000, 1},
		// Enable these for full-scale test (requires more time/resources):
		// {"50K_logs", 50000, 1},
		// {"100K_logs", 100000, 1},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			store, _, _ := createTestLogStore(t)
			defer store.Stop()

			before := captureResources()
			t.Logf("BEFORE: %s", before)

			// Create segments by writing one entry each (triggers writer creation)
			bucketName := "bench"
			rootPath := "test"
			for logId := 0; logId < sc.numLogs; logId++ {
				for seg := 0; seg < sc.segPerLog; seg++ {
					entry := &proto.LogEntry{
						EntryId: 0,
						SegId:   int64(seg),
						Values:  []byte("init"),
					}
					_, err := store.AddEntry(context.Background(), bucketName, rootPath, int64(logId), entry, nil)
					if err != nil {
						t.Fatalf("AddEntry failed at log=%d seg=%d: %v", logId, seg, err)
					}
				}
			}

			// Wait for all writers to initialize their goroutines
			time.Sleep(500 * time.Millisecond)

			after := captureResources()
			t.Logf("AFTER %d logs × %d segs: %s", sc.numLogs, sc.segPerLog, after)
			t.Logf("DELTA: %s", diffResources(before, after))

			totalSegs := sc.numLogs * sc.segPerLog
			goroutinesPerSeg := float64(after.Goroutines-before.Goroutines) / float64(totalSegs)
			memPerSegKB := (after.HeapAllocMB - before.HeapAllocMB) * 1024 / float64(totalSegs)
			stackPerSegKB := (after.StackInUseMB - before.StackInUseMB) * 1024 / float64(totalSegs)

			t.Logf("PER SEGMENT: goroutines=%.2f  heap=%.1fKB  stack=%.1fKB",
				goroutinesPerSeg, memPerSegKB, stackPerSegKB)

			// Extrapolate to 100K
			t.Logf("EXTRAPOLATION to 100K logs:")
			t.Logf("  Goroutines: ~%d", int(goroutinesPerSeg*100000))
			t.Logf("  Heap: ~%.0f MB", memPerSegKB*100000/1024)
			t.Logf("  Stack: ~%.0f MB", stackPerSegKB*100000/1024)
		})
	}
}

// parallelCreateSegments creates N segments using concurrent goroutines to speed up setup.
// Uses batchSize concurrent workers, each creating segments sequentially within its batch.
func parallelCreateSegments(t *testing.T, store server.LogStore, bucketName, rootPath string, totalLogs, batchSize int) time.Duration {
	t.Helper()
	createStart := time.Now()

	var wg sync.WaitGroup
	var createErrors atomic.Int64

	for batchStart := 0; batchStart < totalLogs; batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > totalLogs {
			batchEnd = totalLogs
		}
		wg.Add(1)
		go func(from, to int) {
			defer wg.Done()
			for logId := from; logId < to; logId++ {
				entry := &proto.LogEntry{
					EntryId: 0,
					SegId:   0,
					Values:  []byte("init"),
				}
				_, err := store.AddEntry(context.Background(), bucketName, rootPath, int64(logId), entry, nil)
				if err != nil {
					createErrors.Add(1)
				}
			}
		}(batchStart, batchEnd)
	}
	wg.Wait()
	elapsed := time.Since(createStart)

	if errs := createErrors.Load(); errs > 0 {
		t.Fatalf("segment creation had %d errors", errs)
	}
	t.Logf("Created %d segments in %v (parallel, batch=%d), rate=%.0f segs/sec",
		totalLogs, elapsed, batchSize, float64(totalLogs)/elapsed.Seconds())
	return elapsed
}

// ============================================================================
// Test 2: Write Latency Under Scale
// Creates N total segments (parallel), writes to M active ones, measures latency.
// Gradient: 100 → 1K → 5K → 10K → 20K → 40K → 100K
// ============================================================================

func TestScalability_WriteLatency(t *testing.T) {
	scenarios := []struct {
		name            string
		totalLogs       int
		activeLogs      int
		writesPerActive int
	}{
		{"100total_1active", 100, 1, 500},
		{"1K_total_10active", 1000, 10, 200},
		{"5K_total_50active", 5000, 50, 100},
		{"10K_total_100active", 10000, 100, 100},
		{"20K_total_200active", 20000, 200, 50},
		{"40K_total_400active", 40000, 400, 50},
		{"100K_total_1K_active", 100000, 1000, 30},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			store, _, _ := createTestLogStore(t)
			defer store.Stop()

			bucketName := "bench"
			rootPath := "test"

			// Phase 1: Create all segments in parallel
			t.Logf("=== Creating %d idle segments ===", sc.totalLogs)
			batchSize := 500 // concurrent creation workers
			if sc.totalLogs <= 500 {
				batchSize = 50
			}
			parallelCreateSegments(t, store, bucketName, rootPath, sc.totalLogs, batchSize)

			// Stabilize — let tickers settle
			time.Sleep(2 * time.Second)
			beforeWrite := captureResources()
			t.Logf("After creation, before active writes: %s", beforeWrite)

			// Phase 2: Write concurrently to active logs, measure latency
			payload := make([]byte, 1024) // 1KB entries
			rand.Read(payload)

			var allLatencies []time.Duration
			var mu sync.Mutex
			var wg sync.WaitGroup
			var writeErrors atomic.Int64

			totalWrites := sc.writesPerActive * sc.activeLogs
			t.Logf("=== Writing %d entries/log × %d active logs = %d total writes ===",
				sc.writesPerActive, sc.activeLogs, totalWrites)

			writeStart := time.Now()
			for i := 0; i < sc.activeLogs; i++ {
				wg.Add(1)
				go func(logId int) {
					defer wg.Done()
					localLatencies := make([]time.Duration, 0, sc.writesPerActive)

					for j := 1; j <= sc.writesPerActive; j++ {
						entry := &proto.LogEntry{
							EntryId: int64(j),
							SegId:   0,
							Values:  payload,
						}
						start := time.Now()
						_, err := store.AddEntry(context.Background(), bucketName, rootPath, int64(logId), entry, nil)
						elapsed := time.Since(start)

						if err != nil {
							writeErrors.Add(1)
							continue
						}
						localLatencies = append(localLatencies, elapsed)
					}

					mu.Lock()
					allLatencies = append(allLatencies, localLatencies...)
					mu.Unlock()
				}(i)
			}
			wg.Wait()
			writeDuration := time.Since(writeStart)

			afterWrite := captureResources()
			stats := computeLatencyStats(allLatencies)

			t.Logf("--- Results for %s ---", sc.name)
			t.Logf("Write completed in %v, errors=%d", writeDuration, writeErrors.Load())
			t.Logf("Latency: %s", stats)
			t.Logf("Throughput: %.0f writes/sec", float64(len(allLatencies))/writeDuration.Seconds())
			t.Logf("Resources after writes: %s", afterWrite)
			t.Logf("Delta from idle: %s", diffResources(beforeWrite, afterWrite))
		})
	}
}

// ============================================================================
// Test 3: Goroutine Growth Pattern
// Incrementally adds logs and captures goroutine count at each step.
// ============================================================================

func TestScalability_GoroutineGrowth(t *testing.T) {
	store, _, _ := createTestLogStore(t)
	defer store.Stop()

	bucketName := "bench"
	rootPath := "test"

	checkpoints := []int{1, 10, 50, 100, 500, 1000, 2000, 5000, 10000, 20000}
	logIndex := 0

	baseline := captureResources()
	t.Logf("Baseline: goroutines=%d  heap=%.1fMB", baseline.Goroutines, baseline.HeapAllocMB)

	for _, cp := range checkpoints {
		// Create logs from logIndex up to cp using parallel creation
		remaining := cp - logIndex
		if remaining > 0 {
			// For small increments, do sequential; for large, do parallel
			if remaining <= 100 {
				for logIndex < cp {
					entry := &proto.LogEntry{EntryId: 0, SegId: 0, Values: []byte("x")}
					_, err := store.AddEntry(context.Background(), bucketName, rootPath, int64(logIndex), entry, nil)
					if err != nil {
						t.Fatalf("AddEntry failed at log=%d: %v", logIndex, err)
					}
					logIndex++
				}
			} else {
				// Parallel creation for the batch [logIndex, cp)
				var wg sync.WaitGroup
				batchSize := 200
				for batchStart := logIndex; batchStart < cp; batchStart += batchSize {
					batchEnd := batchStart + batchSize
					if batchEnd > cp {
						batchEnd = cp
					}
					wg.Add(1)
					go func(from, to int) {
						defer wg.Done()
						for id := from; id < to; id++ {
							entry := &proto.LogEntry{EntryId: 0, SegId: 0, Values: []byte("x")}
							store.AddEntry(context.Background(), bucketName, rootPath, int64(id), entry, nil)
						}
					}(batchStart, batchEnd)
				}
				wg.Wait()
				logIndex = cp
			}
		}

		time.Sleep(500 * time.Millisecond)
		snap := captureResources()
		goroutinesPerLog := float64(snap.Goroutines-baseline.Goroutines) / float64(cp)
		t.Logf("At %6d logs: goroutines=%6d (%.2f/log)  heap=%7.1fMB  stack=%6.1fMB  fds=%d",
			cp, snap.Goroutines, goroutinesPerLog, snap.HeapAllocMB, snap.StackInUseMB, snap.OpenFDs)
	}
}

// ============================================================================
// Test 4: Active Processor Count (measures LogStore.GetActiveProcessorCount)
// ============================================================================

func TestScalability_ProcessorCount(t *testing.T) {
	store, _, _ := createTestLogStore(t)
	defer store.Stop()

	bucketName := "bench"
	rootPath := "test"
	totalLogs := 1000

	for i := 0; i < totalLogs; i++ {
		entry := &proto.LogEntry{
			EntryId: 0,
			SegId:   0,
			Values:  []byte("x"),
		}
		_, _ = store.AddEntry(context.Background(), bucketName, rootPath, int64(i), entry, nil)
	}

	time.Sleep(500 * time.Millisecond)
	active := store.GetActiveProcessorCount()
	snap := captureResources()

	t.Logf("Total logs: %d", totalLogs)
	t.Logf("Active processors: %d", active)
	t.Logf("Resources: %s", snap)
}

// ============================================================================
// Test 5: Idle to Active Transition Latency (cold-start penalty)
// ============================================================================

func TestScalability_ColdStartLatency(t *testing.T) {
	store, _, _ := createTestLogStore(t)
	defer store.Stop()

	bucketName := "bench"
	rootPath := "test"

	// Write first entry to create the segment (cold start)
	coldLatencies := make([]time.Duration, 0, 100)
	for logId := 0; logId < 100; logId++ {
		entry := &proto.LogEntry{
			EntryId: 0,
			SegId:   0,
			Values:  []byte("cold-start-entry"),
		}
		start := time.Now()
		_, err := store.AddEntry(context.Background(), bucketName, rootPath, int64(logId), entry, nil)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("cold start write failed: %v", err)
		}
		coldLatencies = append(coldLatencies, elapsed)
	}

	// Write second entry (warm)
	warmLatencies := make([]time.Duration, 0, 100)
	for logId := 0; logId < 100; logId++ {
		entry := &proto.LogEntry{
			EntryId: 1,
			SegId:   0,
			Values:  []byte("warm-entry"),
		}
		start := time.Now()
		_, err := store.AddEntry(context.Background(), bucketName, rootPath, int64(logId), entry, nil)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("warm write failed: %v", err)
		}
		warmLatencies = append(warmLatencies, elapsed)
	}

	coldStats := computeLatencyStats(coldLatencies)
	warmStats := computeLatencyStats(warmLatencies)

	t.Logf("Cold start (first write to new segment): %s", coldStats)
	t.Logf("Warm write (second write to existing):   %s", warmStats)
	t.Logf("Cold-start overhead: avg=%v  p99=%v",
		coldStats.Avg-warmStats.Avg, coldStats.P99-warmStats.P99)
}

// ============================================================================
// Test 6: Scheduler Overhead Measurement
// Measures CPU consumed by idle goroutines (tickers firing with empty buffers).
// ============================================================================

func TestScalability_IdleCPUOverhead(t *testing.T) {
	store, _, _ := createTestLogStore(t)
	defer store.Stop()

	bucketName := "bench"
	rootPath := "test"
	totalLogs := 1000

	// Create all logs
	for i := 0; i < totalLogs; i++ {
		entry := &proto.LogEntry{
			EntryId: 0,
			SegId:   0,
			Values:  []byte("x"),
		}
		_, _ = store.AddEntry(context.Background(), bucketName, rootPath, int64(i), entry, nil)
	}

	time.Sleep(1 * time.Second)

	// Measure CPU time over a 5-second window with all logs idle
	var cpuBefore, cpuAfter runtime.MemStats
	runtime.ReadMemStats(&cpuBefore)

	goroutinesBefore := runtime.NumGoroutine()
	idleStart := time.Now()
	time.Sleep(5 * time.Second)
	idleDuration := time.Since(idleStart)

	runtime.ReadMemStats(&cpuAfter)
	goroutinesAfter := runtime.NumGoroutine()

	// GC count as a proxy for allocation pressure
	gcRuns := cpuAfter.NumGC - cpuBefore.NumGC
	allocBytes := cpuAfter.TotalAlloc - cpuBefore.TotalAlloc

	t.Logf("Idle period: %v with %d logs", idleDuration, totalLogs)
	t.Logf("Goroutines: %d → %d", goroutinesBefore, goroutinesAfter)
	t.Logf("GC runs during idle: %d", gcRuns)
	t.Logf("Bytes allocated during idle: %.1f MB (allocation pressure from tickers/goroutines)",
		float64(allocBytes)/(1024*1024))
	t.Logf("Allocation rate: %.1f MB/sec",
		float64(allocBytes)/(1024*1024)/idleDuration.Seconds())
}
