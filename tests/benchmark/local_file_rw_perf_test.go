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

package benchmark

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/server/storage/disk"
	"github.com/zilliztech/woodpecker/tests/utils"
)

const (
	LOCAL_TEST_ENTRY_COUNT   = 10000
	LOCAL_TEST_ENTRY_SIZE    = 2_000_000
	LOCAL_CONCURRENT_THREADS = 6
	LOCAL_TEST_BASE_DIR      = "/tmp/woodpecker_local_perf_test"
)

func TestLocalFileWriterPerformance(t *testing.T) {
	utils.StartGopsAgent()
	utils.StartMetrics()
	utils.StartReporting()
	startTime := time.Now()

	// Clean up test directory at start and end
	os.RemoveAll(LOCAL_TEST_BASE_DIR)
	defer os.RemoveAll(LOCAL_TEST_BASE_DIR)

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)

	// Create LocalFileWriter
	logId := time.Now().UnixNano()
	segId := int64(1)
	writer, err := disk.NewLocalFileWriter(
		context.Background(),
		LOCAL_TEST_BASE_DIR,
		logId,
		segId,
		cfg,
	)
	assert.NoError(t, err)
	defer writer.Close(context.Background())

	payloadStaticData, err := utils.GenerateRandomBytes(LOCAL_TEST_ENTRY_SIZE)
	assert.NoError(t, err)

	concurrentCh := make(chan int, LOCAL_CONCURRENT_THREADS) // concurrency control
	wg := sync.WaitGroup{}

	// Latency tracking
	var latencies []time.Duration
	var latencyMutex sync.Mutex

	fmt.Printf("Test LocalFileWriter Start, entrySize:%d entryCount:%d concurrent:%d\n",
		LOCAL_TEST_ENTRY_SIZE, LOCAL_TEST_ENTRY_COUNT, LOCAL_CONCURRENT_THREADS)

	for i := 0; i < LOCAL_TEST_ENTRY_COUNT; i++ {
		concurrentCh <- 1
		entryId := int64(i)
		wg.Add(1)
		go func(ch chan int, id int64) {
			start := time.Now()

			// Create result channel
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test_entry_%d", id))
			defer resultCh.Close(context.Background())

			// Write data async
			_, writeErr := writer.WriteDataAsync(
				context.Background(),
				id,
				payloadStaticData,
				resultCh)
			assert.NoError(t, writeErr)

			// Wait for result
			result, readErr := resultCh.ReadResult(context.Background())
			assert.NoError(t, readErr)
			assert.NoError(t, result.Err)

			latency := time.Since(start)

			// Record latency
			latencyMutex.Lock()
			latencies = append(latencies, latency)
			latencyMutex.Unlock()

			<-ch
			wg.Done()

			// Record metrics
			utils.MinioIOBytes.WithLabelValues("local_perf_test").Observe(float64(len(payloadStaticData)))
			utils.MinioIOLatency.WithLabelValues("local_perf_test").Observe(float64(latency.Milliseconds()))
		}(concurrentCh, entryId)

		// Progress reporting during execution
		reportInterval := LOCAL_TEST_ENTRY_COUNT / 10 // Report 10 times during the test
		if reportInterval < 500 {
			reportInterval = 500 // Minimum interval
		}
		if (i+1)%reportInterval == 0 {
			elapsed := time.Since(startTime)
			currentThroughput := float64(i+1) / elapsed.Seconds()
			currentMbps := float64((i+1)*LOCAL_TEST_ENTRY_SIZE) / elapsed.Seconds() / (1024 * 1024)
			fmt.Printf("Progress: %d/%d (%.1f%%), Current throughput: %.2f entries/sec, %.2f MB/s\n",
				i+1, LOCAL_TEST_ENTRY_COUNT, float64(i+1)/float64(LOCAL_TEST_ENTRY_COUNT)*100, currentThroughput, currentMbps)
		}
	}

	fmt.Printf("All entries submitted, waiting for completion...\n")
	wg.Wait()
	fmt.Printf("All entries completed, starting final sync...\n")

	// Final sync
	syncStart := time.Now()
	syncErr := writer.Sync(context.Background())
	assert.NoError(t, syncErr)
	syncDuration := time.Since(syncStart)
	fmt.Printf("Sync completed in %v\n", syncDuration)

	totalTime := time.Since(startTime)
	throughput := float64(LOCAL_TEST_ENTRY_COUNT) / totalTime.Seconds()
	totalBytes := LOCAL_TEST_ENTRY_COUNT * LOCAL_TEST_ENTRY_SIZE
	mbps := float64(totalBytes) / totalTime.Seconds() / (1024 * 1024)

	fmt.Printf("Calculating latency statistics from %d samples...\n", len(latencies))

	// Calculate latency statistics
	var totalLatency time.Duration
	var minLatency time.Duration = time.Hour
	var maxLatency time.Duration

	for _, latency := range latencies {
		totalLatency += latency
		if latency < minLatency {
			minLatency = latency
		}
		if latency > maxLatency {
			maxLatency = latency
		}
	}

	avgLatency := totalLatency / time.Duration(len(latencies))

	// Calculate percentiles (simple sorting for small arrays)
	sortedLatencies := make([]time.Duration, len(latencies))
	copy(sortedLatencies, latencies)
	for i := 0; i < len(sortedLatencies); i++ {
		for j := i + 1; j < len(sortedLatencies); j++ {
			if sortedLatencies[i] > sortedLatencies[j] {
				sortedLatencies[i], sortedLatencies[j] = sortedLatencies[j], sortedLatencies[i]
			}
		}
	}

	p50 := sortedLatencies[len(sortedLatencies)/2]
	p95 := sortedLatencies[int(float64(len(sortedLatencies))*0.95)]
	p99 := sortedLatencies[int(float64(len(sortedLatencies))*0.99)]

	fmt.Printf("\n=== LocalFileWriter Performance Results ===\n")
	fmt.Printf("Test Configuration:\n")
	fmt.Printf("  Entry Size: %d bytes (%.2f MB)\n", LOCAL_TEST_ENTRY_SIZE, float64(LOCAL_TEST_ENTRY_SIZE)/(1024*1024))
	fmt.Printf("  Entry Count: %d\n", LOCAL_TEST_ENTRY_COUNT)
	fmt.Printf("  Concurrency: %d\n", LOCAL_CONCURRENT_THREADS)
	fmt.Printf("  Total Data: %.2f MB\n", float64(totalBytes)/(1024*1024))

	fmt.Printf("\nTiming:\n")
	fmt.Printf("  Total Time: %v\n", totalTime)
	fmt.Printf("  Sync Duration: %v\n", syncDuration)

	fmt.Printf("\nThroughput:\n")
	fmt.Printf("  Entries/sec: %.2f\n", throughput)
	fmt.Printf("  Data Rate: %.2f MB/s\n", mbps)

	fmt.Printf("\nLatency Statistics:\n")
	fmt.Printf("  Average: %.2f ms\n", float64(avgLatency.Microseconds())/1000.0)
	fmt.Printf("  Minimum: %.2f ms\n", float64(minLatency.Microseconds())/1000.0)
	fmt.Printf("  Maximum: %.2f ms\n", float64(maxLatency.Microseconds())/1000.0)
	fmt.Printf("  P50 (Median): %.2f ms\n", float64(p50.Microseconds())/1000.0)
	fmt.Printf("  P95: %.2f ms\n", float64(p95.Microseconds())/1000.0)
	fmt.Printf("  P99: %.2f ms\n", float64(p99.Microseconds())/1000.0)
}
