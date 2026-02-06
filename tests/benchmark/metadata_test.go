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
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/meta"
)

func testMetaCfg() *config.Configuration {
	cfg, _ := config.NewConfiguration()
	return cfg
}

// TestShowEtcd Test only for debug etcd
func TestShowEtcd(t *testing.T) {
	t.Skipf("for manualy debug meta print")
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"}, // Address of etcd server
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. show simple dirs
	directoryPrefix := "woodpecker" // Directory prefix to print
	printDirContents(ctx, cli, directoryPrefix, "")

	// 2. show meta detail
	printMetaContents(t, ctx, cli)
}

func TestCheckLogExists(t *testing.T) {
	t.Skipf("for manualy debug meta check")
	logName := "by-dev-rootcoord-dml_1"
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"}, // Address of etcd server
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	metaProvider := meta.NewMetadataProvider(context.Background(), cli, testMetaCfg())
	defer metaProvider.Close()
	exists, err := metaProvider.CheckExists(context.Background(), logName)
	assert.NoError(t, err)
	assert.False(t, exists)
}

// Test only
func printDirContents(ctx context.Context, cli *clientv3.Client, prefix string, indent string) {
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		fmt.Printf("fatal %v", err)
	}

	for _, kv := range resp.Kvs {
		fmt.Printf("%s%s: %s\n", indent, string(kv.Key), string(kv.Value))

		// Recursively print contents of subdirectories
		if strings.HasSuffix(string(kv.Key), "/") {
			newPrefix := string(kv.Key)
			printDirContents(ctx, cli, newPrefix, indent+"  ")
		}
	}
}

func printMetaContents(t *testing.T, ctx context.Context, cli *clientv3.Client) {
	metaProvider := meta.NewMetadataProvider(ctx, cli, testMetaCfg())
	defer metaProvider.Close()
	logs, err := metaProvider.ListLogs(ctx)
	assert.NoError(t, err)
	for _, log := range logs {
		t.Logf("logName: %s", log)
		meta, err := metaProvider.GetLogMeta(ctx, log)
		if err != nil {
			t.Error(err)
		}
		t.Logf("logName:%s logMeta: %v", log, meta)
		segs, err := metaProvider.GetAllSegmentMetadata(ctx, log)
		if err != nil {
			t.Error(err)
		}
		// Sort the keys in segs
		var keys []int64
		for k := range segs {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})
		// Print segMeta in sorted key order
		for _, key := range keys {
			t.Logf("segMeta: %v", segs[key])
		}
	}
}

// Test only
func TestClear(t *testing.T) {
	t.Skipf("for manualy debug meta clear")
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"}, // Address of etcd server
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = cli.Delete(ctx, meta.ServicePrefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("fatal %v", err)
	}
	t.Logf("clear finished")
}

func TestAcquireLogWriterLockPerformance(t *testing.T) {
	logName := "test_log_lock_perf"
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	assert.NoError(t, err)
	metaProvider := meta.NewMetadataProvider(context.Background(), etcdCli, testMetaCfg())
	defer metaProvider.Close()
	_ = metaProvider.InitIfNecessary(context.TODO())
	exists, err := metaProvider.CheckExists(context.Background(), logName)
	assert.NoError(t, err)
	assert.False(t, exists)

	const iterations = 10000
	acquireTimes := make([]time.Duration, 0, iterations)
	releaseTimes := make([]time.Duration, 0, iterations)

	// Test acquire and release lock performance
	for i := 0; i < iterations; i++ {
		// Measure acquire lock time
		acquireStart := time.Now()
		se, getLockErr := metaProvider.AcquireLogWriterLock(context.Background(), logName)
		acquireDuration := time.Since(acquireStart)
		assert.NoError(t, getLockErr)
		assert.NotNil(t, se)
		acquireTimes = append(acquireTimes, acquireDuration)

		// Measure release lock time
		releaseStart := time.Now()
		releaseErr := metaProvider.ReleaseLogWriterLock(context.Background(), logName)
		releaseDuration := time.Since(releaseStart)
		assert.NoError(t, releaseErr)
		releaseTimes = append(releaseTimes, releaseDuration)
	}

	// Calculate statistics for acquire lock
	acquireStats := calculateStats(acquireTimes)
	t.Logf("=== Acquire Lock Performance ===")
	t.Logf("Total operations: %d", iterations)
	t.Logf("Total time: %v", acquireStats.total)
	t.Logf("Min time: %v (%.3f ms)", acquireStats.min, float64(acquireStats.min.Nanoseconds())/1e6)
	t.Logf("Max time: %v (%.3f ms)", acquireStats.max, float64(acquireStats.max.Nanoseconds())/1e6)
	t.Logf("Avg time: %v (%.3f ms)", acquireStats.avg, float64(acquireStats.avg.Nanoseconds())/1e6)
	t.Logf("P50 time: %v (%.3f ms)", acquireStats.p50, float64(acquireStats.p50.Nanoseconds())/1e6)
	t.Logf("P95 time: %v (%.3f ms)", acquireStats.p95, float64(acquireStats.p95.Nanoseconds())/1e6)
	t.Logf("P99 time: %v (%.3f ms)", acquireStats.p99, float64(acquireStats.p99.Nanoseconds())/1e6)

	// Calculate statistics for release lock
	releaseStats := calculateStats(releaseTimes)
	t.Logf("=== Release Lock Performance ===")
	t.Logf("Total operations: %d", iterations)
	t.Logf("Total time: %v", releaseStats.total)
	t.Logf("Min time: %v (%.3f ms)", releaseStats.min, float64(releaseStats.min.Nanoseconds())/1e6)
	t.Logf("Max time: %v (%.3f ms)", releaseStats.max, float64(releaseStats.max.Nanoseconds())/1e6)
	t.Logf("Avg time: %v (%.3f ms)", releaseStats.avg, float64(releaseStats.avg.Nanoseconds())/1e6)
	t.Logf("P50 time: %v (%.3f ms)", releaseStats.p50, float64(releaseStats.p50.Nanoseconds())/1e6)
	t.Logf("P95 time: %v (%.3f ms)", releaseStats.p95, float64(releaseStats.p95.Nanoseconds())/1e6)
	t.Logf("P99 time: %v (%.3f ms)", releaseStats.p99, float64(releaseStats.p99.Nanoseconds())/1e6)

	// Basic assertion to ensure test runs
	assert.True(t, len(acquireTimes) == iterations)
	assert.True(t, len(releaseTimes) == iterations)
}

type stats struct {
	total time.Duration
	min   time.Duration
	max   time.Duration
	avg   time.Duration
	p50   time.Duration
	p95   time.Duration
	p99   time.Duration
}

func calculateStats(durations []time.Duration) stats {
	if len(durations) == 0 {
		return stats{}
	}

	// Create a copy and sort for percentile calculation
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	var total time.Duration
	for _, d := range durations {
		total += d
	}

	return stats{
		total: total,
		min:   sorted[0],
		max:   sorted[len(sorted)-1],
		avg:   total / time.Duration(len(durations)),
		p50:   sorted[len(sorted)*50/100],
		p95:   sorted[len(sorted)*95/100],
		p99:   sorted[len(sorted)*99/100],
	}
}
