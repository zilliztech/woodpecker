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
	"sync/atomic"
	"testing"
	"time"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/tests/utils"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// runServiceWrite creates a fresh client/log/writer and runs `writeCnt`
// synchronous Writes of `rowSize` bytes each at the given concurrency (a
// closed-loop driver: throughput = concurrency / latency), returning throughput
// metrics. It reads MaxBatchEntries/MaxBatchBytes from the passed cfg, so callers
// can vary client group-commit size between calls.
func runServiceWrite(t *testing.T, cfg *config.Configuration, conc, writeCnt int, rowSize int) (qps, mbps, avgMs float64, fail int64) {
	etcdCli, eerr := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	if eerr != nil {
		t.Fatalf("etcd: %v", eerr)
	}
	client, cerr := woodpecker.NewClient(context.TODO(), cfg, etcdCli, true)
	if cerr != nil {
		t.Fatalf("client: %v", cerr)
	}
	_ = logger.SetLevel("error")
	logName := fmt.Sprintf("iv_c%d_%d", conc, time.Now().UnixNano())
	if e := client.CreateLog(context.Background(), logName); e != nil {
		t.Fatalf("createlog: %v", e)
	}
	lh, e := client.OpenLog(context.Background(), logName)
	if e != nil {
		t.Fatalf("openlog: %v", e)
	}
	lw, e := lh.OpenLogWriter(context.Background())
	if e != nil {
		t.Fatalf("openwriter: %v", e)
	}
	payload := make([]byte, rowSize)
	var success, failC atomic.Int64
	var latSum int64
	var latMu sync.Mutex
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	wg.Add(writeCnt)
	start := time.Now()
	for i := 0; i < writeCnt; i++ {
		sem <- struct{}{}
		idx := i
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			st := time.Now()
			r := lw.Write(context.Background(), &log.WriteMessage{Payload: payload, Properties: map[string]string{"k": fmt.Sprintf("v%d", idx)}})
			if r.Err != nil {
				failC.Add(1)
				return
			}
			success.Add(1)
			latMu.Lock()
			latSum += time.Since(st).Nanoseconds()
			latMu.Unlock()
		}()
	}
	wg.Wait()
	wall := time.Since(start).Seconds()
	_ = lw.Close(context.Background())
	_ = client.Close(context.TODO())
	n := success.Load()
	qps = float64(n) / wall
	mbps = float64(n) * float64(rowSize) / wall / (1024 * 1024)
	if n > 0 {
		avgMs = float64(latSum) / float64(n) / 1e6
	}
	return qps, mbps, avgMs, failC.Load()
}

// TestServiceRowSizeGroupSweep sweeps payload size (rowSize) x client group-commit
// size (MaxBatchEntries) on the full 3-replica service write path, printing a
// throughput/flow table. For each rowSize, concurrency and writeCnt are scaled so
// a cell writes a bounded amount of data and never holds too much in flight
// (conc*rowSize), which keeps memory, disk and wall time in check as rowSize grows.
//
// Note on group size at large rows: MaxBatchBytes caps a batch, so with a 2MB cap a
// 100KB row batches at most ~20 entries and a 1MB row at most ~2, regardless of
// MaxBatchEntries — expect group size to matter only for small rows.
//
// Requires etcd on :2379 and minio on :9000.
// Run: go test ./tests/benchmark/ -run TestServiceRowSizeGroupSweep -v -timeout 40m -count=1
func TestServiceRowSizeGroupSweep(t *testing.T) {
	if testing.Short() {
		t.Skip("service quorum perf is a heavy integration test")
	}
	const nodeCount = 3
	baseDir := "/tmp/wp_svc_rowsweep"
	os.RemoveAll(baseDir)
	defer os.RemoveAll(baseDir)

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	if err != nil {
		t.Fatalf("cfg: %v", err)
	}
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForService = config.NewDurationMillisecondsFromInt(10)

	cluster, cfg2, _, seeds := utils.StartMiniClusterWithCfg(t, nodeCount, baseDir, cfg)
	defer cluster.StopMultiNodeCluster(t)
	cfg2.Log.Level = "error"
	_ = logger.SetLevel("error")
	cfg2.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, seeds)
	cfg2.Woodpecker.Client.SegmentAppend.QueueSize = 16384
	cfg2.Woodpecker.Client.SegmentAppend.MaxBatchBytes = config.ByteSize(2 * 1024 * 1024) // 2MB batch cap

	// rowSize -> (conc, writeCnt): each cell writes ~150-300MB and caps in-flight
	// bytes (conc*rowSize) to a few tens of MB. Tune these for your machine.
	sizes := []struct {
		rowSize  int
		conc     int
		writeCnt int
	}{
		{1024, 8000, 150000},     // 1KB   ~150MB, in-flight ~8MB
		{1024 * 10, 2000, 15000}, // 10KB  ~150MB, in-flight ~20MB
		{1024 * 100, 256, 2000},  // 100KB ~200MB, in-flight ~25MB
		{1024 * 1000, 64, 300},   // 1MB   ~300MB, in-flight ~64MB
	}
	// Group-commit size: OFF (1) vs ON (1000). Add more values to see the curve.
	groupSizes := []int{1, 1000}

	type cell struct {
		qps, mbps, avgMs float64
		fail             int64
	}
	results := map[int]map[int]cell{}

	for _, s := range sizes {
		results[s.rowSize] = map[int]cell{}
		for _, gs := range groupSizes {
			cfg2.Woodpecker.Client.SegmentAppend.MaxBatchEntries = gs
			qps, mbps, avgMs, fail := runServiceWrite(t, cfg2, s.conc, s.writeCnt, s.rowSize)
			results[s.rowSize][gs] = cell{qps, mbps, avgMs, fail}
			fmt.Printf("done rowSize=%-8d conc=%-5d groupSize=%-5d -> %.0f appends/s, %.1f MB/s, avgLat=%.1fms, fail=%d\n",
				s.rowSize, s.conc, gs, qps, mbps, avgMs, fail)
		}
	}

	fmt.Printf("\n=== Service row-size x group-size sweep (3-replica, 10ms flush, 2MB batch cap) ===\n")
	fmt.Printf("%-9s %-6s | %14s %12s %11s %6s\n", "rowSize", "group", "QPS(app/s)", "flow(MB/s)", "avgLat(ms)", "fail")
	for _, s := range sizes {
		for _, gs := range groupSizes {
			r := results[s.rowSize][gs]
			fmt.Printf("%-9d %-6d | %14.0f %12.1f %11.1f %6d\n",
				s.rowSize, gs, r.qps, r.mbps, r.avgMs, r.fail)
		}
	}
}
