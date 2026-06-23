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

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

func main() {
	configFile := flag.String("config-file", "/tmp/test-config.yaml", "woodpecker config path")
	metricsAddr := flag.String("metrics-addr", ":29099", "Prometheus /metrics listen address")
	logName := flag.String("log", "k8s-monitor-loadgen", "log name to write")
	interval := flag.Duration("interval", 50*time.Millisecond, "append interval")
	flag.Parse()

	// Expose client metrics for the woodpecker-client PodMonitor to scrape.
	metrics.RegisterClientMetricsWithRegisterer(prometheus.DefaultRegisterer)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(*metricsAddr, mux); err != nil {
			panic(fmt.Sprintf("metrics server: %v", err))
		}
	}()

	ctx := context.Background()
	cfg, err := config.NewConfiguration(*configFile)
	if err != nil {
		panic(fmt.Sprintf("load config: %v", err))
	}
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	if err != nil {
		panic(fmt.Sprintf("etcd client: %v", err))
	}
	cli, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	if err != nil {
		panic(fmt.Sprintf("woodpecker client: %v", err))
	}
	defer cli.Close(ctx)

	_ = cli.CreateLog(ctx, *logName) // idempotent
	h, err := cli.OpenLog(ctx, *logName)
	if err != nil {
		panic(fmt.Sprintf("open log: %v", err))
	}
	w, err := h.OpenLogWriter(ctx)
	if err != nil {
		panic(fmt.Sprintf("open writer: %v", err))
	}
	defer w.Close(ctx)

	// Tail reader to exercise the read path (read-batch / reader metrics).
	go func() {
		earliest := log.EarliestLogMessageID()
		r, err := h.OpenLogReader(ctx, &earliest, "loadgen-tail")
		if err != nil {
			return
		}
		defer r.Close(ctx)
		for {
			rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			_, _ = r.ReadNext(rctx)
			cancel()
		}
	}()

	fmt.Printf("loadgen: writing to %q every %s, metrics on %s\n", *logName, *interval, *metricsAddr)
	seq := 0
	tick := time.NewTicker(*interval)
	defer tick.Stop()
	for range tick.C {
		seq++
		wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		res := <-w.WriteAsync(wctx, &log.WriteMessage{
			Payload:    []byte(fmt.Sprintf("loadgen-%d", seq)),
			Properties: map[string]string{"seq": fmt.Sprintf("%d", seq)},
		})
		cancel()
		if res.Err != nil {
			fmt.Printf("write %d err: %v\n", seq, res.Err)
		}
	}
}
