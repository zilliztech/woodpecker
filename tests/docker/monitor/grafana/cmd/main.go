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

// Command grafana-setup is a standalone CLI tool to set up the Woodpecker
// Grafana dashboard. It can be run against any existing monitor cluster:
//
//	go run ./tests/docker/monitor/grafana/cmd/
//	go run ./tests/docker/monitor/grafana/cmd/ --grafana-url http://localhost:3000 --prometheus-url http://prometheus:9090
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/zilliztech/woodpecker/tests/docker/monitor/grafana"
)

func main() {
	cfg := grafana.DefaultConfig()

	flag.StringVar(&cfg.GrafanaURL, "grafana-url", cfg.GrafanaURL, "Grafana HTTP URL (host-accessible)")
	flag.StringVar(&cfg.PrometheusURL, "prometheus-url", cfg.PrometheusURL, "Prometheus URL (as seen by Grafana inside Docker)")
	flag.Parse()

	dashURL, err := grafana.SetupDashboard(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Grafana dashboard ready: %s\n", dashURL)
}
