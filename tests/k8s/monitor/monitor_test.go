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

package monitor

import (
	"os"
	"testing"
	"time"
)

// TestK8sMonitor_Metrics verifies scrape targets and the cluster/namespace/log_ns
// label scheme against a Prometheus reachable at $WP_K8S_PROM_URL (a kubectl
// port-forward set up by run_monitor_tests.sh). Skipped when unset so the package
// stays `go test ./...`-safe without a live cluster.
func TestK8sMonitor_Metrics(t *testing.T) {
	base := os.Getenv("WP_K8S_PROM_URL")
	if base == "" {
		t.Skip("WP_K8S_PROM_URL unset; run via run_monitor_tests.sh")
	}
	c := NewK8sMonitorCluster(base)
	c.WaitForPrometheusReady(t, 60*time.Second)

	t.Run("ServerTargetUp", func(t *testing.T) {
		if v, ok := c.QueryPromQL(t, `count(up{job="woodpecker/woodpecker-server"}==1)`); !ok || v == "0" {
			t.Errorf("no server targets up (got %q)", v)
		}
	})
	t.Run("ClientTargetUp", func(t *testing.T) {
		if v, ok := c.QueryPromQL(t, `count(up{job="woodpecker/woodpecker-client"}==1)`); !ok || v == "0" {
			t.Errorf("no client target up (got %q)", v)
		}
	})
	t.Run("ServerMetrics", func(t *testing.T) {
		for _, m := range []string{
			"woodpecker_server_logstore_active_logs",
			"woodpecker_server_logstore_operations_total",
			"woodpecker_server_file_operations_total",
		} {
			if !c.QueryMetricHasValue(t, m) {
				t.Errorf("server metric %s missing/zero", m)
			}
		}
	})
	t.Run("ClientMetrics", func(t *testing.T) {
		for _, m := range []string{
			"woodpecker_client_append_requests_total",
			"woodpecker_client_log_handle_operations_total",
		} {
			if !c.QueryMetricHasValue(t, m) {
				t.Errorf("client metric %s missing/zero", m)
			}
		}
	})
	t.Run("LabelScheme", func(t *testing.T) {
		series := c.QueryVector(t, "woodpecker_server_logstore_active_logs")
		if len(series) == 0 {
			t.Fatal("no logstore_active_logs series")
		}
		for _, m := range series {
			if m["cluster"] != "woodpecker-minikube" {
				t.Errorf("cluster=%q, want woodpecker-minikube", m["cluster"])
			}
			if m["namespace"] != "woodpecker" {
				t.Errorf("namespace=%q, want k8s namespace woodpecker", m["namespace"])
			}
			if m["log_ns"] == "" {
				t.Errorf("missing log_ns label: %v", m)
			}
			if _, bad := m["exported_namespace"]; bad {
				t.Errorf("unexpected exported_namespace: %v", m)
			}
		}
	})
}
