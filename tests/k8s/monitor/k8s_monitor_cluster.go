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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"
)

// K8sMonitorCluster queries a Prometheus reachable at PromBaseURL (typically a
// kubectl port-forward to svc/prometheus-operated in the monitoring namespace).
type K8sMonitorCluster struct {
	PromBaseURL string
}

func NewK8sMonitorCluster(promBaseURL string) *K8sMonitorCluster {
	return &K8sMonitorCluster{PromBaseURL: promBaseURL}
}

type promResult struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

func (c *K8sMonitorCluster) WaitForPrometheusReady(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 5 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(c.PromBaseURL + "/-/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				t.Log("Prometheus is ready")
				return
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("Prometheus at %s not ready within %v", c.PromBaseURL, timeout)
}

func (c *K8sMonitorCluster) query(t *testing.T, q string) *promResult {
	t.Helper()
	reqURL := fmt.Sprintf("%s/api/v1/query?query=%s", c.PromBaseURL, url.QueryEscape(q))
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(reqURL)
	if err != nil {
		t.Logf("prometheus query failed: %v", err)
		return nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var r promResult
	if err := json.Unmarshal(body, &r); err != nil {
		t.Logf("prometheus parse failed: %v", err)
		return nil
	}
	if r.Status != "success" {
		return nil
	}
	return &r
}

// QueryPromQL returns the first sample value as a string.
func (c *K8sMonitorCluster) QueryPromQL(t *testing.T, q string) (string, bool) {
	t.Helper()
	r := c.query(t, q)
	if r == nil || len(r.Data.Result) == 0 || len(r.Data.Result[0].Value) < 2 {
		return "", false
	}
	v, ok := r.Data.Result[0].Value[1].(string)
	return v, ok
}

// QueryVector returns the label set of every result series.
func (c *K8sMonitorCluster) QueryVector(t *testing.T, q string) []map[string]string {
	t.Helper()
	r := c.query(t, q)
	if r == nil {
		return nil
	}
	out := make([]map[string]string, 0, len(r.Data.Result))
	for _, s := range r.Data.Result {
		out = append(out, s.Metric)
	}
	return out
}

// QueryMetricHasValue reports whether the metric exists with a non-zero value.
func (c *K8sMonitorCluster) QueryMetricHasValue(t *testing.T, metricName string) bool {
	t.Helper()
	r := c.query(t, metricName)
	if r == nil {
		return false
	}
	for _, s := range r.Data.Result {
		if len(s.Value) >= 2 {
			if vs, ok := s.Value[1].(string); ok && vs != "0" && vs != "0.0" {
				return true
			}
		}
	}
	return false
}
