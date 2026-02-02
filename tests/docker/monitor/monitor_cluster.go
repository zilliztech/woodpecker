package monitor

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/zilliztech/woodpecker/tests/docker/framework"
	"github.com/zilliztech/woodpecker/tests/docker/monitor/grafana"
)

const (
	// PrometheusURL is the base URL for the Prometheus HTTP API.
	PrometheusURL = "http://localhost:9090"
	// GrafanaURL is the base URL for the Grafana HTTP API.
	GrafanaURL = "http://localhost:3000"
)

// MonitorCluster manages a Docker Compose-based Woodpecker cluster for monitor testing.
// It embeds the shared framework.DockerCluster and adds Prometheus query methods.
type MonitorCluster struct {
	*framework.DockerCluster
}

// monitorDir returns the absolute path to the monitor suite directory,
// derived from this source file's location so it works regardless of CWD.
func monitorDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Dir(thisFile)
}

// NewMonitorCluster creates a MonitorCluster with monitor test configuration.
func NewMonitorCluster(t *testing.T) *MonitorCluster {
	t.Helper()
	base := framework.NewDockerCluster(t, framework.ClusterConfig{
		TestDir:         monitorDir(),
		ProjectName:     "woodpecker-monitor",
		OverrideFile:    "docker-compose.monitor.yaml",
		NetworkName:     "woodpecker-monitor_woodpecker",
		ExtraContainers: []string{"prometheus", "grafana"},
		GossipWait:      15 * time.Second,
	})
	return &MonitorCluster{DockerCluster: base}
}

// --- Prometheus Query Methods ---

// PrometheusQueryResult represents the parsed response from Prometheus /api/v1/query.
type PrometheusQueryResult struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// WaitForPrometheusReady waits until the Prometheus server is ready to accept queries.
func (mc *MonitorCluster) WaitForPrometheusReady(t *testing.T, timeout time.Duration) {
	t.Helper()
	t.Logf("Waiting for Prometheus to be ready (timeout: %v)...", timeout)

	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 5 * time.Second}

	for time.Now().Before(deadline) {
		resp, err := client.Get(PrometheusURL + "/-/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				t.Log("Prometheus is ready")
				return
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("Prometheus did not become ready within %v", timeout)
}

// QueryMetric queries Prometheus for a metric by name and returns whether it exists
// (has at least one result) and the full query result.
func (mc *MonitorCluster) QueryMetric(t *testing.T, metricName string) (bool, *PrometheusQueryResult) {
	t.Helper()

	url := fmt.Sprintf("%s/api/v1/query?query=%s", PrometheusURL, metricName)
	client := &http.Client{Timeout: 10 * time.Second}

	resp, err := client.Get(url)
	if err != nil {
		t.Logf("Failed to query Prometheus for %s: %v", metricName, err)
		return false, nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Logf("Failed to read Prometheus response for %s: %v", metricName, err)
		return false, nil
	}

	var result PrometheusQueryResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Logf("Failed to parse Prometheus response for %s: %v", metricName, err)
		return false, nil
	}

	if result.Status != "success" {
		t.Logf("Prometheus query for %s returned status: %s", metricName, result.Status)
		return false, &result
	}

	exists := len(result.Data.Result) > 0
	return exists, &result
}

// QueryMetricHasValue queries Prometheus for a metric and returns true if it exists
// with at least one non-zero value.
func (mc *MonitorCluster) QueryMetricHasValue(t *testing.T, metricName string) bool {
	t.Helper()

	exists, result := mc.QueryMetric(t, metricName)
	if !exists || result == nil {
		return false
	}

	for _, r := range result.Data.Result {
		if len(r.Value) >= 2 {
			valStr, ok := r.Value[1].(string)
			if ok && valStr != "0" && valStr != "0.0" {
				return true
			}
		}
	}
	return false
}

// QueryPromQL queries Prometheus with an arbitrary PromQL expression
// and returns the first result value as a string.
func (mc *MonitorCluster) QueryPromQL(t *testing.T, query string) (string, bool) {
	t.Helper()

	reqURL := fmt.Sprintf("%s/api/v1/query?query=%s", PrometheusURL, url.QueryEscape(query))
	client := &http.Client{Timeout: 10 * time.Second}

	resp, err := client.Get(reqURL)
	if err != nil {
		t.Logf("Failed to query Prometheus: %v", err)
		return "", false
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Logf("Failed to read Prometheus response: %v", err)
		return "", false
	}

	var result PrometheusQueryResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Logf("Failed to parse Prometheus response: %v", err)
		return "", false
	}

	if result.Status != "success" || len(result.Data.Result) == 0 {
		return "", false
	}

	if len(result.Data.Result[0].Value) >= 2 {
		if val, ok := result.Data.Result[0].Value[1].(string); ok {
			return val, true
		}
	}

	return "", false
}

// SetupGrafanaDashboard creates the Prometheus datasource and imports
// the Woodpecker dashboard into Grafana. Failure is non-fatal — the
// dashboard is a convenience, not a test requirement.
func (mc *MonitorCluster) SetupGrafanaDashboard(t *testing.T) {
	t.Helper()
	cfg := grafana.Config{
		GrafanaURL:    GrafanaURL,
		PrometheusURL: "http://prometheus:9090",
	}
	dashURL, err := grafana.SetupDashboard(cfg)
	if err != nil {
		t.Logf("WARNING: Grafana dashboard setup failed: %v", err)
		return
	}
	t.Logf("Grafana dashboard ready: %s", dashURL)
}
