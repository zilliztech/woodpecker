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

package grafana

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Config holds the connection parameters for Grafana dashboard setup.
type Config struct {
	// GrafanaURL is the host-accessible Grafana URL (e.g. http://localhost:3000).
	GrafanaURL string
	// PrometheusURL is the URL Grafana uses to reach Prometheus.
	// Inside Docker this is typically http://prometheus:9090.
	PrometheusURL string
}

// DefaultConfig returns a Config with default values suitable for the
// Docker Compose monitor cluster.
func DefaultConfig() Config {
	return Config{
		GrafanaURL:    "http://localhost:3000",
		PrometheusURL: "http://prometheus:9090",
	}
}

// SetupDashboard creates the Prometheus datasource in Grafana and imports
// the Woodpecker dashboard. It returns the URL to the dashboard.
func SetupDashboard(cfg Config) (string, error) {
	if err := waitForGrafanaReady(cfg.GrafanaURL, 30*time.Second); err != nil {
		return "", fmt.Errorf("grafana not ready: %w", err)
	}

	dsUID, err := ensureDatasource(cfg.GrafanaURL, cfg.PrometheusURL)
	if err != nil {
		return "", fmt.Errorf("datasource setup failed: %w", err)
	}

	dashboard := BuildDashboard()
	setDatasourceUID(dashboard, dsUID)

	if err := importDashboard(cfg.GrafanaURL, dashboard); err != nil {
		return "", fmt.Errorf("dashboard import failed: %w", err)
	}

	dashURL := fmt.Sprintf("%s/d/%s/woodpecker", cfg.GrafanaURL, DashboardUID)
	return dashURL, nil
}

// waitForGrafanaReady polls the Grafana health endpoint until it responds OK
// or the timeout expires.
func waitForGrafanaReady(grafanaURL string, timeout time.Duration) error {
	client := &http.Client{Timeout: 5 * time.Second}
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		resp, err := client.Get(grafanaURL + "/api/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("grafana at %s did not become ready within %v", grafanaURL, timeout)
}

// ensureDatasource creates a Prometheus datasource in Grafana.
// If one already exists (HTTP 409), it fetches the existing datasource UID.
func ensureDatasource(grafanaURL, prometheusURL string) (string, error) {
	payload := map[string]interface{}{
		"name":      "Prometheus",
		"type":      "prometheus",
		"url":       prometheusURL,
		"access":    "proxy",
		"isDefault": true,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	resp, err := doRequest("POST", grafanaURL+"/api/datasources", body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated:
		// Created successfully — extract UID from response
		var result struct {
			Datasource struct {
				UID string `json:"uid"`
			} `json:"datasource"`
		}
		if err := json.Unmarshal(respBody, &result); err != nil {
			return "", fmt.Errorf("failed to parse datasource response: %w", err)
		}
		return result.Datasource.UID, nil

	case http.StatusConflict:
		// Already exists — fetch by name
		return getDatasourceUID(grafanaURL, "Prometheus")

	default:
		return "", fmt.Errorf("unexpected status %d creating datasource: %s", resp.StatusCode, string(respBody))
	}
}

// getDatasourceUID fetches the UID of an existing datasource by name.
func getDatasourceUID(grafanaURL, name string) (string, error) {
	resp, err := doRequest("GET", grafanaURL+"/api/datasources/name/"+name, nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to get datasource %q: status %d: %s", name, resp.StatusCode, string(body))
	}

	var ds struct {
		UID string `json:"uid"`
	}
	if err := json.Unmarshal(body, &ds); err != nil {
		return "", fmt.Errorf("failed to parse datasource: %w", err)
	}
	return ds.UID, nil
}

// setDatasourceUID walks all panels and sets the datasource UID.
func setDatasourceUID(dashboard *Dashboard, uid string) {
	for i := range dashboard.Panels {
		setDSOnPanel(&dashboard.Panels[i], uid)
	}
}

func setDSOnPanel(p *Panel, uid string) {
	if p.Datasource != nil {
		p.Datasource.UID = uid
	}
	for i := range p.Panels {
		setDSOnPanel(&p.Panels[i], uid)
	}
}

// importDashboard posts the dashboard to Grafana with overwrite enabled.
func importDashboard(grafanaURL string, dashboard *Dashboard) error {
	payload := map[string]interface{}{
		"dashboard": dashboard,
		"overwrite": true,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := doRequest("POST", grafanaURL+"/api/dashboards/db", body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("dashboard import returned status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// doRequest is a small helper for making HTTP requests with JSON content type.
func doRequest(method, url string, body []byte) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	return client.Do(req)
}
