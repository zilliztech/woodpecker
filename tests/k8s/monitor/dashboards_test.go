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
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestK8sDashboards_Valid(t *testing.T) {
	for _, f := range []string{
		"grafana/templates/dashboard_server_k8s.json",
		"grafana/templates/dashboard_client_k8s.json",
	} {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		var obj map[string]any
		if err := json.Unmarshal(raw, &obj); err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		s := string(raw)
		if strings.Contains(s, "__DATASOURCE_UID__") {
			t.Errorf("%s: stray __DATASOURCE_UID__", f)
		}
		if !strings.Contains(s, "${prometheus}") {
			t.Errorf("%s: missing ${prometheus} datasource var", f)
		}
		if strings.Contains(s, "$cluster") {
			t.Errorf("%s: stray $cluster reference", f)
		}
		names := map[string]bool{}
		for _, v := range obj["templating"].(map[string]any)["list"].([]any) {
			names[v.(map[string]any)["name"].(string)] = true
		}
		for _, want := range []string{"prometheus", "namespace", "woodpecker_id", "log_ns"} {
			if !names[want] {
				t.Errorf("%s: missing template var %q", f, want)
			}
		}
	}
}

var updateConfigMaps = flag.Bool("update", false,
	"regenerate manifests/dashboard-configmaps.yaml from the dashboard templates")

const dashboardConfigMapPath = "manifests/dashboard-configmaps.yaml"

var dashboardConfigMaps = []struct {
	name string
	file string
}{
	{"woodpecker-server-k8s-dashboard", "dashboard_server_k8s.json"},
	{"woodpecker-client-k8s-dashboard", "dashboard_client_k8s.json"},
}

// renderDashboardConfigMaps embeds the dashboard templates verbatim into the
// ConfigMap manifest the Grafana sidecar imports.
func renderDashboardConfigMaps() ([]byte, error) {
	var b strings.Builder
	b.WriteString("# Generated from grafana/templates/*.json — Grafana sidecar imports these.\n")
	for _, cm := range dashboardConfigMaps {
		raw, err := os.ReadFile(filepath.Join("grafana", "templates", cm.file))
		if err != nil {
			return nil, err
		}
		b.WriteString("---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n")
		fmt.Fprintf(&b, "  name: %s\n", cm.name)
		b.WriteString("  namespace: monitoring\n")
		b.WriteString("  labels: { grafana_dashboard: \"1\" }\n")
		b.WriteString("data:\n")
		fmt.Fprintf(&b, "  %s: |\n", cm.file)
		for line := range strings.SplitSeq(strings.TrimRight(string(raw), "\n"), "\n") {
			if line == "" {
				b.WriteString("\n")
				continue
			}
			b.WriteString("    " + line + "\n")
		}
	}
	return []byte(b.String()), nil
}

// TestK8sDashboardConfigMaps_InSync guards the generated manifest against
// drifting from the templates: run_monitor_tests.sh applies the ConfigMaps, so
// a stale manifest silently deploys an outdated dashboard.
func TestK8sDashboardConfigMaps_InSync(t *testing.T) {
	want, err := renderDashboardConfigMaps()
	if err != nil {
		t.Fatalf("render config maps: %v", err)
	}
	if *updateConfigMaps {
		if err := os.WriteFile(dashboardConfigMapPath, want, 0o644); err != nil {
			t.Fatalf("write %s: %v", dashboardConfigMapPath, err)
		}
		t.Logf("regenerated %s", dashboardConfigMapPath)
		return
	}
	got, err := os.ReadFile(dashboardConfigMapPath)
	if err != nil {
		t.Fatalf("read %s: %v", dashboardConfigMapPath, err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("%s is out of sync with grafana/templates/*.json; regenerate with:\n"+
			"\tgo test ./tests/k8s/monitor -run TestK8sDashboardConfigMaps_InSync -update",
			dashboardConfigMapPath)
	}
}

// alertsRowExprs returns every PromQL expression in the dashboard's Alerts row, with template
// variables resolved the way Grafana would with everything selected.
func alertsRowExprs(t *testing.T, file string) map[string][]string {
	t.Helper()
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	var d struct {
		Panels []struct {
			Type   string `json:"type"`
			Title  string `json:"title"`
			Panels []struct {
				Title   string `json:"title"`
				Targets []struct {
					Expr string `json:"expr"`
				} `json:"targets"`
			} `json:"panels"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	// The numeric variables are per-environment ceilings; the rest are selectors that Grafana
	// expands to a match-anything regex when "All" is chosen.
	repl := strings.NewReplacer(
		"$vol_iops_limit", "3000",
		"$vol_bw_limit", "81000000",
		"$node_net_limit", "97000000",
		"$namespace", ".*",
		"$woodpecker_id", ".*",
		"$log_ns", ".*",
		"$log_id", ".*",
		"$server_job", ".*",
	)
	out := map[string][]string{}
	for _, row := range d.Panels {
		if row.Type != "row" || !strings.Contains(row.Title, "Alerts") {
			continue
		}
		for _, p := range row.Panels {
			for _, tg := range p.Targets {
				out[p.Title] = append(out[p.Title], repl.Replace(tg.Expr))
			}
		}
	}
	return out
}

// TestK8sDashboards_AlertPanelsQuery runs every Alerts-row expression against the live
// Prometheus.
//
// It exists because three separate bugs shipped in this row, and none was visible in the JSON:
// a placeholder that never got substituted (a parse error), an RPC name that does not exist
// (AppendEntry rather than AddEntry), and a bucket bound written "256" where Prometheus renders
// it "256.0". Only the first is loud; the other two fail open into an empty panel that looks
// exactly like an idle one.
//
// So the check is in two parts. Every expression must be *accepted* -- that catches the loud
// class outright. And a short list of panels that cannot be idle in a cluster under load must
// also return series -- that is what catches the quiet class. Panels driven by error counters
// are deliberately not on that list: empty is their healthy reading.
func TestK8sDashboards_AlertPanelsQuery(t *testing.T) {
	base := os.Getenv("WP_K8S_PROM_URL")
	if base == "" {
		t.Skip("WP_K8S_PROM_URL unset; run via run_monitor_tests.sh")
	}
	c := NewK8sMonitorCluster(base)
	c.WaitForPrometheusReady(t, 60*time.Second)

	mustHaveData := map[string]bool{
		"R1":    true, // node CPU, memory and volume IOPS
		"R2":    true, // the same five signals, cluster mean
		"R6":    true, // disk free and its projection
		"F4/F5": true, // flush latency -- any write produces these
		"F6":    true, // gRPC handling time for the append RPCs
		"S1":    true, // sync scheduler queue against capacity
		"S5":    true, // node selection counts
		"C1/C2": true, // share of slow appends
		"C3":    true, // pending append ops
		"C4":    true, // write frontier movement
	}
	prefix := func(title string) string {
		if i := strings.Index(title, " · "); i > 0 {
			return title[:i]
		}
		return title
	}

	for _, file := range []string{
		"grafana/templates/dashboard_server_k8s.json",
		"grafana/templates/dashboard_client_k8s.json",
	} {
		panels := alertsRowExprs(t, file)
		if len(panels) == 0 {
			t.Errorf("%s: no Alerts row found", file)
			continue
		}
		for title, exprs := range panels {
			t.Run(title, func(t *testing.T) {
				total := 0
				for _, q := range exprs {
					if msg := c.QueryError(t, q); msg != "" {
						t.Errorf("rejected by Prometheus: %s\n  query: %s", msg, q)
						continue
					}
					if n := c.QuerySeriesCount(t, q); n > 0 {
						total += n
					}
				}
				if mustHaveData[prefix(title)] && total == 0 {
					t.Errorf("no series: this panel is fed by a metric that cannot be idle "+
						"under load, so an empty result means the query does not match what "+
						"is actually exported\n  queries: %v", exprs)
				}
			})
		}
	}
}
