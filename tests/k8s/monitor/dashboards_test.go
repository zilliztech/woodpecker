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
