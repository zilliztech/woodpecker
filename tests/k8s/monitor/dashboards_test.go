package monitor

import (
	"encoding/json"
	"os"
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
		names := map[string]bool{}
		for _, v := range obj["templating"].(map[string]any)["list"].([]any) {
			names[v.(map[string]any)["name"].(string)] = true
		}
		for _, want := range []string{"prometheus", "cluster", "namespace", "log_ns"} {
			if !names[want] {
				t.Errorf("%s: missing template var %q", f, want)
			}
		}
	}
}
