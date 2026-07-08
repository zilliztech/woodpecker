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
		if strings.Contains(s, "$cluster") {
			t.Errorf("%s: stray $cluster reference", f)
		}
		names := map[string]bool{}
		for _, v := range obj["templating"].(map[string]any)["list"].([]any) {
			names[v.(map[string]any)["name"].(string)] = true
		}
		for _, want := range []string{"prometheus", "namespace", "log_ns"} {
			if !names[want] {
				t.Errorf("%s: missing template var %q", f, want)
			}
		}
	}
}
