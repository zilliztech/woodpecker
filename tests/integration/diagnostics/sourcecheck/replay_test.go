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

// This optional regression consumes actual CLI output. Mutated counterexamples
// are replay fixtures, never uploaded to Loki/Jaeger or called producer evidence.
package sourcecheck

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/integrated"
	"github.com/zilliztech/woodpecker/diagnostics/loki"
)

func TestDeployedReceiptCorroboration(t *testing.T) {
	path := os.Getenv("WP_SOURCE_ACCEPTANCE_REPORT")
	if path == "" {
		t.Skip("Explicit retained deployed report required; offline synthetic regressions live in diagnostics/integrated")
	}
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (16<<20)+1))
	require.NoError(t, err)
	require.LessOrEqual(t, len(data), 16<<20)
	var original integrated.Report
	require.NoError(t, json.Unmarshal(data, &original))
	require.Equal(t, integrated.Schema, original.SchemaVersion)
	require.NotEmpty(t, original.Summary.Associations, "Input must contain a genuine deployed association")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	baseline := integrated.Summarize(ctx, original.Runs)
	require.Equal(t, original.Summary, baseline)
	for _, a := range baseline.Associations {
		for _, o := range a.Observations {
			require.NotEqual(t, "prometheus", o.ModuleID)
			require.NotEqual(t, "wpcli", o.ModuleID)
		}
	}
	copyRuns := func() []integrated.Run {
		var r integrated.Report
		require.NoError(t, json.Unmarshal(data, &r))
		return r.Runs
	}
	find := func(r []integrated.Run, module string) int {
		for i := range r {
			if r[i].ModuleID == module {
				return i
			}
		}
		t.Fatal("Required real module absent: " + module)
		return -1
	}
	t.Run("duplicate_report_is_same_evidence", func(t *testing.T) {
		r := copyRuns()
		r = append(r, r[find(r, "trace")])
		s := integrated.Summarize(ctx, r)
		require.Equal(t, baseline.Associations, s.Associations)
		require.Equal(t, baseline.Findings, s.Findings)
		require.Contains(t, s.Gaps, d.Gap{Code: "duplicate_module_report_deduplicated", Detail: "trace"})
	})
	for _, name := range []string{"wrong_identity", "outside_actual_span_time", "wrong_deployment", "conflicting_report", "dangling_lineage", "current_is_not_history"} {
		t.Run(name, func(t *testing.T) {
			r := copyRuns()
			li, ti := find(r, "loki"), find(r, "trace")
			switch name {
			case "wrong_identity", "outside_actual_span_time":
				for i := range r[li].Report.Results[0].Observations {
					o := &r[li].Report.Results[0].Observations[i]
					if o.Kind != "loki_log_page" {
						continue
					}
					var page map[string]json.RawMessage
					require.NoError(t, json.Unmarshal(o.Data, &page))
					var events []loki.Event
					require.NoError(t, json.Unmarshal(page["events"], &events))
					for j := range events {
						if name == "wrong_identity" {
							events[j].TraceID = strings.Repeat("f", 32)
						} else {
							events[j].TimestampNS = "1000000000"
						}
					}
					page["events"], err = json.Marshal(events)
					require.NoError(t, err)
					o.Data, err = json.Marshal(page)
					require.NoError(t, err)
				}
			case "wrong_deployment":
				var metadata map[string]any
				require.NoError(t, json.Unmarshal(r[li].Report.Results[0].Metadata, &metadata))
				metadata["bound_deployment"] = "different-deployment"
				r[li].Report.Results[0].Metadata, err = json.Marshal(metadata)
				require.NoError(t, err)
			case "conflicting_report":
				conflict := r[ti]
				conflict.Role = "conflicting-role"
				r = append(r, conflict)
			case "dangling_lineage":
				require.NotEmpty(t, r[li].Report.Results[0].Findings)
				r[li].Report.Results[0].Findings[0].Supporting = []string{"absent-observation"}
			case "current_is_not_history":
				r[ti].Role = "current_context"
			}
			require.Empty(t, integrated.Summarize(ctx, r).Associations)
		})
	}
}
