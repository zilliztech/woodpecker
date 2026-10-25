package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/internal/prom"
)

func stallMetrics(t *testing.T, read, eof, write float64) map[string]*prom.MetricFamily {
	t.Helper()
	text := fmt.Sprintf(`# TYPE woodpecker_server_logstore_operations_total counter
woodpecker_server_logstore_operations_total{node_id="node-1",log_id="2",operation="get_batch_entries",status="success"} %g
woodpecker_server_logstore_operations_total{node_id="node-1",log_id="2",operation="get_batch_entries",status="end_of_file"} %g
woodpecker_server_logstore_operations_total{node_id="node-1",log_id="2",operation="add_entry",status="success"} %g
`, read, eof, write)
	families, err := prom.Parse(strings.NewReader(text))
	require.NoError(t, err)
	return families
}

func TestStalledOps_RequiresPersistentMatchingWork(t *testing.T) {
	read := pendingOp{ID: "read-1", Type: "logstore.get_batch_entries", Log: 2}
	write := pendingOp{ID: "write-1", Type: "logstore.add_entry", Log: 2}
	batch := pendingOp{ID: "batch-1", Type: "logstore.add_entry_batch", Log: 2}
	for _, tc := range []struct {
		name          string
		scenario      string
		first, last   []pendingOp
		firstCounters [3]float64
		lastCounters  [3]float64
		wantFinding   bool
	}{
		{name: "idle historical writes", scenario: "stuck-read", firstCounters: [3]float64{0, 0, 5}, lastCounters: [3]float64{0, 0, 5}},
		{name: "writes cannot count as pending reads", scenario: "stuck-read", first: []pendingOp{write}, last: []pendingOp{write}},
		{name: "pending read with no prior success", scenario: "stuck-read", first: []pendingOp{read}, last: []pendingOp{read}, wantFinding: true},
		{name: "write traffic cannot hide stalled read", scenario: "stuck-read", first: []pendingOp{read}, last: []pendingOp{read}, lastCounters: [3]float64{0, 0, 10}, wantFinding: true},
		{name: "read progress", scenario: "stuck-read", first: []pendingOp{read}, last: []pendingOp{read}, firstCounters: [3]float64{5, 0, 0}, lastCounters: [3]float64{6, 0, 0}},
		{name: "EOF is healthy progress", scenario: "stuck-read", first: []pendingOp{read}, last: []pendingOp{read}, lastCounters: [3]float64{0, 1, 0}},
		{name: "counter reset is inconclusive", scenario: "stuck-read", first: []pendingOp{read}, last: []pendingOp{read}, firstCounters: [3]float64{5, 0, 0}},
		{name: "new read is not sustained", scenario: "stuck-read", last: []pendingOp{read}},
		{name: "completed read is not stalled", scenario: "stuck-read", first: []pendingOp{read}},
		{name: "batch write also tracked", scenario: "stuck-write", first: []pendingOp{batch}, last: []pendingOp{batch}, wantFinding: true},
		{name: "pending single write", scenario: "stuck-write", first: []pendingOp{write}, last: []pendingOp{write}, wantFinding: true},
		{name: "write progress", scenario: "stuck-write", first: []pendingOp{write}, last: []pendingOp{write}, lastCounters: [3]float64{0, 0, 1}},
		{name: "read cannot count as pending write", scenario: "stuck-write", first: []pendingOp{read}, last: []pendingOp{read}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first := stallMetrics(t, tc.firstCounters[0], tc.firstCounters[1], tc.firstCounters[2])
			last := stallMetrics(t, tc.lastCounters[0], tc.lastCounters[1], tc.lastCounters[2])
			got := evaluateStalledOps(builtinScenarios[tc.scenario].Rules[0], first, last, "node-1", tc.first, tc.last)
			if tc.wantFinding {
				require.NotNil(t, got)
				require.Contains(t, got.Message, "remained pending")
			} else {
				require.Nil(t, got)
			}
		})
	}
}

func TestMetricsReport_StallEvidence(t *testing.T) {
	for _, tc := range []struct {
		name     string
		ops      string
		code     int
		want     string
		output   string
		scenario string
	}{
		{name: "idle", ops: "[]", code: 200, want: "Result: OK"},
		{name: "pending read", ops: `[{"op_id":"slow-read","op_type":"logstore.get_batch_entries","log_id":2}]`, code: 200, want: "read_stall"},
		{name: "pending read JSON", ops: `[{"op_id":"slow-read","op_type":"logstore.get_batch_entries","log_id":2}]`, code: 200, want: "read_stall", output: "json"},
		{name: "pending read YAML", ops: `[{"op_id":"slow-read","op_type":"logstore.get_batch_entries","log_id":2}]`, code: 200, want: "read_stall", output: "yaml"},
		{name: "pending write", ops: `[{"op_id":"slow-write","op_type":"logstore.add_entry","log_id":2}]`, code: 200, want: "write_stall", scenario: "stuck-write"},
		{name: "registry unavailable", ops: `{}`, code: 503, want: "status 503"},
		{name: "registry malformed", ops: `{}`, code: 200, want: "decode pending operations"},
		{name: "registry null", ops: `null`, code: 200, want: "expected an array"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := healthTestServer(t, map[string]func(http.ResponseWriter, *http.Request){
				"/metrics": func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte("# TYPE woodpecker_server_logstore_operations_total counter\nwoodpecker_server_logstore_operations_total{node_id=\"node-1\",log_id=\"2\",operation=\"add_entry\",status=\"success\"} 5\n"))
				},
				"/admin/runtime/ops": func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(tc.code)
					_, _ = w.Write([]byte(tc.ops))
				},
			})
			defer srv.Close()
			withCliYAML(t, srv.URL)
			root := NewRootCommand()
			buf := new(bytes.Buffer)
			errBuf := new(bytes.Buffer)
			root.SetOut(buf)
			root.SetErr(errBuf)
			scenario := tc.scenario
			if scenario == "" {
				scenario = "stuck-read"
			}
			args := []string{"metrics", "report", "node-1", "--scenario", scenario, "--window", "50ms", "--interval", "5ms", "--node-admin-url", "node-1=" + srv.URL}
			if tc.output != "" {
				args = append(args, "--output", tc.output)
			}
			root.SetArgs(args)
			err := root.Execute()
			if tc.name == "idle" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Contains(t, buf.String()+errBuf.String(), tc.want)
			if strings.HasPrefix(tc.name, "pending") {
				require.Equal(t, 9, wperrors.ExitCodeFor(err))
			}
			if tc.output != "" {
				var report struct {
					Findings []finding `json:"findings" yaml:"findings"`
				}
				if tc.output == "json" {
					require.NoError(t, json.Unmarshal(buf.Bytes(), &report))
				} else {
					require.NoError(t, yaml.Unmarshal(buf.Bytes(), &report))
				}
				require.Len(t, report.Findings, 1)
			}
		})
	}
}

func TestLegacyStallPreservesRegistryOrdering(t *testing.T) {
	ops := []pendingOp{{ID: "z-oldest", Type: "logstore.add_entry", Log: 2}, {ID: "a-younger", Type: "logstore.add_entry", Log: 2}}
	metrics := stallMetrics(t, 0, 0, 5)
	f := evaluateStalledOps(builtinScenarios["stuck-write"].Rules[0], metrics, metrics, "node-1", ops, ops)
	require.NotNil(t, f)
	require.Contains(t, f.Message, "z-oldest")
}
