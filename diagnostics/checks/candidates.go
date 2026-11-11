package checks

import (
	"github.com/zilliztech/woodpecker/cmd/wpcli/candidates"
	d "github.com/zilliztech/woodpecker/diagnostics"
)

// RememberInputs records only parameters consumed by successful source queries.
// Health verdicts and the presence of samples do not decide connection success.
func RememberInputs(c d.Config, report Report) error {
	values := candidates.Values{}
	add := func(key, value string) {
		if value != "" {
			values[key] = append(values[key], value)
		}
	}
	defs := Definitions(report.Policy)
	for _, result := range report.Checks {
		if result.Execution != "completed" {
			continue
		}
		executed := false
		for _, q := range result.Queries {
			executed = executed || q.Executed
		}
		if !executed {
			continue
		}
		var def Definition
		for _, v := range defs {
			if v.ID == result.ID {
				def = v
				break
			}
		}
		if def.ID == "" {
			continue
		}
		bindings := BindingsForChecks(report.Request.Bindings, []Definition{def})
		if bindings == nil {
			continue
		}
		scope := bindings.scope(CheckComponent(def))
		if scope == nil {
			continue
		}
		ref, endpoint, source := scope.Metrics, scope.MetricsEndpoint, "prometheus"
		if result.Source == "trace" {
			ref, endpoint, source = scope.Trace, scope.TraceEndpoint, "trace"
		}
		if endpoint == "" {
			var settings struct {
				Endpoint string `yaml:"endpoint"`
			}
			conn := c.Sources[source].Connections[ref]
			_ = conn.Settings.Decode(&settings)
			endpoint = settings.Endpoint
		}
		add(result.Source+"_endpoint", endpoint)
		add("log_namespace", scope.LogNamespace)
		add("log_id", scope.LogID)
		add("reader_name", scope.ReaderName)
		add("node_id", scope.NodeID)
		add("path", scope.Path)
		if result.Source == "trace" {
			add("trace_id", report.Request.TraceID)
			add("request_id", report.Request.RequestID)
		}
	}
	return candidates.Save(values)
}
