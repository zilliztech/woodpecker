package prometheus

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func TestQuerySourceUsesSharedRulesAndExplicitPolicies(t *testing.T) {
	server := serve(t, nil)
	for _, scope := range []string{"milvus", "e2e"} {
		engine, err := d.NewEngine(QueryModule{})
		require.NoError(t, err)
		c := connection(t, server.URL, func(s *Settings) { s.Limits.MaxQueries = 24; s.Policy.LatencyMS["milvus.query.queue"] = 1000000 })
		cfg := d.Config{SchemaVersion: d.ConfigSchema, Sources: map[string]d.SourceConfig{"prometheus": {Connections: map[string]d.Connection{"fixture": c}}}}
		r := request(scope, "read")
		r.Selection.Stage = "query"
		report := engine.Run(context.Background(), cfg, r)
		require.Equal(t, "completed", report.Status)
		require.Len(t, report.Results[0].Observations, len(QueryCatalog()))
		require.Empty(t, report.Assessments)
		for _, f := range report.Results[0].Findings {
			if f.RuleID == "milvus.query.queue.latency" {
				require.Equal(t, "within_configured_latency_for_observed_completions", f.Outcome)
			}
		}
		for _, o := range report.Results[0].Observations {
			var data Dataset
			require.NoError(t, json.Unmarshal(o.Data, &data))
			for _, fact := range data.Facts {
				require.NotEmpty(t, fact.ExporterKey)
			}
		}
	}
}

func TestQueryDefaultBudgetAndSourceOnlyGaugeCoverage(t *testing.T) {
	c, err := d.ParseConfig([]byte("schema_version: wpdiag.config/v1\nsources:\n  prometheus:\n    connections:\n      test:\n        settings: {endpoint: 'http://127.0.0.1:1', deployment: test, match_labels: {cluster: test}}\n"), "")
	require.NoError(t, err)
	settings, err := settingsFor(c.Sources["prometheus"].Connections["test"], queryPlan)
	require.NoError(t, err)
	require.Equal(t, 24, settings.Limits.MaxQueries)
	require.GreaterOrEqual(t, len(settings.Policy.LatencyMS), len(queryThresholds()))
	r := request("milvus", "read")
	sig := querySignals()[4]
	data := Dataset{Signal: sig.ID, From: *r.Selection.Window.From, To: *r.Selection.Window.To, Notes: []string{}, Series: []Series{{ID: "single", Labels: map[string]string{"instance": "node"}, AllLabels: map[string]string{"instance": "node"}, Points: []Point{{TimeMS: 1001000, Value: 1}}}}}
	findings := evaluate(&data, sig, r, settings.Policy, "query")
	require.Equal(t, "inconclusive", findings[0].Outcome)
	require.Contains(t, findings[0].Unknown, "insufficient_scrapes")
}
