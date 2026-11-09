package checks

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/evidence"
)

func TestWoodpeckerOnlyCatalogAndScenarios(t *testing.T) {
	defs := Definitions(evidence.Policy{})
	require.Len(t, defs, 55)
	seen := map[string]bool{}
	metrics, trace := 0, 0
	for _, def := range defs {
		require.NotEmpty(t, def.Name)
		require.False(t, seen[def.ID])
		seen[def.ID] = true
		require.False(t, strings.HasPrefix(def.ID, "MV"))
		require.NotContains(t, def.Name, "milvus")
		for _, metric := range def.Metrics {
			require.NotContains(t, metric, "milvus_")
		}
		if def.Source == "metrics" {
			metrics++
		} else if def.Source == "trace" {
			trace++
		} else {
			t.Fatalf("unsupported source %s", def.Source)
		}
	}
	require.Equal(t, 39, metrics)
	require.Equal(t, 16, trace)
	for _, scenario := range Scenarios(evidence.Policy{}) {
		for _, id := range scenario.Checks {
			require.True(t, seen[id], "%s / %s", scenario.ID, id)
		}
	}
}

func TestRetiredMilvusRequestsNeverQuerySources(t *testing.T) {
	var calls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }))
	defer backend.Close()
	var settings yaml.Node
	require.NoError(t, settings.Encode(map[string]any{"endpoint": backend.URL, "deployment": "fixture", "match_labels": map[string]string{"cluster": "fixture"}}))
	c := d.Config{Sources: map[string]d.SourceConfig{"prometheus": {Connections: map[string]d.Connection{"local": {Settings: settings}}}}}
	var targets yaml.Node
	require.NoError(t, targets.Encode(map[string]evidence.Target{"local": {Prometheus: "local"}}))
	c.Targets = targets
	now := time.Now().UTC()
	requests := []Request{
		{Source: "metrics", Check: "MV01"},
		{Source: "metrics", Check: "milvus-wal-append-latency"},
		{Scenario: "all", IncludeMilvus: true},
		{Scenario: "slow-writes", Bindings: &Bindings{Client: &Scope{Metrics: "local", Milvus: &evidence.MilvusBinding{Prometheus: "local", Channel: "channel"}}}},
	}
	for _, q := range requests {
		q.From = now.Add(-time.Minute)
		q.To = now
		q.Target = "local"
		r := Run(context.Background(), c, q, nil)
		require.Equal(t, "invalid", r.Status)
		require.NotEmpty(t, r.Problems)
		require.Empty(t, r.Observations)
	}
	require.Zero(t, calls.Load())
}
