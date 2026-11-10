package wal

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
	p "github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

func config(t *testing.T, text string) d.Config {
	t.Helper()
	c, e := d.ParseConfig([]byte("schema_version: wpdiag.config/v1\nsources: {}\n"+text), t.TempDir())
	require.NoError(t, e)
	return c
}

func TestCatalogAndPlanningHaveExplicitBoundaries(t *testing.T) {
	c := Definitions()
	require.Len(t, c.Problems, 7)
	require.Len(t, c.Evidence, 49)
	defs := map[string]Definition{}
	for _, d := range c.Evidence {
		require.NotEmpty(t, d.Disposition)
		require.NotEmpty(t, d.Limits)
		require.NotContains(t, d.Title, "Demo")
		defs[d.ID] = d
	}
	for _, p := range c.Problems {
		for _, id := range append(p.Evidence, p.Extension...) {
			require.Contains(t, defs, id)
		}
	}
	target := Target{LogNamespace: "tenant", LogID: "1"}
	q := plan(target, []string{"W01", "W02", "W04", "H01", "T01"}, false)
	require.Len(t, q.Queries, 3)
	require.Contains(t, q.Skip["H01"], "node")
	for _, query := range q.Queries {
		require.Equal(t, "tenant", query.Match["log_ns"])
		require.Equal(t, "1", query.Match["log_id"])
	}
	require.Empty(t, plan(target, []string{"MV01"}, true).Queries)
}

func TestMissingSourcesNeverPassAndMetadataIsCurrent(t *testing.T) {
	c := config(t, "workbench:\n  targets:\n    t: {label: My log}\n")
	q := Request{Problem: "write_slow", Target: "t", From: time.Now().Add(-5 * time.Minute), To: time.Now().Add(-time.Second)}
	r := Run(context.Background(), c, q, nil)
	require.True(t, r.Complete)
	for _, e := range r.Evidence {
		require.Equal(t, "unknown", e.Status)
		require.Nil(t, e.Finding)
	}
	q.Problem = "metadata"
	q.From = time.Time{}
	q.To = time.Time{}
	r = Run(context.Background(), c, q, nil)
	require.Equal(t, "snapshot", r.TimeRange.Mode)
	require.NotNil(t, r.TimeRange.ObservedAt)
	require.Len(t, r.Evidence, 4)
	for _, e := range r.Evidence {
		require.Equal(t, "unknown", e.Status)
	}
}

func TestStallNeedsSamePopulationActivityAndPending(t *testing.T) {
	labels := map[string]string{"instance": "sn:9091", "log_ns": "tenant", "log_id": "1"}
	f := p.WALFact{Labels: labels, Minimum: 5, First: 5, Last: 5, ObservedFromMS: 10000, ObservedToMS: 140000}
	es := map[string]Evidence{"W07": {ID: "W07", Status: "context", Facts: []p.WALFact{f}}, "W05": {ID: "W05", Status: "context", Facts: []p.WALFact{f}}, "W04": {ID: "W04", Status: "context", Facts: []p.WALFact{{Labels: labels, Delta: 1, ObservedFromMS: 10000, ObservedToMS: 140000}}}}
	corroborate(es, Policy{SustainMS: 180000}, 2*time.Minute)
	require.Equal(t, "abnormal", es["W07"].Status)
	es["W07"] = Evidence{ID: "W07", Status: "context", Facts: []p.WALFact{f}}
	es["W04"] = Evidence{Facts: []p.WALFact{{Labels: map[string]string{"instance": "other", "log_ns": "tenant", "log_id": "1"}, Delta: 1}}}
	corroborate(es, Policy{SustainMS: 180000}, 2*time.Minute)
	require.Equal(t, "context", es["W07"].Status)
}

func TestAgentPacketIsStoredFindingsOnly(t *testing.T) {
	r := Report{SchemaVersion: "wpdiag.wal/v1", Problem: "write_stuck", RunID: "r", Evidence: []Evidence{{ID: "W07", Status: "abnormal", ObservationIDs: []string{"o1"}}, {ID: "W04", Status: "context", ObservationIDs: []string{"o2"}}, {ID: "S05", Status: "clear"}, {ID: "T02", Status: "unknown", Note: "Deferred"}}, Observations: []d.Observation{{ID: "o1", Data: json.RawMessage(`{"private_raw_matrix":true}`)}, {ID: "o2"}}}
	body := Encode(r)
	packet, err := PacketFromBytes(body)
	require.NoError(t, err)
	require.Len(t, packet.Findings, 1)
	require.Len(t, packet.Supporting, 1)
	require.Len(t, packet.Gaps, 1)
	require.Len(t, packet.Observations, 2)
	encoded, _ := json.Marshal(packet)
	require.NotContains(t, string(encoded), "private_raw_matrix")
	require.Len(t, packet.ReportSHA256, 64)
	_, err = PacketFromBytes([]byte(`{"schema_version":"wpdiag.run/v1"}`))
	require.Error(t, err)
	a := Analyze(context.Background(), AgentConfig{Enabled: true}, Packet{}, nil)
	require.Equal(t, "no_abnormal_findings", a.Code)
}

func TestPrivateBindingsAreNotPublished(t *testing.T) {
	c := config(t, `workbench:
  targets:
    t:
      prometheus: local
      metadata: {etcd: secret-endpoint, prefix: private-prefix}
      milvus: {prometheus: local, channel: private-channel}
`)
	s, err := LoadSettings(c)
	require.NoError(t, err)
	b, err := json.Marshal(PublicTargets(s))
	require.NoError(t, err)
	require.False(t, strings.Contains(string(b), "secret-endpoint"))
	require.NotContains(t, string(b), "private-prefix")
	require.NotContains(t, string(b), "private-channel")
}

func TestIOWaitIsCumulativeSecondsNotUtilization(t *testing.T) {
	from := time.Unix(1800000000, 0)
	to := from.Add(5 * time.Minute)
	def := Definition{ID: "H02", Kind: "io_wait_rate"}
	labels := map[string]string{"instance": "wp:9091", "node_id": "node-1"}
	s := p.Series{ID: "io", Labels: labels, AllLabels: labels}
	for i := 0; i <= 30; i++ {
		s.Points = append(s.Points, p.Point{TimeMS: from.Add(time.Duration(i) * 10 * time.Second).UnixMilli(), Value: 1800 + float64(i)/10})
	}
	data := map[string]p.Dataset{"io": {From: from, To: to, Series: []p.Series{s}}}
	got := evaluate(def, []string{"io"}, data, Policy{MaxGapMS: 30000}, from, to)
	require.Equal(t, "context", got.Status)
	require.Contains(t, got.Value, "s/s")
	require.NotContains(t, got.Value, "%")
	require.Contains(t, got.Note, "not a percentage")
	s.Points[20].Value = 0
	data["io"] = p.Dataset{From: from, To: to, Series: []p.Series{s}}
	got = evaluate(def, []string{"io"}, data, Policy{MaxGapMS: 30000}, from, to)
	require.Equal(t, "unknown", got.Status)
	require.Equal(t, "io_wait_reset", got.Code)
}
