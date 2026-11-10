package wal

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
	p "github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

func TestInspectorExplainsActualPolicyAndQualifyingMinuteData(t *testing.T) {
	policy := Policy{LatencyMS: 250, MinObservations: 100, SustainMS: 180000, MaxGapMS: 30000}
	var def Definition
	for _, item := range Definitions().Evidence {
		if item.ID == "R02" {
			def = item
		}
	}
	lower, upper := 256.0, 512.0
	windows := []p.WALWindow{}
	for i := 0; i < 3; i++ {
		windows = append(windows, p.WALWindow{FromMS: int64(i) * 60000, ToMS: int64(i+1) * 60000, Count: 200, Lower: &lower, Upper: &upper, Above: true, Matched: true})
	}
	r := Report{SchemaVersion: "wpdiag.wal/v1", Policy: policy, Evidence: []Evidence{{ID: "R02", Status: "abnormal", Value: "256–512 ms", Facts: []p.WALFact{{Assessment: "abnormal", Reading: "256–512 ms", Lower: &lower, Upper: &upper, Count: 600, AboveForMS: 180000, Windows: windows}}}}}
	detail, err := Inspect(r, "R02")
	require.NoError(t, err)
	require.Contains(t, detail.Criterion, "250 ms")
	require.Contains(t, detail.Meaning, "Earlier retry/tail waits are excluded")
	require.Len(t, detail.Anomalies, 3)
	require.Len(t, detail.Curves, 1)
	require.Equal(t, 200.0, *detail.Curves[0].Points[0].Count)
	require.True(t, detail.Curves[0].Points[0].Matched)
	before := Encode(r)
	_, err = Inspect(r, "R02")
	require.NoError(t, err)
	require.Equal(t, before, Encode(r), "Inspection must not mutate the stored result")
	_, err = Inspect(r, "S07")
	require.Error(t, err)
	description := Describe(def, policy)
	require.Equal(t, detail.Formula, description.Formula)
}

func TestRawCurvesKeepRateUnitsResetGapsAndSeparateSources(t *testing.T) {
	policy := Policy{MaxGapMS: 30000}
	def := Definition{ID: "R01", Kind: "rate", Unit: "bytes/s"}
	s := p.Series{Labels: map[string]string{"__name__": "woodpecker_client_reader_bytes_read"}, Points: []p.Point{{TimeMS: 10000, Value: 0}, {TimeMS: 20000, Value: 20 << 20}, {TimeMS: 30000, Value: 1}, {TimeMS: 100000, Value: 200}}}
	curve := rawCurve(def, s, policy)
	require.Equal(t, "MiB/s", curve.Unit)
	require.Equal(t, 2.0, *curve.Points[0].Value)
	require.Nil(t, curve.Points[1].Value)
	require.Nil(t, curve.Points[2].Value)
	r := Report{SchemaVersion: "wpdiag.wal/v1", Policy: policy, Evidence: []Evidence{{ID: "R01", Status: "context", ObservationIDs: []string{"own", "dependency"}}}}
	data, _ := json.Marshal(p.Dataset{Series: []p.Series{s}})
	other, _ := json.Marshal(p.Dataset{Series: []p.Series{{Labels: map[string]string{"__name__": "woodpecker_client_quorum_read_total"}}}})
	r.Observations = []d.Observation{{ID: "own", Status: "observed", Data: data}, {ID: "dependency", Status: "observed", Data: other}}
	detail, err := Inspect(r, "R01")
	require.NoError(t, err)
	require.Len(t, detail.Curves, 1)
	require.Empty(t, detail.Anomalies)
}

func TestResourceProofShowsPastHighIntervalAfterRecovery(t *testing.T) {
	from := time.Unix(1800000000, 0)
	s := p.Series{ID: "cpu", Labels: map[string]string{"__name__": "woodpecker_server_system_cpu_usage", "instance": "wp:9091", "node_id": "n"}}
	for i := 0; i <= 24; i++ {
		v := 95.0
		if i > 20 {
			v = 10
		}
		s.Points = append(s.Points, p.Point{TimeMS: from.Add(time.Duration(i) * 10 * time.Second).UnixMilli(), Value: v})
	}
	rows := resourceAnomalies(Definition{ID: "H01"}, s, Policy{SustainMS: 180000, MaxGapMS: 30000})
	require.Len(t, rows, 1)
	require.Contains(t, rows[0].Value, "95%")
	require.Equal(t, from.Add(200*time.Second).UnixMilli(), rows[0].ToMS)
}

func TestAllCatalogDescriptionsAreConciseAndDeferredDoesNotInventData(t *testing.T) {
	policy := Policy{LatencyMS: 100, MinObservations: 100, SustainMS: 180000, MaxGapMS: 30000}
	for _, definition := range Definitions().Evidence {
		description := Describe(definition, policy)
		require.NotEmpty(t, description.Meaning, definition.ID)
		require.NotEmpty(t, description.Formula, definition.ID)
		require.NotEmpty(t, description.Criterion, definition.ID)
	}
	detail, err := Inspect(Report{SchemaVersion: "wpdiag.wal/v1", Policy: policy, Evidence: []Evidence{{ID: "T02", Status: "unknown", Code: "deferred_evidence"}}}, "T02")
	require.NoError(t, err)
	require.Empty(t, detail.Curves)
	require.Empty(t, detail.Anomalies)
	require.Contains(t, detail.Conclusion, "not implemented")
}
