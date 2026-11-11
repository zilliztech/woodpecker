package wal

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

func TestEveryCheckHasExecutableSelectionAndDependencies(t *testing.T) {
	for _, def := range Definitions().Evidence {
		q, err := RequestForCheck(def.ID)
		require.NoError(t, err, def.ID)
		ids, err := SelectedChecks(q)
		require.NoError(t, err)
		require.Contains(t, ids, def.ID)
		for _, dep := range def.Dependencies {
			require.Contains(t, ids, dep)
		}
	}
	q, err := RequestForCheck("W07")
	require.NoError(t, err)
	ids, err := SelectedChecks(q)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"W07", "W04", "W05"}, ids)
	_, err = RequestForCheck("query")
	require.Error(t, err)
}

func TestAvailabilityIsNotAMeasuredZeroOrAHealthVerdict(t *testing.T) {
	for _, tc := range []struct {
		e            Evidence
		state, label string
	}{
		{unknown("R01", "no_matching_series", ""), "no_data", "No data"},
		{unknown("R02", "insufficient_observations", ""), "no_completed_samples", "No completed samples"},
		{Evidence{Status: "unknown", Code: "insufficient_observations", Facts: []prometheus.WALFact{{Count: 5}}}, "insufficient_samples", "Insufficient samples"},
		{unknown("R02", "source_unavailable", ""), "source_unavailable", "Source unavailable"},
		{Evidence{Status: "context", Value: "0 MiB/s"}, "available", "Context"},
	} {
		require.Equal(t, tc.state, DataState(tc.e))
		require.Equal(t, tc.label, DisplayState(tc.e))
	}
}

func TestAgentPacketPreservesAvailabilityFromOlderReports(t *testing.T) {
	body, err := json.Marshal(Report{SchemaVersion: "wpdiag.wal/v1", Evidence: []Evidence{unknown("R02", "insufficient_observations", ""), unknown("R01", "no_matching_series", "")}})
	require.NoError(t, err)
	packet, err := PacketFromBytes(body)
	require.NoError(t, err)
	require.Len(t, packet.Gaps, 2)
	require.Equal(t, "no_completed_samples", packet.Gaps[0].DataState)
	require.Equal(t, "No completed samples", packet.Gaps[0].StatusLabel)
	require.Equal(t, "no_data", packet.Gaps[1].DataState)
	require.Equal(t, "No data", packet.Gaps[1].StatusLabel)
}
