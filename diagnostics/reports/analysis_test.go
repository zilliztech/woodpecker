package reports

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/diagnostics/checks"
	"github.com/zilliztech/woodpecker/diagnostics/evidence"
	"github.com/zilliztech/woodpecker/diagnostics/wal"
)

func TestAnalysisContextUsesSameImmutableEvidence(t *testing.T) {
	body := checks.Encode(checks.Report{SchemaVersion: checks.ReportSchema, RunID: "run-a", Checks: []checks.Result{{Evidence: evidence.Evidence{ID: "S06", Status: "abnormal", Value: "92%"}}}})
	packet, err := Packet(body)
	require.NoError(t, err)
	original, _ := json.Marshal(packet)
	previous := wal.Analysis{Status: "completed", Provider: "codex", Text: "Disk usage is high; symptom impact remains unconfirmed.", ReportSHA256: packet.ReportSHA256, RunID: packet.RunID, FinishedAt: time.Now()}
	followup, err := AnalysisPacket(packet, "analysis-a", &previous, "  What evidence would confirm the impact?  ")
	require.NoError(t, err)
	require.Equal(t, packet.Findings, followup.Findings)
	require.Equal(t, packet.ReportSHA256, followup.ReportSHA256)
	require.Equal(t, previous.Text, followup.PreviousAnalysis.Text)
	require.Equal(t, "analysis-a", followup.PreviousAnalysis.ID)
	require.Equal(t, "What evidence would confirm the impact?", followup.Question)
	unchanged, _ := json.Marshal(packet)
	require.Equal(t, original, unchanged)
	for _, tc := range []struct {
		name   string
		change func(*wal.Analysis)
	}{
		{"other report", func(a *wal.Analysis) { a.ReportSHA256 = "different" }},
		{"other run", func(a *wal.Analysis) { a.RunID = "run-b" }},
		{"legacy unbound answer", func(a *wal.Analysis) { a.ReportSHA256 = "" }},
		{"failed answer", func(a *wal.Analysis) { a.Status = "failed" }},
		{"empty answer", func(a *wal.Analysis) { a.Text = " " }},
		{"oversized answer", func(a *wal.Analysis) { a.Text = strings.Repeat("x", (32<<10)+1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := previous
			tc.change(&bad)
			_, err := AnalysisPacket(packet, "previous", &bad, "")
			require.Error(t, err)
		})
	}
	_, err = AnalysisPacket(packet, "", nil, strings.Repeat("q", 2001))
	require.ErrorContains(t, err, "2000")
	packet.Context = json.RawMessage(`"` + strings.Repeat("x", 256<<10) + `"`)
	_, err = AnalysisPacket(packet, "", nil, "")
	require.ErrorContains(t, err, "256 KiB")
}
