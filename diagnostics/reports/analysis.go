package reports

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/zilliztech/woodpecker/diagnostics/wal"
)

// AnalysisPacket adds only a selected prior interpretation to immutable evidence.
// It never collects new data or treats a model's previous answer as an observation.
func AnalysisPacket(packet wal.Packet, previousID string, previous *wal.Analysis, question string) (wal.Packet, error) {
	question = strings.TrimSpace(question)
	if len(question) > 2000 {
		return wal.Packet{}, fmt.Errorf("follow-up question exceeds 2000 bytes")
	}
	packet.Question = question
	packet.PreviousAnalysis = nil
	if previous != nil {
		if previous.Status != "completed" || strings.TrimSpace(previous.Text) == "" {
			return wal.Packet{}, fmt.Errorf("choose a completed analysis with an answer")
		}
		if previous.ReportSHA256 == "" || previous.ReportSHA256 != packet.ReportSHA256 || previous.RunID != packet.RunID {
			return wal.Packet{}, fmt.Errorf("previous analysis belongs to different diagnostic evidence")
		}
		if len(previous.Text) > 32<<10 {
			return wal.Packet{}, fmt.Errorf("previous analysis exceeds 32 KiB")
		}
		packet.PreviousAnalysis = &wal.AnalysisContext{ID: previousID, Provider: previous.Provider, Text: previous.Text, FinishedAt: previous.FinishedAt}
		packet.Instructions = append(append([]string(nil), packet.Instructions...), "The previous analysis is an interpretation, not new evidence. Reassess it against this original report. Address the follow-up question without querying systems or executing tools.")
	}
	body, err := json.Marshal(packet)
	if err != nil || len(body) > 256<<10 {
		return wal.Packet{}, fmt.Errorf("findings and analysis context exceed 256 KiB")
	}
	return packet, nil
}
