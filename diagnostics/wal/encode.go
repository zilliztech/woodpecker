package wal

import (
	"encoding/json"

	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

// Encode never publishes a truncated report or dangling positive evidence when
// the aggregate report exceeds its byte budget.
func Encode(r Report) []byte {
	r.Evidence = append([]Evidence{}, r.Evidence...)
	for i := range r.Evidence {
		r.Evidence[i].DataState = DataState(r.Evidence[i])
		r.Evidence[i].StatusLabel = DisplayState(r.Evidence[i])
		r.Evidence[i].Facts = append([]prometheus.WALFact{}, r.Evidence[i].Facts...)
		for j := range r.Evidence[i].Facts {
			r.Evidence[i].Facts[j].Samples = nil
		}
	}
	body, err := json.MarshalIndent(r, "", "  ")
	if err != nil || len(body) > 16<<20 {
		r.Status = "budget_exceeded"
		r.Complete = false
		r.Observations = nil
		for i, e := range r.Evidence {
			r.Evidence[i] = unknown(e.ID, "report_output_limit", "Report exceeded its output bound; evidence was withheld rather than truncated")
			r.Evidence[i].DataState = DataState(r.Evidence[i])
			r.Evidence[i].StatusLabel = DisplayState(r.Evidence[i])
		}
		body, _ = json.MarshalIndent(r, "", "  ")
	}
	return append(body, '\n')
}
