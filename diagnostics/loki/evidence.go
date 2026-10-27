package loki

import (
	"encoding/json"
	"strconv"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

// EventEvidence exports event/finding lineage without querying or reinterpreting
// log patterns. It is shared by source-only and integrated presentations.
type EventEvidence struct {
	Event         Event
	ObservationID string
	FindingID     string
	TimeNS        int64
}

func EventLineage(result d.Result) []EventEvidence {
	var meta struct {
		Schema string `json:"source_schema_version"`
	}
	if json.Unmarshal(result.Metadata, &meta) != nil || meta.Schema != "wpdiag.loki/v1" || result.Source != "loki" {
		return nil
	}
	findings := map[string]d.Finding{}
	for _, f := range result.Findings {
		findings[f.ID] = f
	}
	out := []EventEvidence{}
	for _, o := range result.Observations {
		if o.Kind != "loki_log_page" || o.Status != "observed" {
			continue
		}
		var p page
		if json.Unmarshal(o.Data, &p) != nil {
			continue
		}
		for _, event := range p.Events {
			id := eventFindingID(event)
			ns, err := strconv.ParseInt(event.TimestampNS, 10, 64)
			finding, exists := findings[id]
			if !exists || !d.Has(finding.Supporting, o.ID) || event.ID == "" || err != nil || ns <= 0 {
				continue
			}
			out = append(out, EventEvidence{Event: event, ObservationID: o.ID, FindingID: id, TimeNS: ns})
		}
	}
	return out
}

func eventFindingID(e Event) string { return "loki:finding:" + e.Pattern + ":" + e.StreamID }
