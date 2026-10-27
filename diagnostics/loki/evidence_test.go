package loki

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func TestEventLineageKeepsEventIdentityAndOriginalFinding(t *testing.T) {
	a := Event{ID: "event-a", Pattern: "wp.disk_pressure", StreamID: "pod-a", TimestampNS: "1000000000000", TraceID: "11111111111111111111111111111111"}
	b := a
	b.ID = "event-b"
	b.TraceID = "22222222222222222222222222222222"
	data, err := json.Marshal(page{Events: []Event{a, b}})
	require.NoError(t, err)
	r := d.NewResult("loki")
	r.Metadata = json.RawMessage(`{"source_schema_version":"wpdiag.loki/v1"}`)
	r.Observations = []d.Observation{{ID: "page-a", Kind: "loki_log_page", Status: "observed", Data: data}}
	r.Findings = []d.Finding{{ID: eventFindingID(a), Supporting: []string{"page-a"}}}
	refs := EventLineage(r)
	require.Len(t, refs, 2)
	require.Equal(t, a, refs[0].Event)
	require.Equal(t, b, refs[1].Event)
	require.Equal(t, refs[0].FindingID, refs[1].FindingID)
	require.Equal(t, "page-a", refs[0].ObservationID)
	require.EqualValues(t, 1000000000000, refs[0].TimeNS)
	r.Findings[0].Supporting = []string{"unrelated-page"}
	require.Empty(t, EventLineage(r), "same stream does not establish page support")
	r.Metadata = json.RawMessage(`{"source_schema_version":"wpdiag.loki/v2"}`)
	require.Empty(t, EventLineage(r), "unknown schema must not be guessed")
}
