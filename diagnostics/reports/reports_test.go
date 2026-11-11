package reports

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/diagnostics/checks"
	"github.com/zilliztech/woodpecker/diagnostics/evidence"
)

func TestPacketKeepsIndependentFindingsAndGapsWithoutRawData(t *testing.T) {
	r := checks.Report{SchemaVersion: checks.ReportSchema, RunID: "run", Status: "partial", Request: checks.Request{Scenario: "slow-writes", Details: "all"}, Checks: []checks.Result{
		{Evidence: evidence.Evidence{ID: "S06", Status: "abnormal", Value: "92%", ObservationIDs: []string{"metric"}}, Source: "metrics", Step: "server-disk", Rule: "usage >= watermark", Coverage: []string{"Cause unconfirmed"}},
		{Evidence: evidence.Evidence{ID: "TS01", Status: "abnormal", Value: "2000 ms", ObservationIDs: []string{"trace"}}, Source: "trace", Step: "server-append", Rule: "duration > 1000 ms", IdentityScope: "service", Coverage: []string{"No log association"}},
		{Evidence: evidence.Evidence{ID: "W01", Status: "unknown", Code: "insufficient_observations", DataState: "no_completed_samples", StatusLabel: "No completed samples", ObservationIDs: []string{"q"}}, Source: "metrics"},
	}}
	body := checks.Encode(r)
	packet, err := Packet(body)
	require.NoError(t, err)
	require.Len(t, packet.Findings, 2)
	require.Len(t, packet.Gaps, 1)
	require.Equal(t, "No completed samples", packet.Gaps[0].StatusLabel)
	require.Contains(t, string(packet.Context), "summary")
	require.Contains(t, packet.Findings[1].Limits, "service")
	require.Len(t, packet.ReportSHA256, 64)
	require.NotContains(t, string(mustJSON(t, packet)), "curves")
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func TestInspectionDoesNotPretendOmittedDetailExists(t *testing.T) {
	r := checks.Report{SchemaVersion: checks.ReportSchema, Request: checks.Request{Details: "none"}, Checks: []checks.Result{{Evidence: evidence.Evidence{ID: "W01"}, Name: "writer-latency"}}}
	_, err := Inspection(checks.Encode(r), "W01")
	require.ErrorContains(t, err, "omitted")
}

func TestPacketIncludesComponentFiltersWithoutMetadataLocators(t *testing.T) {
	r := checks.Report{SchemaVersion: checks.ReportSchema, Request: checks.Request{Bindings: &checks.Bindings{Client: &checks.Scope{Name: "client-a", LogNamespace: "metrics-namespace", LogID: "7", Metadata: checks.ClientMetadata{Etcd: "http://metadata-only.invalid:2379", Prefix: "metadata-only-prefix"}}, Server: &checks.Scope{Name: "server-b", NodeID: "node-b"}}}}
	body, err := json.Marshal(r)
	require.NoError(t, err)
	p, err := Packet(body)
	require.NoError(t, err)
	require.Contains(t, string(p.Context), `"component_scopes"`)
	require.Contains(t, string(p.Context), "metrics-namespace")
	require.Contains(t, string(p.Context), "node-b")
	require.NotContains(t, string(p.Context), "metadata-only")
}

func TestRetiredMilvusEvidenceCannotBeAnalyzed(t *testing.T) {
	for _, body := range []string{`{"schema_version":"wp.diag/v1","checks":[{"id":"MV01","source":"metrics"}]}`, `{"schema_version":"wpdiag.wal/v1","evidence":[{"id":"MV03"}]}`, `{"schema_version":"wp.diag/v1","checks":[{"id":"custom","step":"milvus-wal"}]}`} {
		_, err := Packet([]byte(body))
		require.ErrorContains(t, err, "retired")
	}
}
