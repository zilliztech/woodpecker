package checks

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/diagnostics/evidence"
)

func TestEvidenceParameterContract(t *testing.T) {
	for _, tc := range []struct {
		id                 string
		required, optional []string
	}{
		{"W01", []string{"window"}, []string{"log_namespace", "log_id"}},
		{"R04", []string{"window"}, []string{"log_namespace", "log_id", "reader_name"}},
		{"S01", []string{"window"}, []string{"log_namespace", "log_id", "node_id"}},
		{"S06", []string{"window", "node_id"}, []string{"path"}},
		{"H01", []string{"window", "node_id"}, nil},
	} {
		t.Run(tc.id, func(t *testing.T) {
			d, ok := Lookup("metrics", tc.id, evidence.Policy{})
			require.True(t, ok)
			require.Equal(t, tc.required, d.Requires.Parameters)
			require.Equal(t, tc.optional, d.Requires.OptionalParameters)
		})
	}
	d, ok := Lookup("trace", "TW01", evidence.Policy{})
	require.True(t, ok)
	require.Equal(t, "identity_tags.log_ns", d.Requires.ConditionalParameters["log_namespace"])
	require.Equal(t, "request_id_tag", d.Requires.ConditionalParameters["request_id"])
}

func TestNodeCheckIgnoresUnrelatedCachedLogAndMetadataFields(t *testing.T) {
	d, ok := Lookup("metrics", "H01", evidence.Policy{})
	require.True(t, ok)
	input := &Bindings{Server: &Scope{Metrics: "local", NodeID: "node-1", LogID: "cached-invalid-log", ReaderName: "cached-reader", Path: "unused-path", Metadata: ClientMetadata{Etcd: "unused", Bucket: "unused"}}}
	out := BindingsForChecks(input, []Definition{d})
	require.Equal(t, "node-1", out.Server.NodeID)
	require.Empty(t, out.Server.LogID)
	require.Empty(t, out.Server.ReaderName)
	require.Empty(t, out.Server.Path)
	require.Empty(t, out.Server.Metadata)
	require.Equal(t, "cached-invalid-log", input.Server.LogID)
}
