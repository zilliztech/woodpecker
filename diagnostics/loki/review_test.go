package loki

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/zilliztech/woodpecker/common/logger"
	d "github.com/zilliztech/woodpecker/diagnostics"
)

func TestCatalogSelectorsOnlyQueryApplicableComponentsAndMessages(t *testing.T) {
	for _, scope := range []string{"wp", "milvus", "e2e"} {
		for _, flow := range []string{"write", "read"} {
			t.Run(scope+"/"+flow, func(t *testing.T) {
				expr := expression(Settings{MatchLabels: map[string]string{"namespace": "fixture"}}, scope, flow, "")
				selector, filter, ok := strings.Cut(expr, " |~ ")
				require.True(t, ok)
				pattern, err := strconv.Unquote(filter)
				require.NoError(t, err)
				compiled, err := regexp.Compile(pattern)
				require.NoError(t, err)
				require.NotContains(t, selector, "querynode")
				roles := map[string]bool{}
				for _, p := range Catalog() {
					if applicable(p, scope, flow) {
						require.True(t, compiled.MatchString(p.Message))
						roles[p.Component] = true
					}
				}
				require.Len(t, components(scope, flow), len(roles))
				for _, role := range components(scope, flow) {
					require.True(t, roles[role])
				}
				require.False(t, compiled.MatchString("normal INFO progress"))
			})
		}
	}
}

func TestTextIdentityFieldsExcludeEmbeddedErrorIDs(t *testing.T) {
	realTrace := strings.Repeat("a", 32)
	fakeTrace := strings.Repeat("b", 32)
	prefix := `[2026-10-26] [WARN] [logstore.go:295] ["add entry failed"] `
	for _, tc := range []struct{ name, fields, request, trace string }{
		{"context before quoted error", `[traceID=` + realTrace + `] [requestID=real] [error="failed [traceID=` + fakeTrace + `] [requestID=fake]"]`, "real", realTrace},
		{"no real context", `[error="failed [traceID=` + fakeTrace + `] [requestID=fake]"]`, "", ""},
		{"nested object and array", `[details={trace=[traceID=` + fakeTrace + `]}] [items=[[requestID=fake]]] [requestID=real]`, "real", ""},
		{"error before real field", `[error="failed [traceID=` + fakeTrace + `]"] [traceID=` + realTrace + `]`, "", realTrace},
		{"first top-level ID wins", `[requestID=real] [request_id=later] [traceID=` + realTrace + `] [trace_id=` + fakeTrace + `]`, "real", realTrace},
		{"quoted IDs", `[requestID="real"] [traceID="` + strings.ToUpper(realTrace) + `"]`, "real", realTrace},
		{"escaped quotes", `[error="quoted \"[requestID=fake]\" [traceID=` + fakeTrace + `]"] [request_id=real]`, "real", ""},
		{"unclosed field", `[error="unfinished [traceID=` + fakeTrace + `]`, "", ""},
		{"invalid value", `[requestID="bad/name"] [traceID=00000000000000000000000000000000]`, "", ""},
		{"not a field list", `error body [requestID=fake]`, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := parseEvent(prefix+tc.fields, "1001000000000", labels("woodpecker", "wp"), "wp", "write")
			require.NotNil(t, e)
			require.Equal(t, tc.request, e.RequestID)
			require.Equal(t, tc.trace, e.TraceID)
		})
	}
	// Reproduce the actual producer encoder ordering and escaping, not only a
	// handwritten approximation of the error field.
	encoder := logger.NewTextEncoderByConfig("text").Clone()
	encoder.AddString("traceID", realTrace)
	encoder.AddString("requestID", "real")
	buffer, err := encoder.EncodeEntry(zapcore.Entry{Level: zapcore.WarnLevel, Time: time.Unix(1000, 0), Caller: zapcore.NewEntryCaller(0, "logstore.go", 295, true), Message: "add entry failed"}, []zapcore.Field{zap.Error(errors.New("failed [traceID=" + fakeTrace + "] [requestID=fake]"))})
	require.NoError(t, err)
	defer buffer.Free()
	event := parseEvent(strings.TrimSpace(buffer.String()), "1001000000000", labels("woodpecker", "wp"), "wp", "write")
	require.NotNil(t, event, buffer.String())
	require.Equal(t, realTrace, event.TraceID)
	require.Equal(t, "real", event.RequestID)
}

func jsonEvent(message, request, trace string) string {
	fields := map[string]string{"msg": message}
	if request != "" {
		fields["requestID"] = request
	}
	if trace != "" {
		fields["traceID"] = trace
	}
	b, _ := json.Marshal(fields)
	return string(b)
}

func associations(t *testing.T, report d.Report) []Association {
	t.Helper()
	var meta struct {
		Associations []Association `json:"associations"`
	}
	require.NoError(t, json.Unmarshal(report.Results[0].Metadata, &meta))
	return meta.Associations
}

func TestBothIDsRemainSearchableWithoutDoubleCountingCorrelations(t *testing.T) {
	trace := strings.Repeat("a", 32)
	for _, tc := range []struct {
		name                   string
		peerTrace, peerRequest string
		extra                  bool
		want                   int
		kinds                  []string
	}{
		{"request-only peer", "", "request-1", false, 1, []string{"request"}},
		{"trace-only peer", trace, "", false, 1, []string{"trace"}},
		{"both identities same events", trace, "request-1", false, 1, []string{"request", "trace"}},
		{"partial overlap preserves separate events", trace, "", true, 2, nil},
		{"unrelated peer", strings.Repeat("b", 32), "request-2", false, 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				streams := []any{
					stream("proxy", "proxy", []any{"1001000000000", jsonEvent("failed to reduce search results", "request-1", trace)}),
					stream("streamingnode", "sn", []any{"1002000000000", jsonEvent("read entries error", tc.peerRequest, tc.peerTrace)}),
				}
				if tc.extra {
					streams = append(streams, stream("woodpecker", "wp", []any{"1003000000000", jsonEvent("get batch entries failed", "request-1", "")}))
				}
				payload(w, streams...)
			}))
			defer server.Close()
			report := run(t, server, request("e2e", "read"), nil)
			require.Equal(t, "completed", report.Status)
			groups := associations(t, report)
			require.Len(t, groups, tc.want)
			count := 0
			for _, f := range report.Results[0].Findings {
				if f.RuleID == "loki.e2e.observed_identity" {
					count++
				}
			}
			require.Equal(t, tc.want, count)
			if tc.want == 1 {
				require.Equal(t, tc.kinds, groups[0].IdentityKinds)
				require.Len(t, groups[0].EventIDs, 2)
			}
			if tc.extra {
				require.NotEqual(t, groups[0].EventIDs, groups[1].EventIDs)
				require.Len(t, groups[0].EventIDs, 2)
				require.Len(t, groups[1].EventIDs, 2)
			}
			if tc.want > 0 {
				for _, gap := range report.Results[0].Gaps {
					require.NotEqual(t, "e2e_identity_mapping_missing", gap.Code)
				}
			}
		})
	}
}

func TestOperationSelectorCannotMatchAnIDInsideErrorBody(t *testing.T) {
	text := `[2026-10-26] [WARN] [logstore.go:295] ["add entry failed"] [requestID=real] [error="failed [requestID=fake]"]`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		payload(w, stream("woodpecker", "wp", []any{"1001000000000", text}))
	}))
	defer server.Close()
	r := request("wp", "write")
	r.Selection.Target.OperationID = "fake"
	report := run(t, server, r, nil)
	require.Equal(t, "inconclusive", report.Results[0].Findings[0].Outcome)
	r.Selection.Target.OperationID = "real"
	report = run(t, server, r, nil)
	require.Equal(t, "suspected_operation_problem", report.Results[0].Findings[0].Outcome)
}
