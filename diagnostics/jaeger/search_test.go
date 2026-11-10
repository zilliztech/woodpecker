package jaeger

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func candidateTrace(n int, op string, duration time.Duration, errorFlag bool, status string) wireTrace {
	id := fmt.Sprintf("%032x", n)
	tags := []tag{}
	if errorFlag {
		tags = append(tags, tag{Key: "error", Type: "bool", Value: json.RawMessage("true")})
	}
	if status != "" {
		tags = append(tags, testTag("otel.status_code", status))
	}
	return wireTrace{ID: id, Processes: map[string]wireProcess{"p": {Service: "wp-server", Tags: []tag{testTag("otel.scope.name", "LogStore"), testTag("service.instance.id", "n")}}}, Spans: []wireSpan{{TraceID: id, ID: fmt.Sprintf("%016x", n), Operation: op, ProcessID: "p", Start: intPointer(time.Now().Add(-time.Minute).UnixMicro()), Duration: intPointer(duration.Microseconds()), Tags: tags}}}
}

func TestOneHourCandidateSearchFiltersNormalTrafficAndDeduplicatesOR(t *testing.T) {
	data := []wireTrace{}
	for n := 1; n <= 200; n++ {
		data = append(data, candidateTrace(n, "AddEntry", time.Millisecond, false, ""))
	}
	alias := "woodpecker.proto.logstore.LogStore/AddEntry"
	data = append(data, candidateTrace(201, "AddEntry", 250*time.Millisecond, false, ""), candidateTrace(202, "AddEntry", time.Millisecond, true, "ERROR"), candidateTrace(203, alias, 500*time.Millisecond, false, ""), candidateTrace(204, "AddEntry", time.Millisecond, false, "ERROR"))
	var mu sync.Mutex
	queries := []map[string]string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		filters := map[string]string{}
		_ = json.Unmarshal([]byte(q.Get("tags")), &filters)
		mu.Lock()
		queries = append(queries, map[string]string{"operation": q.Get("operation"), "minDuration": q.Get("minDuration"), "tags": q.Get("tags")})
		mu.Unlock()
		minimum, _ := time.ParseDuration(q.Get("minDuration"))
		limit, _ := strconv.Atoi(q.Get("limit"))
		rows := []wireTrace{}
		for _, trace := range data {
			span := trace.Spans[0]
			if op := q.Get("operation"); op != "" && span.Operation != op {
				continue
			}
			if *span.Duration < minimum.Microseconds() {
				continue
			}
			matched := true
			for key, want := range filters {
				value, ok, bad := value(span.Tags, key)
				matched = matched && ok && !bad && value == want
			}
			if !matched {
				continue
			}
			rows = append(rows, trace)
			if limit > 0 && len(rows) >= limit {
				break
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": rows})
	}))
	defer server.Close()
	conn := connection(t, server.URL, func(s *Settings) { s.Search.Service = "wp-server"; s.Limits.MaxTraces = 2; s.Limits.MaxSpans = 4 })
	to := time.Now().UTC().Add(-time.Second)
	q := Selection{From: to.Add(-time.Hour), To: to, Flow: "write", Operations: []string{"AddEntry", alias}}
	baseline := Collect(context.Background(), conn, q, d.DefaultLimits())
	require.Equal(t, "trace_budget_exceeded", baseline.Code)
	q.CandidateKind = "duration"
	q.MinDurationMS = 200
	duration := Collect(context.Background(), conn, q, d.DefaultLimits())
	require.Equal(t, "completed", duration.Status, duration.Message)
	require.Len(t, duration.Traces, 2)
	require.Equal(t, "duration", duration.CandidateKind)
	q.CandidateKind = "errors"
	q.MinDurationMS = 99999
	errors := Collect(context.Background(), conn, q, d.DefaultLimits())
	require.Equal(t, "completed", errors.Status, errors.Message)
	require.Len(t, errors.Traces, 2, "the same trace matching both error predicates is counted once")
	for _, trace := range errors.Traces {
		require.EqualValues(t, 1000, trace.Spans[0].DurationUS, "fast failures must not inherit a slow-span duration filter")
	}
	q.CandidateKind = "duration"
	q.MinDurationMS = 600
	empty := Collect(context.Background(), conn, q, d.DefaultLimits())
	require.Equal(t, "completed", empty.Status)
	require.Empty(t, empty.Traces)
	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, queries)
	for _, query := range queries[1:] {
		require.NotEmpty(t, query["operation"])
		if query["tags"] != "" {
			require.Empty(t, query["minDuration"])
		} else {
			require.NotEmpty(t, query["minDuration"])
		}
	}
}

func TestCandidateSearchIsExplicitAboutBudgetsAndExactTraceSelection(t *testing.T) {
	conn := connection(t, "http://unused.invalid", func(s *Settings) { s.Search.Service = "wp-server"; s.Limits.MaxQueries = 1 })
	to := time.Now().UTC().Add(-time.Second)
	q := Selection{From: to.Add(-time.Hour), To: to, Flow: "write", Operations: []string{"AddEntry"}, CandidateKind: "errors"}
	col := Collect(context.Background(), conn, q, d.DefaultLimits())
	require.Equal(t, "query_budget_exceeded", col.Code)
	require.Contains(t, col.Message, "needs 2")
	s, err := settings(conn)
	require.NoError(t, err)
	q.TraceID = testID
	plans, code := candidateSearches(s, q)
	require.Empty(t, code)
	require.Len(t, plans, 1)
	require.Empty(t, plans[0].tags)
	require.Zero(t, plans[0].minDuration)
}

func TestExactTraceFetchDoesNotDiscardNormalContext(t *testing.T) {
	trace := candidateTrace(1, "AddEntry", time.Millisecond, false, "")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/traces/"+trace.ID, r.URL.Path)
		require.Empty(t, r.URL.RawQuery)
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []wireTrace{trace}})
	}))
	defer server.Close()
	conn := connection(t, server.URL, func(s *Settings) { s.Search.Service = "wp-server" })
	to := time.Now().UTC().Add(-time.Second)
	q := Selection{From: to.Add(-time.Hour), To: to, Flow: "write", TraceID: trace.ID, Operations: []string{"AddEntry"}, CandidateKind: "duration", MinDurationMS: 1000}
	result := Collect(context.Background(), conn, q, d.DefaultLimits())
	require.Equal(t, "completed", result.Status, result.Message)
	require.Empty(t, result.CandidateKind)
	require.Len(t, result.Traces, 1)
	require.EqualValues(t, 1000, result.Traces[0].Spans[0].DurationUS)
}
