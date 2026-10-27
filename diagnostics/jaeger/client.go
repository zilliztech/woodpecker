package jaeger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

type budget struct {
	bytes                  int64
	spans, traces, queries int
	targets                map[string]bool
}
type client struct {
	http                  *http.Client
	settings              Settings
	token, user, password string
}

func value(tags []tag, key string) (string, bool, bool) {
	result := ""
	found := false
	for _, t := range tags {
		if t.Key != key {
			continue
		}
		var text string
		switch t.Type {
		case "string":
			if json.Unmarshal(t.Value, &text) != nil {
				return "", false, true
			}
		case "int64":
			var n json.Number
			decoder := json.NewDecoder(bytes.NewReader(t.Value))
			decoder.UseNumber()
			if decoder.Decode(&n) != nil {
				return "", false, true
			}
			if _, err := n.Int64(); err != nil {
				return "", false, true
			}
			text = n.String()
		case "bool":
			var b bool
			if json.Unmarshal(t.Value, &b) != nil {
				return "", false, true
			}
			text = strconv.FormatBool(b)
		default:
			return "", false, true
		}
		if found && text != result {
			return "", false, true
		}
		result, found = text, true
	}
	return result, found, false
}

func mergedValue(span []tag, process []tag, key string) (string, bool, bool) {
	a, hasA, badA := value(span, key)
	b, hasB, badB := value(process, key)
	if badA || badB || hasA && hasB && a != b {
		return "", false, true
	}
	if hasA {
		return a, true, false
	}
	return b, hasB, false
}

func scopeName(tags []tag) string {
	for _, key := range []string{"otel.scope.name", "otel.library.name"} {
		v, ok, bad := value(tags, key)
		if ok && !bad && d.Has([]string{"LogWriter", "LogReader", "AppendOp", "BatchAppendOp", "LogStore", "SegmentProcessor", "StagedFileWriter", "LocalFileWriter", "MinioFileWriter"}, v) {
			return v
		}
	}
	return ""
}

func isError(tags []tag) (bool, bool) {
	result, uncertain := false, false
	for _, key := range []string{"error", "otel.status_code", "rpc.grpc.status_code", "http.status_code", "http.response.status_code"} {
		v, ok, bad := value(tags, key)
		uncertain = uncertain || bad
		if !ok {
			continue
		}
		switch key {
		case "error":
			if v == "true" {
				result = true
			} else if v != "false" {
				uncertain = true
			}
		case "otel.status_code":
			result = result || v == "ERROR" || v == "Error" || v == "2"
			if !d.Has([]string{"ERROR", "Error", "2", "OK", "Ok", "1", "UNSET", "Unset", "0"}, v) {
				uncertain = true
			}
		default:
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 || key == "rpc.grpc.status_code" && n > 16 || key != "rpc.grpc.status_code" && (n < 100 || n > 599) {
				uncertain = true
			} else if key == "rpc.grpc.status_code" {
				result = result || n != 0
			} else {
				result = result || n >= 400
			}
		}
	}
	return result, uncertain
}

func (c client) query(ctx context.Context, r d.Request, from, to time.Time, b *budget) ([]Trace, string, string) {
	if b.queries >= c.settings.Limits.MaxQueries {
		return nil, "query_budget_exceeded", ""
	}
	u, _ := url.Parse(c.settings.Endpoint)
	u.Path = strings.TrimSuffix(u.Path, "/") + "/api/traces"
	q := url.Values{}
	expr := "GET /api/traces/" + r.Selection.Target.TraceID
	if r.Selection.Target.TraceID != "" {
		u.Path += "/" + r.Selection.Target.TraceID
	} else {
		q.Set("service", c.settings.Search.Service)
		if c.settings.Search.Operation != "" {
			q.Set("operation", c.settings.Search.Operation)
		}
		q.Set("start", strconv.FormatInt(from.UnixMicro(), 10))
		q.Set("end", strconv.FormatInt(to.UnixMicro(), 10))
		q.Set("limit", strconv.Itoa(c.settings.Limits.MaxTraces+1))
		tags := map[string]string{}
		for k, v := range c.settings.MatchTags {
			tags[k] = v
		}
		if r.Selection.Target.OperationID != "" {
			tags[c.settings.RequestIDTag] = r.Selection.Target.OperationID
		}
		if len(tags) > 0 {
			raw, _ := json.Marshal(tags)
			q.Set("tags", string(raw))
		}
		u.RawQuery = q.Encode()
		expr = "GET /api/traces?" + q.Encode()
	}
	queryCtx, cancel := context.WithTimeout(ctx, time.Duration(c.settings.Limits.TimeoutMS)*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(queryCtx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, "invalid_request", expr
	}
	req.Header.Set("Accept", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if c.user != "" {
		req.SetBasicAuth(c.user, c.password)
	}
	b.queries++
	response, err := c.http.Do(req)
	if err != nil {
		if queryCtx.Err() != nil {
			return nil, "query_timeout_or_cancelled", expr
		}
		return nil, "transport_failed", expr
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Sprintf("http_%d", response.StatusCode), expr
	}
	capBytes := min(c.settings.Limits.MaxResponseBytes, r.Limits.MaxInputBytes-b.bytes)
	if capBytes < 1 {
		return nil, "input_budget_exceeded", expr
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, capBytes+1))
	b.bytes += int64(len(body))
	if int64(len(body)) > capBytes {
		return nil, "input_budget_exceeded", expr
	}
	if err != nil {
		return nil, "response_read_failed", expr
	}
	var wire struct {
		Data   json.RawMessage   `json:"data"`
		Errors []json.RawMessage `json:"errors"`
	}
	if json.Unmarshal(body, &wire) != nil || len(wire.Data) == 0 || string(wire.Data) == "null" {
		return nil, "invalid_jaeger_response", expr
	}
	var traces []wireTrace
	if json.Unmarshal(wire.Data, &traces) != nil {
		return nil, "invalid_jaeger_response", expr
	}
	if len(traces)+b.traces > c.settings.Limits.MaxTraces {
		return nil, "trace_budget_exceeded", expr
	}
	b.traces += len(traces)
	seen := map[string]wireTrace{}
	out := []Trace{}
	for _, wt := range traces {
		if ctx.Err() != nil {
			return out, "query_timeout_or_cancelled", expr
		}
		id, ok := canonicalID(wt.ID, 32)
		if !ok {
			return nil, "invalid_trace_identity", expr
		}
		b.spans += len(wt.Spans)
		if b.spans > c.settings.Limits.MaxSpans {
			return nil, "span_budget_exceeded", expr
		}
		if previous, ok := seen[id]; ok {
			if !reflect.DeepEqual(previous, wt) {
				return nil, "conflicting_duplicate_trace", expr
			}
			continue
		}
		seen[id] = wt
		if r.Selection.Target.TraceID != "" && id != r.Selection.Target.TraceID {
			return nil, "trace_id_scope_mismatch", expr
		}
		trace, code := decodeTrace(wt, id, r, c.settings, from, to, b)
		if code != "" {
			return nil, code, expr
		}
		if len(wire.Errors) > 0 {
			trace.Notes = append(trace.Notes, "backend_errors_withheld")
		}
		out = append(out, trace)
	}
	if len(wire.Errors) > 0 && len(out) == 0 {
		return nil, "backend_errors_withheld", expr
	}
	return out, "", expr
}

func decodeTrace(wt wireTrace, id string, r d.Request, s Settings, from, to time.Time, b *budget) (Trace, string) {
	out := Trace{ID: id, Path: "/trace/" + id, Match: "exact_trace_id", Spans: []Span{}, Notes: []string{}}
	if r.Selection.Target.TraceID == "" {
		out.Match = "bounded_search_candidate"
		out.Notes = append(out.Notes, "search_is_not_request_identity")
	}
	if len(wt.Warnings) > 0 {
		out.Notes = append(out.Notes, "backend_trace_warnings_withheld")
	}
	if len(s.MatchTags) == 0 {
		out.Notes = append(out.Notes, "deployment_binding_configured_only")
	}
	seen := map[string]wireSpan{}
	anchor := false
	requestAnchors := map[string]bool{}
	requestMatch := false
	for _, ws := range wt.Spans {
		spanID, ok := canonicalID(ws.ID, 16)
		traceID, traceOK := canonicalID(ws.TraceID, 32)
		if !ok || !traceOK || traceID != id {
			return out, "invalid_span_identity"
		}
		if previous, ok := seen[spanID]; ok {
			if !reflect.DeepEqual(previous, ws) {
				return out, "conflicting_duplicate_span"
			}
			out.Notes = appendUnique(out.Notes, "identical_span_deduplicated")
			continue
		}
		seen[spanID] = ws
		if ws.Start == nil || ws.Duration == nil || *ws.Start <= 0 || *ws.Duration < 0 || *ws.Start > math.MaxInt64-*ws.Duration {
			return out, "invalid_span_time"
		}
		process, ok := wt.Processes[ws.ProcessID]
		if !ok || process.Service == "" {
			return out, "missing_span_process"
		}
		role := s.Services[process.Service]
		service := process.Service
		if role == "" {
			role = "unconfigured"
			service = "[unconfigured service]"
		}
		span := Span{ID: spanID, TraceID: id, Service: service, Role: role, Operation: "[unsupported operation]", StartUS: *ws.Start, DurationUS: *ws.Duration, References: []reference{}, Notes: []string{}}
		tags := append(append([]tag{}, ws.Tags...), process.Tags...)
		span.Scope = scopeName(tags)
		known, phase := knownOperation(role, span.Scope, ws.Operation, r.Selection.Flow)
		if known {
			span.Operation, span.Phase = ws.Operation, phase
		}
		// WP ends acknowledged append streams by cancellation without another
		// Recv. Retain the error flag, but do not infer application failure from
		// an exact client RPC cancellation; genuine interruption is also possible.
		if role == "wp_client" && d.Has([]string{"woodpecker.proto.logstore.LogStore/AddEntry", "woodpecker.proto.logstore.LogStore/AddEntries"}, ws.Operation) {
			kind, hasKind, badKind := value(ws.Tags, "span.kind")
			code, hasCode, badCode := value(ws.Tags, "rpc.grpc.status_code")
			if hasKind && !badKind && kind == "client" && hasCode && !badCode && code == "1" {
				span.Notes = append(span.Notes, "append_rpc_cancellation_outcome_unverified")
			}
		}
		span.Error, _ = isError(ws.Tags)
		if _, bad := isError(ws.Tags); bad {
			span.Notes = append(span.Notes, "error_tag_semantics_unverified")
		}
		if len(ws.Warnings) > 0 {
			span.Notes = append(span.Notes, "backend_span_warnings_withheld")
		}
		if ws.ParentID != "" && len(ws.References) == 0 {
			span.Notes = append(span.Notes, "deprecated_parent_reference_unverified")
		}
		if len(ws.References) > 16 {
			return out, "reference_budget_exceeded"
		}
		for _, ref := range ws.References {
			parent, ok := canonicalID(ref.SpanID, 16)
			tid, valid := canonicalID(ref.TraceID, 32)
			if !ok || !valid || !d.Has([]string{"CHILD_OF", "FOLLOWS_FROM"}, ref.Type) {
				return out, "invalid_span_reference"
			}
			candidate := reference{Type: ref.Type, TraceID: tid, SpanID: parent}
			duplicate := false
			for _, previous := range span.References {
				duplicate = duplicate || previous == candidate
			}
			if !duplicate {
				span.References = append(span.References, candidate)
			} else {
				span.Notes = appendUnique(span.Notes, "identical_reference_deduplicated")
			}
		}
		span.InWindow = r.Selection.Window.Mode == "snapshot" || span.StartUS >= from.UnixMicro() && span.StartUS <= to.UnixMicro()
		if span.StartUS+span.DurationUS > time.Now().Add(time.Duration(s.Policy.ClockSkewMS)*time.Millisecond).UnixMicro() {
			span.Notes = append(span.Notes, "span_clock_in_future")
			span.InWindow = false
		}
		span.Selected = role != "unconfigured" && role != "other" && known && selectedSpan(span, r.Selection.Scope) && span.InWindow
		matches := true
		for key, want := range s.MatchTags {
			v, has, bad := mergedValue(ws.Tags, process.Tags, key)
			matches = matches && has && !bad && v == want
			if !has || bad || v != want {
				span.Selected = false
				span.Notes = appendUnique(span.Notes, "span_deployment_binding_mismatch")
			}
		}
		if s.RequestIDTag != "" && r.Selection.Target.OperationID != "" {
			v, has, bad := mergedValue(ws.Tags, process.Tags, s.RequestIDTag)
			if has && !bad && v == r.Selection.Target.OperationID {
				requestMatch = true
			} else {
				matches = false
			}
		}
		if matches && s.Services[process.Service] != "" && (r.Selection.Target.TraceID != "" || process.Service == s.Search.Service && (s.Search.Operation == "" || ws.Operation == s.Search.Operation)) && span.InWindow {
			anchor = true
			if r.Selection.Target.OperationID != "" {
				requestAnchors[span.ID] = true
				span.RequestAnchor = true
			}
		}
		if role != "unconfigured" {
			instance, has, bad := mergedValue(ws.Tags, process.Tags, "service.instance.id")
			if !has {
				instance, has, bad = mergedValue(ws.Tags, process.Tags, "NodeID")
			}
			if bad {
				return out, "ambiguous_process_identity"
			}
			if !has {
				span.Notes = append(span.Notes, "instance_identity_unavailable")
			}
			b.targets[service+":"+instance] = true
			if len(b.targets) > r.Limits.MaxTargets {
				return out, "target_budget_exceeded"
			}
		}
		out.Spans = append(out.Spans, span)
	}
	if !anchor {
		out.Notes = append(out.Notes, "trace_target_or_window_unverified")
		for i := range out.Spans {
			out.Spans[i].Selected = false
		}
	}
	if r.Selection.Target.OperationID != "" {
		if requestMatch && anchor {
			out.Match = "request_tag_candidate"
			if r.Selection.Target.TraceID != "" {
				out.Match = "exact_trace_and_request_tag"
			}
			out.Notes = append(out.Notes, "request_tag_uniqueness_unverified")
			filterRequestSubtree(&out, requestAnchors)
		} else {
			out.Notes = append(out.Notes, "request_id_not_observed")
		}
	}
	return out, ""
}

// Request-tag selection does not pull unrelated sibling requests into findings.
// Unknown/multiple parents and async links do not establish that association.
func filterRequestSubtree(t *Trace, anchors map[string]bool) {
	parents := map[string]string{}
	for _, span := range t.Spans {
		ids := map[string]bool{}
		for _, ref := range span.References {
			if ref.Type == "CHILD_OF" && ref.TraceID == t.ID {
				ids[ref.SpanID] = true
			}
		}
		if len(ids) == 1 {
			for id := range ids {
				parents[span.ID] = id
			}
		}
	}
	seen := map[string]int{}
	matched := map[string]bool{}
	var match func(string) bool
	match = func(id string) bool {
		if anchors[id] {
			return true
		}
		if seen[id] == 2 {
			return matched[id]
		}
		if seen[id] == 1 {
			return false
		}
		seen[id] = 1
		if parent := parents[id]; parent != "" {
			matched[id] = match(parent)
		}
		seen[id] = 2
		return matched[id]
	}
	for i := range t.Spans {
		if t.Spans[i].Selected && !match(t.Spans[i].ID) {
			t.Spans[i].Selected = false
			t.Spans[i].Notes = appendUnique(t.Spans[i].Notes, "outside_selected_request_subtree")
		}
	}
}

func appendUnique(items []string, item string) []string {
	if !d.Has(items, item) {
		items = append(items, item)
	}
	return items
}
