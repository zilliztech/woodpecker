package loki

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func (Module) Run(ctx context.Context, r d.Request, connection d.Connection) d.Result {
	s, err := settings(connection)
	if err != nil {
		return d.Failure("loki", "invalid", "invalid_connection_settings", "Check Loki connection settings")
	}
	if r.Selection.Target.Deployment != "" && r.Selection.Target.Deployment != s.Deployment {
		return d.Failure("loki", "invalid", "deployment_binding_mismatch", "Selected deployment does not match connection binding")
	}
	token, user, password, tenant, err := secrets(s)
	if err != nil {
		return d.Failure("loki", "failed", "authentication_unavailable", "Configured authentication environment reference is unavailable; values withheld")
	}
	var from, to time.Time
	if r.Selection.Window.Mode == "historical" && r.Selection.Window.From != nil && r.Selection.Window.To != nil {
		from, to = *r.Selection.Window.From, *r.Selection.Window.To
	} else if r.Selection.Window.Mode == "live" {
		from = time.Now().UTC()
		to = from.Add(time.Duration(r.Selection.Window.DurationMS) * time.Millisecond)
	} else {
		return d.Failure("loki", "unsupported", "window_mode_not_supported", "Use a bounded live or explicit historical interval")
	}
	if !to.After(from) || to.Sub(from).Milliseconds() > s.Limits.MaxWindowMS || !time.Unix(0, from.UnixNano()).Equal(from) || !time.Unix(0, to.UnixNano()).Equal(to) || from.UnixNano() <= 0 || to.UnixNano() <= from.UnixNano() {
		return d.Failure("loki", "invalid", "window_budget_invalid", "Window must fit max_window_ms and nanosecond timestamp range")
	}
	if r.Selection.Window.Mode == "historical" && to.After(time.Now()) {
		return d.Failure("loki", "invalid", "future_historical_window", "Select a retained past interval")
	}
	if r.Selection.Window.Mode == "live" {
		if deadline, ok := ctx.Deadline(); ok && !deadline.After(to) {
			return d.Failure("loki", "invalid", "live_budget_too_short", "Budget must cover the live interval plus queries")
		}
		timer := time.NewTimer(time.Until(to))
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return d.Failure("loki", cancelStatus(ctx), "cancelled", "Live window interrupted")
		case <-timer.C:
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	c := client{http: &http.Client{Transport: transport, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}, settings: s, token: token, user: user, password: password, tenant: tenant}
	result := d.NewResult("loki")
	result.Status = "completed"
	gap := func(code, detail string) { result.Gaps = append(result.Gaps, d.Gap{Code: code, Detail: detail}) }
	expr := expression(s, r.Selection.Scope, r.Selection.Flow, r.Selection.Target.Instance)
	cursor, end := from.UnixNano(), to.UnixNano()
	seen := map[string]bool{}
	streams := map[string]bool{}
	events := map[string]Event{}
	support := map[string][]string{}
	total, queries, duplicates := 0, 0, 0
	complete := false
	for queries < s.Limits.MaxQueries && total < s.Limits.MaxLines {
		if ctx.Err() != nil {
			break
		}
		limit := min(s.Limits.PageSize, s.Limits.MaxLines-total)
		started := time.Now().UTC()
		rows, code := c.query(ctx, expr, cursor, end, limit, r)
		queries++
		id := "loki:query:" + fingerprint([]any{s.Deployment, expr, cursor, end, queries})
		p := page{FromNS: strconv.FormatInt(cursor, 10), ToNS: strconv.FormatInt(end, 10), Events: []Event{}}
		o := d.Observation{ID: id, SourceID: "loki", Kind: "loki_log_page", Target: d.Target{Deployment: s.Deployment, Instance: r.Selection.Target.Instance}, StartedAt: started, FinishedAt: time.Now().UTC(), Status: "observed", Query: &d.SourceQuery{Language: "logql", Expression: expr, Executed: true}}
		if code != "" {
			o.Status = "unavailable"
			o.Code = code
			result.Status = "partial"
			gap(code, "Loki query failed; backend error bodies and connection details withheld")
		} else {
			p.Returned = len(rows)
			total += len(rows)
			for _, row := range rows {
				streamID := fingerprint(row.labels)
				streams[streamID] = true
				if len(streams) > r.Limits.MaxTargets {
					code = "target_budget_exceeded"
					gap(code, "Stream count exceeded max_targets; remaining rows withheld")
					result.Status = "budget_exceeded"
					break
				}
				eventID := "loki:event:" + fingerprint([]any{row.labels, row.ts, row.line})
				if seen[eventID] {
					p.Duplicates++
					duplicates++
					continue
				}
				seen[eventID] = true
				event := parseEvent(row.line, row.ts, row.labels, r.Selection.Scope, r.Selection.Flow)
				if event == nil || r.Selection.Target.OperationID != "" && event.RequestID != r.Selection.Target.OperationID {
					p.Unmatched++
					continue
				}
				p.Events = append(p.Events, *event)
				events[event.ID] = *event
				support[event.ID] = []string{id}
			}
		}
		o.Data, _ = json.Marshal(p)
		result.Observations = append(result.Observations, o)
		if code != "" {
			if strings.Contains(code, "budget") || strings.Contains(code, "limit_exceeded") {
				result.Status = "budget_exceeded"
			}
			break
		}
		if len(rows) < limit {
			complete = true
			break
		}
		next := rows[len(rows)-1].ns
		if next == cursor {
			gap("pagination_boundary_saturated", "A full inclusive page cannot advance at a shared timestamp; unreturned events may remain. No timestamp increment was used to skip them")
			result.Status = "partial"
			break
		}
		cursor = next
	}
	if !complete && result.Status == "completed" && ctx.Err() == nil {
		result.Status = "budget_exceeded"
		gap("query_or_line_budget_exceeded", "Bounded pagination ended before coverage was established")
	}
	if ctx.Err() != nil {
		result.Status = cancelStatus(ctx)
		gap("collection_interrupted", "Retained pages remain partial evidence")
	}
	if duplicates > 0 {
		gap("duplicate_event_identity", "Identical stream/timestamp/line occurrences were collapsed; pagination replay and producer duplicates cannot be distinguished or counted as independent confirmations")
	}
	if result.Status == "partial" {
		usable := false
		for _, o := range result.Observations {
			usable = usable || o.Status == "observed"
		}
		if !usable {
			result.Status = "failed"
		}
	}
	associations := evaluate(&result, events, support, r, s)
	gap("collector_retention_unknown", "Query success does not verify collector completeness, rotation, retention or delivery lag; no-match is never healthy")
	gap("format_coverage_limited", "Only catalogued exact JSON or bracket Zap messages are interpreted; unmatched fields, unsupported versions and raw lines are withheld")
	result.Metadata, _ = json.Marshal(map[string]any{"source_schema_version": "wpdiag.loki/v1", "catalog_version": 1, "associations": associations, "bound_deployment": s.Deployment, "from": from, "to": to, "end_exclusive": true, "queries": queries, "returned_lines": total, "input_bytes": c.bytes, "duplicate_occurrences": duplicates, "pagination_complete": complete, "limits": s.Limits, "raw_lines_withheld": true})
	return result
}

func cancelStatus(ctx context.Context) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "budget_exceeded"
	}
	return "cancelled"
}
