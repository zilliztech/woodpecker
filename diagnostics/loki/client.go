package loki

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

type page struct {
	FromNS     string  `json:"from_ns"`
	ToNS       string  `json:"to_exclusive_ns"`
	Returned   int     `json:"returned_lines"`
	Duplicates int     `json:"duplicate_occurrences"`
	Unmatched  int     `json:"unmatched_or_unselected_lines"`
	Events     []Event `json:"events"`
}
type row struct {
	labels   map[string]string
	ts, line string
	ns       int64
}
type client struct {
	http                          *http.Client
	settings                      Settings
	token, user, password, tenant string
	bytes                         int64
}

func expression(s Settings, scope, flow, pod string) string {
	parts := []string{}
	for k, v := range s.MatchLabels {
		parts = append(parts, k+"="+strconv.Quote(v))
	}
	parts = append(parts, "component=~"+strconv.Quote(strings.Join(components(scope, flow), "|")))
	if pod != "" {
		parts = append(parts, "pod="+strconv.Quote(pod))
	}
	sort.Strings(parts)
	return "{" + strings.Join(parts, ",") + "}"
}

func (c *client) query(ctx context.Context, expr string, start, end int64, limit int, r d.Request) ([]row, string) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(c.settings.Limits.TimeoutMS)*time.Millisecond)
	defer cancel()
	u, _ := url.Parse(c.settings.Endpoint)
	u.Path = strings.TrimRight(u.Path, "/") + "/loki/api/v1/query_range"
	q := url.Values{"query": {expr}, "start": {strconv.FormatInt(start, 10)}, "end": {strconv.FormatInt(end, 10)}, "direction": {"forward"}, "limit": {strconv.Itoa(limit)}}
	u.RawQuery = q.Encode()
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if e != nil {
		return nil, "request_invalid"
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if c.user != "" {
		req.SetBasicAuth(c.user, c.password)
	}
	if c.tenant != "" {
		req.Header.Set("X-Scope-OrgID", c.tenant)
	}
	response, e := c.http.Do(req)
	if e != nil {
		if ctx.Err() != nil {
			return nil, "query_cancelled_or_timeout"
		}
		return nil, "backend_unavailable"
	}
	defer response.Body.Close()
	capBytes := min(c.settings.Limits.MaxResponseBytes, r.Limits.MaxInputBytes-c.bytes)
	b, e := io.ReadAll(io.LimitReader(response.Body, capBytes+1))
	c.bytes += int64(len(b))
	if int64(len(b)) > capBytes {
		return nil, "input_budget_exceeded"
	}
	if e != nil {
		return nil, "response_read_failed"
	}
	if response.StatusCode != http.StatusOK {
		return nil, "backend_http_error"
	}
	var payload struct {
		Status string
		Data   struct {
			ResultType string
			Result     []struct {
				Stream map[string]string
				Values [][]json.RawMessage
			}
		}
		Warnings []json.RawMessage
	}
	if json.Unmarshal(b, &payload) != nil || payload.Status != "success" || payload.Data.ResultType != "streams" {
		return nil, "unsupported_response"
	}
	if len(payload.Warnings) > 0 {
		return nil, "backend_warnings_withheld"
	}
	rows := []row{}
	for _, stream := range payload.Data.Result {
		for k, v := range c.settings.MatchLabels {
			if stream.Stream[k] != v {
				return nil, "response_scope_mismatch"
			}
		}
		if !d.Has(components(r.Selection.Scope, r.Selection.Flow), stream.Stream["component"]) || stream.Stream["pod"] == "" || (r.Selection.Target.Instance != "" && stream.Stream["pod"] != r.Selection.Target.Instance) {
			return nil, "response_scope_mismatch"
		}
		for _, v := range stream.Values {
			if len(v) < 2 || len(v) > 3 {
				return nil, "unsupported_response"
			}
			var ts, line string
			if json.Unmarshal(v[0], &ts) != nil || json.Unmarshal(v[1], &line) != nil {
				return nil, "unsupported_response"
			}
			ns, ok := validNS(ts)
			if !ok || ns < start || ns >= end {
				return nil, "response_time_mismatch"
			}
			if len(v) == 3 {
				var metadata map[string]string
				if json.Unmarshal(v[2], &metadata) != nil {
					return nil, "unsupported_structured_metadata"
				}
			}
			rows = append(rows, row{stream.Stream, ts, line, ns})
			if len(rows) > limit {
				return nil, "response_line_limit_exceeded"
			}
		}
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].ns < rows[j].ns })
	return rows, ""
}
