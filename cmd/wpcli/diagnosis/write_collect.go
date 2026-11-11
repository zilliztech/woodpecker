package diagnosis

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/zilliztech/woodpecker/cmd/wpcli/internal/prom"
)

// These DTOs deliberately retain only the read-only fields used by the rules.
type Writer struct {
	LogID       int64  `json:"log_id"`
	SegmentID   int64  `json:"segment_id"`
	Backend     string `json:"backend"`
	Writable    bool   `json:"writable"`
	Fenced      bool   `json:"fenced"`
	Finalized   bool   `json:"finalized"`
	Closed      bool   `json:"closed"`
	BufferBytes int64  `json:"buffer_bytes"`
	BufferCount int64  `json:"buffer_entries"`
	QueueDepth  int    `json:"flush_queue_depth"`
	QueueCap    int    `json:"flush_queue_capacity"`
	Durable     int64  `json:"last_entry_id"`
}

type Signal struct {
	Name        string            `json:"name"`
	Labels      map[string]string `json:"labels"`
	Value       float64           `json:"value"`
	TimestampMS *int64            `json:"exported_timestamp_ms,omitempty"`
}

// Quorum comes only from an explicitly selected WP-client metadata namespace.
// Empty NodeIDs retain unresolved members instead of replacing them with peers.
type Quorum struct {
	LogID     int64    `json:"log_id"`
	SegmentID int64    `json:"segment_id"`
	State     string   `json:"segment_state"`
	ES        int      `json:"es"`
	WQ        int      `json:"wq"`
	AQ        int      `json:"aq"`
	NodeIDs   []string `json:"node_ids"`
	Revision  int64    `json:"metadata_revision"`
}

func (o Options) writeEnabled() bool { return o.Scenario == "write" || o.Scenario == "all" }

func (o Options) writerLimit() int {
	if o.Limits.MaxWriters == 0 {
		return 16
	}
	return o.Limits.MaxWriters
}

func writerKey(w Writer) string { return fmt.Sprintf("%d:%d", w.LogID, w.SegmentID) }

func collectWriters(ctx context.Context, client *http.Client, node Node, obs *Observation, o Options) {
	if o.MetadataRequired && o.LogID == nil {
		obs.Status, obs.Code = "unavailable", "explicit_writer_scope_unresolved"
		return
	}
	path := "/admin/logstore/segments"
	if o.LogID != nil {
		path += fmt.Sprintf("?log_id=%d", *o.LogID)
	}
	body, code := fetch(ctx, client, node.URL, path, o.Limits.ResponseBytes)
	if code != "" {
		obs.Status, obs.Code = "unavailable", code
		return
	}
	var list struct {
		Segments json.RawMessage `json:"segments"`
	}
	var entries []json.RawMessage
	if json.Unmarshal(body, &list) != nil || len(list.Segments) == 0 || json.Unmarshal(list.Segments, &entries) != nil {
		obs.Status, obs.Code = "invalid", "invalid_writer_list"
		return
	}
	var selected []Writer
	seen := map[string]bool{}
	for _, raw := range entries {
		var w Writer
		if json.Unmarshal(raw, &w) != nil || !hasFields(raw, "log_id", "segment_id", "backend") || w.LogID < 0 || w.SegmentID < 0 {
			obs.Status, obs.Code = "invalid", "invalid_writer_identity"
			return
		}
		if o.LogID != nil && w.LogID != *o.LogID || o.SegmentID != nil && w.SegmentID != *o.SegmentID {
			continue
		}
		if seen[writerKey(w)] {
			obs.Status, obs.Code = "invalid", "ambiguous_writer_identity"
			return
		}
		seen[writerKey(w)] = true
		selected = append(selected, w)
	}
	sort.Slice(selected, func(i, j int) bool {
		if selected[i].LogID != selected[j].LogID {
			return selected[i].LogID < selected[j].LogID
		}
		return selected[i].SegmentID < selected[j].SegmentID
	})
	if len(selected) > o.writerLimit() {
		selected = selected[:o.writerLimit()]
		obs.Status, obs.Code = "truncated", "writer_limit_exceeded"
	}
	for _, id := range selected {
		body, code := fetch(ctx, client, node.URL, fmt.Sprintf("/admin/logstore/segments/detail?log_id=%d&segment_id=%d", id.LogID, id.SegmentID), o.Limits.ResponseBytes)
		if code != "" {
			obs.Status, obs.Code = "unavailable", "writer_detail_"+code
			continue
		}
		var w Writer
		if json.Unmarshal(body, &w) != nil || !hasFields(body, "log_id", "segment_id", "backend", "writable", "fenced", "finalized", "closed", "buffer_bytes", "buffer_entries", "flush_queue_depth", "flush_queue_capacity", "last_entry_id") ||
			w.LogID != id.LogID || w.SegmentID != id.SegmentID || w.Backend != id.Backend ||
			w.BufferBytes < 0 || w.BufferCount < 0 || w.QueueDepth < 0 || w.QueueCap < w.QueueDepth || w.Durable < -1 ||
			(w.Backend != "stagedstorage" && w.Backend != "disk" && w.Backend != "objectstorage") {
			obs.Status, obs.Code = "invalid", "invalid_writer_detail"
			continue
		}
		obs.Writers = append(obs.Writers, w)
	}
}

func hasFields(body []byte, fields ...string) bool {
	var values map[string]json.RawMessage
	if json.Unmarshal(body, &values) != nil {
		return false
	}
	for _, field := range fields {
		if len(values[field]) == 0 || string(values[field]) == "null" {
			return false
		}
	}
	return true
}

const diskPrefix = "woodpecker_server_logstore_"

func decodeWriteSignals(body []byte, obs *Observation, limit int) {
	families, err := prom.Parse(strings.NewReader(string(body)))
	if err != nil {
		return
	} // Completion decoder already reports parse failure.
	for _, name := range []string{"disk_usage_ratio", "disk_watermark_soft", "disk_watermark_hard", "write_backpressure_state"} {
		for _, m := range families[diskPrefix+name].GetMetric() {
			if len(obs.Counters)+len(obs.Signals) >= limit {
				obs.Status, obs.Code = "truncated", "metric_series_limit"
				return
			}
			if m.GetGauge() == nil || !finite(m.GetGauge().GetValue()) || m.GetGauge().GetValue() < 0 {
				obs.Status, obs.Code = "invalid", "invalid_write_signal"
				continue
			}
			labels := map[string]string{}
			for k, v := range prom.LabelMap(m) {
				if k == "node_id" || k == "path" {
					labels[k] = v
				}
			}
			obs.Signals = append(obs.Signals, Signal{Name: name, Labels: labels, Value: m.GetGauge().GetValue(), TimestampMS: m.TimestampMs})
		}
	}
}
