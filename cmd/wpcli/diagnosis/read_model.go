package diagnosis

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// ReaderPosition is a leased report of the next entry the reader attempts,
// not a delivered-message counter. The generation prevents joining reopened readers.
type ReaderPosition struct {
	Name           string `json:"reader_name"`
	OpenedMS       int64  `json:"opened_at_ms"`
	ReportedMS     int64  `json:"reported_at_ms"`
	SegmentID      int64  `json:"segment_id"`
	NextEntry      int64  `json:"next_entry_id"`
	CreateRevision int64  `json:"create_revision"`
	LeaseTTL       int64  `json:"lease_ttl_seconds"`
}

type ReadState struct {
	LogID            int64            `json:"log_id"`
	SegmentID        int64            `json:"segment_id"`
	SegmentState     string           `json:"segment_state"`
	LastEntry        int64            `json:"metadata_last_entry_id"`
	TruncatedSegment int64            `json:"truncated_segment_id"`
	TruncatedEntry   int64            `json:"truncated_entry_id"`
	ES               int              `json:"es"`
	WQ               int              `json:"wq"`
	AQ               int              `json:"aq"`
	NodeIDs          []string         `json:"quorum_node_ids"`
	Revision         int64            `json:"metadata_revision"`
	ReportIntervalMS int64            `json:"reader_report_interval_ms"`
	Readers          []ReaderPosition `json:"readers"`
}

type ReadPosition struct {
	LogID     int64  `json:"log_id"`
	SegmentID int64  `json:"segment_id"`
	Present   bool   `json:"loaded_writer_present"`
	Durable   int64  `json:"durable_entry_id"`
	Backend   string `json:"backend,omitempty"`
}

func (o Options) readEnabled() bool { return o.Scenario == "read" || o.Scenario == "all" }
func (o Options) readerLimit() int {
	if o.Limits.MaxReaders == 0 {
		return 16
	}
	return o.Limits.MaxReaders
}

func readQuorumNode(state *ReadState, node string) bool {
	for _, id := range state.NodeIDs {
		if id != "" && id == node {
			return true
		}
	}
	return false
}

func collectReadPosition(ctx context.Context, client *http.Client, node Node, obs *Observation, o Options) {
	if o.ReadScope == nil {
		obs.Status, obs.Code = "unavailable", "read_scope_unavailable"
		return
	}
	state := o.ReadScope
	body, code := fetch(ctx, client, node.URL, fmt.Sprintf("/admin/logstore/segments?log_id=%d", state.LogID), o.Limits.ResponseBytes)
	if code != "" {
		obs.Status, obs.Code = "unavailable", code
		return
	}
	var list struct {
		Segments json.RawMessage `json:"segments"`
	}
	var entries []struct {
		LogID     *int64 `json:"log_id"`
		SegmentID *int64 `json:"segment_id"`
		Durable   *int64 `json:"last_entry_id"`
		Backend   string `json:"backend"`
	}
	if json.Unmarshal(body, &list) != nil || len(list.Segments) == 0 || json.Unmarshal(list.Segments, &entries) != nil {
		obs.Status, obs.Code = "invalid", "invalid_read_position_schema"
		return
	}
	obs.ReadPosition = &ReadPosition{LogID: state.LogID, SegmentID: state.SegmentID, Durable: -1}
	for _, entry := range entries {
		if entry.LogID == nil || entry.SegmentID == nil || *entry.LogID != state.LogID || *entry.SegmentID < 0 {
			obs.Status, obs.Code = "invalid", "ambiguous_read_position_identity"
			return
		}
		if *entry.SegmentID != state.SegmentID {
			continue
		}
		if obs.ReadPosition.Present || entry.Durable == nil || *entry.Durable < -1 || (entry.Backend != "disk" && entry.Backend != "stagedstorage" && entry.Backend != "objectstorage") {
			obs.Status, obs.Code = "invalid", "ambiguous_read_position_identity"
			return
		}
		obs.ReadPosition.Present, obs.ReadPosition.Durable, obs.ReadPosition.Backend = true, *entry.Durable, entry.Backend
	}
}

func readRules() []Rule {
	return []Rule{
		{
			ID: "read_activity", Version: 1, Scenario: "read", Flow: "read", Stage: "wal_read", MinimumSamples: 2,
			Description: "Distinguish observed success/EOF, pending server reads and missing expected-work evidence without attributing server ops to a reader",
			Required:    []string{"bounded metrics/ops live samples", "process identity"}, Optional: []string{"registry statistics"},
			Applicability: "WP v0.1.x/dev service APIs; node/log read operation family; snapshot cannot establish progress",
			Comparison:    "per-series success/EOF completion deltas (counts); same pending op identity; no universal latency threshold",
			Exclusions:    []string{"restart/reset", "changed or missing counter series", "ambiguous namespaces", "unverified timestamps", "registry truncation/eviction", "no expected data or reader binding"},
		},
		{
			ID: "reader_progress", Version: 1, Scenario: "read", Flow: "read", Stage: "reader_progress", MinimumSamples: 2,
			Description: "Compare fresh leased reader positions with explicitly scoped readable data and the operator's read expectation",
			Required:    []string{"explicit WP-client log/segment metadata", "fresh leased reader generation", "complete live window", "available data boundary"}, Optional: []string{"node ops, separately attributed"},
			Applicability: "WP v0.1.x/dev metadata; 30s report cadence; current reading of older data is not historical incident evidence",
			Comparison:    "next-entry IDs/segment IDs, report timestamps in milliseconds, lease TTL in seconds; unchanged catch-up positions need renewed attempts and data throughout the window",
			Exclusions:    []string{"tail/unknown expectation", "idle/EOF", "stale/expired/future reports", "window shorter than report cadence", "reader reopened/disappeared", "metadata/quorum changes", "truncated ranges", "namespace ambiguity"},
		},
		{
			ID: "read_quorum_visibility", Version: 1, Scenario: "read", Flow: "read", Stage: "readability", MinimumSamples: 1,
			Description: "Separate sampled quorum durable positions, finalized metadata boundaries and unverified physical readability",
			Required:    []string{"explicit segment metadata quorum", "bounded selected-node positions for Active segments"}, Optional: []string{},
			Applicability: "WP v0.1.x/dev; Active versus Completed/Sealed lifecycle is explicit",
			Comparison:    "aq-th highest observed durable entry ID for an Active segment; finalized metadata end for Completed/Sealed; no payload read",
			Exclusions:    []string{"unresolved quorum members", "duplicate node identity", "partial reachability", "missing loaded writers", "unsupported builds", "physical verification not performed"},
		},
	}
}
