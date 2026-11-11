package opregistry

import (
	"slices"
	"time"
)

// OpType identifies the category of an in-flight operation.
type OpType string

const (
	OpTypeLogstoreAddEntry        OpType = "logstore.add_entry"
	OpTypeLogstoreGetBatchEntries OpType = "logstore.get_batch_entries"
	OpTypeLogstoreComplete        OpType = "logstore.complete"
	OpTypeLogstoreFence           OpType = "logstore.fence"
	OpTypeLogstoreGetSegmentLac   OpType = "logstore.get_segment_lac"
	OpTypeLogstoreGetBlockCount   OpType = "logstore.get_segment_block_count"
	OpTypeLogstoreCompact         OpType = "logstore.compact_segment"
	OpTypeLogstoreClean           OpType = "logstore.clean_segment"
	OpTypeLogstoreUpdateLac       OpType = "logstore.update_lac"
	OpTypeFileFlush               OpType = "file.flush"
	OpTypeFileSync                OpType = "file.sync"
	OpTypeFileCompact             OpType = "file.compact"
	OpTypeFileFinalize            OpType = "file.finalize"
	OpTypeFileFence               OpType = "file.fence"
	OpTypeFileRecover             OpType = "file.recover"
	OpTypeGRPC                    OpType = "grpc"
)

// OpRecord is the immutable record stored in the registry.
// Fields are set at registration time and never mutated.
type OpRecord struct {
	OpID      string    `json:"op_id"`
	OpType    OpType    `json:"op_type"`
	TraceID   string    `json:"trace_id,omitempty"`
	SpanID    string    `json:"span_id,omitempty"`
	StartedAt time.Time `json:"started_at"`
	LogID     int64     `json:"log_id,omitempty"`
	SegmentID int64     `json:"segment_id,omitempty"`
}

// Elapsed returns duration since the op started.
func (r OpRecord) Elapsed() time.Duration {
	return time.Since(r.StartedAt)
}

// ElapsedAt returns duration since the op started, relative to the given time.
func (r OpRecord) ElapsedAt(now time.Time) time.Duration {
	return now.Sub(r.StartedAt)
}

// Filter specifies criteria for listing ops.
type Filter struct {
	Types      []OpType      `json:"types,omitempty"`
	LogID      *int64        `json:"log_id,omitempty"`
	SegmentID  *int64        `json:"segment_id,omitempty"`
	LongerThan time.Duration `json:"longer_than,omitempty"`
	Limit      int           `json:"limit,omitempty"`
	SortBy     string        `json:"sort_by,omitempty"` // "elapsed" (default) or "type"
}

// Matches returns true if the record passes all filter criteria.
func (f Filter) Matches(r OpRecord, now time.Time) bool {
	if len(f.Types) > 0 && !slices.Contains(f.Types, r.OpType) {
		return false
	}
	if f.LogID != nil && r.LogID != *f.LogID {
		return false
	}
	if f.SegmentID != nil && r.SegmentID != *f.SegmentID {
		return false
	}
	if f.LongerThan > 0 && now.Sub(r.StartedAt) < f.LongerThan {
		return false
	}
	return true
}

// Stats holds registry utilization and eviction statistics.
type Stats struct {
	Capacity     int   `json:"capacity"`
	InUse        int   `json:"in_use"`
	WarnAgeMS    int64 `json:"warn_age_ms"`
	EvictedTotal int64 `json:"evicted_total"`
	EvictedYoung int64 `json:"evicted_young"`
	EvictedOld   int64 `json:"evicted_old"`
}
