package jaeger

import (
	"encoding/json"
	"strings"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

type Module struct{}

func (Module) ValidateConnection(c d.Connection) error { _, err := settings(c); return err }
func (Module) Descriptor() d.Descriptor {
	result := d.Descriptor{Source: "trace", Issue: 461, RequiredSettings: []string{"endpoint", "deployment", "services"}, Profiles: []d.Profile{}}
	for _, scope := range []string{"wp", "milvus", "e2e"} {
		for _, flow := range []string{"write", "read"} {
			stage := "append"
			if flow == "read" {
				stage = "query"
				if scope == "wp" {
					stage = "wal_read"
				}
			}
			result.Profiles = append(result.Profiles, d.Profile{ID: scope + "." + flow + ".trace", Version: 1, Scope: scope, Flow: flow, Stage: stage, Modes: []string{"snapshot", "live", "historical"}, TargetFields: []string{"deployment", "trace_id", "operation_id"}, Description: "Retained Jaeger v1 JSON spans: explicit ID or bounded service/request search; errors/slow spans and relationship coverage, never complete-path or population-health guarantees"})
		}
	}
	return result
}

type tag struct {
	Key   string          `json:"key"`
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}
type wireProcess struct {
	Service string `json:"serviceName"`
	Tags    []tag  `json:"tags"`
}
type reference struct {
	Type    string `json:"refType"`
	TraceID string `json:"traceID"`
	SpanID  string `json:"spanID"`
}
type wireSpan struct {
	TraceID    string            `json:"traceID"`
	ID         string            `json:"spanID"`
	Operation  string            `json:"operationName"`
	References []reference       `json:"references"`
	ParentID   string            `json:"parentSpanID"`
	Start      *int64            `json:"startTime"`
	Duration   *int64            `json:"duration"`
	Tags       []tag             `json:"tags"`
	ProcessID  string            `json:"processID"`
	Warnings   []json.RawMessage `json:"warnings"`
}
type wireTrace struct {
	ID        string                 `json:"traceID"`
	Spans     []wireSpan             `json:"spans"`
	Processes map[string]wireProcess `json:"processes"`
	Warnings  []json.RawMessage      `json:"warnings"`
}

type Span struct {
	ID            string      `json:"span_id"`
	TraceID       string      `json:"trace_id"`
	Service       string      `json:"service"`
	Role          string      `json:"component"`
	Operation     string      `json:"operation"`
	Scope         string      `json:"instrumentation_scope,omitempty"`
	Phase         string      `json:"phase,omitempty"`
	StartUS       int64       `json:"start_us"`
	DurationUS    int64       `json:"duration_us"`
	Error         bool        `json:"error"`
	References    []reference `json:"references"`
	Selected      bool        `json:"selected"`
	InWindow      bool        `json:"in_selected_window"`
	RequestAnchor bool        `json:"request_tag_anchor,omitempty"`
	Notes         []string    `json:"coverage_notes"`
}
type Trace struct {
	ID    string   `json:"trace_id"`
	Path  string   `json:"jaeger_ui_path"`
	Match string   `json:"matching"`
	Spans []Span   `json:"spans"`
	Notes []string `json:"coverage_notes"`
}

func wpClientScope(scope string) bool {
	return d.Has([]string{"LogWriter", "LogReader", "AppendOp", "BatchAppendOp"}, scope)
}

func knownOperation(role, scope, op, flow string) (bool, string) {
	// Milvus #50796 uses this exact instrumentation scope. These timers are
	// inclusive WAL boundaries, not disk/fsync or an additive critical path.
	if flow == "write" && scope == "milvus.streaming.wal" && d.Has([]string{"proxy", "streamingnode"}, role) {
		switch op {
		case "wal.autocommit", "wal.txn":
			return true, "logical_wal_write"
		case "wal.dist_append":
			return true, "distributed_wal_append"
		case "wal.append":
			return true, "wal_adaptor_append"
		case "wal.appendimpl":
			return true, "wal_backend_append"
		}
	}
	if role == "wp_server" || role == "wp_client" || wpClientScope(scope) {
		if flow == "write" {
			if op == "Execute" && d.Has([]string{"AppendOp", "BatchAppendOp"}, scope) {
				if scope == "BatchAppendOp" {
					return true, "async"
				}
				return true, "append_send"
			}
			if d.Has([]string{"Write", "WriteAsync", "AddEntry", "AddEntryBatch", "woodpecker.proto.logstore.LogStore/AddEntry", "woodpecker.proto.logstore.LogStore/AddEntries", "AppendAsync", "Sync", "run_sync", "rollBufferUnsafe", "waitIfFlushingBufferSizeExceededUnsafe"}, op) {
				phase := "execution"
				if op == "waitIfFlushingBufferSizeExceededUnsafe" {
					phase = "buffer_wait"
				}
				if op == "WriteAsync" || op == "run_sync" {
					phase = "async"
				}
				return true, phase
			}
		} else if d.Has([]string{"ReadNext", "GetBatchEntriesAdv", "ReadBatchEntries", "ReadBatch", "ReadBatchAdv", "ReadBatchEntriesAdv"}, op) {
			if op == "ReadNext" {
				return true, "tail_or_catchup_wait"
			}
			return true, "execution"
		}
	}
	if flow == "write" && d.Has([]string{"proxy", "streamingnode"}, role) {
		if d.Has([]string{"/v2/vectordb/entities/insert", "/v2/vectordb/entities/upsert", "/v2/vectordb/entities/delete", "InsertTask", "Proxy-Insert", "Proxy-Insert-Execute", "Proxy-Insert-PreExecute", "Proxy-Insert-PostExecute", "AppendMessages", "Append", "WAL-Append"}, op) {
			return true, "inclusive_request_stage"
		}
	}
	if flow == "read" && d.Has([]string{"proxy", "streamingnode", "querynode", "segcore"}, role) {
		if d.Has([]string{"/v2/vectordb/entities/query", "/v2/vectordb/entities/search", "Proxy-Query", "Proxy-Search", "Proxy-Search-Execute", "Proxy-Search-PreExecute", "Proxy-Search-PostExecute", "SearchTask", "RetrieveTask", "QueryPipeline.Execute", "SegCoreSearch", "SegCoreRetrieve", "Retrieve", "reduceResults", "searchReduceOperator", "decodeSearchResults", "milvus.proto.query.QueryNode/Query", "milvus.proto.query.QueryNode/Search"}, op) {
			return true, "inclusive_request_stage"
		}
	}
	return false, ""
}

func selectedSpan(s Span, scope string) bool {
	wp := s.Role == "wp_client" || s.Role == "wp_server" || wpClientScope(s.Scope)
	return scope == "e2e" || scope == "wp" && wp || scope == "milvus" && !wp && d.Has([]string{"proxy", "streamingnode", "querynode", "segcore"}, s.Role)
}

func canonicalID(value string, width int) (string, bool) {
	value = strings.ToLower(value)
	if len(value) != width && (width != 32 || len(value) != 16) {
		return "", false
	}
	for _, c := range value {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return "", false
		}
	}
	if strings.Trim(value, "0") == "" {
		return "", false
	}
	if width == 32 && len(value) == 16 {
		value = strings.Repeat("0", 16) + value
	}
	return value, true
}
