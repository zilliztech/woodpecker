package loki

import (
	"sort"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

type Module struct{}

func (Module) Descriptor() d.Descriptor {
	desc := d.Descriptor{Source: "loki", Issue: 462, RequiredSettings: []string{"endpoint", "deployment", "match_labels.namespace"}}
	for _, scope := range []string{"wp", "milvus", "e2e"} {
		for _, flow := range []string{"write", "read"} {
			stage := "append"
			if flow == "read" {
				stage = "wal_read"
				if scope != "wp" {
					stage = "read_overview"
				}
			}
			desc.Profiles = append(desc.Profiles, d.Profile{ID: scope + "." + flow + ".logs", Version: 1, Scope: scope, Flow: flow, Stage: stage, Modes: []string{"live", "historical"}, TargetFields: []string{"deployment", "instance", "operation_id"}, Description: "Known exact JSON/Zap messages, pod-scoped excerpts and event lineage; no-match is inconclusive; instance means Kubernetes pod"})
		}
	}
	return desc
}
func (Module) ValidateConnection(c d.Connection) error { _, e := settings(c); return e }

type Pattern struct {
	ID             string `json:"id"`
	Version        int    `json:"version"`
	Component      string `json:"component"`
	Flow           string `json:"flow"`
	Message        string `json:"message"`
	Interpretation string `json:"interpretation"`
}

// Exact messages from WP server/logstore.go, server/maintenance.go,
// woodpecker/log/log_reader.go and Milvus WAL metricsutil/wal_write.go / proxy
// search_reduce_util.go. Catalog version is not a claim to recognize every release.
func Catalog() []Pattern {
	return []Pattern{
		{"wp.append_failure", 1, "woodpecker", "write", "add entry failed", "Server reported an append failure; inspect the corresponding bounded native operation and disk admission evidence"},
		{"wp.batch_append_failure", 1, "woodpecker", "write", "add entry batch failed", "Server reported a batch append failure; retry outcome and user impact are unknown"},
		{"wp.disk_pressure", 1, "woodpecker", "write", "local WAL disk usage high: expand the PVC or scale out logstore nodes to bind new PVCs", "Server reported local WAL disk pressure; a warning alone does not establish blocked writes"},
		{"wp.disk_recovered", 1, "woodpecker", "write", "local WAL disk usage recovered below watermarks", "Disk recovery was logged at this event, not proof of recovery throughout the interval"},
		{"wp.server_read_failure", 1, "woodpecker", "read", "get batch entries failed", "Server reported a batch read failure; retry outcome is unknown"},
		{"wp.client_read_failure", 1, "streamingnode", "read", "read entries error", "Embedded WP reader reported a read error; inspect reader progress before declaring a stall"},
		{"milvus.wal_slow", 1, "streamingnode", "write", "append message into wal too slow", "SN reported a slow WAL append; this alone does not identify an interceptor, storage or network cause"},
		{"milvus.reduce_failure", 1, "proxy", "read", "failed to reduce search results", "Proxy reported result reduction failure; surrounding execution and retry outcomes remain unknown"},
		{"milvus.decode_failure", 1, "proxy", "read", "failed to decode search results", "Proxy reported result decoding failure; user-visible outcome remains unknown"},
	}
}

// Scope selectors come from the same catalog as the evaluator: a component
// with no applicable patterns must not consume stream/line budgets.
func components(scope, flow string) []string {
	set := map[string]bool{}
	for _, p := range Catalog() {
		if applicable(p, scope, flow) {
			set[p.Component] = true
		}
	}
	out := []string{}
	for role := range set {
		out = append(out, role)
	}
	sort.Strings(out)
	return out
}

func applicable(p Pattern, scope, flow string) bool {
	if p.Flow != flow {
		return false
	}
	if scope == "wp" {
		return p.ID[:3] == "wp."
	}
	if scope == "milvus" {
		return p.ID[:3] != "wp."
	}
	return true
}
