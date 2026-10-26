package prometheus

import d "github.com/zilliztech/woodpecker/diagnostics"

type Module struct{}

func (Module) Descriptor() d.Descriptor {
	result := d.Descriptor{Source: "prometheus", Issue: 460, RequiredSettings: []string{"endpoint", "deployment", "match_labels"}, Profiles: []d.Profile{}}
	for _, scope := range []string{"wp", "milvus", "e2e"} {
		for _, flow := range []string{"write", "read"} {
			stage := "append"
			if flow == "read" {
				stage = "wal_read"
				if scope == "milvus" {
					stage = "query"
				}
				if scope == "e2e" {
					stage = "read_overview"
				}
			}
			fields := []string{"deployment"}
			if scope != "e2e" {
				fields = append(fields, "instance")
			}
			if scope == "wp" {
				fields = append(fields, "node_id")
			}
			result.Profiles = append(result.Profiles, d.Profile{ID: scope + "." + flow + ".metrics", Version: 1, Scope: scope, Flow: flow, Stage: stage, Modes: []string{"live", "historical"}, TargetFields: fields, Description: "Bounded classic-histogram completion latency/throughput; source-only evidence, not pending-work or root-cause diagnosis"})
		}
	}
	return result
}

func (Module) ValidateConnection(c d.Connection) error { _, err := settings(c); return err }

type signal struct {
	ID        string
	Metric    string
	Component string
	Stage     string
	Factor    float64 // Metric units to milliseconds.
	Match     map[string]string
	Regex     map[string]string
	Gauge     bool
}

func signals(scope, flow string) []signal {
	wp := signal{ID: "wp.append", Metric: "woodpecker_server_logstore_operation_latency", Component: "woodpecker", Stage: "append", Factor: 1, Match: map[string]string{"status": "success"}, Regex: map[string]string{"operation": "add_entry|add_entry_batch"}}
	proxy := signal{ID: "milvus.append", Metric: "milvus_proxy_mutation_latency", Component: "proxy", Stage: "append", Factor: 1, Regex: map[string]string{"msg_type": "insert|upsert|delete"}}
	if flow == "read" {
		wp.ID = "wp.wal_read"
		wp.Stage = "wal_read"
		wp.Regex["operation"] = "get_batch_entries"
		proxy.ID = "milvus.query"
		proxy.Stage = "query"
		proxy.Metric = "milvus_proxy_sq_latency"
		proxy.Regex = nil
	}
	result := []signal{}
	if scope != "milvus" {
		result = append(result, wp)
	}
	if scope != "wp" {
		result = append(result, proxy)
	}
	if scope == "e2e" && flow == "write" {
		result = append(result, signal{ID: "milvus.wal_append", Metric: "milvus_wal_append_message_duration_seconds", Component: "streamingnode", Stage: "append", Factor: 1000})
	}
	if scope != "milvus" && flow == "write" {
		result = append(result, signal{ID: "wp.admission", Metric: "woodpecker_server_logstore_write_backpressure_state", Component: "woodpecker", Stage: "append", Gauge: true})
	}
	return result
}
