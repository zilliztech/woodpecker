package checks

import (
	"fmt"
	"strings"

	"github.com/zilliztech/woodpecker/diagnostics/evidence"
)

var metricNames = map[string]string{
	"W01": "writer-latency", "W02": "writer-errors", "W03": "writer-throughput", "W04": "append-submissions", "W05": "append-pending", "W06": "client-append-latency", "W07": "writer-progress", "W08": "replica-append-errors",
	"S01": "server-append-errors", "S02": "server-buffer-wait", "S03": "server-flush-latency", "S04": "server-flush-throughput", "S05": "disk-rejections", "S06": "server-disk-watermark", "S07": "server-read-latency", "S08": "server-read-errors",
	"R01": "reader-throughput", "R02": "reader-iteration-latency", "R03": "reader-errors", "R04": "reader-progress", "R06": "direct-read-latency", "R07": "direct-read-errors", "R08": "quorum-read-errors", "R09": "read-failovers", "R10": "read-branch-activity",
	"OR01": "reader-open-success", "OR02": "reader-open-errors", "OR03": "reader-open-latency", "OR04": "reader-open-error-latency",
	"OW01": "writer-open-success", "OW02": "writer-lock-errors", "OW03": "writer-fencing-errors", "OW04": "writer-open-error-latency",
	"B01": "segment-rolls", "B02": "compaction-errors", "B03": "compaction-progress", "H01": "cpu-utilization", "H02": "io-wait-rate", "H03": "memory-utilization",
}

func metricStep(id string) string {
	switch {
	case strings.HasPrefix(id, "OR"):
		return "open-reader"
	case strings.HasPrefix(id, "OW"):
		return "open-writer"
	case strings.HasPrefix(id, "H"):
		return "resources"
	case strings.HasPrefix(id, "B"):
		return "maintenance"
	case id == "W01" || id == "W02" || id == "W03" || id == "W07":
		return "writer"
	case strings.HasPrefix(id, "W"):
		return "client-append"
	case id == "S02":
		return "server-buffer"
	case id == "S03" || id == "S04":
		return "server-flush"
	case id == "S05" || id == "S06":
		return "server-disk"
	case id == "S07" || id == "S08":
		return "server-read"
	case strings.HasPrefix(id, "S"):
		return "server-append"
	case id == "R06" || id == "R07":
		return "direct-read"
	case id == "R08" || id == "R09" || id == "R10":
		return "replica-read"
	default:
		return "reader"
	}
}

var (
	metricCatalog     = evidence.Definitions()
	metricDefinitions = metricCatalog.Evidence
)

func rawDefinition(id string) evidence.Definition {
	for _, d := range metricDefinitions {
		if d.ID == id {
			return d
		}
	}
	return evidence.Definition{}
}

func metricDescription(raw evidence.Definition, p evidence.Policy) evidence.Description {
	x := evidence.Describe(raw, p)
	if raw.Kind == "p99" {
		x.Formula = fmt.Sprintf("Per %g-second calculation block: P99 is the bucket interval at rank 0.99 × Δcount, using Δbucket.", float64(p.CalculationWindowMS)/1000)
	}
	switch raw.ID {
	case "S01", "S08":
		x.Meaning = "Recorded server operation outcomes with status=error or error_get_processor; routine EOF is excluded."
		for i, m := range x.Metrics {
			x.Metrics[i] = strings.ReplaceAll(m, `status="error"`, `status=~"error|error_get_processor"`)
		}
	case "W05":
		x.Formula = "For each exact series, find nondecreasing intervals with a positive net increase."
		x.Criterion = fmt.Sprintf("Growth sustained for at least %g min; submission activity and a write stall are assessed separately.", float64(p.SustainMS)/60000)
	case "W07", "R04", "B03":
		x.Criterion = "Report paired-position advancement as context; no stall or readable-backlog verdict."
	case "R09":
		x.Meaning = "Recorded Reader failover events."
		x.Criterion = "Report observed failovers as context; a failover alone does not establish a failed read."
	}
	return x
}

func Definitions(p evidence.Policy) []Definition {
	out := []Definition{}
	for _, raw := range metricDefinitions {
		if raw.Source != "metrics" {
			continue
		}
		flow := "write"
		if strings.HasPrefix(raw.ID, "R") || strings.HasPrefix(raw.ID, "OR") {
			flow = "read"
		}
		out = append(out, Definition{ID: raw.ID, Name: metricNames[raw.ID], Source: "metrics", Step: metricStep(raw.ID), Title: raw.Title, Flow: flow, Kind: raw.Kind, RuleVersion: 2, Metrics: raw.Metrics, References: raw.References, Description: metricDescription(raw, p), Limits: raw.Limits})
	}
	type phase struct {
		id, name, step, flow, role string
		ops, scopes, refs          []string
	}
	phases := []phase{
		{"TW", "writer-operation", "writer", "write", "wp_client", []string{"Write", "WriteAsync"}, []string{"LogWriter"}, []string{"woodpecker/log/log_writer.go"}},
		{"TA", "client-append", "client-append", "write", "wp_client", []string{"Execute"}, []string{"AppendOp", "BatchAppendOp"}, []string{"woodpecker/segment/append_op.go"}},
		{"TS", "server-append", "server-append", "write", "wp_server", []string{"AddEntry", "woodpecker.proto.logstore.LogStore/AddEntry", "woodpecker.proto.logstore.LogStore/AddEntries"}, nil, []string{"server/logstore.go", "server/processor/segment_processor.go"}},
		{"TF", "server-flush", "server-flush", "write", "wp_server", []string{"processFlushTask"}, []string{"LocalFileWriter", "StagedFileWriter"}, []string{"server/storage/disk/writer_impl.go", "server/storage/stagedstorage/writer_impl.go"}},
		{"TR", "reader-operation", "reader", "read", "wp_client", []string{"ReadNext"}, []string{"LogReader"}, []string{"woodpecker/log/log_reader.go"}},
		{"TB", "server-read", "server-read", "read", "wp_server", []string{"GetBatchEntriesAdv", "woodpecker.proto.logstore.LogStore/GetBatchEntriesAdv"}, nil, []string{"server/logstore.go"}},
		{"TO", "open-reader", "open-reader", "read", "wp_client", []string{"OpenLogReader"}, []string{"LogHandle"}, []string{"woodpecker/log/log_handle.go"}},
		{"TP", "open-writer", "open-writer", "write", "wp_client", []string{"OpenLogWriter", "OpenInternalLogWriter"}, []string{"LogHandle"}, []string{"woodpecker/log/log_handle.go"}},
	}
	for _, a := range phases {
		for i, kind := range []string{"duration", "errors"} {
			desc := evidence.Description{Metrics: a.ops, Meaning: "Retained, selected WP operation spans; only spans starting inside the incident window are assessed.", Formula: "For each matching span, compare duration with the configured millisecond reference.", Criterion: "Duration exceeds the configured reference."}
			if kind == "errors" {
				desc.Formula = "Count matching spans with a verified error status."
				desc.Criterion = "At least one matching span has an error flag; application outcome is not inferred."
			}
			limits := "Sampled spans are not a population-health guarantee. Service-scoped evidence does not establish a log/request association. Parent/child timers are inclusive and cannot be subtracted."
			if a.step == "reader" {
				limits += " ReadNext includes normal tail waits; a long span alone does not establish a slow Reader."
			}
			out = append(out, Definition{ID: fmt.Sprintf("%s%02d", a.id, i+1), Name: a.name + "-" + kind, Source: "trace", Step: a.step, Title: strings.ReplaceAll(a.name, "-", " ") + " " + kind, Flow: a.flow, Kind: kind, Role: a.role, Operations: a.ops, Scopes: a.scopes, References: a.refs, RuleVersion: 2, Description: desc, Limits: limits})
		}
	}
	for i, def := range out {
		if def.Source == "trace" && def.Kind == "duration" {
			out[i].Description.Criterion = fmt.Sprintf("Recorded span duration > %g ms", threshold(def, p))
		}
	}
	for i := range out {
		out[i].Assessment = RuleFor(out[i], p)
		if countRule(out[i]) && out[i].Source == "trace" {
			out[i].Description.Criterion = fmt.Sprintf("Verified error span count > %g in matching retained spans", *out[i].Assessment.Threshold)
		}
		out[i].Component = CheckComponent(out[i])
		out[i].Requires = RequirementsFor(out[i])
	}
	return out
}

func Lookup(source, name string, p evidence.Policy) (Definition, bool) {
	for _, d := range Definitions(p) {
		if d.Source == source && (d.Name == name || strings.EqualFold(d.ID, name)) {
			return d, true
		}
	}
	return Definition{}, false
}

func Scenarios(p evidence.Policy) []Scenario {
	aliases := map[string]string{"read_slow": "slow-reads", "write_slow": "slow-writes", "read_stuck": "stuck-reads", "write_stuck": "stuck-writes", "open_reader": "open-reader", "open_writer": "open-writer"}
	out := []Scenario{}
	defs := Definitions(p)
	for _, old := range metricCatalog.Problems {
		id, ok := aliases[old.ID]
		if !ok {
			continue
		}
		s := Scenario{ID: id, Title: old.Title, Checks: []string{}}
		wanted := map[string]bool{}
		steps := map[string]bool{}
		for _, key := range append(append([]string{}, old.Evidence...), old.Extension...) {
			if metricNames[key] != "" {
				wanted[key] = true
				steps[metricStep(key)] = true
			}
		}
		for _, d := range defs {
			if wanted[d.ID] || d.Source == "trace" && steps[d.Step] {
				s.Checks = append(s.Checks, d.ID)
			}
		}
		if old.ID == "read_stuck" {
			s.Gaps = append(s.Gaps, "Historical readable backlog and expected demand are not established by these position/throughput checks.")
		}
		out = append(out, s)
	}
	all := Scenario{ID: "all", Title: "All agreed WAL checks", Checks: []string{}}
	for _, d := range defs {
		all.Checks = append(all.Checks, d.ID)
	}
	return append(out, all)
}

func Selection(q Request, p evidence.Policy) ([]Definition, []string, error) {
	if q.Bindings != nil {
		for _, scope := range []*Scope{q.Bindings.Client, q.Bindings.Server} {
			if scope != nil && scope.Milvus != nil {
				return nil, nil, fmt.Errorf("Milvus diagnostic associations are no longer supported")
			}
		}
	}

	if q.IncludeMilvus {
		return nil, nil, fmt.Errorf("Milvus diagnosis has been retired; only Woodpecker checks are supported")
	}
	defs := Definitions(p)
	if q.Scenario == "" {
		d, ok := Lookup(q.Source, q.Check, p)
		if !ok {
			return nil, nil, fmt.Errorf("unknown %s check; use wp diag catalog --source %s", q.Source, q.Source)
		}
		return []Definition{d}, nil, nil
	}
	if q.Source != "" || q.Check != "" {
		return nil, nil, fmt.Errorf("scenario cannot also select an atomic check")
	}
	var chosen *Scenario
	for _, s := range Scenarios(p) {
		if s.ID == q.Scenario {
			cp := s
			chosen = &cp
		}
	}
	if chosen == nil {
		return nil, nil, fmt.Errorf("unknown scenario; use wp diag catalog")
	}
	wanted := map[string]bool{}
	for _, id := range chosen.Checks {
		wanted[id] = true
	}
	out := []Definition{}
	for _, d := range defs {
		if wanted[d.ID] {
			out = append(out, d)
		}
	}
	return out, chosen.Gaps, nil
}
