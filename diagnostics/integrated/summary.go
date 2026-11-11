package integrated

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/jaeger"
	"github.com/zilliztech/woodpecker/diagnostics/loki"
)

func decodeChild(data []byte, report *d.Report) error { return json.Unmarshal(data, report) }
func digest(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Resolve returns the original finding, without changing its IDs, impact,
// evidence strength, support, contradictions or next checks.
func Resolve(r Report, ref Reference) (d.Finding, bool) {
	for _, run := range r.Runs {
		if run.ModuleID == ref.ModuleID {
			for _, a := range run.Report.Assessments {
				for _, f := range a.Findings {
					if f.ID == ref.ID {
						return f, true
					}
				}
			}
			for _, result := range run.Report.Results {
				for _, f := range result.Findings {
					if f.ID == ref.ID {
						return f, true
					}
				}
			}
		}
	}
	return d.Finding{}, false
}

func validLineage(report d.Report) bool {
	if report.SchemaVersion != d.ReportSchema || report.RunID == "" || len(report.Results) != 1 {
		return false
	}
	obs := map[string]d.Observation{}
	findings := map[string]d.Finding{}
	for _, result := range report.Results {
		for _, o := range result.Observations {
			if o.ID == "" {
				return false
			}
			if old, ok := obs[o.ID]; ok && !reflect.DeepEqual(old, o) {
				return false
			}
			obs[o.ID] = o
		}
		for _, f := range result.Findings {
			if f.ID == "" || f.RuleVersion < 1 {
				return false
			}
			if old, ok := findings[f.ID]; ok && !reflect.DeepEqual(old, f) {
				return false
			}
			findings[f.ID] = f
		}
	}
	check := func(f d.Finding) bool {
		if f.ID == "" || f.RuleVersion < 1 || f.Flow != report.Selection.Flow {
			return false
		}
		for _, id := range append(append([]string{}, f.Supporting...), f.Contradicting...) {
			if _, ok := obs[id]; !ok {
				return false
			}
		}
		return true
	}
	for _, f := range findings {
		if !check(f) {
			return false
		}
	}
	for _, a := range report.Assessments {
		if a.Profile == "" || a.Version < 1 {
			return false
		}
		for _, f := range a.Findings {
			if !check(f) || len(f.DerivedFrom) == 0 {
				return false
			}
			for _, id := range f.DerivedFrom {
				if _, ok := findings[id]; !ok {
					return false
				}
			}
		}
	}
	return true
}

type anchor struct {
	source, deployment, traceID string
	from, to                    time.Time
	observation                 Reference
	finding                     Reference
}

var traceIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

func anchors(run Run) []anchor {
	if run.Role == "current_context" {
		return nil
	}
	result := run.Report.Results[0]
	var meta struct {
		Schema               string `json:"source_schema_version"`
		Deployment           string `json:"bound_deployment"`
		ConfiguredDeployment string `json:"configured_deployment"`
	}
	if json.Unmarshal(result.Metadata, &meta) != nil {
		return nil
	}
	if meta.Deployment == "" {
		meta.Deployment = meta.ConfiguredDeployment
	}
	if meta.Deployment == "" {
		return nil
	}
	out := []anchor{}
	if result.Source == "trace" && meta.Schema == "wpdiag.jaeger/v1" {
		for _, o := range result.Observations {
			if o.Kind != "jaeger_trace" || o.Status != "observed" {
				continue
			}
			var tr jaeger.Trace
			if json.Unmarshal(o.Data, &tr) != nil || !traceIDPattern.MatchString(tr.ID) || strings.Trim(tr.ID, "0") == "" {
				continue
			}
			for _, span := range tr.Spans {
				if span.Selected && span.ID != "" && span.TraceID == tr.ID && span.StartUS > 0 && span.DurationUS >= 0 && span.DurationUS < 86400000000 {
					from := time.UnixMicro(span.StartUS)
					to := from.Add(time.Duration(span.DurationUS) * time.Microsecond)
					out = append(out, anchor{source: "trace", deployment: meta.Deployment, traceID: tr.ID, from: from, to: to, observation: Reference{ModuleID: run.ModuleID, ID: o.ID, SpanID: span.ID}})
				}
			}
		}
	}
	if result.Source == "loki" && meta.Schema == "wpdiag.loki/v1" {
		for _, e := range loki.EventLineage(result) {
			if !traceIDPattern.MatchString(e.Event.TraceID) || strings.Trim(e.Event.TraceID, "0") == "" {
				continue
			}
			at := time.Unix(0, e.TimeNS)
			out = append(out, anchor{source: "loki", deployment: meta.Deployment, traceID: e.Event.TraceID, from: at, to: at, observation: Reference{ModuleID: run.ModuleID, ID: e.ObservationID, EventID: e.Event.ID}, finding: Reference{ModuleID: run.ModuleID, ID: e.FindingID}})
		}
	}
	return out
}

// Summarize is a pure presentation/association step for existing source reports;
// it does not interpret metric thresholds, native rules, logs or span timings.
func Summarize(ctx context.Context, runs []Run) Summary {
	s := Summary{Schema: "wpdiag.summary/v1", Findings: []View{}, Associations: []Association{}, Gaps: []d.Gap{}}
	seen := map[string]Run{}
	conflicts := map[string]bool{}
	for _, run := range runs {
		if old, ok := seen[run.ModuleID]; ok && !reflect.DeepEqual(old, run) {
			conflicts[run.ModuleID] = true
		}
		seen[run.ModuleID] = run
	}
	seen = map[string]Run{}
	views := map[string]bool{}
	groups := map[string][]anchor{}
	count := 0
	gap := func(code, detail string) { s.Gaps = append(s.Gaps, d.Gap{Code: code, Detail: detail}) }
	for _, run := range runs {
		if ctx.Err() != nil {
			gap("summary_cancelled", "Summary interrupted; original module reports remain available")
			return s
		}
		if old, ok := seen[run.ModuleID]; ok {
			if reflect.DeepEqual(old, run) {
				gap("duplicate_module_report_deduplicated", run.ModuleID)
			} else {
				gap("conflicting_module_identity", run.ModuleID)
			}
			continue
		}
		seen[run.ModuleID] = run
		if conflicts[run.ModuleID] {
			gap("conflicting_module_identity", run.ModuleID)
			continue
		}
		if !validLineage(run.Report) {
			gap("invalid_report_lineage", run.ModuleID+": incompatible or dangling/conflicting evidence; no summary or association derived")
			continue
		}
		if run.Role == "current_context" {
			gap("current_runtime_not_historical_evidence", run.ModuleID+": current snapshot stays separate and cannot clear the historical incident")
		}
		add := func(f d.Finding, kind string) {
			key := run.ModuleID + ":" + f.ID
			if views[key] || len(s.Findings) >= 2048 {
				return
			}
			views[key] = true
			s.Findings = append(s.Findings, View{Reference: Reference{ModuleID: run.ModuleID, ID: f.ID}, Stage: f.Stage, Kind: kind, Context: run.Role})
		}
		if len(run.Report.Assessments) > 0 {
			for _, a := range run.Report.Assessments {
				for _, f := range a.Findings {
					add(f, "stage")
				}
			}
		} else {
			for _, result := range run.Report.Results {
				for _, f := range result.Findings {
					add(f, "source")
				}
			}
		}
		for _, a := range anchors(run) {
			if count >= 2048 {
				gap("association_anchor_budget", "At most 2048 anchors are summarized; original source reports retain the remainder")
				break
			}
			groups[a.deployment+":"+a.traceID] = append(groups[a.deployment+":"+a.traceID], a)
			count++
		}
	}
	if len(s.Findings) == 2048 {
		gap("summary_finding_budget", "At most 2048 finding references are displayed; full source reports remain available")
	}
	keys := []string{}
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if ctx.Err() != nil {
			gap("summary_cancelled", "Association interrupted")
			break
		}
		list := groups[key]
		refs := map[string]Reference{}
		frefs := map[string]Reference{}
		comparisons := 0
		for i, a := range list {
			for _, b := range list[i+1:] {
				comparisons++
				if comparisons > 10000 || ctx.Err() != nil {
					break
				}
				if a.source == b.source || a.to.Before(b.from) || b.to.Before(a.from) {
					continue
				}
				for _, x := range []anchor{a, b} {
					refs[digest(x.observation)] = x.observation
					if x.finding.ID != "" {
						frefs[digest(x.finding)] = x.finding
					}
				}
			}
			if comparisons > 10000 || ctx.Err() != nil {
				break
			}
		}
		if comparisons > 10000 {
			gap("association_comparison_budget", "Identity group exceeded 10000 bounded comparisons")
		}
		if len(refs) < 2 {
			continue
		}
		observations := []Reference{}
		findings := []Reference{}
		for _, ref := range refs {
			observations = append(observations, ref)
		}
		for _, ref := range frefs {
			findings = append(findings, ref)
		}
		sortRefs(observations)
		sortRefs(findings)
		s.Associations = append(s.Associations, Association{ID: "integrated:identity:" + digest(observations), Kind: "observed_trace_identity_and_event_time", Identity: list[0].traceID, Findings: findings, Observations: observations, Unknown: []string{"Matching observed trace IDs and overlapping event times suggest association, not causation or an additional independent confirmation of the same event", "Configured deployment binding is not a Milvus channel/WP-client metadata namespace authority", "No quantile subtraction, source-count confidence or current-versus-past contradiction was inferred"}})
		if len(s.Associations) == 128 {
			gap("association_count_budget", "At most 128 associations are displayed")
			break
		}
	}
	sort.SliceStable(s.Findings, func(i, j int) bool {
		a, b := s.Findings[i], s.Findings[j]
		if (a.Context == "current_context") != (b.Context == "current_context") {
			return a.Context != "current_context"
		}
		return moduleOrder(a.ModuleID) < moduleOrder(b.ModuleID)
	})
	if len(runs) > 1 {
		gap("cross_source_metadata_mapping_unavailable", "Numeric node/log IDs, service contexts, exporter/Pod names and same-window anomalies are not authoritative Milvus/WP client mappings. Unassociated source findings remain independent; source-owned support/contradiction is preserved")
	}
	if len(s.Associations) == 0 && len(runs) > 1 {
		gap("cross_source_identity_or_time_unresolved", "No supported observed cross-source trace/event identity with compatible actual times was established")
	}
	return s
}

func sortRefs(refs []Reference) {
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].ModuleID+":"+refs[i].ID+":"+refs[i].EventID+":"+refs[i].SpanID < refs[j].ModuleID+":"+refs[j].ID+":"+refs[j].EventID+":"+refs[j].SpanID
	})
}
