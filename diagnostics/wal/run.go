package wal

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/evidence"
	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
	"github.com/zilliztech/woodpecker/diagnostics/wpcli"
)

type planned = evidence.MetricPlan

func plan(t Target, ids []string, mv bool) planned { return evidence.PlanMetrics(t, ids, mv) }
func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func connected(c d.Config, source, name string) (d.Connection, bool) {
	x, ok := c.Sources[source].Connections[name]
	return x, ok && x.IsEnabled()
}

// Run reuses existing source clients and returns a new versioned problem report.
// Old source/stage report contracts and CLI rules are not reinterpreted.
func Run(ctx context.Context, c d.Config, q Request, events chan<- Event) Report {
	s, err := LoadSettings(c)
	now := time.Now().UTC()
	r := Report{SchemaVersion: "wpdiag.wal/v1", RunID: uuid.NewString(), Problem: q.Problem, Check: q.Check, Status: "completed", StartedAt: now, Evidence: []Evidence{}, Observations: []d.Observation{}, Problems: []d.Problem{}, Policy: s.Policy}
	finish := func() Report {
		for i := range r.Evidence {
			r.Evidence[i].DataState = DataState(r.Evidence[i])
			r.Evidence[i].StatusLabel = DisplayState(r.Evidence[i])
		}
		r.FinishedAt = time.Now().UTC()
		if ctx.Err() != nil {
			r.Status = "cancelled"
			r.Complete = false
		}
		return r
	}
	if err != nil {
		r.Status = "invalid"
		r.Problems = append(r.Problems, d.Problem{Code: "invalid_workbench_config", Message: err.Error()})
		return finish()
	}
	if err = ValidateRequest(s, q); err != nil {
		r.Status = "invalid"
		r.Problems = append(r.Problems, d.Problem{Code: "invalid_selection", Message: err.Error()})
		return finish()
	}
	r.Target = s.Targets[q.Target]
	r.Target.ID = q.Target
	p, _ := ProblemByID(q.Problem)
	ids, _ := SelectedChecks(q)
	by := map[string]Definition{}
	for _, d := range Definitions().Evidence {
		by[d.ID] = d
	}
	if q.Problem == "metadata" {
		r.TimeRange = TimeRange{Mode: "snapshot", ObservedAt: &now}
		connection, ok := connected(c, "wpcli", r.Target.WPCLI)
		for _, id := range ids {
			e := unknown(id, "metadata_authority_missing", "Configure an existing wpcli connection, explicit log name, etcd endpoints and metadata prefix")
			if ctx.Err() != nil {
				e = unknown(id, "cancelled", "Metadata collection stopped before this command")
			} else if ok && r.Target.MetadataReady {
				emit(ctx, events, id, "collecting", "Running the existing read-only wpcli command")
				o, xerr := wpcli.ReadMetadata(ctx, connection, wpcli.MetadataRequest{ID: id, LogName: r.Target.LogName, Etcd: r.Target.Metadata.Etcd, Prefix: r.Target.Metadata.Prefix}, 2<<20)
				o.ID = r.RunID + ":" + id
				if xerr != nil {
					o.Code = "native_collection_failed"
					r.Observations = append(r.Observations, o)
					e = evidenceError(id, xerr)
					e.ObservationIDs = []string{o.ID}
				} else {
					r.Observations = append(r.Observations, o)
					e = Evidence{ID: id, Status: "context", Value: "Current snapshot", Note: "Original wpcli output; this view does not independently validate consistency", ObservationIDs: []string{o.ID}}
					if id == "M04" {
						var original struct {
							State  string   `json:"state"`
							Issues []string `json:"issues"`
						}
						if json.Unmarshal(o.Data, &original) != nil || original.State == "" {
							e = unknown(id, "invalid_native_result", "Existing meta check returned an unsupported output contract")
						} else if original.State == "invalid" {
							e.Status = "abnormal"
							e.Value = fmt.Sprintf("%d findings", len(original.Issues))
							e.Note = "Existing wpcli meta check returned constraint violations; inspect its original output"
						} else if original.State == "ok" {
							e.Status = "clear"
							e.Value = "Existing checks pass"
							e.Note = "Only the checks already implemented in wpcli passed"
						} else {
							e = unknown(id, "unknown_native_state", "Existing meta check did not return a recognized state")
						}
					}
				}
			}
			e = withFinding(e, by[id], r)
			r.Evidence = append(r.Evidence, e)
			emit(ctx, events, id, "complete", e.Status)
		}
		r.Complete = ctx.Err() == nil
		return finish()
	}
	r.TimeRange = TimeRange{Mode: "historical", Start: q.From.UTC(), End: q.To.UTC()}
	plans := []planned{plan(r.Target, ids, false)}
	if q.IncludeMilvus && len(p.Extension) > 0 {
		plans = append(plans, plan(r.Target, ids, true))
	}
	datasets := map[string]prometheus.Dataset{}
	keys := map[string][]string{}
	skip := map[string]string{}
	refs := map[string]string{}
	queryCodes := map[string]string{}
	for index, pl := range plans {
		for id, v := range pl.Keys {
			keys[id] = v
		}
		for id, v := range pl.Skip {
			skip[id] = v
		}
		if len(pl.Queries) == 0 {
			continue
		}
		name := r.Target.Prometheus
		if index == 1 {
			if r.Target.Milvus == nil {
				for _, id := range p.Extension {
					skip[id] = "No explicit Milvus association configured"
				}
				continue
			}
			name = r.Target.Milvus.Prometheus
		}
		connection, ok := connected(c, "prometheus", name)
		if !ok {
			for id := range pl.Keys {
				skip[id] = "Prometheus connection is absent or disabled"
			}
			continue
		}
		target := d.Target{}
		collection := prometheus.CollectWAL(ctx, connection, target, q.From, q.To, pl.Queries, d.DefaultLimits(), func(key string) {
			for id, ks := range pl.Keys {
				if contains(ks, key) {
					emit(ctx, events, id, "collecting", "Collecting historical source evidence")
				}
			}
		})
		for k, v := range collection.Datasets {
			datasets[k] = v
		}
		for _, o := range collection.Observations {
			key := strings.TrimPrefix(o.SourceID, "prometheus:")
			refs[key] = o.ID
			if o.Code != "" {
				queryCodes[key] = o.Code
			}
			r.Observations = append(r.Observations, o)
		}
		r.Problems = append(r.Problems, collection.Problems...)
		if collection.Status != "completed" {
			r.Status = "partial"
		}
	}
	initial := map[string]Evidence{}
	for _, id := range ids {
		e := evaluate(by[id], keys[id], datasets, s.Policy, q.From, q.To)
		if reason, ok := skip[id]; ok {
			e = unknown(id, "target_or_source_unavailable", reason)
		}
		if by[id].Source != "metrics" {
			e = unknown(id, "deferred_evidence", by[id].Availability+" Use the existing independent source command for additional investigation; no automatic correlation is assumed.")
		}
		collectedRefs := []string{}
		for _, key := range keys[id] {
			if code := queryCodes[key]; code != "" {
				e = unknown(id, code, qualityExplanation(code))
			}
			if ref := refs[key]; ref != "" {
				collectedRefs = append(collectedRefs, ref)
			}
		}
		e.ObservationIDs = unique(collectedRefs)
		initial[id] = e
	}
	corroborate(initial, s.Policy, q.To.Sub(q.From))
	for _, id := range ids {
		e := initial[id]
		if e.Status == "abnormal" || e.Status == "not_applicable" {
			for _, dep := range by[id].Dependencies {
				e.ObservationIDs = append(e.ObservationIDs, initial[dep].ObservationIDs...)
			}
			e.ObservationIDs = unique(e.ObservationIDs)
		}
		e = withFinding(e, by[id], r)
		r.Evidence = append(r.Evidence, e)
		emit(ctx, events, id, "complete", e.Status)
	}
	r.Complete = ctx.Err() == nil
	if !r.Complete {
		for i := range r.Evidence {
			if len(r.Evidence[i].ObservationIDs) == 0 {
				r.Evidence[i] = unknown(r.Evidence[i].ID, "cancelled", "No evidence was collected before cancellation")
			}
		}
	}
	return finish()
}

func unique(xs []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, x := range xs {
		if !seen[x] {
			out = append(out, x)
			seen[x] = true
		}
	}
	return out
}

func withFinding(e Evidence, def Definition, r Report) Evidence {
	if e.Status != "abnormal" {
		return e
	}
	e.Finding = &d.Finding{ID: r.RunID + ":" + e.ID, RuleID: observationCode(e.ID), RuleVersion: 1, Flow: map[bool]string{true: "read", false: "write"}[strings.HasPrefix(r.Problem, "read") || r.Problem == "open_reader"], Stage: "wal", Outcome: "observed_rule_match", Severity: "warning", Strength: "observed", Summary: e.Note, Supporting: append([]string{}, e.ObservationIDs...), Contradicting: []string{}, Unknown: []string{def.Limits}, NextChecks: []string{"Collect the missing evidence needed to distinguish causes; suggested, not executed"}}
	return e
}

func evaluate(d Definition, k []string, data map[string]prometheus.Dataset, p Policy, from, to time.Time) Evidence {
	return evidence.Evaluate(d, k, data, p, from, to)
}

func samePopulation(a, b prometheus.WALFact) bool {
	for _, key := range []string{"instance", "node_id", "log_ns", "log_id"} {
		if a.Labels[key] != b.Labels[key] {
			return false
		}
	}
	if a.JoinKey != "" && b.JoinKey != "" && a.JoinKey != b.JoinKey {
		return false
	}
	return a.Labels["instance"] != "" && a.Labels["log_id"] != "" && a.Labels["log_ns"] != ""
}

func corroborate(es map[string]Evidence, p Policy, window time.Duration) {
	pending, hasPending := es["W05"]
	submits, hasSubmits := es["W04"]
	if hasPending && hasSubmits {
		for index, a := range pending.Facts {
			if a.Code != "" {
				continue
			}
			xs := a.Samples
			for _, b := range submits.Facts {
				if b.Code != "" || b.Delta <= 0 || !samePopulation(a, b) || a.ObservedFromMS != b.ObservedFromMS || a.ObservedToMS != b.ObservedToMS {
					continue
				}
				if len(xs) > 1 {
					start := xs[0].TimeMS
					first := xs[0].Value
					best := int64(0)
					for i := 1; i < len(xs); i++ {
						if xs[i].Value < xs[i-1].Value {
							start = xs[i].TimeMS
							first = xs[i].Value
						}
						if xs[i].Value > first {
							best = max(best, xs[i].TimeMS-start)
						}
					}
					if best >= p.SustainMS {
						pending.Facts[index].Assessment = "abnormal"
						pending.Facts[index].Reading = fmt.Sprintf("%.0f → %.0f", a.First, a.Last)
						pending.Status = "abnormal"
						pending.Value = fmt.Sprintf("%.0f → %.0f", a.First, a.Last)
						pending.Note = "Pending operations remained nondecreasing and increased over the configured sustained interval, with append submissions in the same exporter/log window"
					}
				}
			}
		}
		es["W05"] = pending
	}
	pos, ok := es["W07"]
	if ok && hasPending && hasSubmits && window >= 120*time.Second {
		for index, a := range pos.Facts {
			if a.Code != "" || a.Delta != 0 || a.ObservedToMS-a.ObservedFromMS < 120000 {
				continue
			}
			for _, b := range submits.Facts {
				if b.Code != "" || b.Delta <= 0 || !samePopulation(a, b) || a.ObservedFromMS != b.ObservedFromMS || a.ObservedToMS != b.ObservedToMS {
					continue
				}
				for _, c := range pending.Facts {
					if c.Code == "" && c.Minimum > 0 && samePopulation(a, c) && a.ObservedFromMS == c.ObservedFromMS && a.ObservedToMS == c.ObservedToMS {
						pos.Facts[index].Assessment = "abnormal"
						pos.Facts[index].Reading = "No advancement"
						pos.Status = "abnormal"
						pos.Value = "No advancement"
						pos.Note = "Acknowledgment position did not advance over a >=120s retained window with same-population submissions and continuously pending work; cause unconfirmed"
					}
				}
			}
		}
		es["W07"] = pos
	}
	branch, ok := es["R10"]
	if ok {
		for _, direct := range branch.Facts {
			if direct.Code != "" || direct.Delta != 0 || !strings.Contains(direct.Labels["__name__"], "direct_read_requests") {
				continue
			}
			hasSuccess, hasError := false, false
			for _, f := range branch.Facts {
				if !samePopulation(direct, f) || f.Code != "" || f.Delta != 0 || !strings.Contains(f.Labels["__name__"], "direct_read_requests") {
					continue
				}
				hasSuccess = hasSuccess || f.Labels["status"] == "success"
				hasError = hasError || f.Labels["status"] == "error"
			}
			replicaActive := false
			for _, f := range branch.Facts {
				if samePopulation(direct, f) && f.Code == "" && f.Delta > 0 && strings.Contains(f.Labels["__name__"], "quorum_read") {
					replicaActive = true
				}
			}
			if hasSuccess && hasError && replicaActive {
				for _, id := range []string{"R06", "R07"} {
					if old, exists := es[id]; exists && old.Status != "abnormal" && len(branch.Facts) == 3 {
						old.Status = "not_applicable"
						old.Value = "Direct branch unused"
						old.Note = "Qualified same-population direct success/error counters stayed zero while replica activity was observed"
						old.Code = ""
						es[id] = old
					}
				}
			}
		}
	}
}

func SortedEvidenceIDs(r Report) []string {
	out := []string{}
	for _, e := range r.Evidence {
		out = append(out, e.ID)
	}
	sort.Strings(out)
	return out
}

func qualityExplanation(code string) string { return evidence.QualityExplanation(code) }
