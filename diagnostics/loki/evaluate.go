package loki

import (
	"fmt"
	"sort"
	"strings"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

// Association keeps event-level lineage: page IDs alone cannot distinguish two
// different correlations whose events happened to be returned in the same page.
type Association struct {
	FindingID     string   `json:"finding_id"`
	EventIDs      []string `json:"event_ids"`
	IdentityKinds []string `json:"identity_kinds"`
}

func evaluate(result *d.Result, events map[string]Event, support map[string][]string, r d.Request, s Settings) []Association {
	groups := map[string][]Event{}
	related := map[string][]Event{}
	for _, e := range events {
		key := e.Pattern + ":" + e.StreamID
		groups[key] = append(groups[key], e)
		// IDs are observed content, never a service namespace/log association.
		if e.TraceID != "" {
			related["trace:"+e.TraceID] = append(related["trace:"+e.TraceID], e)
		}
		if e.RequestID != "" {
			related["request:"+e.RequestID] = append(related["request:"+e.RequestID], e)
		}
	}
	keys := []string{}
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		list := groups[key]
		sort.Slice(list, func(i, j int) bool { return eventNS(list[i]) < eventNS(list[j]) })
		e := list[0]
		var pattern Pattern
		for _, p := range Catalog() {
			if p.ID == e.Pattern {
				pattern = p
				break
			}
		}
		outcome, severity := "suspected_operation_problem", "warning"
		if e.Pattern == "wp.disk_recovered" {
			outcome = "observed_recovery_event"
			severity = "info"
		}
		refs := []string{}
		for _, event := range list {
			refs = append(refs, support[event.ID]...)
		}
		refs = unique(refs)
		f := d.Finding{ID: eventFindingID(e), RuleID: "loki." + pattern.ID, RuleVersion: 1, Flow: r.Selection.Flow, Stage: r.Selection.Stage, Target: d.Target{Deployment: s.Deployment, Instance: e.Labels["pod"]}, Outcome: outcome, Severity: severity, Strength: "observed_log_event", Summary: fmt.Sprintf("%s (%d unique event identities; excerpt: %q)", pattern.Interpretation, len(list), e.Excerpt), Supporting: refs, Contradicting: []string{}, Unknown: []string{"A logged symptom is not a confirmed root cause or proof of request failure; retry outcomes and collection completeness are unknown"}, NextChecks: []string{"Inspect the executed LogQL and exact window in the evidence; narrow by this Pod and observed request/trace ID", "Compare bounded component metrics and, when retained, the actual matching trace; these checks have not been executed"}}
		if e.Pattern == "wp.disk_pressure" {
			for _, candidate := range events {
				if candidate.Pattern == "wp.disk_recovered" && candidate.StreamID == e.StreamID && eventNS(candidate) > eventNS(list[len(list)-1]) {
					f.Contradicting = append(f.Contradicting, support[candidate.ID]...)
					f.Unknown = append(f.Unknown, "A later recovery message on this stream limits persistence of the pressure hypothesis; intervening state and later relapse are unknown")
				}
			}
			f.Contradicting = unique(f.Contradicting)
		}
		result.Findings = append(result.Findings, f)
	}
	if len(groups) == 0 {
		result.Findings = append(result.Findings, d.Finding{ID: "loki:no-match", RuleID: "loki.known_patterns", RuleVersion: 1, Flow: r.Selection.Flow, Stage: r.Selection.Stage, Target: d.Target{Deployment: s.Deployment}, Outcome: "inconclusive", Severity: "unknown", Strength: "insufficient", Summary: "No selected supported message matched; this does not establish normal operation", Supporting: []string{}, Contradicting: []string{}, Unknown: []string{"Idle traffic, missing collectors, delivery lag, retention, selection and unsupported log formats cannot be distinguished by no-match"}, NextChecks: []string{"Check collector readiness/RBAC, application version, retained interval and expected traffic; no check was executed"}})
	}
	if r.Selection.Scope != "e2e" {
		return []Association{}
	}
	joins := 0
	keys = nil
	for key := range related {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	// Two identity keys can describe exactly the same events. Keep one
	// association per event set; sharing a page ID is not event-set equality.
	type association struct {
		events []Event
		kinds  []string
		ids    []string
	}
	associations := map[string]*association{}
	for _, key := range keys {
		list := related[key]
		roles := map[string]bool{}
		eventIDs := []string{}
		for _, e := range list {
			roles[e.Labels["component"]] = true
			eventIDs = append(eventIDs, e.ID)
		}
		if len(roles) < 2 {
			continue
		}
		eventIDs = unique(eventIDs)
		setID := fingerprint(eventIDs)
		kind := strings.SplitN(key, ":", 2)[0]
		if old := associations[setID]; old != nil {
			old.kinds = unique(append(old.kinds, kind))
			continue
		}
		associations[setID] = &association{events: list, kinds: []string{kind}, ids: eventIDs}
	}
	setIDs := []string{}
	for key := range associations {
		setIDs = append(setIDs, key)
	}
	sort.Strings(setIDs)
	lineage := []Association{}
	for _, setID := range setIDs {
		group := associations[setID]
		roles := map[string]bool{}
		refs := []string{}
		for _, e := range group.events {
			roles[e.Labels["component"]] = true
			refs = append(refs, support[e.ID]...)
		}
		joins++
		lineage = append(lineage, Association{FindingID: "loki:association:" + setID, EventIDs: group.ids, IdentityKinds: group.kinds})
		result.Findings = append(result.Findings, d.Finding{ID: "loki:association:" + setID, RuleID: "loki.e2e.observed_identity", RuleVersion: 1, Flow: r.Selection.Flow, Stage: r.Selection.Stage, Target: d.Target{Deployment: s.Deployment}, Outcome: "related_component_events", Severity: "info", Strength: "matching_observed_identity", Summary: fmt.Sprintf("%d component roles logged supported events with the same observed %s ID(s)", len(roles), strings.Join(group.kinds, "/")), Supporting: unique(refs), Contradicting: []string{}, Unknown: []string{"Matching IDs suggest association, not causation; ID uniqueness, propagation and retries require confirmation", "No collection/channel to WP metadata namespace/log mapping is established", "Shared event IDs across associations are the same evidence, not independent confirmations"}, NextChecks: []string{"Inspect the retained matching trace and component event IDs before attributing a client request's delay"}})
	}

	if joins == 0 {
		result.Gaps = append(result.Gaps, d.Gap{Code: "e2e_identity_mapping_missing", Detail: "No supported events across component roles share an observed request/trace ID; same-window events are not joined. Channel/log joins are unsupported in catalog v1"})
	}
	return lineage
}

func unique(values []string) []string {
	m := map[string]bool{}
	out := []string{}
	for _, v := range values {
		if !m[v] {
			m[v] = true
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}

func eventNS(e Event) int64 { n, _ := validNS(e.TimestampNS); return n }
