package diagnosis

import (
	"context"
	"fmt"
	"sort"
)

func evaluateReplicas(ctx context.Context, report *Report, rule Rule) {
	f := writeBase(rule, "")
	f.NextChecks = []string{"Repeat with --scenario write --log-name NAME --segment ID --etcd ENDPOINTS --meta-prefix WP_CLIENT_PREFIX, selecting all discovered replicas", "wp logstore lac NAME SEGMENT --etcd ENDPOINTS --meta-prefix WP_CLIENT_PREFIX", "Verify client acknowledgements and authoritative namespace before declaring quorum impact"}
	var metadata *Observation
	for i := range report.Observations {
		if report.Observations[i].Kind == "quorum" {
			metadata = &report.Observations[i]
			break
		}
	}
	if metadata == nil || metadata.Status != "observed" || metadata.Quorum == nil {
		f.Summary = "Replica participation is unknown without an explicit WP-client metadata log/segment quorum"
		f.Unknown = append(f.Unknown, "explicit_quorum_unavailable")
		appendFinding(report, f)
		return
	}
	q := metadata.Quorum
	f.Supporting = appendIDs(f.Supporting, metadata.ID)
	f.Scope.LogID, f.Scope.SegmentID = &q.LogID, &q.SegmentID
	f.Unknown = append(f.Unknown, "runtime_tenant_binding_operator_selected", "metadata_may_change_after_snapshot", "client_acknowledgements_unobserved", "sampled_not_continuous")
	if q.State != "Active" || q.ES != len(q.NodeIDs) || q.AQ < 1 || q.WQ < q.AQ || q.WQ > q.ES || q.Revision <= 0 {
		f.Summary = "Finished segment or invalid configured quorum cannot establish live replica pressure"
		f.Unknown = append(f.Unknown, "inactive_or_invalid_quorum")
		appendFinding(report, f)
		return
	}
	seen := map[string]bool{}
	first, last := map[string]int64{}, map[string]int64{}
	complete := true
	for _, id := range q.NodeIDs {
		if ctx.Err() != nil {
			f.Unknown = append(f.Unknown, "evaluation_interrupted")
			complete = false
			break
		}
		if id == "" || seen[id] {
			complete = false
			f.Unknown = append(f.Unknown, "quorum_identity_unresolved_or_ambiguous")
			continue
		}
		seen[id] = true
		for _, gap := range report.Gaps {
			if gap.NodeID == id && gap.Code == "conflicting_duplicate_observation" {
				complete = false
				f.Unknown = append(f.Unknown, "conflicting_replica_observation")
			}
		}
		selected := false
		for _, target := range report.Targets {
			selected = selected || target.NodeID == id && target.Identity == "discovered"
		}
		if !selected {
			complete = false
			f.Unknown = append(f.Unknown, "quorum_replica_not_selected")
			continue
		}
		byRound := map[int]map[string]Observation{}
		for _, obs := range report.Observations {
			if obs.NodeID != id {
				continue
			}
			if byRound[obs.Round] == nil {
				byRound[obs.Round] = map[string]Observation{}
			}
			byRound[obs.Round][obs.Kind] = obs
		}
		order := make([]int, 0, len(byRound))
		for round := range byRound {
			order = append(order, round)
		}
		sort.Ints(order)
		var writers, metrics []Observation
		for _, round := range order {
			writers = append(writers, byRound[round]["writers"])
			metrics = append(metrics, byRound[round]["metrics"])
			f.Supporting = appendIDs(f.Supporting, byRound[round]["metrics"].ID)
			if build := byRound[round]["build"]; build.Code == "unsupported_version" {
				complete = false
				f.Unknown = append(f.Unknown, "unsupported_replica_build")
			}
		}
		if problem := writeWindow(report, writers, metrics); problem != "" {
			complete = false
			f.Unknown = append(f.Unknown, id+":"+problem)
			continue
		}
		valid := true
		var before int64
		for i, obs := range writers {
			w, ok := findWriter(obs, q.LogID, q.SegmentID)
			if !ok || !w.Writable || w.Fenced || w.Finalized || w.Closed || i > 0 && w.Durable < before {
				valid = false
				break
			}
			if i == 0 {
				first[id] = w.Durable
			}
			before = w.Durable
			f.Supporting = appendIDs(f.Supporting, obs.ID)
		}
		if !valid {
			complete = false
			f.Unknown = append(f.Unknown, id+":writer_missing_changed_or_reset")
			continue
		}
		last[id] = before
	}
	f.Summary = fmt.Sprintf("Observed stable loaded writers on %d/%d configured replicas; wq=%d, aq=%d. Loaded writers are not successful acknowledgements", len(last), q.ES, q.WQ, q.AQ)
	if !complete || len(last) != q.ES {
		f.Unknown = append(f.Unknown, "partial_replica_fanout_not_quorum_outage")
		appendFinding(report, f)
		return
	}
	var maxFirst, maxLast int64 = -1, -1
	for _, v := range first {
		if v > maxFirst {
			maxFirst = v
		}
	}
	for _, v := range last {
		if v > maxLast {
			maxLast = v
		}
	}
	lagging := []string{}
	for _, id := range q.NodeIDs {
		if maxLast > maxFirst && last[id] < maxLast && (last[id] == first[id] || maxLast-last[id] > maxFirst-first[id]) {
			lagging = append(lagging, id)
		}
		f.Summary += fmt.Sprintf("; %s durable=%d→%d", id, first[id], last[id])
	}
	if len(lagging) > 0 {
		f.Outcome, f.Severity, f.Strength = "suspected_replica_lag", "warning", "suspected"
		f.Summary += fmt.Sprintf("; lagging candidates=%v while peers advanced. Sampling is non-atomic; this is not a quorum outage or a latency SLO verdict", lagging)
	} else {
		f.Outcome, f.Severity = "no_replica_lag_observed", "none"
		f.Summary += "; no increasing lag was observed; idle/equal positions do not establish write health"
	}
	appendFinding(report, f)
}
