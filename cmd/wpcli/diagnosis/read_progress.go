package diagnosis

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
)

func metadataSamples(report *Report) []Observation {
	var samples []Observation
	for _, obs := range report.Observations {
		if obs.Kind == "read_metadata" {
			samples = append(samples, obs)
		}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i].Round < samples[j].Round })
	return samples
}

func sameReadScope(a, b ReadState) bool {
	a.Readers, b.Readers = nil, nil
	a.Revision, b.Revision = 0, 0
	return reflect.DeepEqual(a, b)
}

func readBoundary(report *Report, metadata Observation, conflicts map[string]bool) (int64, string, []string) {
	refs := []string{metadata.ID}
	s := metadata.ReadState
	if metadata.Status != "observed" || s == nil || conflicts[""] {
		return -1, "read_metadata_incomplete_or_conflicting", refs
	}
	if s.ReportIntervalMS != 30000 || s.Revision <= 0 || s.ES != len(s.NodeIDs) || s.AQ < 1 || s.AQ > s.ES || s.WQ < s.AQ || s.WQ > s.ES {
		return -1, "invalid_or_unsupported_read_quorum", refs
	}
	if s.SegmentState == "Truncated" {
		return -1, "segment_truncated", refs
	}
	if s.SegmentState == "Completed" || s.SegmentState == "Sealed" {
		if s.LastEntry < -1 {
			return -1, "invalid_finalized_boundary", refs
		}
		return s.LastEntry, "", refs
	}
	if s.SegmentState != "Active" {
		return -1, "unsupported_segment_state", refs
	}
	positions := []int64{}
	seen := map[string]bool{}
	gap := ""
	for _, id := range s.NodeIDs {
		if id == "" || seen[id] {
			return -1, "quorum_identity_ambiguous", refs
		}
		seen[id] = true
		selected := false
		for _, target := range report.Targets {
			selected = selected || target.NodeID == id && target.Identity == "discovered"
		}
		if !selected || conflicts[id] {
			gap = "partial_quorum_observation"
			continue
		}
		var position, metric Observation
		unsupported := false
		for _, obs := range report.Observations {
			if obs.NodeID != id {
				continue
			}
			if obs.Kind == "build" && obs.Code == "unsupported_version" {
				unsupported = true
			}
			if obs.Round != metadata.Round {
				continue
			}
			if obs.Kind == "read_position" {
				position = obs
			}
			if obs.Kind == "metrics" {
				metric = obs
			}
		}
		p := position.ReadPosition
		if unsupported || metric.Status != "observed" || metric.ProcessStart == nil || position.Status != "observed" || p == nil || !p.Present || p.LogID != s.LogID || p.SegmentID != s.SegmentID {
			gap = "partial_quorum_observation"
			continue
		}
		// Two node mappings may accidentally reach the same exporter. Its
		// labels must agree with the discovered identity before counting votes.
		nodeLabels := map[string]bool{}
		for _, counter := range metric.Counters {
			if node := counter.Labels["node_id"]; node != "" {
				nodeLabels[node] = true
			}
		}
		if len(nodeLabels) != 1 || !nodeLabels[id] {
			gap = "quorum_node_identity_unverified"
			continue
		}
		positions = append(positions, p.Durable)
		refs = appendIDs(refs, position.ID)
		refs = appendIDs(refs, metric.ID)
	}
	if len(positions) < s.AQ {
		if gap == "quorum_node_identity_unverified" {
			return -1, gap, refs
		}
		return -1, "insufficient_quorum_positions", refs
	}
	sort.Slice(positions, func(i, j int) bool { return positions[i] > positions[j] })
	return positions[s.AQ-1], gap, refs
}

func evaluateReaderProgress(ctx context.Context, report *Report, rule Rule, conflicts map[string]bool) {
	f := readFinding(rule, "")
	f.Scope.Tenant = "operator-selected WP-client metadata namespace; runtime tenant binding unverified"
	f.Unknown = append(f.Unknown, "physical_readability_not_verified", "runtime_reader_binding_unavailable", "reader_client_profile_unverified", "sampled_not_continuous")
	f.NextChecks = append(f.NextChecks, "Use bounded wp segment probe NAME SEGMENT --max-entries N or wp segment inspect NAME SEGMENT --max-blocks N with the same explicit metadata selection; never infer corruption from reachability alone", "wp log scan NAME --help (choose explicit scan budgets before execution)")
	samples := metadataSamples(report)
	if len(samples) == 0 {
		f.Summary = "Reader and quorum diagnosis requires an explicitly selected WP-client metadata log/segment"
		f.Unknown = append(f.Unknown, "explicit_read_metadata_unavailable")
		appendFinding(report, f)
		return
	}
	first := samples[0].ReadState
	if first != nil {
		f.Scope.LogID, f.Scope.SegmentID = &first.LogID, &first.SegmentID
	}
	if rule.ID == "read_quorum_visibility" {
		latest := samples[len(samples)-1]
		if latest.ReadState != nil {
			f.Scope.LogID, f.Scope.SegmentID = &latest.ReadState.LogID, &latest.ReadState.SegmentID
		}
		boundary, gap, refs := readBoundary(report, latest, conflicts)
		f.Supporting = refs
		if gap != "" {
			f.Summary = "Quorum readability evidence is incomplete; missing/unloaded replicas do not establish an outage or corruption"
			f.Unknown = append(f.Unknown, gap)
			if gap == "partial_quorum_observation" || gap == "insufficient_quorum_positions" {
				f.Outcome = "partial_reachability"
			}
		} else {
			f.Outcome, f.Severity = "readable_boundary_observed", "none"
			origin := "sampled quorum durable positions"
			if latest.ReadState.SegmentState != "Active" {
				origin = "finalized segment metadata"
			}
			f.Summary = fmt.Sprintf("Observed boundary entry=%d from %s (aq=%d of es=%d); metadata/runtime durability is distinct from physical data verification", boundary, origin, latest.ReadState.AQ, latest.ReadState.ES)
		}
		appendFinding(report, f)
		return
	}
	if ctx.Err() != nil || conflicts[""] || report.Mode != "live_window" || len(samples) < 2 || first == nil {
		f.Summary = "Reader progress needs an uninterrupted live window with metadata evidence"
		f.Unknown = append(f.Unknown, "insufficient_reader_window")
		appendFinding(report, f)
		return
	}
	if samples[len(samples)-1].StartedAt.Sub(samples[0].FinishedAt).Milliseconds() < report.WindowMS {
		f.Summary = "Metadata observations do not span the requested window"
		f.Unknown = append(f.Unknown, "reader_window_incomplete")
		appendFinding(report, f)
		return
	}
	boundaries := []int64{}
	boundaryProblem := ""
	previousRevision := int64(0)
	for _, obs := range samples {
		if ctx.Err() != nil {
			f.Unknown = append(f.Unknown, "evaluation_interrupted")
			f.Summary = "Reader evaluation interrupted"
			appendFinding(report, f)
			return
		}
		if obs.Status != "observed" || obs.ReadState == nil || !sameReadScope(*first, *obs.ReadState) || obs.ReadState.Revision < previousRevision {
			f.Summary = "Reader metadata scope or quorum changed, or collection is incomplete"
			f.Unknown = append(f.Unknown, "reader_metadata_scope_changed_or_incomplete")
			appendFinding(report, f)
			return
		}
		previousRevision = obs.ReadState.Revision
		boundary, gap, refs := readBoundary(report, obs, conflicts)
		f.Supporting = append(f.Supporting, refs...)
		if gap != "" {
			boundaryProblem = gap
			f.Unknown = append(f.Unknown, gap)
		}
		boundaries = append(boundaries, boundary)
	}
	// A restarted replica cannot silently support a no-progress comparison.
	starts := map[string]float64{}
	positions := map[string]ReadPosition{}
	for _, obs := range report.Observations {
		if obs.Kind == "read_position" && readQuorumNode(first, obs.NodeID) && obs.ReadPosition != nil && obs.ReadPosition.Present {
			position := *obs.ReadPosition
			if before, ok := positions[obs.NodeID]; ok && (position.Durable < before.Durable || position.Backend != before.Backend) {
				boundaryProblem = "durable_position_reset_or_backend_changed"
				f.Unknown = append(f.Unknown, boundaryProblem)
			}
			positions[obs.NodeID] = position
		}
		if obs.Kind != "metrics" || !readQuorumNode(first, obs.NodeID) || obs.ProcessStart == nil {
			continue
		}
		if previous, ok := starts[obs.NodeID]; ok && previous != *obs.ProcessStart {
			boundaryProblem = "process_restart_in_window"
			f.Unknown = append(f.Unknown, boundaryProblem)
		}
		starts[obs.NodeID] = *obs.ProcessStart
	}
	if len(first.Readers) == 0 {
		f.Outcome, f.Severity = "no_reader_observed", "none"
		f.Summary = "No reader record was present in the baseline; absence may reflect close/lease loss, not global read health"
		for _, obs := range samples {
			if len(obs.ReadState.Readers) > 0 {
				f.Outcome, f.Severity = "inconclusive", "unknown"
				f.Unknown = append(f.Unknown, "reader_set_changed")
			}
		}
		appendFinding(report, f)
		return
	}
	for _, reader := range first.Readers {
		current := f
		current.Unknown = append([]string{}, f.Unknown...)
		current.Scope.Reader = reader.Name
		current.Scope.SegmentID = &reader.SegmentID
		if reader.SegmentID == math.MaxInt64 || reader.NextEntry == math.MaxInt64 {
			current.Summary = "Reader opened at Latest has not established a resolved read position for this comparison"
			current.Unknown = append(current.Unknown, "reader_start_position_unresolved")
			appendFinding(report, current)
			continue
		}
		var series []ReaderPosition
		problem := ""
		previous := reader
		for _, obs := range samples {
			var found *ReaderPosition
			for _, r := range obs.ReadState.Readers {
				if r.Name == reader.Name {
					if found != nil {
						problem = "ambiguous_reader_identity"
					}
					value := r
					found = &value
				}
			}
			if found == nil {
				problem = "reader_disappeared_or_lease_expired"
				break
			}
			r := *found
			age := obs.FinishedAt.UnixMilli() - r.ReportedMS
			switch {
			case r.LeaseTTL <= 0:
				problem = "reader_lease_expired_or_unverified"
			case r.OpenedMS != reader.OpenedMS || r.CreateRevision != reader.CreateRevision:
				problem = "reader_generation_changed"
			case age < 0 || r.OpenedMS > r.ReportedMS:
				problem = "reader_clock_or_timestamp_unverified"
			case age > 2*first.ReportIntervalMS:
				problem = "reader_report_stale"
			case r.ReportedMS < previous.ReportedMS || r.SegmentID < previous.SegmentID || r.SegmentID == previous.SegmentID && r.NextEntry < previous.NextEntry:
				problem = "reader_position_or_report_reset"
			}
			if problem != "" {
				break
			}
			series = append(series, r)
			previous = r
		}
		if problem != "" {
			current.Summary = "Reader progress is unverified: " + problem
			current.Unknown = append(current.Unknown, problem)
			appendFinding(report, current)
			continue
		}
		last := series[len(series)-1]
		if last.SegmentID > reader.SegmentID || last.NextEntry > reader.NextEntry {
			current.Outcome, current.Severity = "reader_forward_progress_observed", "none"
			current.Summary = fmt.Sprintf("Reader next position %d:%d→%d:%d; attempt timestamps alone were not used as progress", reader.SegmentID, reader.NextEntry, last.SegmentID, last.NextEntry)
		} else if boundaryProblem != "" {
			current.Summary = "Expected data is unverified throughout the reader window; missing/reset replica evidence cannot establish a reader stall"
		} else if report.WindowMS < first.ReportIntervalMS {
			current.Summary = "Window is shorter than the deployed reader reporting interval"
			current.Unknown = append(current.Unknown, "reader_window_shorter_than_report_cadence")
		} else if reader.SegmentID != first.SegmentID {
			current.Summary = "Reader is on another segment; no cross-segment numeric entry comparison is made"
			current.Unknown = append(current.Unknown, "reader_segment_outside_selection")
		} else if reader.SegmentID < first.TruncatedSegment || reader.SegmentID == first.TruncatedSegment && reader.NextEntry <= first.TruncatedEntry {
			current.Summary = "Reader position falls within declared truncation; this is not a storage fault verdict"
			current.Unknown = append(current.Unknown, "reader_position_truncated")
		} else if reader.NextEntry > boundaries[len(boundaries)-1] {
			current.Outcome, current.Severity = "caught_up_or_awaiting_data", "none"
			current.Summary = "Published next entry is beyond the selected segment's current boundary; waiting/EOF is expected for this sampled boundary, not proof the entire log is caught up"
		} else if report.ReadMode != "catch-up" {
			current.Outcome = "read_expectation_unverified"
			current.Summary = "Data may be available, but catch-up progress was not requested; tail/unknown mode does not establish a stall"
		} else if last.ReportedMS-reader.ReportedMS < first.ReportIntervalMS {
			current.Summary = "Unchanged position without a full renewed attempt-report interval cannot establish active catch-up attempts"
			current.Unknown = append(current.Unknown, "reader_attempt_renewal_unverified")
		} else {
			available := true
			for _, boundary := range boundaries {
				available = available && reader.NextEntry <= boundary
			}
			if !available {
				current.Summary = "Data was not established as available at every sample"
				current.Unknown = append(current.Unknown, "expected_data_not_continuously_observed")
			} else {
				current.Outcome, current.Severity, current.Strength = "suspected_catchup_no_progress", "warning", "suspected"
				current.Summary = fmt.Sprintf("Reader remained at next entry %d:%d while fresh leased attempt reports renewed and expected catch-up data was present at each sampled boundary; investigate reader/client and storage evidence, not a confirmed root cause", reader.SegmentID, reader.NextEntry)
				current.Unknown = append(current.Unknown, "storage_cause_not_established", "skip_ranges_not_checked")
			}
		}
		appendFinding(report, current)
	}
	known := map[string]bool{}
	for _, r := range first.Readers {
		known[r.Name] = true
	}
	for _, obs := range samples[1:] {
		for _, r := range obs.ReadState.Readers {
			if !known[r.Name] {
				f.Summary = "Reader set changed; new readers lack baseline coverage"
				f.Unknown = append(f.Unknown, "reader_set_changed")
				appendFinding(report, f)
				return
			}
		}
	}
}
