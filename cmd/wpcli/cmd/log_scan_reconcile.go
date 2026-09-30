package cmd

import (
	"fmt"
	"strings"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/proto"
)

// Per-segment verdicts. They answer one question -- would a reader get through this segment -- and
// nothing else, so the table stays readable on a log with hundreds of segments.
const (
	scanVerdictOK        = "ok"
	scanVerdictOpen      = "open"
	scanVerdictShort     = "short"
	scanVerdictUnknown   = "unknown"
	scanVerdictCompacted = "compacted"
	scanVerdictReclaimed = "reclaimed"
)

// gapProbeBudget bounds the fan-out when metadata is missing many ids. The quorum lives in the
// metadata that is missing, so each id has to be asked of every node; a log that lost a long run of
// records would otherwise turn one scan into thousands of requests.
const gapProbeBudget = 16

// gapNameBudget bounds how many ids one finding names, so a long run stays readable.
const gapNameBudget = 32

// scanNode is what one replica reported for one segment: the entries its structure claims, or the
// entries a read actually reached, depending on the mode.
type scanNode struct {
	label    string
	answered bool
	state    string // why it did not answer, when it did not
	coverage rangeSet
	// stopped is where the node's survey ended, and it decides what the coverage amounts to: a
	// survey that reached the end of the segment makes the coverage a claim about what this replica
	// holds, and any other ending makes it a lower bound.
	stopped string
}

// completeClaim reports whether this replica's coverage may be read as everything it holds.
func (n scanNode) completeClaim() bool { return n.stopped == surveyStopEnd }

// scanSegment is one segment as metadata describes it, with what the replicas reported.
type scanSegment struct {
	id       int64
	state    proto.SegmentState
	metaLast int64 // -1 when the segment is still open, or metadata knows no last entry
	nodes    []scanNode
}

// scanRow is one line of the report.
type scanRow struct {
	SegmentID int64  `json:"segment_id"`
	State     string `json:"state"`
	MetaLast  int64  `json:"meta_last_entry_id"`
	Reaches   string `json:"reaches"`
	Replicas  string `json:"replicas"`
	Verdict   string `json:"verdict"`
	Detail    string `json:"detail,omitempty"`
	verdict   string
}

// reconcileScan compares what metadata says against what the replicas reported, and returns one row
// per segment plus the findings. truncatedThrough is the log's truncation point: segment ids at or
// below it are expected to be gone.
// gapProbe answers whether any node still holds data for a segment id metadata does not know about.
// A nil probe means the question is not asked.
type gapProbe func(segmentID int64) (holders []string, asked bool)

func reconcileScan(segments []scanSegment, truncatedThrough, fromSegment int64) ([]scanRow, []string, error) {
	return reconcileScanWithGaps(segments, truncatedThrough, fromSegment, nil)
}

func reconcileScanWithGaps(segments []scanSegment, truncatedThrough, fromSegment int64, probe gapProbe) ([]scanRow, []string, error) {
	rows := make([]scanRow, 0, len(segments))
	findings := make([]string, 0, 4)
	problems := 0

	for _, segment := range segments {
		row, segFindings, bad := reconcileSegment(segment)
		rows = append(rows, row)
		findings = append(findings, segFindings...)
		if bad {
			problems++
		}
	}

	idFindings, idProblems := checkSegmentIDs(segments, truncatedThrough, fromSegment, probe)
	findings = append(findings, idFindings...)
	problems += idProblems

	findings = append(findings, checkSegmentStates(segments)...)

	if problems == 0 {
		findings = append(findings, fmt.Sprintf(
			"%d segments scanned; the log reads through with no problem found.", len(segments),
		))
		return rows, findings, nil
	}
	return rows, findings, wperrors.NewRedFindingError(fmt.Sprintf(
		"%s found across %d segments scanned", plural(problems, "problem"), len(segments),
	))
}

// reconcileSegment judges one segment. A read is served by any replica, so the segment is sound when
// some replica holds all of it; a replica holding less than the others is a separate finding,
// because nothing else would ever mention it.
func reconcileSegment(segment scanSegment) (scanRow, []string, bool) {
	row := scanRow{
		SegmentID: segment.id, State: segment.state.String(), MetaLast: segment.metaLast,
		Reaches: "-", Verdict: scanVerdictUnknown, verdict: scanVerdictUnknown,
	}
	findings := make([]string, 0, 2)

	held := rangeSet{}
	answered, settled := 0, false
	silent := make([]string, 0, len(segment.nodes))
	for _, node := range segment.nodes {
		if !node.answered {
			silent = append(silent, fmt.Sprintf("%s (%s)", node.label, node.state))
			continue
		}
		answered++
		if node.completeClaim() {
			settled = true
		}
		held = held.union(node.coverage)
	}
	row.Replicas = fmt.Sprintf("%d/%d", answered, len(segment.nodes))
	if answered == 0 {
		findings = append(findings, fmt.Sprintf(
			"segment %d: no replica answered (%s), so nothing is known about it.",
			segment.id, strings.Join(silent, ", "),
		))
		return row, findings, false
	}
	if len(held) > 0 {
		row.Reaches = held.String()
	}

	// A compacted segment is one object every replica shares, and the replicas reclaim their staged
	// copies once the mark is distributed. So what a replica holds locally is not what the segment
	// holds, in either direction: emptiness is the expected steady state, and a surviving partial
	// copy is not a shortfall. This scan does not read the object, so it says so rather than
	// guessing from the local copies.
	if segment.state == proto.SegmentState_Sealed {
		row.Verdict, row.verdict = scanVerdictCompacted, scanVerdictCompacted
		row.Detail = "served from object storage; local replica copies are not the authority"
		return row, findings, false
	}
	// Retention is deleting this segment's data on purpose. Between the state change and the
	// reclaim finishing, a replica answers with nothing.
	if segment.state == proto.SegmentState_Truncated {
		row.Verdict, row.verdict = scanVerdictReclaimed, scanVerdictReclaimed
		row.Detail = "being reclaimed by retention"
		return row, findings, false
	}

	// A replica holding less than another is not a fault in the segment, but it is a replica that
	// needs resyncing, and no other command reports it. A replica of a compacted segment is not
	// measured this way at all, which the verdict above has already settled.
	behind := make([]string, 0, len(segment.nodes))
	for _, node := range segment.nodes {
		if !node.answered {
			continue
		}
		if missing := held.subtract(node.coverage); len(missing) > 0 {
			behind = append(behind, fmt.Sprintf("%s is missing %s", node.label, missing))
		}
	}
	if len(behind) > 0 {
		findings = append(findings, fmt.Sprintf(
			"segment %d: %s, which another replica holds — those replicas are behind and need resyncing.",
			segment.id, strings.Join(behind, "; "),
		))
	}

	if segment.metaLast < 0 {
		// Still open: metadata records no last entry yet, so there is nothing to measure against.
		// A hole inside what was written is still a hole, and the operator needs its position.
		row.Verdict, row.verdict = scanVerdictOpen, scanVerdictOpen
		if len(held) > 1 {
			span := rangeSet{}.add(entryRange{held[0].From, held[len(held)-1].To})
			findings = append(findings, fmt.Sprintf(
				"segment %d is open and its coverage has a gap at %s — entries are missing in the middle of what was written.",
				segment.id, span.subtract(held),
			))
		}
		return row, findings, len(held) > 1
	}

	expected := rangeSet{}.add(entryRange{0, segment.metaLast})
	missing := expected.subtract(held)
	if len(missing) == 0 {
		row.Verdict, row.verdict = scanVerdictOK, scanVerdictOK
		return row, findings, false
	}

	// Every replica stopped before the end of its own copy, so the coverage is a lower bound and the
	// entries past it were never looked at. Calling that a shortfall would report intact data as
	// missing, which is what a node's own survey bound does to a segment larger than it.
	if !settled {
		row.Detail = "survey bounded; coverage is a lower bound"
		findings = append(findings, fmt.Sprintf(
			"segment %d: no replica surveyed to the end of the segment (%s), so nothing is known about %s. Run `wp segment inspect <logName> %d` to survey it without a whole-log sweep's bound.",
			segment.id, boundedReasons(segment.nodes), missing, segment.id,
		))
		return row, findings, false
	}

	row.Verdict, row.verdict = scanVerdictShort, scanVerdictShort
	row.Detail = fmt.Sprintf("missing %s", missing)
	reached := held.String()
	if len(held) == 0 {
		reached = "nothing"
	}
	findings = append(findings, fmt.Sprintf(
		"segment %d: metadata says 0-%d, the replicas together reach %s — no replica has %s. Run `wp segment inspect <logName> %d` to see whether that is damage or data that was never written.",
		segment.id, segment.metaLast, reached, missing, segment.id,
	))
	return row, findings, true
}

// boundedReasons names where each answering replica's survey stopped, so a bounded verdict says
// whether a budget or a broken chain ended it.
func boundedReasons(nodes []scanNode) string {
	parts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if !node.answered {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s stopped at %s", node.label, node.stopped))
	}
	return strings.Join(parts, "; ")
}

// checkSegmentIDs reports ids that metadata should hold a record for and does not. The walk starts
// at the truncation point rather than at the oldest listed segment: ids strictly below the point
// are expected to be gone, the segment AT the point is deliberately kept (truncation skips it when
// marking, and the reclaim only deletes segments already marked Truncated), and everything above it
// must exist. Starting at the oldest listed id instead would never look at the low end of the log,
// which is the end a reader resumes from.
//
// segments must be ordered by id.
func checkSegmentIDs(segments []scanSegment, truncatedThrough, fromSegment int64, probe gapProbe) ([]string, int) {
	if len(segments) == 0 {
		return nil, 0
	}
	start := truncatedThrough
	if start < 0 {
		start = 0
	}
	if fromSegment > start {
		start = fromSegment
	}
	present := make(map[int64]struct{}, len(segments))
	for _, segment := range segments {
		present[segment.id] = struct{}{}
	}

	newest := segments[len(segments)-1].id
	missing := make([]int64, 0)
	for id := start; id < newest; id++ {
		if _, ok := present[id]; ok {
			continue
		}
		missing = append(missing, id)
	}
	if len(missing) == 0 {
		return nil, 0
	}

	findings := make([]string, 0, len(missing))
	// An id metadata lost is one thing; an id metadata lost while a node still holds its data is
	// another, and only the second leaves data nobody owns. The quorum to ask lives in the record
	// that is gone, so the question costs a fan-out per id and is paid within a budget; past it the
	// ids are still named, without that answer.
	probed := 0
	for i, id := range missing {
		if i >= gapNameBudget {
			findings = append(findings, fmt.Sprintf(
				"%s also missing from metadata, beyond the %d named above: %s.",
				plural(len(missing)-gapNameBudget, "segment id"), gapNameBudget,
				joinSegmentIDs(missing[gapNameBudget:]),
			))
			break
		}
		holders, asked := []string(nil), false
		if probe != nil && probed < gapProbeBudget {
			probed++
			holders, asked = probe(id)
		}
		switch {
		case !asked:
			findings = append(findings, fmt.Sprintf(
				"segment id %d is missing from metadata and is above the truncation point (%d) — nothing accounts for it.",
				id, truncatedThrough,
			))
		case len(holders) > 0:
			findings = append(findings, fmt.Sprintf(
				"segment id %d has no metadata, but %s holds data for it — data with no metadata record, which nothing will read and nothing will reclaim.",
				id, strings.Join(holders, ", "),
			))
		default:
			findings = append(findings, fmt.Sprintf(
				"segment id %d is missing from metadata and no node holds data for it; it is above the truncation point (%d), so the record is gone rather than the data.",
				id, truncatedThrough,
			))
		}
	}
	return findings, len(missing)
}

// checkSegmentStates reports what the segment states look like without failing on them. A roll that
// finds a non-empty append queue leaves the previous segment Active until its fence and complete
// RPCs finish while the new segment takes writes (see `log_handle.go`), so more than one Active
// segment, and an Active segment that is not the newest, are both states a healthy log passes
// through. One observation cannot tell either from a segment left behind, so the scan says what it
// saw and why it may be transient.
func checkSegmentStates(segments []scanSegment) []string {
	active := make([]int64, 0, 1)
	newest := int64(-1)
	for _, segment := range segments {
		if segment.id > newest {
			newest = segment.id
		}
		if segment.state == proto.SegmentState_Active {
			active = append(active, segment.id)
		}
	}
	switch {
	case len(active) > 1:
		return []string{fmt.Sprintf(
			"Segments %s are all Active. A roll with queued appends leaves the previous segment Active until its completion finishes, so this is expected during a roll; re-run to see whether it persists.",
			joinSegmentIDs(active),
		)}
	case len(active) == 1 && active[0] != newest:
		return []string{fmt.Sprintf(
			"Segment %d is Active but %d is newer. The same roll window leaves the previous segment Active until its completion finishes; re-run to see whether it persists.",
			active[0], newest,
		)}
	}
	return nil
}

// plural keeps a count reading as English, since these lines are what an operator acts on.
func plural(n int, word string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", word)
	}
	return fmt.Sprintf("%d %ss", n, word)
}

func joinSegmentIDs(ids []int64) string {
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%d", id))
	}
	return strings.Join(parts, ", ")
}
