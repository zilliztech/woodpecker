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
	scanVerdictOK      = "ok"
	scanVerdictOpen    = "open"
	scanVerdictShort   = "short"
	scanVerdictUnknown = "unknown"
)

// scanNode is what one replica reported for one segment: the entries its structure claims, or the
// entries a read actually reached, depending on the mode.
type scanNode struct {
	label    string
	answered bool
	state    string // why it did not answer, when it did not
	coverage rangeSet
	stopped  string
}

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

	stateFindings, stateProblems := checkSegmentStates(segments)
	findings = append(findings, stateFindings...)
	problems += stateProblems

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
	answered, silent := 0, make([]string, 0, len(segment.nodes))
	for _, node := range segment.nodes {
		if !node.answered {
			silent = append(silent, fmt.Sprintf("%s (%s)", node.label, node.state))
			continue
		}
		answered++
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

	// A replica holding less than another is not a fault in the segment, but it is a replica that
	// needs resyncing, and no other command reports it.
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
		row.Verdict, row.verdict = scanVerdictOpen, scanVerdictOpen
		if len(held) > 1 {
			findings = append(findings, fmt.Sprintf(
				"segment %d is open and its coverage has a gap at %s — entries are missing in the middle of what was written.",
				segment.id, held.subtract(rangeSet{}.add(entryRange{held[0].From, held[len(held)-1].To}).intersect(held)),
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

// checkSegmentIDs reports holes in the id sequence. Ids at or below the log's truncation point are
// expected to be gone; above it, a hole is a segment nothing accounts for.
func checkSegmentIDs(segments []scanSegment, truncatedThrough, fromSegment int64, probe gapProbe) ([]string, int) {
	if len(segments) < 2 {
		return nil, 0
	}
	findings := make([]string, 0, 1)
	missing := make([]int64, 0)
	reclaimed := 0
	for i := 1; i < len(segments); i++ {
		for id := segments[i-1].id + 1; id < segments[i].id; id++ {
			if id < fromSegment {
				continue
			}
			if id <= truncatedThrough {
				reclaimed++
				continue
			}
			missing = append(missing, id)
		}
	}
	if reclaimed > 0 {
		findings = append(findings, fmt.Sprintf(
			"%d segment ids below the truncation point (%d) are absent, which is what truncation does.",
			reclaimed, truncatedThrough,
		))
	}
	if len(missing) == 0 {
		return findings, 0
	}
	// An id metadata lost is one thing; an id metadata lost while a node still holds its data is
	// another, and only the second leaves data nobody owns.
	for _, id := range missing {
		holders, asked := []string(nil), false
		if probe != nil {
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

// plural keeps a count reading as English, since these lines are what an operator acts on.
func plural(n int, word string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", word)
	}
	return fmt.Sprintf("%d %ss", n, word)
}

// checkSegmentStates reports metadata that contradicts itself. Only the newest segment can be open,
// and a truncated one should already be below the truncation point.
func checkSegmentStates(segments []scanSegment) ([]string, int) {
	findings := make([]string, 0, 2)
	problems := 0

	active := make([]int64, 0, 1)
	highest := int64(-1)
	for _, segment := range segments {
		if segment.id > highest {
			highest = segment.id
		}
		if segment.state == proto.SegmentState_Active {
			active = append(active, segment.id)
		}
	}
	switch {
	case len(active) > 1:
		findings = append(findings, fmt.Sprintf(
			"Two or more segments are Active (%s); only the newest segment can be open.",
			joinSegmentIDs(active),
		))
		problems++
	case len(active) == 1 && active[0] != highest:
		findings = append(findings, fmt.Sprintf(
			"Segment %d is Active but %d is newer; an open segment should be the newest one.",
			active[0], highest,
		))
		problems++
	}
	return findings, problems
}

func joinSegmentIDs(ids []int64) string {
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%d", id))
	}
	return strings.Join(parts, ", ")
}
