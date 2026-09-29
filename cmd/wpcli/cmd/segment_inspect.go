package cmd

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

// newSegmentInspectCommand walks a segment's blocks on every replica.
//
// `segment probe` reports where a replica stops, because a read stops at the first block it cannot
// read. This goes past that point, which is the difference between skipping a few entries and
// skipping the rest of the segment.
func newSegmentInspectCommand() *cobra.Command {
	var flags metaEtcdFlags
	var fromBlock, maxBlocks int64
	cmd := &cobra.Command{
		Use:   "inspect <logName> <segmentId>",
		Short: "Walk a segment's blocks on every replica and report what is wrong where",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			segmentID, parseErr := strconv.ParseInt(args[1], 10, 64)
			if parseErr != nil {
				return wperrors.NewUsageError(fmt.Sprintf("invalid segmentId %q", args[1]))
			}
			res, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			conn, err := resolveMetaEtcd(&flags)
			if err != nil {
				return err
			}
			cli, err := metaEtcdClient(conn)
			if err != nil {
				return err
			}
			defer cli.Close()
			return runSegmentInspect(cmd, cli, conn.kb, res.Client, res.Members, args[0], segmentID, fromBlock, maxBlocks)
		},
	}
	flags.register(cmd)
	cmd.Flags().Int64Var(&fromBlock, "from-block", 0, "First block to walk (default: the start of the segment)")
	cmd.Flags().Int64Var(&maxBlocks, "max-blocks", 0, "Stop after this many blocks per node (default: the node's own bound)")
	return cmd
}

// Block statuses and survey stop reasons, as the node reports them. Wire values, like the JSON
// field names beside them.
const (
	blockStatusOK         = "ok"
	surveyStopChainBroken = "chain_broken"
	surveyStopBound       = "bound"
	surveyStopNoBlocks    = "no_local_blocks"
	surveyStopEnd         = "end_of_segment"

	blockStatusIncomplete = "data_incomplete"
)

// inspectBlock is one block as one replica found it.
type inspectBlock struct {
	Number       int64  `json:"block"`
	Offset       int64  `json:"offset"`
	Bytes        int64  `json:"bytes"`
	FirstEntryID int64  `json:"first_entry_id"`
	LastEntryID  int64  `json:"last_entry_id"`
	Status       string `json:"status"`
	Detail       string `json:"detail,omitempty"`
	// RecordsOK and LastGoodEntryID come from the block's own records, which carry their own
	// checksums and so survive the block checksum failure that makes a reader abandon the block.
	RecordsOK       int   `json:"records_ok"`
	LastGoodEntryID int64 `json:"last_good_entry_id"`
}

// inspectNode is one replica's survey, or the reason it has none.
type inspectNode struct {
	Node         string         `json:"node"`
	NodeID       string         `json:"node_id,omitempty"`
	State        string         `json:"state"`
	Source       string         `json:"source,omitempty"`
	Sealed       bool           `json:"sealed"`
	IndexUsable  bool           `json:"index_usable"`
	LAC          int64          `json:"lac"`
	TotalBlocks  int32          `json:"total_blocks_known"`
	StoppedEarly bool           `json:"stopped_early"`
	StopReason   string         `json:"stop_reason,omitempty"`
	StopOffset   int64          `json:"stop_offset,omitempty"`
	Blocks       []inspectBlock `json:"blocks,omitempty"`
	Detail       string         `json:"detail,omitempty"`
}

func (n inspectNode) answered() bool { return n.State == posOK }

func runSegmentInspect(cmd *cobra.Command, cli *clientv3.Client, kb *meta.KeyBuilder,
	ac *client.Client, members *client.Memberlist, logName string, segmentID, fromBlock, maxBlocks int64,
) error {
	ctx, cancel := metaCtx()
	defer cancel()

	logMeta := &proto.LogMeta{}
	if err := getProto(ctx, cli, kb.BuildLogKey(logName), logMeta); err != nil {
		return wperrors.NewTargetNotFoundError(fmt.Sprintf("log %s: %v", logName, err))
	}
	segMeta := &proto.SegmentMetadata{}
	segKey := kb.BuildSegmentInstanceKey(logName, strconv.FormatInt(segmentID, 10))
	if err := getProto(ctx, cli, segKey, segMeta); err != nil {
		return wperrors.NewTargetNotFoundError(fmt.Sprintf("segment %d of log %s: %v", segmentID, logName, err))
	}
	quorum := segMeta.GetQuorum()
	if quorum == nil {
		return wperrors.NewTargetNotFoundError(fmt.Sprintf(
			"segment %d of log %s carries no quorum", segmentID, logName))
	}

	results := inspectEachNode(ac, members, quorum, logMeta.LogId, segmentID, fromBlock, maxBlocks)
	findings, damaged := readInspectFindings(results)

	w := cmd.OutOrStdout()
	if renderedOutput() {
		payload := map[string]any{
			"log_name": logName, "log_id": logMeta.LogId, "segment_id": segmentID,
			"state": segMeta.State.String(), "from_block": fromBlock,
			"nodes": results, "findings": findings,
		}
		var renderErr error
		if Globals.Output == "yaml" {
			renderErr = output.RenderYAML(w, payload)
		} else {
			renderErr = output.RenderJSON(w, payload)
		}
		if renderErr != nil {
			return renderErr
		}
		return damaged
	}

	fmt.Fprintf(w, "Segment %d of log %s — state %s, quorum %d (es %d, wq %d, aq %d), from block %d\n\n",
		segmentID, logName, segMeta.State.String(), quorum.Id, quorum.Es, quorum.Wq, quorum.Aq, fromBlock)

	rows := make([][]string, 0, len(results)*4)
	for _, n := range results {
		if !n.answered() {
			rows = append(rows, []string{n.Node, n.NodeID, "-", "-", "-", "-", "-", n.State, n.Detail})
			continue
		}
		if len(n.Blocks) == 0 {
			rows = append(rows, []string{n.Node, n.NodeID, "-", "-", "-", "-", "-", n.StopReason, n.Detail})
			continue
		}
		for _, b := range n.Blocks {
			// A reader drops a block that fails its checksum whole, so nothing in it is readable.
			// What its records still hold is a separate column: a repair could use it, a read
			// cannot.
			readable, recoverable := "all", "-"
			if b.Status != blockStatusOK {
				readable = "none"
				if b.LastGoodEntryID >= b.FirstEntryID {
					recoverable = fmt.Sprintf("%d-%d", b.FirstEntryID, b.LastGoodEntryID)
				}
			}
			rows = append(rows, []string{
				n.Node, n.NodeID,
				strconv.FormatInt(b.Number, 10),
				fmt.Sprintf("%d-%d", b.FirstEntryID, b.LastEntryID),
				readable, recoverable,
				strconv.FormatInt(b.Bytes, 10),
				b.Status, b.Detail,
			})
		}
	}
	if err := output.RenderRowTable(w,
		[]string{"NODE", "NODE_ID", "BLOCK", "ENTRIES", "READABLE", "RECOVERABLE", "BYTES", "STATUS", "DETAIL"}, rows); err != nil {
		return err
	}

	fmt.Fprintln(w)
	for _, line := range findings {
		fmt.Fprintf(w, "%s\n", line)
	}
	return damaged
}

// inspectEachNode asks each node the segment's quorum names to walk its own copy.
func inspectEachNode(ac *client.Client, members *client.Memberlist, quorum *proto.QuorumInfo,
	logID, segmentID, fromBlock, maxBlocks int64,
) []inspectNode {
	results := make([]inspectNode, 0, len(quorum.Nodes))
	for _, addr := range quorum.Nodes {
		n := inspectNode{Node: addr, State: posUnreachable, TotalBlocks: -1}
		member, found := memberByAddr(members, addr)
		if !found {
			n.State = posUnknownNode
			results = append(results, n)
			continue
		}
		n.NodeID = member.ID

		path := fmt.Sprintf("/admin/logstore/segment/inspect?log_id=%d&segment_id=%d&from_block=%d",
			logID, segmentID, fromBlock)
		if maxBlocks > 0 {
			path += fmt.Sprintf("&max_blocks=%d", maxBlocks)
		}
		body, status, err := fetchAdminJSONWithStatus(ac.PeerAdminURL(member), path)
		switch {
		case err != nil:
			n.State, n.Detail = posUnreachable, err.Error()
			results = append(results, n)
			continue
		case status != 200:
			n.State, n.Detail = probeStateCannotAnswer, adminErrorText(body, status)
			results = append(results, n)
			continue
		}
		var resp struct {
			Source string `json:"source"`
			Survey struct {
				Blocks           []inspectBlock `json:"blocks"`
				Sealed           bool           `json:"sealed"`
				TotalBlocksKnown int32          `json:"total_blocks_known"`
				IndexUsable      bool           `json:"index_usable"`
				LAC              int64          `json:"lac"`
				StoppedEarly     bool           `json:"stopped_early"`
				StopReason       string         `json:"stop_reason"`
				StopOffset       int64          `json:"stop_offset"`
			} `json:"survey"`
		}
		if jsonErr := json.Unmarshal(body, &resp); jsonErr != nil {
			n.State = posBadResponse
			results = append(results, n)
			continue
		}
		n.State, n.Source = posOK, resp.Source
		n.Blocks, n.Sealed, n.TotalBlocks = resp.Survey.Blocks, resp.Survey.Sealed, resp.Survey.TotalBlocksKnown
		n.StoppedEarly, n.StopReason = resp.Survey.StoppedEarly, resp.Survey.StopReason
		n.StopOffset = resp.Survey.StopOffset
		n.IndexUsable, n.LAC = resp.Survey.IndexUsable, resp.Survey.LAC
		results = append(results, n)
	}
	return results
}

// entryRange is an inclusive range of entry ids. Replicas are compared by these rather than by
// block number: each node flushes on its own timer and size, so the same entries land in different
// blocks on different nodes, and a block number means nothing across them.
type entryRange struct {
	From int64 `json:"from"`
	To   int64 `json:"to"`
}

// rangeSet is a sorted set of non-overlapping ranges.
type rangeSet []entryRange

func (s rangeSet) add(r entryRange) rangeSet {
	if r.To < r.From {
		return s
	}
	out := append(rangeSet{}, s...)
	out = append(out, r)
	sort.Slice(out, func(i, j int) bool { return out[i].From < out[j].From })
	merged := make(rangeSet, 0, len(out))
	for _, item := range out {
		if n := len(merged); n > 0 && item.From <= merged[n-1].To+1 {
			if item.To > merged[n-1].To {
				merged[n-1].To = item.To
			}
			continue
		}
		merged = append(merged, item)
	}
	return merged
}

func (s rangeSet) union(o rangeSet) rangeSet {
	out := s
	for _, r := range o {
		out = out.add(r)
	}
	return out
}

// subtract removes every entry in o from s.
func (s rangeSet) subtract(o rangeSet) rangeSet {
	out := append(rangeSet{}, s...)
	for _, cut := range o {
		next := make(rangeSet, 0, len(out)+1)
		for _, r := range out {
			if cut.To < r.From || cut.From > r.To {
				next = append(next, r)
				continue
			}
			if cut.From > r.From {
				next = append(next, entryRange{r.From, cut.From - 1})
			}
			if cut.To < r.To {
				next = append(next, entryRange{cut.To + 1, r.To})
			}
		}
		out = next
	}
	return out
}

func (s rangeSet) intersect(o rangeSet) rangeSet {
	out := make(rangeSet, 0, len(s))
	for _, a := range s {
		for _, b := range o {
			from, to := max64(a.From, b.From), min64(a.To, b.To)
			if from <= to {
				out = out.add(entryRange{from, to})
			}
		}
	}
	return out
}

func (s rangeSet) String() string {
	parts := make([]string, 0, len(s))
	for _, r := range s {
		if r.From == r.To {
			parts = append(parts, strconv.FormatInt(r.From, 10))
			continue
		}
		parts = append(parts, fmt.Sprintf("%d-%d", r.From, r.To))
	}
	return strings.Join(parts, ", ")
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// replicaView is what one replica looked at and what of it it could read. The two are different:
// a replica that stopped early examined less than the segment holds, and its silence about the rest
// is not evidence about the rest.
type replicaView struct {
	node     inspectNode
	examined rangeSet
	readable rangeSet
	// absent is what the segment is confirmed to hold and this replica does not. A finalized
	// replica may intentionally be behind the quorum, and its footer carries the LAC the majority
	// acknowledged (stagedstorage/writer_impl.go:1173, :1250) -- so once such a replica has walked
	// to the end, everything between its last entry and that LAC is known to exist and known to be
	// missing here. Counting it as "not examined" instead would drop a real loss out of the report.
	absent rangeSet
	// recoverable is the prefix of a damaged block whose own records still verify. No reader serves
	// it: every backend drops a block whose integrity check fails, without returning the records
	// that passed (stagedstorage/reader_impl.go:1090, disk:705, objectstorage:763). It is what a
	// repair could salvage, which is a different question from what a reader can serve today.
	recoverable rangeSet
}

func viewOf(n inspectNode) replicaView {
	view := replicaView{node: n}
	highest := int64(-1)
	for _, b := range n.Blocks {
		if b.FirstEntryID < 0 {
			continue
		}
		if b.Status == blockStatusIncomplete {
			// A header whose data has not been written yet. Those entries are not part of what this
			// replica holds, and they are not damage either.
			continue
		}
		view.examined = view.examined.add(entryRange{b.FirstEntryID, b.LastEntryID})
		if b.LastEntryID > highest {
			highest = b.LastEntryID
		}
		if b.Status == blockStatusOK {
			view.readable = view.readable.add(entryRange{b.FirstEntryID, b.LastEntryID})
			continue
		}
		if b.LastGoodEntryID >= b.FirstEntryID {
			view.recoverable = view.recoverable.add(entryRange{b.FirstEntryID, b.LastGoodEntryID})
		}
	}
	// Only a replica that finished looking can say it does not have something, and only a sealed
	// segment's footer says what the segment holds. An active segment may still be receiving writes,
	// so a shorter replica there is behind rather than missing anything.
	if n.Sealed && n.LAC >= 0 && (n.StopReason == surveyStopEnd || n.StopReason == surveyStopNoBlocks) {
		if highest < n.LAC {
			view.absent = view.absent.add(entryRange{highest + 1, n.LAC})
		}
	}
	return view
}

// accounted is everything this replica has spoken about: what it looked at, and what its footer says
// it should have and does not.
func (v replicaView) accounted() rangeSet { return v.examined.union(v.absent) }

// readInspectFindings states the readings a per-replica survey cannot make on its own, and returns
// an error only for entries no replica can read that every replica actually looked at.
func readInspectFindings(results []inspectNode) ([]string, error) {
	findings := make([]string, 0, 6)

	views := make([]replicaView, 0, len(results))
	silent := make([]string, 0, len(results))
	for _, n := range results {
		if n.answered() {
			views = append(views, viewOf(n))
			continue
		}
		silent = append(silent, fmt.Sprintf("%s (%s)", inspectLabel(n), n.State))
	}
	if len(views) == 0 {
		findings = append(findings, fmt.Sprintf(
			"No replica answered (%s), so nothing can be said about this segment's blocks.", strings.Join(silent, ", ")))
		return findings, wperrors.NewNetworkError("no replica answered")
	}
	if len(silent) > 0 {
		findings = append(findings, fmt.Sprintf(
			"%d of %d replicas did not answer (%s). Nothing below is a statement about the whole quorum.",
			len(silent), len(results), strings.Join(silent, ", ")))
	}

	// Why a replica saw less than the others, which is the difference between "it is not there" and
	// "it was not looked at".
	for _, view := range views {
		switch view.node.StopReason {
		case surveyStopChainBroken:
			why := "this segment has no index to locate the next block"
			if view.node.Sealed && !view.node.IndexUsable {
				why = "this segment's index could not be read in full, so the walk fell back to the block chain"
			}
			findings = append(findings, fmt.Sprintf(
				"%s could not walk past offset %d: no block header could be read there, and %s. Entries beyond %s have not been looked at on that replica.",
				inspectLabel(view.node), view.node.StopOffset, why, examinedHorizon(view)))
		case surveyStopBound:
			findings = append(findings, fmt.Sprintf(
				"%s stopped at the block limit, not at the end of the segment — raise --max-blocks or move --from-block to look further.",
				inspectLabel(view.node)))
		case surveyStopNoBlocks:
			findings = append(findings, fmt.Sprintf(
				"%s holds no local blocks to walk (source %s).", inspectLabel(view.node), view.node.Source))
		}
	}

	readableSomewhere := rangeSet{}
	recoverableSomewhere := rangeSet{}
	examinedByAny := rangeSet{}
	examinedByAll := views[0].accounted()
	for _, view := range views {
		readableSomewhere = readableSomewhere.union(view.readable)
		recoverableSomewhere = recoverableSomewhere.union(view.recoverable)
		examinedByAny = examinedByAny.union(view.accounted())
		examinedByAll = examinedByAll.intersect(view.accounted())
	}

	for _, view := range views {
		if len(view.absent) > 0 {
			findings = append(findings, fmt.Sprintf(
				"%s does not hold entries %s at all, though its own footer says the quorum confirmed them — that replica needs resyncing.",
				inspectLabel(view.node), view.absent))
		}
	}

	// Entries a replica looked at or should have had and cannot read, that another replica still holds.
	atRisk := make([]string, 0, len(views))
	for _, view := range views {
		lostHere := view.accounted().subtract(view.readable)
		if held := lostHere.intersect(readableSomewhere); len(held) > 0 {
			atRisk = append(atRisk, fmt.Sprintf("%s lost %s", inspectLabel(view.node), held))
		}
	}
	if len(atRisk) > 0 {
		findings = append(findings, fmt.Sprintf(
			"%s — those entries are readable on another replica, so the data exists and failover is already serving it.",
			strings.Join(atRisk, "; ")))
	}

	// Entries that every replica looked at and none can read. Only what all of them examined counts:
	// a replica that never got that far has said nothing about it.
	lost := examinedByAll.subtract(readableSomewhere)
	if len(silent) > 0 && len(lost) > 0 {
		findings = append(findings, fmt.Sprintf(
			"Entries %s could not be read on any replica that answered, but %d did not answer and may still hold them.",
			lost, len(silent)))
		return findings, nil
	}
	if len(lost) == 0 {
		if unexamined := examinedByAny.subtract(examinedByAll); len(unexamined) > 0 {
			findings = append(findings, fmt.Sprintf(
				"Entries %s were looked at by some replicas and not others, so nothing about them holds for the whole quorum.",
				unexamined))
		}
		if len(findings) == 0 {
			findings = append(findings, "Every block every replica walked verified.")
		}
		return findings, nil
	}

	findings = append(findings, fmt.Sprintf(
		"No replica can read entries %s, and every replica looked. Only skipping them gets a reader past.", lost))
	if salvage := recoverableSomewhere.intersect(lost); len(salvage) > 0 {
		findings = append(findings, fmt.Sprintf(
			"Of those, entries %s are still intact at record level on some replica — no reader serves them, because a block that fails its checksum is dropped whole, but a repair could recover them.",
			salvage))
	}
	if resume, ok := resumeAfter(readableSomewhere, lost); ok {
		findings = append(findings, fmt.Sprintf(
			"Readable data resumes at entry %d, so the damage is bounded — %s is what a skip would have to cover.",
			resume, lost))
	} else {
		findings = append(findings, "No replica reads anything after those entries, so the damage is not known to end within the blocks surveyed.")
	}
	return findings, wperrors.NewRedFindingError(fmt.Sprintf("entries %s are unreadable on every replica", lost))
}

// examinedHorizon is the furthest entry a replica looked at, for saying what its silence covers.
func examinedHorizon(view replicaView) string {
	if len(view.examined) == 0 {
		return "the start of the segment"
	}
	return fmt.Sprintf("entry %d", view.examined[len(view.examined)-1].To)
}

// resumeAfter is the first entry readable again after the lost range, which is what a skip range has
// to reach.
func resumeAfter(readable rangeSet, lost rangeSet) (int64, bool) {
	after := lost[len(lost)-1].To
	for _, r := range readable {
		if r.To <= after {
			continue
		}
		if r.From > after {
			return r.From, true
		}
		return after + 1, true
	}
	return 0, false
}

func inspectLabel(n inspectNode) string {
	if n.NodeID != "" {
		return n.NodeID
	}
	return n.Node
}
