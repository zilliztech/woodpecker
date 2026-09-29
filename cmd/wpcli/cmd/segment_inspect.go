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
}

// inspectNode is one replica's survey, or the reason it has none.
type inspectNode struct {
	Node         string         `json:"node"`
	NodeID       string         `json:"node_id,omitempty"`
	State        string         `json:"state"`
	Source       string         `json:"source,omitempty"`
	Sealed       bool           `json:"sealed"`
	TotalBlocks  int32          `json:"total_blocks_known"`
	StoppedEarly bool           `json:"stopped_early"`
	StopReason   string         `json:"stop_reason,omitempty"`
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
			rows = append(rows, []string{n.Node, n.NodeID, "-", "-", "-", n.State, n.Detail})
			continue
		}
		if len(n.Blocks) == 0 {
			rows = append(rows, []string{n.Node, n.NodeID, "-", "-", "-", n.StopReason, n.Detail})
			continue
		}
		for _, b := range n.Blocks {
			rows = append(rows, []string{
				n.Node, n.NodeID,
				strconv.FormatInt(b.Number, 10),
				fmt.Sprintf("%d-%d", b.FirstEntryID, b.LastEntryID),
				strconv.FormatInt(b.Bytes, 10),
				b.Status, b.Detail,
			})
		}
	}
	if err := output.RenderRowTable(w,
		[]string{"NODE", "NODE_ID", "BLOCK", "ENTRIES", "BYTES", "STATUS", "DETAIL"}, rows); err != nil {
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
				StoppedEarly     bool           `json:"stopped_early"`
				StopReason       string         `json:"stop_reason"`
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
		results = append(results, n)
	}
	return results
}

// readInspectFindings states the readings a per-replica survey cannot make on its own, and returns
// an error when a block is unreadable everywhere.
func readInspectFindings(results []inspectNode) ([]string, error) {
	findings := make([]string, 0, 4)

	answered := make([]inspectNode, 0, len(results))
	silent := make([]string, 0, len(results))
	for _, n := range results {
		if n.answered() {
			answered = append(answered, n)
			continue
		}
		silent = append(silent, fmt.Sprintf("%s (%s)", inspectLabel(n), n.State))
	}
	if len(answered) == 0 {
		findings = append(findings, fmt.Sprintf(
			"No replica answered (%s), so nothing can be said about this segment's blocks.", strings.Join(silent, ", ")))
		return findings, wperrors.NewNetworkError("no replica answered")
	}
	if len(silent) > 0 {
		findings = append(findings, fmt.Sprintf(
			"%d of %d replicas did not answer (%s). Nothing below is a statement about the whole quorum.",
			len(silent), len(results), strings.Join(silent, ", ")))
	}

	// Which replicas hold a good copy of each block, and which do not.
	good := make(map[int64][]string)
	bad := make(map[int64][]string)
	surveyed := make(map[int64]struct{})
	for _, n := range answered {
		for _, b := range n.Blocks {
			surveyed[b.Number] = struct{}{}
			if b.Status == blockStatusOK {
				good[b.Number] = append(good[b.Number], inspectLabel(n))
			} else {
				bad[b.Number] = append(bad[b.Number], fmt.Sprintf("%s (%s)", inspectLabel(n), b.Status))
			}
		}
	}

	for _, n := range answered {
		if n.StopReason == surveyStopChainBroken {
			findings = append(findings, fmt.Sprintf(
				"%s could not walk past block %d: its header cannot be read, and this segment has no index to locate the next one. The blocks beyond it have not been looked at — that is not the same as their being damaged.",
				inspectLabel(n), lastBlockNumber(n)))
		}
		if n.StopReason == surveyStopNoBlocks {
			findings = append(findings, fmt.Sprintf(
				"%s holds no local blocks to walk (source %s).", inspectLabel(n), n.Source))
		}
		if n.StopReason == surveyStopBound {
			findings = append(findings, fmt.Sprintf(
				"%s stopped at the block limit, not at the end of the segment — raise --max-blocks or move --from-block to look further.",
				inspectLabel(n)))
		}
	}

	unreadable := make([]int64, 0)
	repairable := make([]int64, 0)
	for number := range surveyed {
		if len(bad[number]) == 0 {
			continue
		}
		if len(good[number]) > 0 {
			repairable = append(repairable, number)
		} else {
			unreadable = append(unreadable, number)
		}
	}
	sort.Slice(repairable, func(i, j int) bool { return repairable[i] < repairable[j] })
	sort.Slice(unreadable, func(i, j int) bool { return unreadable[i] < unreadable[j] })

	for _, number := range repairable {
		findings = append(findings, fmt.Sprintf(
			"Block %d is damaged on %s but intact on %s — the data exists, and failover is already serving it.",
			number, strings.Join(bad[number], ", "), strings.Join(good[number], ", ")))
	}
	if len(unreadable) == 0 {
		if len(repairable) == 0 && len(findings) == 0 {
			findings = append(findings, "Every block every replica walked verified.")
		}
		return findings, nil
	}

	names := make([]string, 0, len(unreadable))
	for _, number := range unreadable {
		names = append(names, fmt.Sprintf("%d (%s)", number, strings.Join(bad[number], ", ")))
	}
	if len(silent) > 0 {
		// A replica that was never asked may hold these blocks intact, so nothing here is a loss
		// yet -- the same reason the whole-quorum conclusions above are withheld.
		findings = append(findings, fmt.Sprintf(
			"Block(s) %s are damaged on every replica that answered. The %d that did not may still hold them; ask again when they are reachable.",
			strings.Join(names, "; "), len(silent)))
		return findings, nil
	}
	findings = append(findings, fmt.Sprintf(
		"No replica holds a readable copy of block(s) %s. Those entries cannot be read anywhere; only skipping them gets a reader past.",
		strings.Join(names, "; ")))
	if entries := entriesLostIn(answered, unreadable); entries != "" {
		findings = append(findings, fmt.Sprintf("That is entries %s.", entries))
	}
	if resume, block, ok := resumePoint(answered, unreadable); ok {
		findings = append(findings, fmt.Sprintf(
			"Readable data resumes at entry %d (block %d), so the damage is bounded — that range is what a skip would have to cover.",
			resume, block))
	} else {
		findings = append(findings, "No replica reads anything after the damage, so it is not known to end within the blocks surveyed.")
	}
	return findings, wperrors.NewRedFindingError(fmt.Sprintf(
		"block(s) %s are unreadable on every replica", joinInts(unreadable)))
}

// resumePoint is the first entry readable again after the damage: the first block past the last
// unreadable one that some replica could read. It is what a skip range would have to reach.
func resumePoint(answered []inspectNode, unreadable []int64) (int64, int64, bool) {
	lastBad := unreadable[len(unreadable)-1]
	entry, block, found := int64(0), int64(0), false
	for _, n := range answered {
		for _, b := range n.Blocks {
			if b.Number <= lastBad || b.Status != blockStatusOK || b.FirstEntryID < 0 {
				continue
			}
			if !found || b.Number < block {
				entry, block, found = b.FirstEntryID, b.Number, true
			}
		}
	}
	return entry, block, found
}

// entriesLostIn names the entry range the unreadable blocks cover, taken from whichever replica
// could still read their headers.
func entriesLostIn(answered []inspectNode, unreadable []int64) string {
	first, last := int64(-1), int64(-1)
	for _, n := range answered {
		for _, b := range n.Blocks {
			if !containsInt(unreadable, b.Number) || b.FirstEntryID < 0 {
				continue
			}
			if first < 0 || b.FirstEntryID < first {
				first = b.FirstEntryID
			}
			if b.LastEntryID > last {
				last = b.LastEntryID
			}
		}
	}
	if first < 0 {
		return ""
	}
	return fmt.Sprintf("%d-%d", first, last)
}

func lastBlockNumber(n inspectNode) int64 {
	if len(n.Blocks) == 0 {
		return -1
	}
	return n.Blocks[len(n.Blocks)-1].Number
}

func inspectLabel(n inspectNode) string {
	if n.NodeID != "" {
		return n.NodeID
	}
	return n.Node
}

func containsInt(list []int64, v int64) bool {
	for _, item := range list {
		if item == v {
			return true
		}
	}
	return false
}

func joinInts(list []int64) string {
	parts := make([]string, 0, len(list))
	for _, item := range list {
		parts = append(parts, strconv.FormatInt(item, 10))
	}
	return strings.Join(parts, ", ")
}
