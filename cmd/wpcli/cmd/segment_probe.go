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

// newSegmentCommand groups the commands that ask a segment's whole quorum the same question, as
// opposed to the logstore family, which asks one named node.
func newSegmentCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "segment",
		Short: "Inspect one segment across its quorum",
	}
	cmd.AddCommand(newSegmentProbeCommand())
	return cmd
}

// newSegmentProbeCommand asks every replica how far it can read a segment.
//
// A read failing on one replica is invisible from outside: the client fails over to the next node
// in the quorum on every error, so a damaged copy keeps serving reads until all copies are damaged.
// The quorum lives in metadata, so --etcd applies as it does to `logstore lac`.
func newSegmentProbeCommand() *cobra.Command {
	var flags metaEtcdFlags
	var fromEntry, maxEntries int64
	cmd := &cobra.Command{
		Use:   "probe <logName> <segmentId>",
		Short: "Ask every replica how far it can read a segment",
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
			return runSegmentProbe(cmd, cli, conn.kb, res.Client, res.Members, args[0], segmentID, fromEntry, maxEntries)
		},
	}
	flags.register(cmd)
	cmd.Flags().Int64Var(&fromEntry, "from-entry", 0, "Entry to read from (default: the start of the segment)")
	cmd.Flags().Int64Var(&maxEntries, "max-entries", 0, "Stop after this many entries per node (default: the node's own bound)")
	return cmd
}

// probeNode is one replica's answer, or the reason it has none. The states are shared with
// `logstore lac` so the same condition reads the same way in both commands.
type probeNode struct {
	Node       string `json:"node"`
	NodeID     string `json:"node_id,omitempty"`
	State      string `json:"state"`
	Source     string `json:"source,omitempty"`
	FirstEntry int64  `json:"first_entry"`
	LastEntry  int64  `json:"last_entry"`
	Entries    int64  `json:"entries_read"`
	StopReason string `json:"stop_reason,omitempty"`
	Detail     string `json:"detail,omitempty"`
	ElapsedMs  int64  `json:"elapsed_ms,omitempty"`
}

func (p probeNode) answered() bool { return p.State == posOK }

// Stop reasons, as the node reports them. These are wire values, like the JSON field names beside
// them: wp talks to a node over HTTP only, so it reads the vocabulary rather than importing the
// server package, which would couple the CLI to the server it is diagnosing.
const (
	probeStopError         = "error"
	probeStopNotYetWritten = "not_yet_written"
	probeStopEndOfSegment  = "end_of_segment"
	probeSourceObjectStore = "object_storage"

	// probeStateCannotAnswer is this command's own: a node that answered the request but holds
	// nothing it could read for this segment. lac's "no writer" would be the wrong word, because
	// nothing here is about writers.
	probeStateCannotAnswer = "cannot answer"
)

func runSegmentProbe(cmd *cobra.Command, cli *clientv3.Client, kb *meta.KeyBuilder,
	ac *client.Client, members *client.Memberlist, logName string, segmentID, fromEntry, maxEntries int64,
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

	results := probeEachNode(ac, members, quorum, logMeta.LogId, segmentID, fromEntry, maxEntries)
	findings, unreadable := readProbeFindings(results)

	w := cmd.OutOrStdout()
	if renderedOutput() {
		payload := map[string]any{
			"log_name": logName, "log_id": logMeta.LogId, "segment_id": segmentID,
			"state": segMeta.State.String(), "from_entry": fromEntry,
			"es": quorum.Es, "wq": quorum.Wq, "aq": quorum.Aq,
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
		return unreadable
	}

	fmt.Fprintf(w, "Segment %d of log %s — state %s, quorum %d (es %d, wq %d, aq %d), from entry %d\n\n",
		segmentID, logName, segMeta.State.String(), quorum.Id, quorum.Es, quorum.Wq, quorum.Aq, fromEntry)

	rows := make([][]string, 0, len(results))
	for _, r := range results {
		served, stop := "-", r.StopReason
		if r.answered() {
			if r.Entries > 0 {
				served = fmt.Sprintf("%d-%d (%d)", r.FirstEntry, r.LastEntry, r.Entries)
			} else {
				served = "nothing"
			}
		} else {
			stop = r.State
		}
		rows = append(rows, []string{r.Node, r.NodeID, r.Source, served, stop, r.Detail})
	}
	if err := output.RenderRowTable(w,
		[]string{"NODE", "NODE_ID", "SOURCE", "SERVED", "STOP", "DETAIL"}, rows); err != nil {
		return err
	}

	fmt.Fprintln(w)
	for _, line := range findings {
		fmt.Fprintf(w, "%s\n", line)
	}
	return unreadable
}

// probeEachNode asks each node the segment's quorum names. A node absent from the memberlist is not
// dialed: the quorum records service addresses, and only a memberlist entry supplies the admin port.
func probeEachNode(ac *client.Client, members *client.Memberlist, quorum *proto.QuorumInfo,
	logID, segmentID, fromEntry, maxEntries int64,
) []probeNode {
	results := make([]probeNode, 0, len(quorum.Nodes))
	for _, addr := range quorum.Nodes {
		r := probeNode{Node: addr, State: posUnreachable, FirstEntry: -1, LastEntry: -1}
		member, found := memberByAddr(members, addr)
		if !found {
			r.State = posUnknownNode
			results = append(results, r)
			continue
		}
		r.NodeID = member.ID

		path := fmt.Sprintf("/admin/logstore/segment/probe?log_id=%d&segment_id=%d&from_entry=%d",
			logID, segmentID, fromEntry)
		if maxEntries > 0 {
			path += fmt.Sprintf("&max_entries=%d", maxEntries)
		}
		body, status, err := fetchAdminJSONWithStatus(ac.PeerAdminURL(member), path)
		switch {
		case err != nil:
			r.State, r.Detail = posUnreachable, err.Error()
			results = append(results, r)
			continue
		case status != 200:
			// A node that cannot answer about this segment says why; that reason is the answer.
			r.State, r.Detail = probeStateCannotAnswer, adminErrorText(body, status)
			results = append(results, r)
			continue
		}
		var resp struct {
			Source     string `json:"source"`
			FirstEntry int64  `json:"first_entry"`
			LastEntry  int64  `json:"last_entry"`
			Entries    int64  `json:"entries_read"`
			StopReason string `json:"stop_reason"`
			Error      string `json:"error"`
			ElapsedMs  int64  `json:"elapsed_ms"`
		}
		if jsonErr := json.Unmarshal(body, &resp); jsonErr != nil {
			r.State = posBadResponse
			results = append(results, r)
			continue
		}
		r.State = posOK
		r.Source, r.FirstEntry, r.LastEntry = resp.Source, resp.FirstEntry, resp.LastEntry
		r.Entries, r.StopReason, r.Detail, r.ElapsedMs = resp.Entries, resp.StopReason, resp.Error, resp.ElapsedMs
		results = append(results, r)
	}
	return results
}

// readProbeFindings turns the per-node answers into the readings that mean different things, and
// returns an error for the one that means a reader cannot get past this point.
func readProbeFindings(results []probeNode) ([]string, error) {
	answered := make([]probeNode, 0, len(results))
	failing := make([]probeNode, 0, len(results))
	shared := 0
	for _, r := range results {
		if !r.answered() {
			continue
		}
		answered = append(answered, r)
		if r.Source == probeSourceObjectStore {
			shared++
		}
		if r.StopReason == probeStopError {
			failing = append(failing, r)
		}
	}

	findings := make([]string, 0, 3)
	if len(answered) == 0 {
		findings = append(findings, "No replica answered, so nothing can be said about this segment.")
		return findings, wperrors.NewNetworkError("no replica answered the probe")
	}
	if shared == len(answered) && len(answered) > 1 {
		findings = append(findings, fmt.Sprintf(
			"All %d replicas served this from object storage — one shared copy answering %d times, not %d independent confirmations.",
			len(answered), len(answered), len(answered)))
	}

	switch {
	case len(failing) == len(answered):
		// Every replica stopped with an error. A reader here has nowhere to fail over to.
		stuckAt := firstUnreadable(failing)
		findings = append(findings, fmt.Sprintf(
			"No replica can read entry %d: %s. A reader at that position waits forever.",
			stuckAt, describeStops(failing)))
		return findings, wperrors.NewRedFindingError(fmt.Sprintf(
			"entry %d is unreadable on every replica", stuckAt))
	case len(failing) > 0:
		names := make([]string, 0, len(failing))
		for _, r := range failing {
			names = append(names, fmt.Sprintf("%s (stops after %d)", nodeLabel(r), r.LastEntry))
		}
		findings = append(findings, fmt.Sprintf(
			"%d of %d replicas are damaged: %s. Failover is covering for them, so reads still work and nothing else reports this.",
			len(failing), len(answered), strings.Join(names, ", ")))
		return findings, nil
	}

	reason := answered[0].StopReason
	sameStop := true
	for _, r := range answered {
		if r.LastEntry != answered[0].LastEntry || r.StopReason != reason {
			sameStop = false
			break
		}
	}
	switch {
	case sameStop && reason == probeStopNotYetWritten:
		findings = append(findings, fmt.Sprintf(
			"Every replica stops after entry %d because the data ends there — nothing is wrong with this segment; entry %d has not been written yet.",
			answered[0].LastEntry, answered[0].LastEntry+1))
	case sameStop && reason == probeStopEndOfSegment:
		findings = append(findings, fmt.Sprintf(
			"Every replica reads to entry %d and the segment ends there — nothing is wrong with this segment.",
			answered[0].LastEntry))
	case sameStop:
		findings = append(findings, fmt.Sprintf(
			"Every replica stopped at entry %d (%s).", answered[0].LastEntry, reason))
	default:
		behind := make([]string, 0, len(answered))
		furthest := furthestServed(answered)
		for _, r := range answered {
			if r.LastEntry < furthest {
				behind = append(behind, fmt.Sprintf("%s (has %d)", nodeLabel(r), r.LastEntry))
			}
		}
		findings = append(findings, fmt.Sprintf(
			"Replicas hold different amounts, the furthest reaching entry %d: %s. No read failed, so this is a replica that is behind, not a damaged one.",
			furthest, strings.Join(behind, ", ")))
	}
	return findings, nil
}

// firstUnreadable is the lowest entry no replica could serve: the position a reader stops at.
func firstUnreadable(failing []probeNode) int64 {
	stuck := failing[0].LastEntry + 1
	for _, r := range failing {
		if r.LastEntry+1 < stuck {
			stuck = r.LastEntry + 1
		}
	}
	return stuck
}

func furthestServed(answered []probeNode) int64 {
	furthest := answered[0].LastEntry
	for _, r := range answered {
		if r.LastEntry > furthest {
			furthest = r.LastEntry
		}
	}
	return furthest
}

// describeStops lists the distinct reasons the replicas gave, so a single shared cause reads as one
// cause rather than as a list repeating itself.
func describeStops(failing []probeNode) string {
	seen := make(map[string]struct{}, len(failing))
	reasons := make([]string, 0, len(failing))
	for _, r := range failing {
		detail := r.Detail
		if detail == "" {
			detail = r.StopReason
		}
		if _, ok := seen[detail]; ok {
			continue
		}
		seen[detail] = struct{}{}
		reasons = append(reasons, detail)
	}
	sort.Strings(reasons)
	return strings.Join(reasons, "; ")
}

func nodeLabel(r probeNode) string {
	if r.NodeID != "" {
		return r.NodeID
	}
	return r.Node
}
