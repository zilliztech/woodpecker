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

// probeNode is one replica's answer, or the reason it has none. The unreachable/unknown/bad-response
// states are shared with `logstore lac` so the same condition reads the same way in both commands.
type probeNode struct {
	Node          string `json:"node"`
	NodeID        string `json:"node_id,omitempty"`
	State         string `json:"state"`
	Instance      string `json:"instance,omitempty"`
	Source        string `json:"source,omitempty"`
	DataLog       bool   `json:"data_log"`
	CompactedMark bool   `json:"compacted_mark"`
	DeleteMarked  bool   `json:"delete_marked"`
	FirstEntry    int64  `json:"first_entry"`
	LastEntry     int64  `json:"last_entry"`
	Entries       int64  `json:"entries_read"`
	Outcome       string `json:"outcome,omitempty"`
	Detail        string `json:"detail,omitempty"`
	ElapsedMs     int64  `json:"elapsed_ms,omitempty"`
	// Verdict is this command's reading of the answer, which the node cannot make on its own.
	Verdict string `json:"verdict,omitempty"`
}

func (p probeNode) answered() bool { return p.State == posOK }

// Outcomes as the node reports them, and the sources it reads from. These are wire values, like the
// JSON field names beside them: wp talks to a node over HTTP only, so it reads the vocabulary
// rather than importing the server package, which would couple the CLI to the server it diagnoses.
const (
	probeOutcomeError         = "error"
	probeOutcomeEntryNotFound = "entry_not_found"
	probeOutcomeEndOfFile     = "end_of_file"
	probeOutcomeCapReached    = "cap_reached"
	probeOutcomeNoLocalData   = "no_local_data"

	probeSourceObjectStore = "object_storage"

	// probeStateCannotAnswer is this command's own: a node that answered the request but cannot
	// say anything about this segment. lac's "no writer" would be the wrong word, because nothing
	// here is about writers.
	probeStateCannotAnswer = "cannot answer"
)

// Per-replica verdicts. The node reports what it saw; only here, with the segment's metadata, can
// "nothing was written" be told apart from "this copy cannot serve what was written".
const (
	probeVerdictServed    = "served"
	probeVerdictShort     = "cannot serve"
	probeVerdictNoData    = "no data here"
	probeVerdictDeleted   = "log deleted here"
	probeVerdictReclaimed = "truncated"
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
	findings, results, unreadable := readProbeFindings(results, segMeta)

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
		served, outcome, verdict := "-", r.Outcome, r.Verdict
		if r.answered() {
			if r.Entries > 0 && r.FirstEntry >= 0 {
				served = fmt.Sprintf("%d-%d (%d)", r.FirstEntry, r.LastEntry, r.Entries)
			} else {
				served = "nothing"
			}
		} else {
			outcome, verdict = r.State, "-"
		}
		rows = append(rows, []string{r.Node, r.NodeID, localFacts(r), r.Source, served, outcome, verdict, r.Detail})
	}
	if err := output.RenderRowTable(w,
		[]string{"NODE", "NODE_ID", "LOCAL", "SOURCE", "SERVED", "OUTCOME", "VERDICT", "DETAIL"}, rows); err != nil {
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
			Bucket   string `json:"bucket_name"`
			RootPath string `json:"root_path"`
			Local    struct {
				DataLog       bool  `json:"data_log"`
				DataLogBytes  int64 `json:"data_log_bytes"`
				CompactedMark bool  `json:"compacted_mark"`
				DeleteMarked  bool  `json:"delete_marked"`
			} `json:"local"`
			Source     string `json:"source"`
			FirstEntry int64  `json:"first_entry"`
			LastEntry  int64  `json:"last_entry"`
			Entries    int64  `json:"entries_read"`
			Outcome    string `json:"outcome"`
			Error      string `json:"error"`
			ElapsedMs  int64  `json:"elapsed_ms"`
		}
		if jsonErr := json.Unmarshal(body, &resp); jsonErr != nil {
			r.State = posBadResponse
			results = append(results, r)
			continue
		}
		r.State = posOK
		// The instance the node answered for: with no bucket and root path given, the node picks
		// the one it associates with the segment, and which one that was has to be visible.
		r.Instance = resp.Bucket + "/" + resp.RootPath
		r.DataLog, r.CompactedMark, r.DeleteMarked = resp.Local.DataLog, resp.Local.CompactedMark, resp.Local.DeleteMarked
		r.Source, r.FirstEntry, r.LastEntry = resp.Source, resp.FirstEntry, resp.LastEntry
		r.Entries, r.Outcome, r.Detail, r.ElapsedMs = resp.Entries, resp.Outcome, resp.Error, resp.ElapsedMs
		results = append(results, r)
	}
	return results
}

// segmentExpectation is what the segment's metadata says should be readable, and whether that is
// something a replica can be held to.
type segmentExpectation struct {
	lastEntry int64
	known     bool
	truncated bool
}

// expectSegment reads the metadata for what it can be held to. An Active segment's tail is not known
// here -- more may be written at any moment. A Truncated one is the opposite: cleanup deletes each
// node's data before the metadata (segment_cleanup_manager.go:169), so the metadata still carries
// the original last entry while the replicas correctly hold nothing.
func expectSegment(segMeta *proto.SegmentMetadata) segmentExpectation {
	if segMeta.GetState() == proto.SegmentState_Truncated {
		return segmentExpectation{truncated: true}
	}
	if segMeta.GetState() == proto.SegmentState_Active || segMeta.GetLastEntryId() < 0 {
		return segmentExpectation{}
	}
	return segmentExpectation{lastEntry: segMeta.GetLastEntryId(), known: true}
}

// judgeReplica reads one answer against what the segment is known to hold. The node cannot do this:
// a missing file, a block that failed its checksum and a caught-up tail all reach it as the same
// "entry not found", so only the segment's metadata separates them.
func judgeReplica(r probeNode, exp segmentExpectation) (string, string) {
	switch {
	case r.Outcome == probeOutcomeError:
		return probeVerdictShort, r.Detail
	case exp.truncated && r.Outcome != probeOutcomeCapReached:
		return probeVerdictReclaimed, "the segment is truncated; this data was removed on purpose"
	case r.Outcome == probeOutcomeNoLocalData && r.DeleteMarked:
		return probeVerdictDeleted, "the log is marked deleted on this node"
	case r.Outcome == probeOutcomeNoLocalData:
		return probeVerdictNoData, "no data.log and no compacted mark: this replica holds none of the segment"
	case r.Outcome == probeOutcomeCapReached:
		// The probe stopped where it was told to stop. That is a statement about the request, not
		// about the replica: judging it against the segment's end would call every healthy segment
		// longer than the probe window a dead end.
		return probeVerdictServed, ""
	case exp.known && r.LastEntry < exp.lastEntry:
		return probeVerdictShort, fmt.Sprintf("stops after %d, but the segment ends at %d", r.LastEntry, exp.lastEntry)
	}
	return probeVerdictServed, ""
}

// readProbeFindings turns the per-node answers into the readings that mean different things, and
// returns an error for the one that means a reader cannot get past this point.
func readProbeFindings(results []probeNode, segMeta *proto.SegmentMetadata) ([]string, []probeNode, error) {
	exp := expectSegment(segMeta)

	answered := make([]probeNode, 0, len(results))
	failing := make([]probeNode, 0, len(results))
	silent := make([]string, 0, len(results))
	shared := 0
	judged := make([]probeNode, 0, len(results))
	for _, r := range results {
		if !r.answered() {
			silent = append(silent, fmt.Sprintf("%s (%s)", nodeLabel(r), r.State))
			judged = append(judged, r)
			continue
		}
		verdict, why := judgeReplica(r, exp)
		r.Verdict = verdict
		if why != "" && r.Detail == "" {
			r.Detail = why
		}
		judged = append(judged, r)
		answered = append(answered, r)
		if r.Source == probeSourceObjectStore {
			shared++
		}
		if verdict == probeVerdictShort || verdict == probeVerdictNoData {
			failing = append(failing, r)
		}
	}

	findings := make([]string, 0, 4)
	if exp.truncated {
		findings = append(findings, fmt.Sprintf(
			"Segment %d is truncated: its data is being, or has been, reclaimed on purpose. A replica holding nothing here is expected.",
			segMeta.GetSegNo()))
	}
	if len(answered) == 0 {
		findings = append(findings, fmt.Sprintf(
			"No replica answered (%s), so nothing can be said about this segment.", strings.Join(silent, ", ")))
		return findings, judged, wperrors.NewNetworkError("no replica answered the probe")
	}
	if len(silent) > 0 {
		// Whatever the replicas that answered agree on, they are not the whole quorum: a reader
		// can still be served by one that did not answer here.
		findings = append(findings, fmt.Sprintf(
			"%d of %d replicas did not answer (%s). Nothing below is a statement about the whole quorum.",
			len(silent), len(results), strings.Join(silent, ", ")))
	}
	if shared == len(answered) && len(answered) > 1 {
		findings = append(findings, fmt.Sprintf(
			"All %d replicas that answered served this from object storage — one shared copy answering %d times, not %d independent confirmations.",
			len(answered), len(answered), len(answered)))
	}

	switch {
	case len(failing) == len(answered) && len(silent) == 0:
		// No replica can serve past this point, and every replica was asked: a reader here has
		// nowhere to fail over to.
		stuck := firstUnreadable(failing)
		findings = append(findings, fmt.Sprintf(
			"No replica can serve entry %d: %s. A reader at that position waits forever.",
			stuck, describeStops(failing)))
		return findings, judged, wperrors.NewRedFindingError(fmt.Sprintf(
			"entry %d cannot be served by any replica", stuck))
	case len(failing) > 0:
		names := make([]string, 0, len(failing))
		for _, r := range failing {
			names = append(names, fmt.Sprintf("%s (%s)", nodeLabel(r), r.Verdict))
		}
		findings = append(findings, fmt.Sprintf(
			"%d of %d replicas that answered cannot serve the segment: %s. Failover covers for them, so reads still work and nothing else reports this.",
			len(failing), len(answered), strings.Join(names, ", ")))
		return findings, judged, nil
	}

	if exp.truncated {
		// Comparing positions between replicas of reclaimed data says nothing.
		return findings, judged, nil
	}

	samePosition, sameOutcome := true, true
	for _, r := range answered {
		if r.LastEntry != answered[0].LastEntry {
			samePosition = false
		}
		if r.Outcome != answered[0].Outcome {
			sameOutcome = false
		}
	}
	switch {
	case samePosition && sameOutcome && answered[0].Outcome == probeOutcomeCapReached:
		ends := ""
		if exp.known {
			ends = fmt.Sprintf(" The segment ends at %d, so this says nothing about entries past %d.",
				exp.lastEntry, answered[0].LastEntry)
		}
		findings = append(findings, fmt.Sprintf(
			"Every replica served up to entry %d, where this probe's window ended.%s Raise --max-entries or move --from-entry to look further.",
			answered[0].LastEntry, ends))
	case samePosition && sameOutcome && answered[0].Outcome == probeOutcomeEntryNotFound:
		findings = append(findings, fmt.Sprintf(
			"Every replica that answered stops after entry %d because the data ends there — entry %d has not been written yet.",
			answered[0].LastEntry, answered[0].LastEntry+1))
	case samePosition && sameOutcome && answered[0].Outcome == probeOutcomeEndOfFile:
		findings = append(findings, fmt.Sprintf(
			"Every replica that answered reads to entry %d and the segment ends there.", answered[0].LastEntry))
	case samePosition && sameOutcome:
		findings = append(findings, fmt.Sprintf(
			"Every replica that answered stopped at entry %d (%s).", answered[0].LastEntry, answered[0].Outcome))
	case samePosition:
		reasons := make([]string, 0, len(answered))
		for _, r := range answered {
			reasons = append(reasons, fmt.Sprintf("%s (%s)", nodeLabel(r), r.Outcome))
		}
		findings = append(findings, fmt.Sprintf(
			"Every replica that answered holds the same data, up to entry %d, but gave different reasons for stopping: %s.",
			answered[0].LastEntry, strings.Join(reasons, ", ")))
	default:
		behind := make([]string, 0, len(answered))
		furthest := furthestServed(answered)
		for _, r := range answered {
			if r.LastEntry < furthest {
				behind = append(behind, fmt.Sprintf("%s (has %d)", nodeLabel(r), r.LastEntry))
			}
		}
		findings = append(findings, fmt.Sprintf(
			"Replicas hold different amounts, the furthest reaching entry %d: %s. No read failed and the segment is still being written, so this is a replica that is behind, not a damaged one.",
			furthest, strings.Join(behind, ", ")))
	}
	return findings, judged, nil
}

// firstUnreadable is the first entry no replica can serve. A reader fails over, so it is the entry
// after the furthest any replica reached -- not after the nearest.
func firstUnreadable(failing []probeNode) int64 {
	stuck := failing[0].LastEntry + 1
	for _, r := range failing {
		if r.LastEntry+1 > stuck {
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
			detail = r.Outcome
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

// localFacts renders what the node holds, which is what separates "nothing was written" from "this
// copy is gone".
func localFacts(r probeNode) string {
	if !r.answered() {
		return "-"
	}
	parts := make([]string, 0, 3)
	if r.DataLog {
		parts = append(parts, "data.log")
	}
	if r.CompactedMark {
		parts = append(parts, "compacted")
	}
	if r.DeleteMarked {
		parts = append(parts, "deleted")
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, "+")
}

func nodeLabel(r probeNode) string {
	if r.NodeID != "" {
		return r.NodeID
	}
	return r.Node
}
