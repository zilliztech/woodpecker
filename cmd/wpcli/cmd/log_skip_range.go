package cmd

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

// newLogSkipRangeCommand edits the entry ranges an operator has declared unreadable.
//
// Everything else in this family only reports. This writes, and what it writes makes data
// unreachable, so `add` asks the quorum before it agrees that there is nothing left to read.
func newLogSkipRangeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "skip-range",
		Short: "List, declare or withdraw the entry ranges a reader should skip",
	}
	cmd.AddCommand(newLogSkipRangeListCommand(), newLogSkipRangeAddCommand(), newLogSkipRangeRemoveCommand())
	return cmd
}

func newLogSkipRangeListCommand() *cobra.Command {
	var flags metaEtcdFlags
	cmd := &cobra.Command{
		Use:   "list [logName]",
		Short: "Show the declared skip ranges, for one log or for every log",
		Long: `Show the declared skip ranges.

With no log name every log is listed, including ranges whose log no longer exists: log ids
are never reused, so such a range can never apply to anything again, but it stays in the
record until it is removed.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := resolveMetaEtcd(&flags)
			if err != nil {
				return err
			}
			cli, err := metaEtcdClient(conn)
			if err != nil {
				return err
			}
			defer cli.Close()
			logName := ""
			if len(args) == 1 {
				logName = args[0]
			}
			return runSkipRangeList(cmd, cli, conn.kb, logName)
		},
	}
	flags.register(cmd)
	return cmd
}

func newLogSkipRangeAddCommand() *cobra.Command {
	var flags metaEtcdFlags
	var fromEntry, toEntry int64
	var reason string
	var confirmed, force bool
	cmd := &cobra.Command{
		Use:   "add <logName> <segmentId>",
		Short: "Declare an entry range unreadable so readers move past it",
		Long: `Declare an entry range unreadable so readers move past it.

This gives up data. Entries in the range are never delivered to any reader of this log
again, and nothing restores them: withdrawing the range only exposes whatever is still on
disk. So the quorum is asked first, and a replica that can still read part of the range
refuses the declaration.`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			segmentID, parseErr := strconv.ParseInt(args[1], 10, 64)
			if parseErr != nil {
				return wperrors.NewUsageError(fmt.Sprintf("segment id %q is not a number", args[1]))
			}
			if toEntry < fromEntry || fromEntry < 0 {
				return wperrors.NewUsageError(fmt.Sprintf(
					"--from-entry %d --to-entry %d is not a range; both ends are inclusive and from must not exceed to",
					fromEntry, toEntry,
				))
			}
			if strings.TrimSpace(reason) == "" {
				return wperrors.NewUsageError("--reason is required: this record is the only account of why the data was given up")
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
			return runSkipRangeAdd(cmd, cli, conn.kb, res.Client, res.Members, skipRangeAddRequest{
				logName: args[0], segmentID: segmentID,
				from: fromEntry, to: toEntry, reason: reason,
				confirmed: confirmed, force: force,
			})
		},
	}
	flags.register(cmd)
	cmd.Flags().Int64Var(&fromEntry, "from-entry", -1, "First entry to skip (inclusive)")
	cmd.Flags().Int64Var(&toEntry, "to-entry", -1, "Last entry to skip (inclusive)")
	cmd.Flags().StringVar(&reason, "reason", "", "Why these entries are being given up (required)")
	cmd.Flags().BoolVarP(&confirmed, "yes", "y", false, "Proceed without the confirmation prompt")
	cmd.Flags().BoolVar(&force, "force", false,
		"Declare the range even though a replica can still read part of it")
	return cmd
}

func newLogSkipRangeRemoveCommand() *cobra.Command {
	var flags metaEtcdFlags
	var fromEntry, toEntry int64
	cmd := &cobra.Command{
		Use:   "remove <logName> <segmentId>",
		Short: "Withdraw a declared range, so readers stop skipping it",
		Long: `Withdraw a declared range, so readers stop skipping it.

Withdrawing does not restore anything: it only lets readers try the entries again, which
is what to do once a replica has been rebuilt. A reader that already moved past the range
does not come back for it.`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			segmentID, parseErr := strconv.ParseInt(args[1], 10, 64)
			if parseErr != nil {
				return wperrors.NewUsageError(fmt.Sprintf("segment id %q is not a number", args[1]))
			}
			if toEntry < fromEntry || fromEntry < 0 {
				return wperrors.NewUsageError(fmt.Sprintf(
					"--from-entry %d --to-entry %d is not a range", fromEntry, toEntry,
				))
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
			return runSkipRangeRemove(cmd, cli, conn.kb, args[0], segmentID, fromEntry, toEntry)
		},
	}
	flags.register(cmd)
	cmd.Flags().Int64Var(&fromEntry, "from-entry", -1, "First entry of the range to withdraw (inclusive)")
	cmd.Flags().Int64Var(&toEntry, "to-entry", -1, "Last entry of the range to withdraw (inclusive)")
	return cmd
}

// skipRangeRecord is the stored record plus the revision it was read at, so a write can refuse
// to overwrite one that moved.
type skipRangeRecord struct {
	set      *proto.AllSkipRanges
	revision int64
}

// readSkipRanges reads the one record holding every log's ranges. An absent record is an empty
// one: most clusters never have any, and reporting that as an error would bury the real signal.
func readSkipRanges(ctx context.Context, cli *clientv3.Client, kb *meta.KeyBuilder) (*skipRangeRecord, error) {
	resp, err := cli.Get(ctx, kb.AllSkipRangesKey())
	if err != nil {
		return nil, wperrors.NewNetworkError(fmt.Sprintf("etcd get %s: %v", kb.AllSkipRangesKey(), err))
	}
	rec := &skipRangeRecord{set: &proto.AllSkipRanges{}}
	if len(resp.Kvs) == 0 {
		return rec, nil
	}
	rec.revision = resp.Kvs[0].ModRevision
	if err := pb.Unmarshal(resp.Kvs[0].Value, rec.set); err != nil {
		return nil, wperrors.NewStateConflictError(fmt.Sprintf("skip range record is not decodable: %v", err))
	}
	return rec, nil
}

// writeSkipRanges stores the record, refusing if it moved since it was read. One key holds every
// log's ranges, so a blind write would drop whatever another operator added in between.
func writeSkipRanges(ctx context.Context, cli *clientv3.Client, kb *meta.KeyBuilder, rec *skipRangeRecord) error {
	value, err := pb.Marshal(rec.set)
	if err != nil {
		return wperrors.NewStateConflictError(fmt.Sprintf("encode skip ranges: %v", err))
	}
	if len(value) > meta.MaxAllSkipRangesBytes {
		return wperrors.NewStateConflictError(fmt.Sprintf(
			"skip ranges would be %d bytes, over the %d-byte limit for one record; remove ranges that no longer apply",
			len(value), meta.MaxAllSkipRangesBytes,
		))
	}
	key := kb.AllSkipRangesKey()
	txn, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", rec.revision)).
		Then(clientv3.OpPut(key, string(value))).Commit()
	if err != nil {
		return wperrors.NewNetworkError(fmt.Sprintf("etcd put %s: %v", key, err))
	}
	if !txn.Succeeded {
		return wperrors.NewStateConflictError(
			"the skip range record changed while this command was running; re-run it",
		)
	}
	return nil
}

// segmentRangesOf returns the ranges declared for one segment, and whether the record holds any.
func segmentRangesOf(set *proto.AllSkipRanges, logID, segmentID int64) []*proto.SkipRange {
	return set.GetByLogId()[logID].GetBySegmentId()[segmentID].GetRanges()
}

// normaliseRanges sorts by from_entry_id and coalesces ranges that overlap. Adjacent ranges are
// left alone: they may be separate incidents, and each one's reason and timestamp are worth
// keeping. This is canonical form for the record's own sake -- a reader scanning a handful of
// ranges answers correctly however they are ordered.
func normaliseRanges(ranges []*proto.SkipRange) []*proto.SkipRange {
	if len(ranges) < 2 {
		return ranges
	}
	sort.SliceStable(ranges, func(i, j int) bool { return ranges[i].FromEntryId < ranges[j].FromEntryId })
	out := []*proto.SkipRange{ranges[0]}
	for _, r := range ranges[1:] {
		last := out[len(out)-1]
		if r.FromEntryId > last.ToEntryId {
			out = append(out, r)
			continue
		}
		// Overlapping: one declaration refining another. Keep the wider span, the earlier
		// timestamp, and both reasons, so neither account of the incident is lost.
		last.ToEntryId = max64(last.ToEntryId, r.ToEntryId)
		if r.CreationTimestamp != 0 && (last.CreationTimestamp == 0 || r.CreationTimestamp < last.CreationTimestamp) {
			last.CreationTimestamp = r.CreationTimestamp
		}
		last.Reason = joinReasons(last.Reason, r.Reason)
	}
	return out
}

// joinReasons keeps both accounts within the field's budget, so repeated edits cannot grow the
// record without bound.
func joinReasons(a, b string) string {
	if a == b || b == "" {
		return a
	}
	if a == "" {
		return b
	}
	joined := a + "; " + b
	if len(joined) > meta.MaxSkipRangeReasonBytes {
		joined = joined[:meta.MaxSkipRangeReasonBytes]
	}
	return joined
}

func putSegmentRanges(set *proto.AllSkipRanges, logID, segmentID int64, ranges []*proto.SkipRange) {
	if len(ranges) == 0 {
		if log := set.GetByLogId()[logID]; log != nil {
			delete(log.BySegmentId, segmentID)
			if len(log.BySegmentId) == 0 {
				delete(set.ByLogId, logID)
			}
		}
		return
	}
	if set.ByLogId == nil {
		set.ByLogId = map[int64]*proto.LogSkipRanges{}
	}
	if set.ByLogId[logID] == nil {
		set.ByLogId[logID] = &proto.LogSkipRanges{}
	}
	if set.ByLogId[logID].BySegmentId == nil {
		set.ByLogId[logID].BySegmentId = map[int64]*proto.SegmentSkipRanges{}
	}
	set.ByLogId[logID].BySegmentId[segmentID] = &proto.SegmentSkipRanges{Ranges: ranges}
}

func runSkipRangeList(cmd *cobra.Command, cli *clientv3.Client, kb *meta.KeyBuilder, logName string) error {
	ctx, cancel := metaCtx()
	defer cancel()

	rec, err := readSkipRanges(ctx, cli, kb)
	if err != nil {
		return err
	}

	// A name is resolved to an id, and a name that no longer exists is not an error here: the
	// point of listing is to find ranges nothing accounts for.
	wanted := int64(-1)
	if logName != "" {
		logMeta := &proto.LogMeta{}
		if err := getProto(ctx, cli, kb.BuildLogKey(logName), logMeta); err != nil {
			return wperrors.NewTargetNotFoundError(fmt.Sprintf("log %s: %v", logName, err))
		}
		wanted = logMeta.LogId
	}

	type row struct {
		LogID     int64  `json:"log_id"`
		SegmentID int64  `json:"segment_id"`
		From      int64  `json:"from_entry_id"`
		To        int64  `json:"to_entry_id"`
		Entries   int64  `json:"entries"`
		Age       string `json:"age"`
		Reason    string `json:"reason"`
	}
	rows := make([]row, 0)
	for logID, byLog := range rec.set.GetByLogId() {
		if wanted >= 0 && logID != wanted {
			continue
		}
		for segID, bySeg := range byLog.GetBySegmentId() {
			for _, r := range bySeg.GetRanges() {
				rows = append(rows, row{
					LogID: logID, SegmentID: segID,
					From: r.FromEntryId, To: r.ToEntryId,
					Entries: r.ToEntryId - r.FromEntryId + 1,
					Age:     skipRangeAge(r.CreationTimestamp),
					Reason:  r.Reason,
				})
			}
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].LogID != rows[j].LogID {
			return rows[i].LogID < rows[j].LogID
		}
		if rows[i].SegmentID != rows[j].SegmentID {
			return rows[i].SegmentID < rows[j].SegmentID
		}
		return rows[i].From < rows[j].From
	})

	w := cmd.OutOrStdout()
	if renderedOutput() {
		payload := map[string]any{"source": kb.AllSkipRangesKey(), "ranges": rows}
		if Globals.Output == "yaml" {
			return output.RenderYAML(w, payload)
		}
		return output.RenderJSON(w, payload)
	}

	// Naming the source matters: a host application can supply ranges of its own at runtime,
	// and those never reach this record, so a caller has to know which one was read.
	fmt.Fprintf(w, "Skip ranges declared in %s\n\n", kb.AllSkipRangesKey())
	if len(rows) == 0 {
		fmt.Fprintln(w, "None declared.")
		return nil
	}
	table := make([][]string, 0, len(rows))
	for _, r := range rows {
		table = append(table, []string{
			strconv.FormatInt(r.LogID, 10), strconv.FormatInt(r.SegmentID, 10),
			fmt.Sprintf("%d-%d", r.From, r.To), strconv.FormatInt(r.Entries, 10), r.Age, r.Reason,
		})
	}
	return output.RenderRowTable(w,
		[]string{"LOG ID", "SEGMENT", "ENTRIES SKIPPED", "COUNT", "AGE", "REASON"}, table)
}

// skipRangeAge is how long ago a range was declared. A range nobody has revisited is the thing
// an operator most wants to notice, so the age is a column rather than a raw timestamp.
func skipRangeAge(createdUnix uint64) string {
	if createdUnix == 0 {
		return "unknown"
	}
	return formatAge(time.Since(time.Unix(int64(createdUnix), 0)).Milliseconds())
}

type skipRangeAddRequest struct {
	logName   string
	segmentID int64
	from, to  int64
	reason    string
	confirmed bool
	force     bool
}

func runSkipRangeAdd(cmd *cobra.Command, cli *clientv3.Client, kb *meta.KeyBuilder,
	ac *client.Client, members *client.Memberlist, req skipRangeAddRequest,
) error {
	ctx, cancel := metaCtx()
	defer cancel()

	logMeta := &proto.LogMeta{}
	if err := getProto(ctx, cli, kb.BuildLogKey(req.logName), logMeta); err != nil {
		return wperrors.NewTargetNotFoundError(fmt.Sprintf("log %s: %v", req.logName, err))
	}
	segMeta := &proto.SegmentMetadata{}
	if err := getProto(ctx, cli, kb.BuildSegmentInstanceKey(req.logName, strconv.FormatInt(req.segmentID, 10)), segMeta); err != nil {
		return wperrors.NewTargetNotFoundError(fmt.Sprintf(
			"log %s segment %d: %v", req.logName, req.segmentID, err,
		))
	}

	asked := entryRange{req.from, req.to}
	errOut := cmd.ErrOrStderr()

	// The quorum is asked before the range is agreed to, because a range over data some
	// replica can still serve is not a range to skip: failover would have read it.
	readable, unasked := skipRangeReadability(ac, members, segMeta.GetQuorum(), logMeta.LogId, req.segmentID)
	if still := readable.intersect(rangeSet{}.add(asked)); len(still) > 0 && !req.force {
		return wperrors.NewStateConflictError(fmt.Sprintf(
			"%s can still read %s of the requested range; failover would serve those entries. Withdraw the range or pass --force to declare it anyway",
			strings.Join(readable.holders, ", "), still,
		))
	}
	if len(unasked) > 0 {
		fmt.Fprintf(errOut, "WARNING: %s could not be asked, so this view of the range is incomplete\n",
			strings.Join(unasked, ", "))
	}

	rec, err := readSkipRanges(ctx, cli, kb)
	if err != nil {
		return err
	}
	reason := req.reason
	if len(reason) > meta.MaxSkipRangeReasonBytes {
		reason = reason[:meta.MaxSkipRangeReasonBytes]
	}
	existing := segmentRangesOf(rec.set, logMeta.LogId, req.segmentID)
	proposed := normaliseRanges(append(append([]*proto.SkipRange(nil), existing...), &proto.SkipRange{
		FromEntryId: req.from, ToEntryId: req.to,
		CreationTimestamp: uint64(time.Now().Unix()), Reason: reason,
	}))

	before := rangeSet{}
	for _, r := range existing {
		before = before.add(entryRange{r.FromEntryId, r.ToEntryId})
	}
	newlyLost := rangeSet{}.add(asked).subtract(before)

	fmt.Fprintf(errOut, "Declaring log %s (id %d) segment %d entries %s unreadable.\n",
		req.logName, logMeta.LogId, req.segmentID, rangeSet{}.add(asked))
	if len(newlyLost) == 0 {
		fmt.Fprintln(errOut, "Every entry asked for is already declared; nothing more becomes unreachable.")
	} else {
		fmt.Fprintf(errOut, "Entries %s become unreachable to every reader of this log, and withdrawing the range does not bring them back.\n", newlyLost)
	}
	if !req.confirmed {
		return wperrors.NewUserAbortError()
	}

	putSegmentRanges(rec.set, logMeta.LogId, req.segmentID, proposed)
	if err := writeSkipRanges(ctx, cli, kb, rec); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Declared: log %d segment %d now skips %s\n",
		logMeta.LogId, req.segmentID, rangesString(proposed))
	return nil
}

func runSkipRangeRemove(cmd *cobra.Command, cli *clientv3.Client, kb *meta.KeyBuilder,
	logName string, segmentID, from, to int64,
) error {
	ctx, cancel := metaCtx()
	defer cancel()

	logMeta := &proto.LogMeta{}
	if err := getProto(ctx, cli, kb.BuildLogKey(logName), logMeta); err != nil {
		return wperrors.NewTargetNotFoundError(fmt.Sprintf("log %s: %v", logName, err))
	}
	rec, err := readSkipRanges(ctx, cli, kb)
	if err != nil {
		return err
	}
	existing := segmentRangesOf(rec.set, logMeta.LogId, segmentID)
	if len(existing) == 0 {
		return wperrors.NewTargetNotFoundError(fmt.Sprintf(
			"log %s segment %d has no declared skip range", logName, segmentID,
		))
	}

	// Withdrawing part of a declared range splits it, so the arithmetic is done on the ranges
	// rather than by matching a declaration exactly: an operator withdrawing what a replica
	// rebuild recovered does not have to reproduce the original boundaries.
	withdrawn := rangeSet{}.add(entryRange{from, to})
	kept := make([]*proto.SkipRange, 0, len(existing)+1)
	for _, r := range existing {
		pieces := rangeSet{}.add(entryRange{r.FromEntryId, r.ToEntryId}).subtract(withdrawn)
		for _, piece := range pieces {
			kept = append(kept, &proto.SkipRange{
				FromEntryId: piece.From, ToEntryId: piece.To,
				CreationTimestamp: r.CreationTimestamp, Reason: r.Reason,
			})
		}
	}
	if len(kept) == len(existing) && rangesString(kept) == rangesString(existing) {
		return wperrors.NewTargetNotFoundError(fmt.Sprintf(
			"log %s segment %d declares %s, which does not overlap %d-%d",
			logName, segmentID, rangesString(existing), from, to,
		))
	}

	putSegmentRanges(rec.set, logMeta.LogId, segmentID, normaliseRanges(kept))
	if err := writeSkipRanges(ctx, cli, kb, rec); err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if len(kept) == 0 {
		fmt.Fprintf(w, "Withdrawn: log %d segment %d declares no skip range now\n", logMeta.LogId, segmentID)
	} else {
		fmt.Fprintf(w, "Withdrawn: log %d segment %d now skips %s\n", logMeta.LogId, segmentID, rangesString(kept))
	}
	fmt.Fprintln(w, "Readers will try those entries again; a reader that already moved past them does not come back.")
	return nil
}

// readableHolders is the entries the quorum can still serve, with the replicas that said so.
type readableHolders struct {
	rangeSet
	holders []string
}

// skipRangeReadability asks every replica what it can still read of the segment, and returns the
// union -- a read is served by any one replica, so one replica holding an entry is enough for
// that entry not to be lost. Replicas that could not answer are named separately: an incomplete
// view must not read as agreement.
func skipRangeReadability(ac *client.Client, members *client.Memberlist, quorum *proto.QuorumInfo,
	logID, segmentID int64,
) (readableHolders, []string) {
	out := readableHolders{}
	unasked := make([]string, 0)
	if quorum == nil || len(quorum.Nodes) == 0 {
		return out, unasked
	}
	for _, n := range inspectEachNode(ac, members, quorum, logID, segmentID, 0, 0) {
		if !n.answered() {
			unasked = append(unasked, fmt.Sprintf("%s (%s)", inspectLabel(n), n.State))
			continue
		}
		view := viewOf(n, 0)
		if len(view.readable) == 0 {
			continue
		}
		out.rangeSet = out.rangeSet.union(view.readable)
		out.holders = append(out.holders, inspectLabel(n))
	}
	return out, unasked
}

func rangesString(ranges []*proto.SkipRange) string {
	parts := make([]string, 0, len(ranges))
	for _, r := range ranges {
		parts = append(parts, fmt.Sprintf("%d-%d", r.FromEntryId, r.ToEntryId))
	}
	if len(parts) == 0 {
		return "nothing"
	}
	return strings.Join(parts, ", ")
}
