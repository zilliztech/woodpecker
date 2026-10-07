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

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
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
	var fromEntry, toEntry, logID int64
	cmd := &cobra.Command{
		Use:   "remove <logName> <segmentId> | --log-id N <segmentId>",
		Short: "Withdraw a declared range, so readers stop skipping it",
		Long: `Withdraw a declared range, so readers stop skipping it.

Withdrawing does not restore anything: it only lets readers try the entries again, which
is what to do once a replica has been rebuilt. A reader that already moved past the range
does not come back for it.`,
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			// A range whose log has been deleted has no name to give, so the id may stand in.
			logName, segmentArg := "", args[0]
			switch {
			case logID >= 0 && len(args) == 1:
			case logID < 0 && len(args) == 2:
				logName, segmentArg = args[0], args[1]
			default:
				return wperrors.NewUsageError(
					"give either <logName> <segmentId>, or --log-id N <segmentId> for a range whose log is gone",
				)
			}
			segmentID, parseErr := strconv.ParseInt(segmentArg, 10, 64)
			if parseErr != nil {
				return wperrors.NewUsageError(fmt.Sprintf("segment id %q is not a number", segmentArg))
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
			return runSkipRangeRemove(cmd, cli, conn.kb, logName, logID, segmentID, fromEntry, toEntry)
		},
	}
	flags.register(cmd)
	cmd.Flags().Int64Var(&fromEntry, "from-entry", -1, "First entry of the range to withdraw (inclusive)")
	cmd.Flags().Int64Var(&toEntry, "to-entry", -1, "Last entry of the range to withdraw (inclusive)")
	cmd.Flags().Int64Var(&logID, "log-id", -1,
		"Withdraw by log id instead of name, for a range whose log has been deleted")
	return cmd
}

// skipRangeMeta wraps the resolved etcd connection in the metadata provider every reader uses, so the
// record's key, its compare-and-swap and its size bound have one implementation rather than this
// command's copy of them. It also hands the provider the same decision a reader makes -- which
// metadata prefix this cluster is actually on -- because a declaration written under any other prefix
// is one no reader will ever see.
//
// Nothing else in `wp` goes through the provider: the plain "get a key and unmarshal it" reads carry
// no invariant worth sharing, and this is not the command that changes them.
func skipRangeMeta(ctx context.Context, cli *clientv3.Client, kb *meta.KeyBuilder) meta.MetadataProvider {
	cfg := &config.Configuration{}
	cfg.Woodpecker.Meta.Prefix = kb.Prefix()
	return meta.NewMetadataProvider(ctx, cli, cfg)
}

// readSkipRanges, writeSkipRanges and removeSkipRanges translate the provider's errors into the ones
// a `wp` caller needs an exit code from. That translation is all they do: `meta` has no business
// knowing about exit codes, and this command has none reimplementing the record.
func readSkipRanges(ctx context.Context, provider meta.MetadataProvider, kb *meta.KeyBuilder) (*meta.AllSkipRanges, error) {
	rec, err := provider.GetAllSkipRanges(ctx)
	if err != nil {
		if werr.ErrMetadataDecode.Is(err) {
			return nil, wperrors.NewStateConflictError(fmt.Sprintf("skip range record is not decodable: %v", err))
		}
		return nil, wperrors.NewNetworkError(fmt.Sprintf("read %s: %v", kb.AllSkipRangesKey(), err))
	}
	return rec, nil
}

func writeSkipRanges(ctx context.Context, provider meta.MetadataProvider, kb *meta.KeyBuilder, rec *meta.AllSkipRanges) error {
	return translateSkipRangeWrite(provider.UpdateAllSkipRanges(ctx, rec), kb)
}

// removeSkipRanges drops the record rather than storing an empty one, so a cluster whose last
// declaration has been withdrawn looks like one that never had any.
func removeSkipRanges(ctx context.Context, provider meta.MetadataProvider, kb *meta.KeyBuilder, rec *meta.AllSkipRanges) error {
	return translateSkipRangeWrite(provider.RemoveAllSkipRanges(ctx, rec), kb)
}

func translateSkipRangeWrite(err error, kb *meta.KeyBuilder) error {
	switch {
	case err == nil:
		return nil
	case werr.ErrMetadataRevisionInvalid.Is(err):
		return wperrors.NewStateConflictError(
			"the skip range record changed while this command was running; re-run it",
		)
	case werr.ErrMetadataEncode.Is(err):
		// Over the size limit, or unencodable: either way the operator has something to remove, and
		// the message from meta already names the sizes.
		return wperrors.NewStateConflictError(err.Error())
	default:
		return wperrors.NewNetworkError(fmt.Sprintf("write %s: %v", kb.AllSkipRangesKey(), err))
	}
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
	return meta.TruncateSkipRangeReason(a + "; " + b)
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
	// Reading a nil map is legal and reading through a nil message is what the generated getters
	// are for, so everything else in this file chains them freely. Writing is the exception: an
	// assignment into a nil map panics, so each level is created before the next line touches it.
	// The direct field accesses below are safe only because of that order.
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

	rec, err := readSkipRanges(ctx, skipRangeMeta(ctx, cli, kb), kb)
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
	for logID, byLog := range rec.Metadata.GetByLogId() {
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

	// The quorum is asked before the range is agreed to. Two answers refuse it, and neither is
	// "some replica reported an error": a replica that can still serve part of the range means
	// failover already covers those entries, and a replica that said nothing about part of it
	// means nothing was established there -- an unread range is not an empty one.
	answers := skipRangeAnswers(ac, members, segMeta.GetQuorum(), logMeta.LogId, req.segmentID, asked)
	holders, silent := make([]string, 0, len(answers)), make([]string, 0, len(answers))
	served := rangeSet{}
	for _, a := range answers {
		if len(a.readable) > 0 {
			holders = append(holders, a.label)
			served = served.union(a.readable)
		}
		if len(a.unaccounted) > 0 {
			reason := fmt.Sprintf("said nothing about %s", a.unaccounted)
			if a.why != "" {
				reason = a.why
			}
			silent = append(silent, fmt.Sprintf("%s (%s)", a.label, reason))
		}
	}
	switch {
	case len(holders) > 0 && !req.force:
		return wperrors.NewStateConflictError(fmt.Sprintf(
			"%s can still read %s of the requested range; failover would serve those entries. Withdraw the range or pass --force to declare it anyway",
			strings.Join(holders, ", "), served,
		))
	case len(answers) == 0 && !req.force:
		return wperrors.NewStateConflictError(fmt.Sprintf(
			"log %s segment %d names no replica, so nothing could be consulted; pass --force to declare the range on metadata alone",
			req.logName, req.segmentID,
		))
	case len(silent) > 0 && !req.force:
		return wperrors.NewStateConflictError(fmt.Sprintf(
			"%s did not account for the whole range, so it is not established as unreadable there: %s. Survey it with `wp segment inspect %s %d`, or pass --force to declare it anyway",
			plural(len(silent), "replica"), strings.Join(silent, "; "), req.logName, req.segmentID,
		))
	}
	if req.force && (len(holders) > 0 || len(silent) > 0 || len(answers) == 0) {
		fmt.Fprintln(errOut, "WARNING: --force is overriding the quorum's answer about this range")
	}

	// A fresh deadline for the metadata read and write. The inspection above talks to every
	// replica over HTTP on the same budget this context was given, so one replica that accepts a
	// connection and never answers would leave nothing left for the write.
	metaWriteCtx, cancelWrite := metaCtx()
	defer cancelWrite()

	provider := skipRangeMeta(metaWriteCtx, cli, kb)
	rec, err := readSkipRanges(metaWriteCtx, provider, kb)
	if err != nil {
		return err
	}
	existing := segmentRangesOf(rec.Metadata, logMeta.LogId, req.segmentID)

	// What is newly given up, computed before the merge: normalising rewrites the stored ranges in
	// place, so a preview taken afterwards would measure the new range against itself and report
	// that nothing changed.
	before := rangeSet{}
	for _, r := range existing {
		before = before.add(entryRange{r.FromEntryId, r.ToEntryId})
	}
	newlyLost := rangeSet{}.add(asked).subtract(before)

	proposed := normaliseRanges(append(append([]*proto.SkipRange(nil), existing...), &proto.SkipRange{
		FromEntryId: req.from, ToEntryId: req.to,
		CreationTimestamp: uint64(time.Now().Unix()), Reason: meta.TruncateSkipRangeReason(req.reason),
	}))

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

	putSegmentRanges(rec.Metadata, logMeta.LogId, req.segmentID, proposed)
	if err := writeSkipRanges(metaWriteCtx, provider, kb, rec); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Declared: log %d segment %d now skips %s\n",
		logMeta.LogId, req.segmentID, rangesString(proposed))
	return nil
}

// runSkipRangeRemove withdraws a range, by log name or, when the log is gone, by log id. Ranges
// outlive their log by design -- ids are never reused, so a leftover range is inert -- but the
// listing says they stay until removed and the size refusal says to remove them, so a name that no
// longer resolves must not be the end of the road.
func runSkipRangeRemove(cmd *cobra.Command, cli *clientv3.Client, kb *meta.KeyBuilder,
	logName string, logID, segmentID, from, to int64,
) error {
	ctx, cancel := metaCtx()
	defer cancel()

	if logName != "" {
		logMeta := &proto.LogMeta{}
		if err := getProto(ctx, cli, kb.BuildLogKey(logName), logMeta); err != nil {
			return wperrors.NewTargetNotFoundError(fmt.Sprintf(
				"log %s: %v; pass --log-id to withdraw a range whose log is gone", logName, err,
			))
		}
		logID = logMeta.LogId
	}
	provider := skipRangeMeta(ctx, cli, kb)
	rec, err := readSkipRanges(ctx, provider, kb)
	if err != nil {
		return err
	}
	existing := segmentRangesOf(rec.Metadata, logID, segmentID)
	if len(existing) == 0 {
		return wperrors.NewTargetNotFoundError(fmt.Sprintf(
			"log %d segment %d has no declared skip range", logID, segmentID,
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
			"log %d segment %d declares %s, which does not overlap %d-%d",
			logID, segmentID, rangesString(existing), from, to,
		))
	}

	putSegmentRanges(rec.Metadata, logID, segmentID, normaliseRanges(kept))
	var writeErr error
	if len(rec.Metadata.GetByLogId()) == 0 {
		// Dropping the last declared range drops the record rather than storing an empty one, so a
		// cluster whose declarations have all been withdrawn looks like one that never had any. The
		// write and the delete are alternatives, not a sequence: writing first would move the record's
		// revision and the delete's compare-and-swap would refuse on the revision it was read at.
		writeErr = removeSkipRanges(ctx, provider, kb, rec)
	} else {
		writeErr = writeSkipRanges(ctx, provider, kb, rec)
	}
	if writeErr != nil {
		return writeErr
	}
	w := cmd.OutOrStdout()
	if len(kept) == 0 {
		fmt.Fprintf(w, "Withdrawn: log %d segment %d declares no skip range now\n", logID, segmentID)
	} else {
		fmt.Fprintf(w, "Withdrawn: log %d segment %d now skips %s\n", logID, segmentID, rangesString(kept))
	}
	fmt.Fprintln(w, "Readers will try those entries again; a reader that already moved past them does not come back.")
	return nil
}

// replicaAnswer is what one replica said about the range being declared.
type replicaAnswer struct {
	label string
	// unaccounted is the part of the range this replica said nothing about. A survey reports only
	// what it walked, and it stops at the node's own block bound, at a broken chain, or at once
	// when the node holds no local copy -- so silence about an entry is not a statement that the
	// entry is gone. This is the field the gate turns on.
	unaccounted rangeSet
	// readable is the part of the range this replica can still serve. A read is served by any one
	// replica, so anything here is data failover already covers.
	readable rangeSet
	// why is how the replica failed to answer at all, when it did not.
	why string
}

// skipRangeAnswers asks every replica what it knows about the range about to be declared.
//
// It asks for the whole segment rather than taking the node's default bound: a verifying survey
// stops at 64 blocks by default and a full segment holds about 128, so a bounded answer would
// leave most of a segment unaccounted for and the gate would have nothing to object to.
func skipRangeAnswers(ac *client.Client, members *client.Memberlist, quorum *proto.QuorumInfo,
	logID, segmentID int64, asked entryRange,
) []replicaAnswer {
	if quorum == nil || len(quorum.Nodes) == 0 {
		return nil
	}
	askedSet := rangeSet{}.add(asked)
	answers := make([]replicaAnswer, 0, len(quorum.Nodes))
	for _, n := range inspectEachNode(ac, members, quorum, logID, segmentID, 0, skipRangeMaxBlocks) {
		a := replicaAnswer{label: inspectLabel(n)}
		if !n.answered() {
			a.why, a.unaccounted = n.State, askedSet
			answers = append(answers, a)
			continue
		}
		view := viewOf(n, 0)
		a.unaccounted = askedSet.subtract(view.accounted())
		a.readable = view.readable.intersect(askedSet)
		answers = append(answers, a)
	}
	return answers
}

// skipRangeMaxBlocks is what a declaration asks each node to survey, above the 1000 blocks a
// segment's rolling policy allows, so one request covers a whole segment. A segment that somehow
// exceeds it answers `bound`, which leaves entries unaccounted for and refuses the declaration
// rather than waving it through.
const skipRangeMaxBlocks = 4096

// rangesString renders a non-empty list; both callers check for empty first and say so in their
// own words, which read better than a shared placeholder.
func rangesString(ranges []*proto.SkipRange) string {
	parts := make([]string, 0, len(ranges))
	for _, r := range ranges {
		parts = append(parts, fmt.Sprintf("%d-%d", r.FromEntryId, r.ToEntryId))
	}
	return strings.Join(parts, ", ")
}
