package cmd

import (
	"fmt"
	"sort"
	"time"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"
)

// newLogCommand groups the commands that answer questions about a log through its metadata, as
// opposed to the logstore family, which asks individual nodes.
func newLogCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "log",
		Short: "Inspect a log through its metadata (not log levels — see 'wp logging')",
	}
	cmd.AddCommand(newLogReadersCommand())
	cmd.AddCommand(newLogScanCommand())
	return cmd
}

// newLogReadersCommand shows where each of a log's readers has read to.
//
// A reader publishes its own position into metadata under a lease, so the answer comes from etcd
// alone and stays available when nodes do not: an unreachable node is often why a reader is being
// looked at in the first place.
func newLogReadersCommand() *cobra.Command {
	var flags metaEtcdFlags
	cmd := &cobra.Command{
		Use:   "readers <logName>",
		Short: "Show where each of a log's readers has read to",
		Args:  cobra.ExactArgs(1),
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
			return runLogReaders(cmd, cli, conn.kb, args[0])
		},
	}
	flags.register(cmd)
	return cmd
}

// A reader republishes its position on this interval even when the position has not moved, so a
// report older than a couple of intervals means the reader is not reporting at all.
const readerStaleAfterMs = 2 * wplog.UpdateReaderInfoIntervalMs

// Per-reader readings that mean something on their own.
const (
	readerStateLive   = "live"
	readerStateAtOpen = "no read since open"
	readerStateStale  = "stale report"
)

// readerPosition is one reader's published position, with the two ages computed against this host's
// clock so a caller does not have to.
type readerPosition struct {
	Name                string `json:"reader_name"`
	OpenSegmentID       int64  `json:"open_segment_id"`
	OpenEntryID         int64  `json:"open_entry_id"`
	OpenedAgeMs         int64  `json:"opened_age_ms"`
	RecentReadSegmentID int64  `json:"recent_read_segment_id"`
	RecentReadEntryID   int64  `json:"recent_read_entry_id"`
	LastReportAgeMs     int64  `json:"last_report_age_ms"`
	State               string `json:"state"`
}

func runLogReaders(cmd *cobra.Command, cli *clientv3.Client, kb *meta.KeyBuilder, logName string) error {
	ctx, cancel := metaCtx()
	defer cancel()

	logMeta := &proto.LogMeta{}
	if err := getProto(ctx, cli, kb.BuildLogKey(logName), logMeta); err != nil {
		return wperrors.NewTargetNotFoundError(fmt.Sprintf("log %s: %v", logName, err))
	}

	prefix := kb.BuildLogAllReaderTempInfosKey(logMeta.LogId)
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return wperrors.NewNetworkError(fmt.Sprintf("etcd get %s: %v", prefix, err))
	}

	nowMs := time.Now().UnixMilli()
	readers := make([]readerPosition, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		info := &proto.ReaderTempInfo{}
		if unmarshalErr := pb.Unmarshal(kv.Value, info); unmarshalErr != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "warn: skipping unparseable reader record %s: %v\n",
				string(kv.Key), unmarshalErr)
			continue
		}
		readers = append(readers, describeReader(info, nowMs))
	}
	sort.Slice(readers, func(i, j int) bool { return readers[i].Name < readers[j].Name })

	w := cmd.OutOrStdout()
	if Globals.Output == "json" || Globals.Output == "yaml" {
		payload := map[string]any{
			"log_name": logName, "log_id": logMeta.LogId,
			"report_interval_ms": wplog.UpdateReaderInfoIntervalMs,
			"readers":            readers,
		}
		if Globals.Output == "yaml" {
			return output.RenderYAML(w, payload)
		}
		return output.RenderJSON(w, payload)
	}

	if len(readers) == 0 {
		// The key is held by the reader's lease, so its absence is the answer: nobody is connected.
		fmt.Fprintf(w, "Log %s (id %d) — no live readers\n", logName, logMeta.LogId)
		return nil
	}

	fmt.Fprintf(w, "Log %s (id %d) — %d live reader(s)\n\n", logName, logMeta.LogId, len(readers))
	rows := make([][]string, 0, len(readers))
	for _, r := range readers {
		rows = append(rows, []string{
			r.Name,
			fmt.Sprintf("%d:%d", r.OpenSegmentID, r.OpenEntryID),
			fmt.Sprintf("%d:%d", r.RecentReadSegmentID, r.RecentReadEntryID),
			formatAge(r.LastReportAgeMs),
			formatAge(r.OpenedAgeMs),
			r.State,
		})
	}
	if err := output.RenderRowTable(w,
		[]string{"READER", "OPEN_POS", "CURRENT", "LAST_REPORT", "OPEN_FOR", "STATE"}, rows); err != nil {
		return err
	}
	fmt.Fprintf(w, "\nA reader republishes every %ds even when its position has not moved, so sample no faster\n"+
		"than that. Whether a position is advancing takes two readings a little apart.\n",
		wplog.UpdateReaderInfoIntervalMs/1000)
	return nil
}

// describeReader turns one published record into the row, naming the readings an operator would
// otherwise have to derive: a reader still at the position it opened at never reported a read, and
// a report older than two intervals means the reader has stopped reporting.
func describeReader(info *proto.ReaderTempInfo, nowMs int64) readerPosition {
	r := readerPosition{
		Name:                info.ReaderName,
		OpenSegmentID:       info.OpenSegmentId,
		OpenEntryID:         info.OpenEntryId,
		OpenedAgeMs:         nowMs - int64(info.OpenTimestamp),
		RecentReadSegmentID: info.RecentReadSegmentId,
		RecentReadEntryID:   info.RecentReadEntryId,
		LastReportAgeMs:     nowMs - int64(info.RecentReadTimestamp),
	}
	atOpenPosition := info.RecentReadSegmentId == info.OpenSegmentId &&
		info.RecentReadEntryId == info.OpenEntryId &&
		info.RecentReadTimestamp == info.OpenTimestamp
	switch {
	case r.LastReportAgeMs > readerStaleAfterMs:
		r.State = readerStateStale
	case atOpenPosition:
		r.State = readerStateAtOpen
	default:
		r.State = readerStateLive
	}
	return r
}

// formatAge renders a millisecond age as a duration. A negative age is a difference between the
// publishing host's clock and this one, and is shown as zero rather than as time running backwards.
func formatAge(ms int64) string {
	if ms < 0 {
		return "0s"
	}
	return (time.Duration(ms) * time.Millisecond).Round(time.Second).String()
}
