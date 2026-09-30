package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

// Scan modes. Quick reads only what says where the entries are; raw reads the data through the same
// decoding and checksums a reader uses, so it verifies rather than trusts.
const (
	scanModeQuick = "quick"
	scanModeRaw   = "raw"
)

// newLogScanCommand sweeps a whole log to check that reading runs through and that the data matches
// its metadata.
//
// Every other command in this family starts from a segment id, so nothing could ask a log whether
// it is sound. This does, cheaply enough to run on a whole log: quick mode reads a sealed segment's
// index, a live writer's snapshot, and the metadata, and no entry data at all.
func newLogScanCommand() *cobra.Command {
	var flags metaEtcdFlags
	var mode string
	var from, to int64
	cmd := &cobra.Command{
		Use:   "scan <logName>",
		Short: "Sweep a log to check that reading runs through and the data matches its metadata",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if mode != scanModeQuick && mode != scanModeRaw {
				return wperrors.NewUsageError(fmt.Sprintf("unknown --mode %q: use quick or raw", mode))
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
			return runLogScan(cmd, cli, conn.kb, res.Client, res.Members, args[0], mode, from, to)
		},
	}
	flags.register(cmd)
	cmd.Flags().StringVar(&mode, "mode", scanModeQuick,
		"quick reads only structure and metadata; raw reads the data through the real checksums")
	cmd.Flags().Int64Var(&from, "from-segment", 0, "First segment to scan")
	cmd.Flags().Int64Var(&to, "to-segment", -1, "Last segment to scan (default: the newest)")
	return cmd
}

func runLogScan(cmd *cobra.Command, cli *clientv3.Client, kb *meta.KeyBuilder,
	ac *client.Client, members *client.Memberlist, logName, mode string, from, to int64,
) error {
	ctx, cancel := metaCtx()
	defer cancel()

	logMeta := &proto.LogMeta{}
	if err := getProto(ctx, cli, kb.BuildLogKey(logName), logMeta); err != nil {
		return wperrors.NewTargetNotFoundError(fmt.Sprintf("log %s: %v", logName, err))
	}

	metas, err := readSegmentMetas(ctx, cli, kb, logName)
	if err != nil {
		return err
	}
	if len(metas) == 0 {
		fmt.Fprintf(cmd.OutOrStdout(), "Log %s (id %d) has no segments in metadata.\n", logName, logMeta.LogId)
		return nil
	}

	segments := make([]scanSegment, 0, len(metas))
	for _, sm := range metas {
		if sm.id < from || (to >= 0 && sm.id > to) {
			continue
		}
		segments = append(segments, scanSegment{
			id: sm.id, state: sm.meta.GetState(), metaLast: sm.meta.GetLastEntryId(),
			nodes: scanSegmentNodes(ac, members, sm, logMeta.LogId, mode),
		})
	}

	rows, findings, problems := reconcileScanWithGaps(segments, logMeta.GetTruncatedSegmentId(), from,
		func(segmentID int64) ([]string, bool) {
			return probeGapForData(ac, members, logMeta.LogId, segmentID)
		})

	w := cmd.OutOrStdout()
	if renderedOutput() {
		payload := map[string]any{
			"log_name": logName, "log_id": logMeta.LogId, "mode": mode,
			"truncated_through_segment": logMeta.GetTruncatedSegmentId(),
			"segments":                  rows, "findings": findings,
		}
		if Globals.Output == "yaml" {
			if renderErr := output.RenderYAML(w, payload); renderErr != nil {
				return renderErr
			}
			return problems
		}
		if renderErr := output.RenderJSON(w, payload); renderErr != nil {
			return renderErr
		}
		return problems
	}

	fmt.Fprintf(w, "Log %s (id %d) — %d segments, %s scan, truncated through segment %d\n\n",
		logName, logMeta.LogId, len(segments), mode, logMeta.GetTruncatedSegmentId())

	table := make([][]string, 0, len(rows))
	for _, row := range rows {
		metaLast := strconv.FormatInt(row.MetaLast, 10)
		if row.MetaLast < 0 {
			metaLast = "-1 (open)"
		}
		table = append(table, []string{
			strconv.FormatInt(row.SegmentID, 10), row.State, metaLast,
			row.Reaches, row.Replicas, row.Verdict, row.Detail,
		})
	}
	if renderErr := output.RenderRowTable(w,
		[]string{"SEGMENT", "STATE", "META LAST", "REACHES", "REPLICAS", "VERDICT", "DETAIL"}, table); renderErr != nil {
		return renderErr
	}

	fmt.Fprintln(w)
	for _, line := range findings {
		fmt.Fprintf(w, "%s\n", line)
	}
	return problems
}

// probeGapForData asks every known node whether it holds a segment metadata does not know about.
// The quorum lives in the segment's metadata, which is the thing that is missing, so there is no
// list to narrow this to -- but gaps are rare, so the fan-out is paid only when there is one.
func probeGapForData(ac *client.Client, members *client.Memberlist, logID, segmentID int64) ([]string, bool) {
	if members == nil || len(members.Members) == 0 {
		return nil, false
	}
	holders := make([]string, 0, 1)
	asked := false
	for _, member := range members.Members {
		path := fmt.Sprintf("/admin/logstore/segment/inspect?log_id=%d&segment_id=%d&verify=false", logID, segmentID)
		body, status, err := fetchAdminJSONWithStatus(ac.PeerAdminURL(member), path)
		if err != nil {
			continue
		}
		asked = true
		if status != 200 {
			continue // the node says it has nothing for this segment
		}
		var resp struct {
			Survey struct {
				Blocks []struct{} `json:"blocks"`
			} `json:"survey"`
		}
		if jsonErr := json.Unmarshal(body, &resp); jsonErr != nil {
			continue
		}
		if len(resp.Survey.Blocks) > 0 {
			holders = append(holders, member.ID)
		}
	}
	return holders, asked
}

// segmentMeta is one segment's metadata record, with the id taken from its key.
type segmentMeta struct {
	id   int64
	meta *proto.SegmentMetadata
}

// readSegmentMetas lists a log's segments from metadata. The ids come from the keys, so a hole in
// the sequence is visible as a hole rather than as an absence of information.
func readSegmentMetas(ctx context.Context, cli *clientv3.Client, kb *meta.KeyBuilder, logName string) ([]segmentMeta, error) {
	prefix := fmt.Sprintf("%s/%s/segments/", kb.LogsPrefix(), logName)
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, wperrors.NewNetworkError(fmt.Sprintf("etcd get %s: %v", prefix, err))
	}
	metas := make([]segmentMeta, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		idText := strings.TrimPrefix(string(kv.Key), prefix)
		id, parseErr := strconv.ParseInt(idText, 10, 64)
		if parseErr != nil {
			continue // not a segment record
		}
		sm := &proto.SegmentMetadata{}
		if unmarshalErr := pb.Unmarshal(kv.Value, sm); unmarshalErr != nil {
			continue
		}
		metas = append(metas, segmentMeta{id: id, meta: sm})
	}
	sort.Slice(metas, func(i, j int) bool { return metas[i].id < metas[j].id })
	return metas, nil
}

// scanSegmentNodes asks each of the segment's replicas what it holds. Quick mode asks for coverage,
// which reads no entry data; raw mode asks for the verifying walk.
func scanSegmentNodes(ac *client.Client, members *client.Memberlist, sm segmentMeta, logID int64, mode string) []scanNode {
	quorum := sm.meta.GetQuorum()
	if quorum == nil || len(quorum.Nodes) == 0 {
		return nil
	}
	nodes := make([]scanNode, 0, len(quorum.Nodes))
	for _, addr := range quorum.Nodes {
		node := scanNode{label: addr, state: posUnreachable}
		member, found := memberByAddr(members, addr)
		if !found {
			node.state = posUnknownNode
			nodes = append(nodes, node)
			continue
		}
		node.label = member.ID

		path := fmt.Sprintf("/admin/logstore/segment/inspect?log_id=%d&segment_id=%d", logID, sm.id)
		if mode == scanModeQuick {
			path += "&verify=false"
		}
		body, status, err := fetchAdminJSONWithStatus(ac.PeerAdminURL(member), path)
		switch {
		case err != nil:
			node.state = posUnreachable
			nodes = append(nodes, node)
			continue
		case status != 200:
			node.state = probeStateCannotAnswer
			nodes = append(nodes, node)
			continue
		}
		var resp struct {
			Survey struct {
				Blocks []struct {
					FirstEntryID int64  `json:"first_entry_id"`
					LastEntryID  int64  `json:"last_entry_id"`
					Status       string `json:"status"`
				} `json:"blocks"`
				StopReason string `json:"stop_reason"`
			} `json:"survey"`
		}
		if jsonErr := json.Unmarshal(body, &resp); jsonErr != nil {
			node.state = posBadResponse
			nodes = append(nodes, node)
			continue
		}
		node.answered, node.stopped = true, resp.Survey.StopReason
		for _, block := range resp.Survey.Blocks {
			if block.FirstEntryID < 0 || block.LastEntryID < block.FirstEntryID {
				continue
			}
			// In raw mode only a block that verified counts as reached; in quick mode nothing was
			// verified, so the coverage is what the structure claims.
			if mode == scanModeRaw && block.Status != blockStatusOK {
				continue
			}
			node.coverage = node.coverage.add(entryRange{block.FirstEntryID, block.LastEntryID})
		}
		nodes = append(nodes, node)
	}
	return nodes
}
