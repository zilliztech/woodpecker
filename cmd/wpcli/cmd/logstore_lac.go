package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

// newLogstoreLACCommand assembles a segment's quorum view of how far it is confirmed readable.
//
// A node reports only its own durable position: last_entry_id in its writer snapshot, which every
// backend sets after a block completes. The LAC is a quorum property — the position Aq nodes have
// durably reached — so it is computed here, from the positions, rather than asked of any node.
//
// The segment's quorum lives in metadata, which the server never reads, so --etcd applies as it
// does to the marking family.
func newLogstoreLACCommand() *cobra.Command {
	var flags metaEtcdFlags
	cmd := &cobra.Command{
		Use:   "lac <logName> <segmentId>",
		Short: "Show how far a segment is confirmed readable across its quorum",
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
			return runSegmentLAC(cmd, cli, conn.kb, res.Client, res.Members, args[0], segmentID)
		},
	}
	flags.register(cmd)
	return cmd
}

// nodePosition is one quorum member's answer, or the reason it has none.
type nodePosition struct {
	Node      string `json:"node"`
	NodeID    string `json:"node_id,omitempty"`
	Durable   int64  `json:"durable_entry_id"`
	Reachable bool   `json:"reachable"`
	Note      string `json:"note,omitempty"`
}

func runSegmentLAC(cmd *cobra.Command, cli *clientv3.Client, kb *meta.KeyBuilder,
	ac *client.Client, members *client.Memberlist, logName string, segmentID int64,
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
	quorum := &proto.QuorumInfo{}
	qKey := kb.BuildQuorumInfoKey(strconv.FormatInt(segMeta.QuorumId, 10))
	if err := getProto(ctx, cli, qKey, quorum); err != nil {
		return wperrors.NewTargetNotFoundError(fmt.Sprintf("quorum %d: %v", segMeta.QuorumId, err))
	}

	positions := collectQuorumPositions(ac, members, quorum, logMeta.LogId, segmentID)

	w := cmd.OutOrStdout()
	if Globals.Output == "json" || Globals.Output == "yaml" {
		payload := map[string]any{
			"log_name": logName, "log_id": logMeta.LogId, "segment_id": segmentID,
			"state": segMeta.State.String(), "quorum_id": segMeta.QuorumId,
			"es": quorum.Es, "wq": quorum.Wq, "aq": quorum.Aq,
			"nodes": positions,
		}
		if lac, ok := quorumLAC(positions, int(quorum.Aq)); ok {
			payload["quorum_lac"] = lac
		}
		if Globals.Output == "yaml" {
			return output.RenderYAML(w, payload)
		}
		return output.RenderJSON(w, payload)
	}

	fmt.Fprintf(w, "Segment %d of log %s — state %s, quorum %d (es %d, wq %d, aq %d)\n\n",
		segmentID, logName, segMeta.State.String(), segMeta.QuorumId, quorum.Es, quorum.Wq, quorum.Aq)

	rows := make([][]string, 0, len(positions))
	for _, p := range positions {
		durable := "-"
		if p.Reachable {
			durable = strconv.FormatInt(p.Durable, 10)
		}
		rows = append(rows, []string{p.Node, p.NodeID, durable, p.Note})
	}
	if err := output.RenderRowTable(w, []string{"NODE", "NODE_ID", "DURABLE_ENTRY", "NOTE"}, rows); err != nil {
		return err
	}

	reachable := 0
	for _, p := range positions {
		if p.Reachable {
			reachable++
		}
	}
	unreachable := len(positions) - reachable
	lac, ok := quorumLAC(positions, int(quorum.Aq))
	if !ok {
		fmt.Fprintf(w, "\nquorum LAC: unknown — %d of %d nodes answered, fewer than aq=%d\n",
			reachable, len(positions), quorum.Aq)
		warnIfPartial(cmd.ErrOrStderr(), unreachable, len(positions))
		return wperrors.NewNetworkError(fmt.Sprintf(
			"only %d of %d quorum nodes answered, below aq=%d: no position can be called confirmed",
			reachable, len(positions), quorum.Aq))
	}
	fmt.Fprintf(w, "\nquorum LAC: %d (aq=%d of %d nodes at or past it)\n", lac, quorum.Aq, len(positions))
	warnIfPartial(cmd.ErrOrStderr(), unreachable, len(positions))
	return nil
}

// collectQuorumPositions asks each node the segment's quorum names for its own durable position.
// A node absent from the memberlist is not dialed: the quorum records service addresses, and the
// admin port is a separate thing only a memberlist entry can supply.
func collectQuorumPositions(ac *client.Client, members *client.Memberlist, quorum *proto.QuorumInfo,
	logID, segmentID int64,
) []nodePosition {
	positions := make([]nodePosition, 0, len(quorum.Nodes))
	for _, addr := range quorum.Nodes {
		p := nodePosition{Node: addr, Durable: -1}
		member, found := memberByAddr(members, addr)
		if !found {
			p.Note = "not in memberlist"
			positions = append(positions, p)
			continue
		}
		p.NodeID = member.ID
		path := fmt.Sprintf("/admin/logstore/segments?log_id=%d", logID)
		body, err := fetchAdminJSON(ac.PeerAdminURL(member), path)
		if err != nil {
			p.Note = "unreachable"
			positions = append(positions, p)
			continue
		}
		var resp struct {
			Segments []struct {
				SegmentID int64 `json:"segment_id"`
				LastEntry int64 `json:"last_entry_id"`
			} `json:"segments"`
		}
		if jsonErr := json.Unmarshal(body, &resp); jsonErr != nil {
			p.Note = "invalid response"
			positions = append(positions, p)
			continue
		}
		p.Note = "no active writer"
		for _, s := range resp.Segments {
			if s.SegmentID == segmentID {
				p.Durable, p.Reachable, p.Note = s.LastEntry, true, ""
				break
			}
		}
		positions = append(positions, p)
	}
	return positions
}

// memberByAddr matches a quorum entry against the memberlist by id or advertised address, and
// deliberately does not fall back to treating the address as a direct target: a quorum records
// service addresses, and dialing one as if it were an admin address reaches the wrong port.
func memberByAddr(members *client.Memberlist, addr string) (client.Member, bool) {
	if members == nil {
		return client.Member{}, false
	}
	for _, m := range members.Members {
		if m.ID == addr || m.ServiceAddr == addr || m.GossipAddr == addr {
			return m, true
		}
	}
	return client.Member{}, false
}

// quorumLAC is the aq-th highest durable position: the furthest point aq nodes have all reached.
// Fewer than aq answers means no position can be called confirmed, which is not the same as zero.
func quorumLAC(positions []nodePosition, aq int) (int64, bool) {
	if aq <= 0 {
		return 0, false
	}
	durable := make([]int64, 0, len(positions))
	for _, p := range positions {
		if p.Reachable {
			durable = append(durable, p.Durable)
		}
	}
	if len(durable) < aq {
		return 0, false
	}
	sort.Slice(durable, func(i, j int) bool { return durable[i] > durable[j] })
	return durable[aq-1], true
}

// getProto reads one key and unmarshals it, treating an absent key as an error rather than as an
// empty message: a missing segment and a segment with no entries are different answers.
func getProto(ctx context.Context, cli *clientv3.Client, key string, msg pb.Message) error {
	resp, err := cli.Get(ctx, key)
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return fmt.Errorf("not found at %s", key)
	}
	return pb.Unmarshal(resp.Kvs[0].Value, msg)
}
