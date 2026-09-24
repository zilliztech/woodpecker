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

// Per-node outcomes. "no writer" is deliberately not "unreachable": a node that answered and
// simply has no live writer for the segment is healthy, and a sealed or compacted segment has
// none anywhere. Counting those as unreachable turns a finished segment into a network fault.
const (
	posOK          = "ok"
	posNoWriter    = "no writer"
	posUnreachable = "unreachable"
	posUnknownNode = "not in memberlist"
	posBadResponse = "invalid response"
)

// nodePosition is one quorum member's answer, or the reason it has none.
type nodePosition struct {
	Node    string `json:"node"`
	NodeID  string `json:"node_id,omitempty"`
	Durable int64  `json:"durable_entry_id"`
	State   string `json:"state"`
}

func (p nodePosition) answered() bool { return p.State == posOK }

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
	// The quorum is stored inline on the segment. storeNewSegmentMeta sets only Quorum, quorumId
	// is deprecated and left at zero, and StoreQuorumInfo has no production caller -- so the
	// quorums/ keyspace is empty on a real cluster and reading it would find nothing.
	quorum := segMeta.GetQuorum()
	if quorum == nil {
		return wperrors.NewTargetNotFoundError(fmt.Sprintf(
			"segment %d of log %s carries no quorum", segmentID, logName))
	}

	positions := collectQuorumPositions(ac, members, quorum, logMeta.LogId, segmentID)

	answered, unreachable := 0, 0
	for _, p := range positions {
		switch {
		case p.answered():
			answered++
		case p.State != posNoWriter:
			unreachable++
		}
	}

	// A segment past Active has its end recorded in metadata, and no node needs a live writer
	// for it -- compaction, a processor close or idle eviction all drop one. Falling back here
	// keeps a finished segment from being reported as a network fault.
	lac, ok := quorumLAC(positions, int(quorum.Aq))
	source := "quorum"
	if !ok && segMeta.State != proto.SegmentState_Active && segMeta.LastEntryId >= 0 {
		lac, ok, source = segMeta.LastEntryId, true, "segment metadata"
	}

	var unresolved error
	if !ok {
		unresolved = wperrors.NewNetworkError(fmt.Sprintf(
			"only %d of %d quorum nodes answered, below aq=%d: no position can be called confirmed",
			answered, len(positions), quorum.Aq))
	}

	w := cmd.OutOrStdout()
	if Globals.Output == "json" || Globals.Output == "yaml" {
		payload := map[string]any{
			"log_name": logName, "log_id": logMeta.LogId, "segment_id": segmentID,
			"state": segMeta.State.String(), "quorum_id": quorum.Id,
			"es": quorum.Es, "wq": quorum.Wq, "aq": quorum.Aq,
			"nodes": positions,
		}
		if ok {
			payload["quorum_lac"] = lac
			payload["quorum_lac_source"] = source
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
		// Same outcome as text mode: a script reading the exit code must not take an absent
		// quorum_lac for a successful query.
		return unresolved
	}

	fmt.Fprintf(w, "Segment %d of log %s — state %s, quorum %d (es %d, wq %d, aq %d)\n\n",
		segmentID, logName, segMeta.State.String(), quorum.Id, quorum.Es, quorum.Wq, quorum.Aq)

	rows := make([][]string, 0, len(positions))
	for _, p := range positions {
		durable := "-"
		if p.answered() {
			durable = strconv.FormatInt(p.Durable, 10)
		}
		rows = append(rows, []string{p.Node, p.NodeID, durable, p.State})
	}
	if err := output.RenderRowTable(w, []string{"NODE", "NODE_ID", "DURABLE_ENTRY", "STATE"}, rows); err != nil {
		return err
	}

	if !ok {
		fmt.Fprintf(w, "\nquorum LAC: unknown — %d of %d nodes answered, fewer than aq=%d\n",
			answered, len(positions), quorum.Aq)
	} else if source == "quorum" {
		fmt.Fprintf(w, "\nquorum LAC: %d (aq=%d of %d nodes at or past it)\n", lac, quorum.Aq, len(positions))
	} else {
		fmt.Fprintf(w, "\nquorum LAC: %d (from %s; segment is %s, no live writer needed)\n",
			lac, source, segMeta.State.String())
	}
	warnIfPartial(cmd.ErrOrStderr(), unreachable, len(positions))
	return unresolved
}

// collectQuorumPositions asks each node the segment's quorum names for its own durable position.
// A node absent from the memberlist is not dialed: the quorum records service addresses, and the
// admin port is a separate thing only a memberlist entry can supply.
func collectQuorumPositions(ac *client.Client, members *client.Memberlist, quorum *proto.QuorumInfo,
	logID, segmentID int64,
) []nodePosition {
	positions := make([]nodePosition, 0, len(quorum.Nodes))
	for _, addr := range quorum.Nodes {
		p := nodePosition{Node: addr, Durable: -1, State: posUnreachable}
		member, found := memberByAddr(members, addr)
		if !found {
			p.State = posUnknownNode
			positions = append(positions, p)
			continue
		}
		p.NodeID = member.ID
		path := fmt.Sprintf("/admin/logstore/segments?log_id=%d", logID)
		body, err := fetchAdminJSON(ac.PeerAdminURL(member), path)
		if err != nil {
			p.State = posUnreachable
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
			p.State = posBadResponse
			positions = append(positions, p)
			continue
		}
		p.State = posNoWriter
		for _, s := range resp.Segments {
			if s.SegmentID == segmentID {
				p.Durable, p.State = s.LastEntry, posOK
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
		if p.answered() {
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
