package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

// newLogstoreFenceQuorumCommand fences a segment across enough of its quorum to stop a write.
//
// An append completes once aq nodes acknowledge it, so fencing one node removes one
// acknowledgement and the write carries on through the rest. Interrupting it takes wq-aq+1 nodes:
// with the remaining aq-1 unable to form a quorum, the append fails and the writer is invalidated.
//
// The quorum lives in segment metadata, which the server never reads, so --etcd applies as it does
// to `logstore lac` and the marking family.
func newLogstoreFenceQuorumCommand() *cobra.Command {
	var flags metaEtcdFlags
	var reason string
	var nodes []string
	var yes bool
	cmd := &cobra.Command{
		Use:   "fence-quorum <logName> <segmentId>",
		Short: "Fence a segment across enough of its quorum to interrupt writes (high-risk, requires --reason)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			segmentID, parseErr := strconv.ParseInt(args[1], 10, 64)
			if parseErr != nil {
				return wperrors.NewUsageError(fmt.Sprintf("invalid segmentId %q", args[1]))
			}
			if reason == "" {
				return wperrors.NewUsageError("--reason is required for fence operations")
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
			return runFenceQuorum(cmd, cli, conn.kb, res.Client, res.Members, fenceQuorumRequest{
				logName:   args[0],
				segmentID: segmentID,
				reason:    reason,
				nodes:     nodes,
				confirmed: yes,
			})
		},
	}
	flags.register(cmd)
	cmd.Flags().StringVar(&reason, "reason", "", "Reason for fencing (required)")
	cmd.Flags().StringSliceVar(&nodes, "nodes", nil, "Fence only these quorum members (default: every node in the quorum)")
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "Skip confirmation")
	return cmd
}

// Per-node fence outcomes. posUnreachable, posUnknownNode and posBadResponse are shared with
// `logstore lac` so the same condition reads the same way in both commands.
const (
	fenceDone    = "fenced"
	fenceRefused = "refused"
)

// fenceQuorumRequest is what the operator asked for: which segment, why, and which of its nodes.
type fenceQuorumRequest struct {
	logName   string
	segmentID int64
	reason    string
	nodes     []string // empty means every node in the quorum
	confirmed bool
}

// nodeFence is one node's outcome, with the node's own words when it refused.
type nodeFence struct {
	Node   string `json:"node"`
	NodeID string `json:"node_id,omitempty"`
	State  string `json:"state"`
	Detail string `json:"detail,omitempty"`
}

func runFenceQuorum(cmd *cobra.Command, cli *clientv3.Client, kb *meta.KeyBuilder,
	ac *client.Client, members *client.Memberlist, req fenceQuorumRequest,
) error {
	ctx, cancel := metaCtx()
	defer cancel()

	logMeta := &proto.LogMeta{}
	if err := getProto(ctx, cli, kb.BuildLogKey(req.logName), logMeta); err != nil {
		return wperrors.NewTargetNotFoundError(fmt.Sprintf("log %s: %v", req.logName, err))
	}
	segMeta := &proto.SegmentMetadata{}
	segKey := kb.BuildSegmentInstanceKey(req.logName, strconv.FormatInt(req.segmentID, 10))
	if err := getProto(ctx, cli, segKey, segMeta); err != nil {
		return wperrors.NewTargetNotFoundError(fmt.Sprintf("segment %d of log %s: %v", req.segmentID, req.logName, err))
	}
	quorum := segMeta.GetQuorum()
	if quorum == nil {
		return wperrors.NewTargetNotFoundError(fmt.Sprintf(
			"segment %d of log %s carries no quorum", req.segmentID, req.logName,
		))
	}
	if quorum.Wq <= 0 || quorum.Aq <= 0 || quorum.Aq > quorum.Wq {
		return wperrors.NewStateConflictError(fmt.Sprintf(
			"segment %d of log %s records wq=%d aq=%d: how many nodes must be fenced cannot be derived from it",
			req.segmentID, req.logName, quorum.Wq, quorum.Aq,
		))
	}
	required := int(quorum.Wq - quorum.Aq + 1)

	targets, err := resolveFenceTargets(quorum, members, req.nodes)
	if err != nil {
		return err
	}

	w := cmd.OutOrStdout()
	// The plan is for a human. When stdout carries a payload it goes to stderr instead, so the
	// payload can be piped.
	plan := w
	if renderedOutput() {
		plan = cmd.ErrOrStderr()
	}
	fmt.Fprintf(plan, "Segment %d of log %s — state %s, quorum %d (es %d, wq %d, aq %d)\n",
		req.segmentID, req.logName, segMeta.State.String(), quorum.Id, quorum.Es, quorum.Wq, quorum.Aq)
	fmt.Fprintf(plan, "A write completes at aq=%d acknowledgements, so %d of %d nodes must be fenced to interrupt one.\n",
		quorum.Aq, required, quorum.Wq)
	fmt.Fprintf(plan, "About to fence %d node(s): %s. Reason: %s\n",
		len(targets), strings.Join(targets, ", "), req.reason)

	// Fencing part of a quorum is not a partial success: the fenced node fails the client's append,
	// which marks the segment rolling, while the writer still reaches aq and carries on. Both
	// numbers are known here, so a request that cannot reach the required count is refused before
	// any of it is sent.
	if len(targets) < required {
		return wperrors.NewUsageError(fmt.Sprintf(
			"%d node(s) named, but %d of %d are required to interrupt writes; "+
				"to fence a single node deliberately, use 'wp logstore fence <node>'",
			len(targets), required, quorum.Wq,
		))
	}

	if !req.confirmed {
		fmt.Fprintf(plan, "This is a destructive operation. Use -y to skip confirmation.\n")
		return wperrors.NewUserAbortError()
	}

	results := fenceEachNode(ac, members, targets, logMeta.LogId, req.segmentID, req.reason)
	fenced := 0
	for _, r := range results {
		if r.State == fenceDone {
			fenced++
		}
	}

	// Fencing fewer nodes than the ack quorum needs leaves aq nodes still acknowledging, so the
	// write continues. Nothing was broken, but nothing was interrupted either, and reporting it
	// as done would leave the operator believing the writer had been stopped.
	var shortfall error
	if fenced < required {
		shortfall = wperrors.NewStateConflictError(fmt.Sprintf(
			"fenced %d of the %d nodes required to interrupt writes: the writer can still reach aq=%d",
			fenced, required, quorum.Aq,
		))
	}

	if renderedOutput() {
		payload := map[string]any{
			"log_name": req.logName, "log_id": logMeta.LogId, "segment_id": req.segmentID,
			"state": segMeta.State.String(), "quorum_id": quorum.Id,
			"es": quorum.Es, "wq": quorum.Wq, "aq": quorum.Aq,
			"required": required, "fenced": fenced, "reason": req.reason,
			"nodes": results,
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
		return shortfall
	}

	fmt.Fprintln(w)
	rows := make([][]string, 0, len(results))
	for _, r := range results {
		rows = append(rows, []string{r.Node, r.NodeID, r.State, r.Detail})
	}
	if err := output.RenderRowTable(w, []string{"NODE", "NODE_ID", "RESULT", "DETAIL"}, rows); err != nil {
		return err
	}
	fmt.Fprintf(w, "\nfenced %d of %d targeted; %d required to interrupt writes\n",
		fenced, len(results), required)
	return shortfall
}

// resolveFenceTargets turns what the operator named into quorum entries. A name that is not one of
// the segment's quorum nodes is refused rather than dialed: fencing a node that holds none of the
// segment's data stops no write, and the name is more likely a mistake than an intent.
func resolveFenceTargets(quorum *proto.QuorumInfo, members *client.Memberlist, named []string) ([]string, error) {
	if len(named) == 0 {
		if len(quorum.Nodes) == 0 {
			return nil, wperrors.NewStateConflictError("the segment's quorum lists no nodes")
		}
		return dedupe(quorum.Nodes), nil
	}
	targets := make([]string, 0, len(named))
	for _, name := range named {
		member, known := memberByAddr(members, name)
		matched := ""
		for _, node := range quorum.Nodes {
			if node == name || (known && memberMatchesAddr(member, node)) {
				matched = node
				break
			}
		}
		if matched == "" {
			return nil, wperrors.NewUsageError(fmt.Sprintf(
				"%s is not a member of this segment's quorum (%s)", name, strings.Join(quorum.Nodes, ", "),
			))
		}
		targets = append(targets, matched)
	}
	// A node answers to its id, its service address and its gossip address, so two names can be one
	// node. Fencing is idempotent, so the repeat would answer with the same success and be counted
	// again: a quorum reported as interrupted on the strength of one fenced node.
	return dedupe(targets), nil
}

// dedupe keeps the first occurrence of each address, preserving order.
func dedupe(addrs []string) []string {
	seen := make(map[string]struct{}, len(addrs))
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if _, ok := seen[a]; ok {
			continue
		}
		seen[a] = struct{}{}
		out = append(out, a)
	}
	return out
}

// renderedOutput reports whether stdout carries a machine-readable payload rather than a report.
func renderedOutput() bool {
	return Globals.Output == "json" || Globals.Output == "yaml"
}

// fenceEachNode posts the fence to every target in turn. A node absent from the memberlist is contacted only with an explicit admin URL mapping.
func fenceEachNode(ac *client.Client, members *client.Memberlist, targets []string,
	logID, segmentID int64, reason string,
) []nodeFence {
	results := make([]nodeFence, 0, len(targets))
	for _, addr := range targets {
		r := nodeFence{Node: addr, State: posUnreachable}
		member, found := ac.QuorumMember(members, addr)
		if !found {
			r.State = posUnknownNode
			results = append(results, r)
			continue
		}
		r.NodeID = member.ID
		body, status, err := postAdminJSON(ac.PeerAdminURL(member), "/admin/logstore/fence", map[string]any{
			"log_id": logID, "segment_id": segmentID, "reason": reason,
		})
		switch {
		case err != nil:
			r.State, r.Detail = posUnreachable, err.Error()
		case status == http.StatusOK:
			r.State = fenceDone
		default:
			// A node with no live segment processor answers here. It is not fencing the segment,
			// so it does not count towards the required number.
			r.State, r.Detail = fenceRefused, adminErrorText(body, status)
		}
		results = append(results, r)
	}
	return results
}

// postAdminJSON posts a body to one peer's admin endpoint and returns the response whatever the
// status: an admin endpoint answers a refusal with the reason for it, and that reason is the answer.
func postAdminJSON(peerURL, path string, payload map[string]any) ([]byte, int, error) {
	body, marshalErr := json.Marshal(payload)
	if marshalErr != nil {
		return nil, 0, marshalErr
	}
	httpClient := &http.Client{Timeout: Globals.Timeout}
	resp, err := httpClient.Post(peerURL+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, 0, wperrors.NewNetworkError(fmt.Sprintf("POST %s: %v", path, err))
	}
	defer resp.Body.Close()
	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, resp.StatusCode, wperrors.NewNetworkError(fmt.Sprintf("read %s: %v", path, readErr))
	}
	return respBody, resp.StatusCode, nil
}

// adminErrorText prefers the endpoint's own error message and falls back to the status code.
func adminErrorText(body []byte, status int) string {
	var payload struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &payload); err == nil && payload.Error != "" {
		return payload.Error
	}
	text := strings.TrimSpace(string(body))
	if text == "" {
		return fmt.Sprintf("status %d", status)
	}
	return fmt.Sprintf("status %d: %s", status, text)
}
