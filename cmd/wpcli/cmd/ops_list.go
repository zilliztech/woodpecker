package cmd

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
)

func newOpsListCommand() *cobra.Command {
	var opType string
	var logID, segID int64
	var longerThanMs int64
	var limit int
	cmd := &cobra.Command{
		Use:   "list <node>",
		Short: "List in-flight operations",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			res, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			target, ok := res.Members.Resolve(args[0])
			if !ok {
				return wperrors.NewTargetNotFoundError(args[0])
			}
			peerURL := res.Client.PeerAdminURL(target)

			params := url.Values{}
			if opType != "" {
				params.Set("type", opType)
			}
			if cmd.Flags().Changed("log") {
				params.Set("log_id", fmt.Sprintf("%d", logID))
			}
			if cmd.Flags().Changed("seg") {
				params.Set("segment_id", fmt.Sprintf("%d", segID))
			}
			if longerThanMs > 0 {
				params.Set("longer_than_ms", fmt.Sprintf("%d", longerThanMs))
			}
			if limit > 0 {
				params.Set("limit", fmt.Sprintf("%d", limit))
			}

			path := "/admin/runtime/ops"
			if len(params) > 0 {
				path += "?" + params.Encode()
			}
			body, err := fetchAdminJSON(peerURL, path)
			if err != nil {
				return err
			}

			if Globals.Output == "json" || Globals.Output == "yaml" {
				_, _ = cmd.OutOrStdout().Write(body)
				return nil
			}

			var ops []struct {
				OpID      string `json:"op_id"`
				OpType    string `json:"op_type"`
				TraceID   string `json:"trace_id"`
				StartedAt string `json:"started_at"`
				LogID     int64  `json:"log_id"`
				SegmentID int64  `json:"segment_id"`
			}
			if err := json.Unmarshal(body, &ops); err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("invalid response: %v", err))
			}

			headers := []string{"OP_TYPE", "OP_ID", "TRACE_ID", "LOG:SEG"}
			var rows [][]string
			for _, op := range ops {
				traceID := op.TraceID
				if len(traceID) > 12 {
					traceID = traceID[:12] + "..."
				}
				rows = append(rows, []string{
					op.OpType,
					op.OpID,
					traceID,
					fmt.Sprintf("%d:%d", op.LogID, op.SegmentID),
				})
			}
			return output.RenderRowTable(cmd.OutOrStdout(), headers, rows)
		},
	}
	cmd.Flags().StringVar(&opType, "type", "", "Filter by op type (e.g. file.flush)")
	cmd.Flags().Int64Var(&logID, "log", 0, "Filter by log ID")
	cmd.Flags().Int64Var(&segID, "seg", 0, "Filter by segment ID")
	cmd.Flags().Int64Var(&longerThanMs, "longer-than", 0, "Only ops running longer than N ms")
	cmd.Flags().IntVar(&limit, "limit", 0, "Max results")
	return cmd
}
