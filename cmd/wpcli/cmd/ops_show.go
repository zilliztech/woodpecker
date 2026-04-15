package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
)

func newOpsShowCommand() *cobra.Command {
	var opID string
	cmd := &cobra.Command{
		Use:   "show <node>",
		Short: "Show detail of a single in-flight operation",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opID == "" {
				return wperrors.NewUsageError("--op-id is required")
			}
			res, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			target, ok := res.Members.Resolve(args[0])
			if !ok {
				return wperrors.NewTargetNotFoundError(args[0])
			}
			peerURL := res.Client.PeerAdminURL(target)
			path := fmt.Sprintf("/admin/runtime/ops/get?op_id=%s", opID)
			body, err := fetchAdminJSON(peerURL, path)
			if err != nil {
				return err
			}

			// Check for "not found" response
			var checkErr struct {
				Error string `json:"error"`
			}
			if json.Unmarshal(body, &checkErr) == nil && checkErr.Error != "" {
				return wperrors.NewResourceNotFoundError(opID)
			}

			if Globals.Output == "json" || Globals.Output == "yaml" {
				_, _ = cmd.OutOrStdout().Write(body)
				return nil
			}

			var op struct {
				OpID      string `json:"op_id"`
				OpType    string `json:"op_type"`
				TraceID   string `json:"trace_id"`
				SpanID    string `json:"span_id"`
				StartedAt string `json:"started_at"`
				LogID     int64  `json:"log_id"`
				SegmentID int64  `json:"segment_id"`
			}
			if err := json.Unmarshal(body, &op); err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("invalid response: %v", err))
			}

			w := cmd.OutOrStdout()
			sections := []output.Section{
				{Title: "Identity", Pairs: [][2]string{
					{"op_type", op.OpType},
					{"op_id", op.OpID},
					{"trace_id", op.TraceID},
					{"span_id", op.SpanID},
				}},
				{Title: "Context", Pairs: [][2]string{
					{"log_id", fmt.Sprintf("%d", op.LogID)},
					{"segment_id", fmt.Sprintf("%d", op.SegmentID)},
				}},
			}
			if op.TraceID != "" {
				sections = append(sections, output.Section{
					Title: "Trace",
					Pairs: [][2]string{
						{"", "Use this trace_id with your trace backend:"},
						{"", "  " + op.TraceID},
					},
				})
			}
			return output.RenderSectionedTable(w, sections)
		},
	}
	cmd.Flags().StringVar(&opID, "op-id", "", "Operation ID (required)")
	return cmd
}
