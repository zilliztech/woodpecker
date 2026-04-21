package cmd

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
)

func newLogstoreSegmentsCommand() *cobra.Command {
	var logID int64
	var writable string
	cmd := &cobra.Command{
		Use:   "segments <node>",
		Short: "List active segments on a node",
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

			// Build query params
			params := url.Values{}
			if cmd.Flags().Changed("log") {
				params.Set("log_id", fmt.Sprintf("%d", logID))
			}
			if writable != "" {
				params.Set("writable", writable)
			}
			path := "/admin/logstore/segments"
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

			var resp struct {
				Segments []struct {
					LogID      int64  `json:"log_id"`
					SegmentID  int64  `json:"segment_id"`
					Backend    string `json:"backend"`
					Writable   bool   `json:"writable"`
					Fenced     bool   `json:"fenced"`
					Finalized  bool   `json:"finalized"`
					EntryCount int64  `json:"entry_count"`
					BlockCount int64  `json:"block_count"`
				} `json:"segments"`
			}
			if err := json.Unmarshal(body, &resp); err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("invalid response: %v", err))
			}

			headers := []string{"LOG_ID", "SEG_ID", "BACKEND", "WRITABLE", "FENCED", "FINALIZED", "ENTRIES", "BLOCKS"}
			var rows [][]string
			for _, s := range resp.Segments {
				rows = append(rows, []string{
					fmt.Sprintf("%d", s.LogID),
					fmt.Sprintf("%d", s.SegmentID),
					s.Backend,
					fmt.Sprintf("%v", s.Writable),
					fmt.Sprintf("%v", s.Fenced),
					fmt.Sprintf("%v", s.Finalized),
					fmt.Sprintf("%d", s.EntryCount),
					fmt.Sprintf("%d", s.BlockCount),
				})
			}
			return output.RenderRowTable(cmd.OutOrStdout(), headers, rows)
		},
	}
	cmd.Flags().Int64Var(&logID, "log", 0, "Filter by log ID")
	cmd.Flags().StringVar(&writable, "writable", "", "Filter by writable state (true/false)")
	return cmd
}

func newLogstoreSegmentShowCommand() *cobra.Command {
	var logID, segID int64
	cmd := &cobra.Command{
		Use:   "segment-show <node>",
		Short: "Show detailed state of a single segment",
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
			path := fmt.Sprintf("/admin/logstore/segments/detail?log_id=%d&segment_id=%d", logID, segID)
			body, err := fetchAdminJSON(peerURL, path)
			if err != nil {
				return err
			}
			if Globals.Output == "json" || Globals.Output == "yaml" {
				_, _ = cmd.OutOrStdout().Write(body)
				return nil
			}
			// Render as indented JSON for readability
			var pretty json.RawMessage
			if err := json.Unmarshal(body, &pretty); err == nil {
				formatted, _ := json.MarshalIndent(pretty, "", "  ")
				_, _ = cmd.OutOrStdout().Write(formatted)
				_, _ = fmt.Fprintln(cmd.OutOrStdout())
			}
			return nil
		},
	}
	cmd.Flags().Int64Var(&logID, "log", 0, "Log ID (required)")
	cmd.Flags().Int64Var(&segID, "seg", 0, "Segment ID (required)")
	_ = cmd.MarkFlagRequired("log")
	_ = cmd.MarkFlagRequired("seg")
	return cmd
}

func newLogstoreBufferCommand() *cobra.Command {
	var topN int
	cmd := &cobra.Command{
		Use:   "buffer <node>",
		Short: "Show buffer bytes summary, sorted by size",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return logstoreAggCommand(cmd, args[0], "buffer_bytes", topN, []string{"LOG:SEG", "BUFFER_BYTES", "BUFFER_ENTRIES"}, func(s map[string]any) []string {
				return []string{
					fmt.Sprintf("%v:%v", s["log_id"], s["segment_id"]),
					fmt.Sprintf("%v", s["buffer_bytes"]),
					fmt.Sprintf("%v", s["buffer_entries"]),
				}
			})
		},
	}
	cmd.Flags().IntVar(&topN, "top", 20, "Show top N segments")
	return cmd
}

func newLogstoreFlushQueueCommand() *cobra.Command {
	var topN int
	cmd := &cobra.Command{
		Use:   "flush-queue <node>",
		Short: "Show flush queue depth summary, sorted by depth",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return logstoreAggCommand(cmd, args[0], "flush_queue_depth", topN, []string{"LOG:SEG", "QUEUE_DEPTH", "QUEUE_CAP"}, func(s map[string]any) []string {
				return []string{
					fmt.Sprintf("%v:%v", s["log_id"], s["segment_id"]),
					fmt.Sprintf("%v", s["flush_queue_depth"]),
					fmt.Sprintf("%v", s["flush_queue_capacity"]),
				}
			})
		},
	}
	cmd.Flags().IntVar(&topN, "top", 20, "Show top N segments")
	return cmd
}

// logstoreAggCommand is a shared helper for buffer and flush-queue commands that
// fetch segments, sort by a numeric field, and render a table.
func logstoreAggCommand(cmd *cobra.Command, node, sortField string, topN int, headers []string, rowFn func(map[string]any) []string) error {
	res, err := resolveAndDiscover()
	if err != nil {
		return err
	}
	target, ok := res.Members.Resolve(node)
	if !ok {
		return wperrors.NewTargetNotFoundError(node)
	}
	peerURL := res.Client.PeerAdminURL(target)
	body, err := fetchAdminJSON(peerURL, "/admin/logstore/segments")
	if err != nil {
		return err
	}
	var resp struct {
		Segments []map[string]any `json:"segments"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return wperrors.NewNetworkError(fmt.Sprintf("invalid response: %v", err))
	}

	// Sort by sortField descending (simple bubble sort for small N)
	for i := 0; i < len(resp.Segments); i++ {
		for j := i + 1; j < len(resp.Segments); j++ {
			vi, _ := resp.Segments[i][sortField].(float64)
			vj, _ := resp.Segments[j][sortField].(float64)
			if vj > vi {
				resp.Segments[i], resp.Segments[j] = resp.Segments[j], resp.Segments[i]
			}
		}
	}
	if topN > 0 && len(resp.Segments) > topN {
		resp.Segments = resp.Segments[:topN]
	}

	var rows [][]string
	for _, s := range resp.Segments {
		rows = append(rows, rowFn(s))
	}
	return output.RenderRowTable(cmd.OutOrStdout(), headers, rows)
}
