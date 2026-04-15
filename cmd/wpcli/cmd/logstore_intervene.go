package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newLogstoreForceFlushCommand() *cobra.Command {
	var logID, segID int64
	cmd := &cobra.Command{
		Use:   "force-flush <node>",
		Short: "Force sync on a specific segment or all segments",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return logstorePost(cmd, args[0], "/admin/logstore/flush", map[string]any{
				"log_id": logID, "segment_id": segID,
			}, "flush")
		},
	}
	cmd.Flags().Int64Var(&logID, "log", 0, "Log ID (0 = all)")
	cmd.Flags().Int64Var(&segID, "seg", 0, "Segment ID (0 = all)")
	return cmd
}

func newLogstoreFenceCommand() *cobra.Command {
	var logID, segID int64
	var reason string
	var yes bool
	cmd := &cobra.Command{
		Use:   "fence <node>",
		Short: "Force fence on a segment (high-risk, requires --reason)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if reason == "" {
				return wperrors.NewUsageError("--reason is required for fence operations")
			}
			if !yes {
				fmt.Fprintf(cmd.OutOrStdout(), "About to fence log %d segment %d on %s. Reason: %s\n", logID, segID, args[0], reason)
				fmt.Fprintf(cmd.OutOrStdout(), "This is a destructive operation. Use -y to skip confirmation.\n")
				return wperrors.NewUserAbortError()
			}
			return logstorePost(cmd, args[0], "/admin/logstore/fence", map[string]any{
				"log_id": logID, "segment_id": segID, "reason": reason,
			}, "fence")
		},
	}
	cmd.Flags().Int64Var(&logID, "log", 0, "Log ID (required)")
	cmd.Flags().Int64Var(&segID, "seg", 0, "Segment ID (required)")
	cmd.Flags().StringVar(&reason, "reason", "", "Reason for fencing (required)")
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "Skip confirmation")
	_ = cmd.MarkFlagRequired("log")
	_ = cmd.MarkFlagRequired("seg")
	return cmd
}

func newLogstoreCompactCommand() *cobra.Command {
	var logID, segID int64
	cmd := &cobra.Command{
		Use:   "compact <node>",
		Short: "Force compaction on a segment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return logstorePost(cmd, args[0], "/admin/logstore/compact", map[string]any{
				"log_id": logID, "segment_id": segID,
			}, "compact")
		},
	}
	cmd.Flags().Int64Var(&logID, "log", 0, "Log ID (required)")
	cmd.Flags().Int64Var(&segID, "seg", 0, "Segment ID (required)")
	_ = cmd.MarkFlagRequired("log")
	_ = cmd.MarkFlagRequired("seg")
	return cmd
}

// logstorePost is a shared helper for POST operations (flush, fence, compact).
func logstorePost(cmd *cobra.Command, node, path string, payload map[string]any, opName string) error {
	res, err := resolveAndDiscover()
	if err != nil {
		return err
	}
	target, ok := res.Members.Resolve(node)
	if !ok {
		return wperrors.NewTargetNotFoundError(node)
	}
	peerURL := res.Client.PeerAdminURL(target)

	body, _ := json.Marshal(payload)
	client := &http.Client{Timeout: Globals.Timeout}
	resp, err := client.Post(peerURL+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return wperrors.NewNetworkError(fmt.Sprintf("POST %s: %v", path, err))
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusConflict {
		return wperrors.NewStateConflictError(string(respBody))
	}
	if resp.StatusCode != http.StatusOK {
		return wperrors.NewNetworkError(fmt.Sprintf("%s returned %d: %s", path, resp.StatusCode, string(respBody)))
	}

	fmt.Fprintf(cmd.OutOrStdout(), "%s completed on %s\n", opName, target.ID)
	return nil
}
