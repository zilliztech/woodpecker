package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
)

func newLoggingSetLevelCommand() *cobra.Command {
	var level string
	var allNodes bool
	cmd := &cobra.Command{
		Use:   "set-level [node]",
		Short: "Change log level of a node (or all nodes)",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if level == "" {
				return wperrors.NewUsageError("--level is required")
			}

			res, err := resolveAndDiscover()
			if err != nil {
				return err
			}

			setLevel := func(peerURL, nodeID string) (string, error) {
				body, _ := json.Marshal(map[string]string{"level": level})
				client := &http.Client{Timeout: Globals.Timeout}
				resp, err := client.Post(peerURL+"/log/level", "application/json", bytes.NewReader(body))
				if err != nil {
					return "", wperrors.NewNetworkError(fmt.Sprintf("failed to reach %s: %v", nodeID, err))
				}
				defer resp.Body.Close()
				respBody, _ := io.ReadAll(resp.Body)
				if resp.StatusCode != http.StatusOK {
					return "", wperrors.NewNetworkError(fmt.Sprintf("%s: %s", nodeID, string(respBody)))
				}
				var result map[string]string
				_ = json.Unmarshal(respBody, &result)
				return result["level"], nil
			}

			if allNodes || len(args) == 0 {
				headers := []string{"NODE", "LEVEL", "STATUS"}
				var rows [][]string
				for _, m := range res.Members.Members {
					peerURL := res.Client.PeerAdminURL(m)
					newLevel, err := setLevel(peerURL, m.ID)
					if err != nil {
						rows = append(rows, []string{m.ID, "", fmt.Sprintf("error: %v", err)})
					} else {
						rows = append(rows, []string{m.ID, newLevel, "ok"})
					}
				}
				return output.RenderRowTable(cmd.OutOrStdout(), headers, rows)
			}

			target := args[0]
			member, ok := res.Members.Resolve(target)
			if !ok {
				return wperrors.NewTargetNotFoundError(target)
			}
			peerURL := res.Client.PeerAdminURL(member)
			newLevel, err := setLevel(peerURL, member.ID)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Log level set to %q on %s\n", newLevel, member.ID)
			return nil
		},
	}
	cmd.Flags().StringVar(&level, "level", "", "Target log level (debug, info, warn, error)")
	cmd.Flags().BoolVar(&allNodes, "all", false, "Set log level on all nodes")
	return cmd
}
