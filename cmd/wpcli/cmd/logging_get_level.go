package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
)

func newLoggingGetLevelCommand() *cobra.Command {
	var allNodes bool
	cmd := &cobra.Command{
		Use:   "get-level [node]",
		Short: "Show current log level of a node (or all nodes)",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			res, err := resolveAndDiscover()
			if err != nil {
				return err
			}

			type row struct {
				Node  string `json:"node" yaml:"node"`
				Level string `json:"level" yaml:"level"`
			}

			if allNodes || len(args) == 0 {
				var rows []row
				for _, m := range res.Members.Members {
					peerURL := res.Client.PeerAdminURL(m)
					body, err := fetchAdminJSON(peerURL, "/log/level")
					if err != nil {
						rows = append(rows, row{Node: m.ID, Level: "(unreachable)"})
						continue
					}
					var resp map[string]string
					_ = json.Unmarshal(body, &resp)
					rows = append(rows, row{Node: m.ID, Level: resp["level"]})
				}

				if Globals.Output == "json" || Globals.Output == "yaml" {
					return output.RenderJSON(cmd.OutOrStdout(), rows)
				}
				headers := []string{"NODE", "LEVEL"}
				var tableRows [][]string
				for _, r := range rows {
					tableRows = append(tableRows, []string{r.Node, r.Level})
				}
				return output.RenderRowTable(cmd.OutOrStdout(), headers, tableRows)
			}

			// Single node
			target := args[0]
			member, ok := res.Members.Resolve(target)
			if !ok {
				return wperrors.NewTargetNotFoundError(target)
			}
			peerURL := res.Client.PeerAdminURL(member)
			body, err := fetchAdminJSON(peerURL, "/log/level")
			if err != nil {
				return err
			}
			var resp map[string]string
			if jsonErr := json.Unmarshal(body, &resp); jsonErr != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("invalid response: %v", jsonErr))
			}

			if Globals.Output == "json" || Globals.Output == "yaml" {
				return output.RenderJSON(cmd.OutOrStdout(), resp)
			}
			return output.RenderRowTable(cmd.OutOrStdout(), []string{"NODE", "LEVEL"}, [][]string{
				{member.ID, resp["level"]},
			})
		},
	}
	cmd.Flags().BoolVar(&allNodes, "all", false, "Show log level on all nodes")
	return cmd
}
