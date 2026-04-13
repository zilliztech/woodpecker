package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/internal/prom"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
)

func newMetricsListCommand() *cobra.Command {
	var filter string
	cmd := &cobra.Command{
		Use:   "list <node>",
		Short: "List all metric series exposed by a node",
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
			families, err := prom.ScrapeNode(peerURL, Globals.Timeout)
			if err != nil {
				return wperrors.NewNetworkError(err.Error())
			}

			list := prom.ListMetrics(families)
			if filter != "" {
				var filtered []prom.MetricInfo
				for _, m := range list {
					if containsSubstring(m.Name, filter) {
						filtered = append(filtered, m)
					}
				}
				list = filtered
			}

			if Globals.Output == "json" || Globals.Output == "yaml" {
				return output.RenderJSON(cmd.OutOrStdout(), list)
			}

			headers := []string{"NAME", "TYPE", "SERIES", "HELP"}
			var rows [][]string
			for _, m := range list {
				help := m.Help
				if len(help) > 60 {
					help = help[:57] + "..."
				}
				rows = append(rows, []string{m.Name, m.Type, fmt.Sprintf("%d", m.SeriesCount), help})
			}
			return output.RenderRowTable(cmd.OutOrStdout(), headers, rows)
		},
	}
	cmd.Flags().StringVar(&filter, "filter", "", "Substring filter on metric name")
	return cmd
}

func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (substr == "" || findSubstring(s, substr))
}

func findSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
