package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/internal/prom"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
)

func newMetricsSnapshotCommand() *cobra.Command {
	var metricName string
	var allNodes bool
	cmd := &cobra.Command{
		Use:   "snapshot [node]",
		Short: "Point-in-time metric snapshot from one or all nodes",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			res, err := resolveAndDiscover()
			if err != nil {
				return err
			}

			type target struct {
				nodeID  string
				peerURL string
			}
			var targets []target
			if allNodes || len(args) == 0 {
				for _, m := range res.Members.Members {
					targets = append(targets, target{nodeID: m.ID, peerURL: res.Client.PeerAdminURL(m)})
				}
			} else {
				member, ok := res.Members.Resolve(args[0])
				if !ok {
					return wperrors.NewTargetNotFoundError(args[0])
				}
				targets = append(targets, target{nodeID: member.ID, peerURL: res.Client.PeerAdminURL(member)})
			}

			type row struct {
				Node   string  `json:"node"`
				Metric string  `json:"metric"`
				Labels string  `json:"labels"`
				Value  float64 `json:"value"`
			}
			var rows []row
			for _, t := range targets {
				families, err := prom.ScrapeNode(t.peerURL, Globals.Timeout)
				if err != nil {
					continue
				}
				for name, mf := range families {
					if metricName != "" && name != metricName {
						continue
					}
					for _, m := range mf.GetMetric() {
						rows = append(rows, row{
							Node:   t.nodeID,
							Metric: name,
							Labels: prom.FormatLabels(m),
							Value:  prom.MetricValue(m),
						})
					}
				}
			}

			if Globals.Output == "json" || Globals.Output == "yaml" {
				return output.RenderJSON(cmd.OutOrStdout(), rows)
			}

			headers := []string{"NODE", "METRIC", "LABELS", "VALUE"}
			var tableRows [][]string
			for _, r := range rows {
				labels := r.Labels
				if len(labels) > 50 {
					labels = labels[:47] + "..."
				}
				tableRows = append(tableRows, []string{r.Node, r.Metric, labels, fmt.Sprintf("%.4g", r.Value)})
			}
			return output.RenderRowTable(cmd.OutOrStdout(), headers, tableRows)
		},
	}
	cmd.Flags().StringVar(&metricName, "metric", "", "Filter to specific metric name")
	cmd.Flags().BoolVar(&allNodes, "all", false, "Snapshot from all nodes")
	return cmd
}
