package cmd

import (
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/internal/prom"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
)

func newMetricsTopCommand() *cobra.Command {
	var byMetric string
	var topN int
	cmd := &cobra.Command{
		Use:   "top",
		Short: "Cross-node top-N comparison of a specific metric",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if byMetric == "" {
				return wperrors.NewUsageError("--by is required")
			}
			res, err := resolveAndDiscover()
			if err != nil {
				return err
			}

			type nodeVal struct {
				NodeID string  `json:"node"`
				Value  float64 `json:"value"`
				Labels string  `json:"labels"`
			}
			var results []nodeVal
			for _, m := range res.Members.Members {
				peerURL := res.Client.PeerAdminURL(m)
				families, err := prom.ScrapeNode(peerURL, Globals.Timeout)
				if err != nil {
					continue
				}
				mf, ok := families[byMetric]
				if !ok {
					continue
				}
				// Sum all series for this metric on this node
				var total float64
				var labels string
				for _, metric := range mf.GetMetric() {
					v := prom.MetricValue(metric)
					total += v
					if labels == "" {
						labels = prom.FormatLabels(metric)
					}
				}
				results = append(results, nodeVal{NodeID: m.ID, Value: total, Labels: labels})
			}

			sort.Slice(results, func(i, j int) bool {
				return results[i].Value > results[j].Value
			})
			if topN > 0 && len(results) > topN {
				results = results[:topN]
			}

			if Globals.Output == "json" || Globals.Output == "yaml" {
				return output.RenderJSON(cmd.OutOrStdout(), results)
			}

			headers := []string{"RANK", "NODE", "VALUE"}
			var rows [][]string
			for i, r := range results {
				rows = append(rows, []string{
					fmt.Sprintf("%d", i+1),
					r.NodeID,
					fmt.Sprintf("%.4g", r.Value),
				})
			}
			return output.RenderRowTable(cmd.OutOrStdout(), headers, rows)
		},
	}
	cmd.Flags().StringVar(&byMetric, "by", "", "Metric name to rank by (required)")
	cmd.Flags().IntVar(&topN, "top", 10, "Show top N nodes")
	return cmd
}
