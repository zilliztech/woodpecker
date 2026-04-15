package cmd

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/internal/prom"
)

func newMetricsWatchCommand() *cobra.Command {
	var interval time.Duration
	cmd := &cobra.Command{
		Use:   "watch <metric-name> <node>",
		Short: "Real-time stream of a metric with trend arrows",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			metricName := args[0]
			nodeTarget := args[1]

			res, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			member, ok := res.Members.Resolve(nodeTarget)
			if !ok {
				return wperrors.NewTargetNotFoundError(nodeTarget)
			}
			peerURL := res.Client.PeerAdminURL(member)

			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "Watching %s on %s (interval %s, Ctrl+C to stop)\n", metricName, member.ID, interval)
			fmt.Fprintf(w, "%-24s  %12s  %12s  %s\n", "TIMESTAMP", "VALUE", "DELTA", "TREND")

			var prevValue *float64
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			scrapeAndPrint := func() {
				families, err := prom.ScrapeNode(peerURL, Globals.Timeout)
				if err != nil {
					fmt.Fprintf(w, "%-24s  %12s\n", time.Now().Format("15:04:05.000"), "(error)")
					return
				}
				mf, ok := families[metricName]
				if !ok {
					fmt.Fprintf(w, "%-24s  %12s\n", time.Now().Format("15:04:05.000"), "(not found)")
					return
				}
				// Sum all series for aggregate value
				var total float64
				for _, m := range mf.GetMetric() {
					total += prom.MetricValue(m)
				}

				deltaStr := ""
				trendStr := ""
				if prevValue != nil {
					delta := total - *prevValue
					deltaStr = fmt.Sprintf("%+.4g", delta)
					if delta > 0 {
						trendStr = "^"
					} else if delta < 0 {
						trendStr = "v"
					} else {
						trendStr = "="
					}
				}
				prevValue = &total
				fmt.Fprintf(w, "%-24s  %12.4g  %12s  %s\n", time.Now().Format("15:04:05.000"), total, deltaStr, trendStr)
			}

			// First scrape immediately
			scrapeAndPrint()

			for {
				select {
				case <-ctx.Done():
					return nil
				case <-ticker.C:
					scrapeAndPrint()
				}
			}
		},
	}
	cmd.Flags().DurationVar(&interval, "interval", 1*time.Second, "Scrape interval")
	return cmd
}
