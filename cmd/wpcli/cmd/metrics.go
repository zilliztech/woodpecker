package cmd

import "github.com/spf13/cobra"

func newMetricsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "metrics",
		Short: "Scrape and analyze Prometheus metrics from nodes",
	}
	cmd.AddCommand(
		newMetricsListCommand(),
		newMetricsSnapshotCommand(),
		newMetricsTopCommand(),
		newMetricsWatchCommand(),
		newMetricsReportCommand(),
	)
	return cmd
}
