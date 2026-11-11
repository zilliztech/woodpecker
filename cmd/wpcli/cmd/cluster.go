package cmd

import "github.com/spf13/cobra"

func newClusterCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "cluster",
		Short: "Cluster-level overview and health checks",
	}
	c.AddCommand(
		newClusterInfoCommand(),
		newClusterHealthCommand(),
		newClusterGossipDiffCommand(),
	)
	return c
}
