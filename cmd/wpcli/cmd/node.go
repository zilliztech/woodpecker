package cmd

import "github.com/spf13/cobra"

func newNodeCommand() *cobra.Command {
	n := &cobra.Command{
		Use:   "node",
		Short: "Manage and inspect server nodes",
	}
	n.AddCommand(
		newNodeListCommand(),
		newNodeShowCommand(),
		newNodeDecommissionCommand(),
		newNodeDrainStatusCommand(),
		newNodeCancelDecommissionCommand(),
	)
	return n
}
