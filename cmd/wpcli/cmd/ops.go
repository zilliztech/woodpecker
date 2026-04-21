package cmd

import "github.com/spf13/cobra"

func newOpsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ops",
		Short: "Inspect in-flight operations in the op registry",
	}
	cmd.AddCommand(
		newOpsListCommand(),
		newOpsShowCommand(),
		newOpsStatsCommand(),
	)
	return cmd
}
