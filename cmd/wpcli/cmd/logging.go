package cmd

import "github.com/spf13/cobra"

func newLoggingCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logging",
		Short: "Inspect and change server log levels at runtime",
	}
	cmd.AddCommand(
		newLoggingGetLevelCommand(),
		newLoggingSetLevelCommand(),
	)
	return cmd
}
