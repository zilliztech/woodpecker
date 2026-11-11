package cmd

import "github.com/spf13/cobra"

func newConfigCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "config",
		Short: "Inspect and compare server configurations",
	}
	c.AddCommand(newConfigShowCommand(), newConfigDiffCommand())
	return c
}
