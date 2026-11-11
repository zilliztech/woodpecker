package cmd

import "github.com/spf13/cobra"

func newEnvCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "env",
		Short: "Inspect process environment, Go runtime, host, and build info",
	}
	c.AddCommand(newEnvShowCommand(), newEnvDiffCommand())
	return c
}
