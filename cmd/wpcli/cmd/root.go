// Package cmd holds the cobra commands for the wp CLI.
package cmd

import "github.com/spf13/cobra"

// NewRootCommand creates the root cobra command for wp.
// Full flag wiring and sub-commands are added in later tasks.
func NewRootCommand() *cobra.Command {
	return &cobra.Command{
		Use:          "wp",
		Short:        "Woodpecker operational CLI",
		SilenceUsage: true,
	}
}
