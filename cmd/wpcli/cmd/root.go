// Package cmd holds the cobra commands for the wp CLI.
package cmd

import (
	"time"

	"github.com/spf13/cobra"
)

// NewRootCommand creates the root cobra command for wp with all global flags.
func NewRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:          "wp",
		Short:        "Woodpecker operational CLI",
		Long:         "wp is the Woodpecker operational CLI for service-mode clusters.",
		SilenceUsage: true,
		// RunE prints help when wp is invoked with no sub-command so that
		// the global flags section is visible in the output.
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	pf := root.PersistentFlags()
	pf.StringVar(&Globals.Context, "context", "", "CLI context name (overrides current-context in cli.yaml)")
	pf.StringVar(&Globals.Endpoint, "endpoint", "", "Admin HTTP seed endpoint (e.g. http://node:9091)")
	pf.IntVar(&Globals.AdminPort, "admin-port", 9091, "Admin port for fan-out peer discovery")
	pf.DurationVar(&Globals.Timeout, "timeout", 30*time.Second, "Per-request timeout")
	pf.IntVar(&Globals.Concurrency, "concurrency", 8, "Fan-out concurrency")
	pf.BoolVar(&Globals.Strict, "strict", false, "Treat partial fan-out failures as errors")
	pf.StringVarP(&Globals.Output, "output", "o", "table", "Output format: table|wide|json|yaml")
	pf.BoolVar(&Globals.NoColor, "no-color", false, "Disable color in output")
	pf.CountVarP(&Globals.Verbose, "verbose", "v", "Increase verbosity (-v, -vv, -vvv)")

	return root
}
