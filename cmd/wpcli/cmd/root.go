// Package cmd holds the cobra commands for the wp CLI.
package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	diagcommand "github.com/zilliztech/woodpecker/diagnostics/command"
)

// NewRootCommand creates the root cobra command for wp with all global flags.
func NewRootCommand() *cobra.Command {
	Globals.changed = nil
	root := &cobra.Command{
		Use:          "wp",
		Short:        "Woodpecker operational CLI",
		Long:         "wp is the Woodpecker operational CLI for service-mode clusters.",
		SilenceUsage: true,
		// Capture the command's error stream so resolveAndDiscover can name the cluster this
		// run is pointed at. It goes to stderr rather than stdout so piping a command into a
		// parser is unaffected.
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			sourceWriter = cmd.ErrOrStderr()
			Globals.changed = make(map[string]bool)
			cmd.Flags().Visit(func(flag *pflag.Flag) { Globals.changed[flag.Name] = true })
			return applySessionCommandDefaults(cmd)
		},
		PersistentPostRun: rememberCommandInputs,
		// RunE prints help when wp is invoked with no sub-command so that
		// the global flags section is visible in the output.
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	d := shellRootDefaults()
	pf := root.PersistentFlags()
	pf.StringArrayVar(&Globals.NodeAdminURLs, "node-admin-url", nil, "Map a node ID, advertised address, or host to an admin URL (KEY=URL; repeatable; overrides context mappings)")
	pf.StringVar(&Globals.Context, "context", "", "CLI context name (overrides current-context in cli.yaml)")
	pf.StringVar(&Globals.Endpoint, "endpoint", "", "Admin HTTP seed endpoint (e.g. http://node:9091)")
	pf.IntVar(&Globals.AdminPort, "admin-port", d.AdminPort, "Admin port for fan-out peer discovery")
	pf.DurationVar(&Globals.Timeout, "timeout", d.Timeout, "Per-request timeout")
	pf.IntVar(&Globals.Concurrency, "concurrency", d.Concurrency, "Fan-out concurrency")
	pf.BoolVar(&Globals.Strict, "strict", d.Strict, "Treat partial fan-out failures as errors")
	pf.StringVarP(&Globals.Output, "output", "o", d.Output, "Output format: table|wide|json|yaml")
	pf.BoolVar(&Globals.NoColor, "no-color", d.NoColor, "Disable color in output")
	pf.CountVarP(&Globals.Verbose, "verbose", "v", "Increase verbosity (-v, -vv, -vvv)")

	root.AddCommand(newVersionCommand())
	root.AddCommand(newCtxCommand())
	root.AddCommand(newNodeCommand())
	root.AddCommand(newClusterCommand())
	root.AddCommand(newConfigCommand())
	root.AddCommand(newEnvCommand())
	root.AddCommand(newProfileCommand())
	root.AddCommand(newLoggingCommand())
	root.AddCommand(newLogCommand())
	root.AddCommand(newMetaCommand())
	root.AddCommand(newEtcdCommand())
	root.AddCommand(newLogstoreCommand())
	root.AddCommand(newSegmentCommand())
	root.AddCommand(newInstanceCommand())
	root.AddCommand(newMarkingCommand())
	root.AddCommand(newOpsCommand())
	root.AddCommand(newMetricsCommand())
	root.AddCommand(newDiagnoseCommand())
	root.AddCommand(diagcommand.NewCommand(diagcommand.Options{Context: func() string { return Globals.Context }, Output: func() string { return Globals.Output }}))
	root.AddCommand(newK8sCommand())
	root.AddCommand(newShellCommand())
	root.AddCommand(newWebUICommand())
	root.AddCommand(diagcommand.NewReportCommand(diagcommand.Options{Context: func() string { return Globals.Context }, Output: func() string { return Globals.Output }}))

	return root
}
