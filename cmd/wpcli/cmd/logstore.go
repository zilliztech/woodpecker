package cmd

import "github.com/spf13/cobra"

func newLogstoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logstore",
		Short: "Inspect and manage logstore segments",
	}
	cmd.AddCommand(
		newLogstoreSegmentsCommand(),
		newLogstoreSegmentShowCommand(),
		newLogstoreBufferCommand(),
		newLogstoreFlushQueueCommand(),
		newLogstoreForceFlushCommand(),
		newLogstoreFenceCommand(),
		newLogstoreCompactCommand(),
	)
	return cmd
}
