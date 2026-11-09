package cmd

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpdiag/webui"
)

func newWebUICommand() *cobra.Command {
	var path, address, metricsRules, traceRules string
	c := &cobra.Command{Use: "webui", Short: "Open a local operations and diagnosis workspace using cli.yaml", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		// Resolve startup flags once. All subsequent HTTP requests carry their own
		// session/context and never use the process-wide Cobra Globals singleton.
		name := Globals.Context
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return webui.ServeWorkspace(ctx, address, path, name, cmd.OutOrStdout(), map[string]string{"metrics": metricsRules, "trace": traceRules})
	}}
	c.Flags().StringVar(&path, "config", "", "Shared cli.yaml (default: normal wp configuration discovery)")
	c.Flags().StringVar(&metricsRules, "metrics-rules", "", "Metrics rules file beside cli.yaml by default")
	c.Flags().StringVar(&traceRules, "trace-rules", "", "Trace rules file beside cli.yaml by default")
	c.Flags().StringVar(&address, "listen", "127.0.0.1:8794", "Loopback host:port")
	return c
}
