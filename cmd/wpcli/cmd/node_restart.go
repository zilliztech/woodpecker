package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newNodeRestartCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "restart <node>",
		Short: "(Not implemented) Restart a node — use your orchestrator instead",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintln(cmd.OutOrStdout(), `wp node restart is intentionally not implemented by this CLI.

On Kubernetes (managed by Woodpecker Operator):
  kubectl rollout restart sts/<cluster-name>-server

On bare metal / systemd:
  ssh to the node and run: sudo systemctl restart woodpecker

Why: the wp CLI does not assume a specific supervisor for process restart.
Restart responsibility belongs to the orchestrator.`)
			return wperrors.NewNotImplementedError("wp node restart")
		},
	}
}
