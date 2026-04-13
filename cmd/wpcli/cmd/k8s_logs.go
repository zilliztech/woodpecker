package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/internal/k8s"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newK8sLogsCommand() *cobra.Command {
	var flags K8sFlags
	var follow bool
	var tail int
	var since string
	cmd := &cobra.Command{
		Use:   "logs <node-or-pod>",
		Short: "Tail logs from a Woodpecker pod",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cliK8s := resolveK8sConfig()
			executor := flags.ResolveExecutor(cliK8s)
			cluster := flags.ResolveCluster(cliK8s)
			logsArgs := k8s.LogsCommand(cluster, args[0], follow, tail, since)

			w := cmd.OutOrStdout()
			if !flags.Execute {
				executor.PrintCommands(w, [][]string{logsArgs})
				return nil
			}
			if !executor.Available() {
				fmt.Fprintln(w, "Warning: kubectl not found, falling back to print mode")
				executor.PrintCommands(w, [][]string{logsArgs})
				return nil
			}
			code, err := executor.RunCommands(w, cmd.ErrOrStderr(), [][]string{logsArgs})
			if err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("kubectl failed: %v", err))
			}
			if code != 0 {
				return wperrors.NewKubectlPassthroughError(code)
			}
			return nil
		},
	}
	AddK8sFlags(cmd, &flags)
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Stream logs")
	cmd.Flags().IntVar(&tail, "tail", 100, "Number of recent lines to show")
	cmd.Flags().StringVar(&since, "since", "", "Only show logs newer than duration (e.g. 1h)")
	return cmd
}
