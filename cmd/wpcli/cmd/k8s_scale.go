package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/internal/k8s"
)

func newK8sScaleCommand() *cobra.Command {
	var flags K8sFlags
	var replicas int
	cmd := &cobra.Command{
		Use:   "scale",
		Short: "Scale the cluster via kubectl patch",
		RunE: func(cmd *cobra.Command, args []string) error {
			if replicas <= 0 {
				return wperrors.NewUsageError("--replicas must be > 0")
			}
			cliK8s := resolveK8sConfig()
			executor := flags.ResolveExecutor(cliK8s)
			cluster := flags.ResolveCluster(cliK8s)
			scaleArgs := k8s.ScaleCommand(cluster, replicas)

			w := cmd.OutOrStdout()
			if !flags.Execute {
				executor.PrintCommands(w, [][]string{scaleArgs})
				return nil
			}
			if !executor.Available() {
				fmt.Fprintln(w, "Warning: kubectl not found, falling back to print mode")
				executor.PrintCommands(w, [][]string{scaleArgs})
				return nil
			}
			fmt.Fprintf(w, "Scaling %s to %d replicas...\n", cluster, replicas)
			code, err := executor.RunCommands(w, cmd.ErrOrStderr(), [][]string{scaleArgs})
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
	cmd.Flags().IntVar(&replicas, "replicas", 0, "Target replica count (required)")
	_ = cmd.MarkFlagRequired("replicas")
	return cmd
}
