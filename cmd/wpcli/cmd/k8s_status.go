package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/internal/k8s"
)

func newK8sStatusCommand() *cobra.Command {
	var flags K8sFlags
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show cluster status via kubectl",
		RunE: func(cmd *cobra.Command, args []string) error {
			cliK8s := resolveK8sConfig()
			executor := flags.ResolveExecutor(cliK8s)
			cluster := flags.ResolveCluster(cliK8s)
			commands := k8s.StatusCommands(cluster)

			w := cmd.OutOrStdout()
			if !flags.Execute {
				executor.PrintCommands(w, commands)
				return nil
			}
			if !executor.Available() {
				fmt.Fprintln(w, "Warning: kubectl not found, falling back to print mode")
				executor.PrintCommands(w, commands)
				return nil
			}
			code, err := executor.RunCommands(w, cmd.ErrOrStderr(), commands)
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
	return cmd
}

// resolveK8sConfig tries to load K8s config from cli.yaml.
// Returns zero value if cli.yaml is not available.
func resolveK8sConfig() config.K8sConfig {
	for _, p := range config.DefaultConfigPaths() {
		f, err := config.Load(p)
		if err != nil {
			continue
		}
		ctx, err := f.ResolveContext(Globals.Context)
		if err != nil {
			continue
		}
		return ctx.K8s
	}
	return config.K8sConfig{}
}
