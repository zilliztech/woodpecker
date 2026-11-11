package cmd

import (
	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	"github.com/zilliztech/woodpecker/cmd/wpcli/internal/k8s"
)

// K8sFlags holds the shared flags for F family commands.
type K8sFlags struct {
	Execute     bool
	Kubectl     string
	Namespace   string
	WpCluster   string
	KubeContext string
	Kubeconfig  string
}

// AddK8sFlags registers the shared K8s flags on the given command.
func AddK8sFlags(cmd *cobra.Command, flags *K8sFlags) {
	cmd.Flags().BoolVarP(&flags.Execute, "execute", "x", false, "Execute kubectl commands (default: print only)")
	cmd.Flags().StringVar(&flags.Kubectl, "kubectl", "", "Path to kubectl binary")
	cmd.Flags().StringVarP(&flags.Namespace, "namespace", "n", "", "Kubernetes namespace")
	cmd.Flags().StringVar(&flags.WpCluster, "wp-cluster", "", "WoodpeckerCluster CR name")
	cmd.Flags().StringVar(&flags.KubeContext, "kube-context", "", "kubectl context name")
	cmd.Flags().StringVar(&flags.Kubeconfig, "kubeconfig", "", "Path to kubeconfig file")
}

// ResolveExecutor builds a k8s.Executor from the flags, merging with cli.yaml k8s config.
func (f *K8sFlags) ResolveExecutor(cliK8s config.K8sConfig) *k8s.Executor {
	opts := k8s.Opts{
		Kubectl:     cliK8s.Kubectl,
		Kubeconfig:  cliK8s.Kubeconfig,
		KubeContext: cliK8s.KubeContext,
		Namespace:   cliK8s.Namespace,
	}
	// Flags override cli.yaml
	if f.Kubectl != "" {
		opts.Kubectl = f.Kubectl
	}
	if f.Kubeconfig != "" {
		opts.Kubeconfig = f.Kubeconfig
	}
	if f.KubeContext != "" {
		opts.KubeContext = f.KubeContext
	}
	if f.Namespace != "" {
		opts.Namespace = f.Namespace
	}
	return k8s.NewExecutor(opts)
}

// ResolveCluster returns the WoodpeckerCluster CR name from flags or cli.yaml.
func (f *K8sFlags) ResolveCluster(cliK8s config.K8sConfig) string {
	if f.WpCluster != "" {
		return f.WpCluster
	}
	if cliK8s.Cluster != "" {
		return cliK8s.Cluster
	}
	return "woodpecker"
}

func newK8sCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "k8s",
		Short: "Kubernetes integration commands (print or execute kubectl)",
		Long: `Commands for managing Woodpecker on Kubernetes via the operator.
Default mode prints kubectl commands without executing.
Use -x / --execute to actually run them.`,
	}
	cmd.AddCommand(
		newK8sStatusCommand(),
		newK8sScaleCommand(),
		newK8sLogsCommand(),
		newK8sDoctorCommand(),
	)
	return cmd
}
