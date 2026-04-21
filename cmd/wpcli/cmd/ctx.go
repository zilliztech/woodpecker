package cmd

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newCtxCommand() *cobra.Command {
	ctx := &cobra.Command{
		Use:   "ctx",
		Short: "Manage CLI contexts (cli.yaml)",
	}
	ctx.AddCommand(newCtxListCommand(), newCtxUseCommand(), newCtxViewCommand())
	return ctx
}

// loadFileFromEnvOrDefault resolves which cli.yaml to operate on.
func loadFileFromEnvOrDefault() (*config.File, string, error) {
	for _, p := range config.DefaultConfigPaths() {
		if _, err := os.Stat(p); err == nil {
			f, err := config.Load(p)
			if err != nil {
				return nil, p, wperrors.NewConfigError(err.Error())
			}
			return f, p, nil
		}
	}
	return nil, "", wperrors.NewConfigError("no cli.yaml found in any known location")
}

func newCtxListCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all contexts defined in cli.yaml",
		RunE: func(cmd *cobra.Command, args []string) error {
			f, _, err := loadFileFromEnvOrDefault()
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "NAME\tENDPOINT\tACTIVE")
			for name, c := range f.Contexts {
				active := ""
				if name == f.CurrentContext {
					active = "*"
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\n", name, c.Endpoint, active)
			}
			return tw.Flush()
		},
	}
}

func newCtxUseCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "use <name>",
		Short: "Switch current-context to the given name and persist to cli.yaml",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			f, path, err := loadFileFromEnvOrDefault()
			if err != nil {
				return err
			}
			name := args[0]
			if _, ok := f.Contexts[name]; !ok {
				return wperrors.NewConfigError(fmt.Sprintf("context %q does not exist", name))
			}
			f.CurrentContext = name
			data, err := yaml.Marshal(f)
			if err != nil {
				return wperrors.NewConfigError(fmt.Sprintf("marshal cli.yaml: %v", err))
			}
			if err := os.WriteFile(path, data, 0o600); err != nil {
				return wperrors.NewConfigError(fmt.Sprintf("write cli.yaml: %v", err))
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Switched to context %q\n", name)
			return nil
		},
	}
}

func newCtxViewCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "view",
		Short: "Print the resolved fields of the active context",
		RunE: func(cmd *cobra.Command, args []string) error {
			f, _, err := loadFileFromEnvOrDefault()
			if err != nil {
				return err
			}
			active, err := f.ResolveContext(Globals.Context)
			if err != nil {
				return wperrors.NewConfigError(err.Error())
			}
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "Active context:\n")
			fmt.Fprintf(out, "  endpoint:     %s\n", active.Endpoint)
			fmt.Fprintf(out, "  admin_port:   %d\n", active.AdminPort)
			fmt.Fprintf(out, "  timeout:      %s\n", active.Timeout)
			fmt.Fprintf(out, "  concurrency:  %d\n", active.Concurrency)
			fmt.Fprintf(out, "  strict:       %v\n", active.Strict)
			return nil
		},
	}
}
