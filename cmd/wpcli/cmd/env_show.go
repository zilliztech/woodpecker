package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newEnvShowCommand() *cobra.Command {
	var section string
	cmd := &cobra.Command{
		Use:   "show <node>",
		Short: "Show env vars + runtime + host + build for a single node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			target, ok := r.Members.Resolve(args[0])
			if !ok {
				return wperrors.NewTargetNotFoundError(args[0])
			}
			body, err := fetchAdminJSON(r.Client.PeerAdminURL(target), "/admin/env")
			if err != nil {
				return err
			}

			var full map[string]any
			if err := json.Unmarshal(body, &full); err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("decode env: %v", err))
			}

			// Filter by --section.
			var out any = full
			if section != "" && section != "all" {
				sect, ok := full[section]
				if !ok {
					return wperrors.NewUsageError(fmt.Sprintf("unknown section %q (valid: env, runtime, host, build, all)", section))
				}
				out = sect
			}

			w := cmd.OutOrStdout()
			if Globals.Output == "json" {
				enc := json.NewEncoder(w)
				enc.SetIndent("", "  ")
				return enc.Encode(out)
			}
			enc := yaml.NewEncoder(w)
			enc.SetIndent(2)
			defer enc.Close()
			return enc.Encode(out)
		},
	}
	cmd.Flags().StringVar(&section, "section", "all", "section filter: env|runtime|host|build|all")
	return cmd
}
