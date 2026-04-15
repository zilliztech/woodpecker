package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/common/version"
)

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Show wp CLI version and build info",
		RunE: func(cmd *cobra.Command, args []string) error {
			info := version.Info()
			out := cmd.OutOrStdout()
			switch Globals.Output {
			case "json":
				enc := json.NewEncoder(out)
				enc.SetIndent("", "  ")
				return enc.Encode(info)
			case "yaml":
				fmt.Fprintf(out, "version: %s\ncommit: %s\nbuild_time: %s\ngo_version: %s\n",
					info.Version, info.Commit, info.BuildTime, info.GoVersion)
				return nil
			default:
				fmt.Fprintf(out, "wp version %s\n", info.String())
				return nil
			}
		},
	}
}
