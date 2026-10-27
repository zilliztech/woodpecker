package cli

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpdiag/webui"
	d "github.com/zilliztech/woodpecker/diagnostics"
)

func serveCommand(code *int, out, errout io.Writer) *cobra.Command {
	var path, address string
	cmd := &cobra.Command{Use: "serve", Short: "Open a local interactive diagnosis UI using the same runner and private connections", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		config, err := d.LoadConfig(path)
		if err != nil {
			*code = 2
			fmt.Fprintln(errout, "Cannot load wpdiag configuration; set --config to a valid wpdiag.config/v1 file. See cmd/wpdiag/wpdiag.example.yaml.")
			return nil
		}
		if err = webui.Serve(cmd.Context(), address, config, out); err != nil {
			*code = 1
			fmt.Fprintln(errout, err.Error())
		}
		return nil
	}}
	cmd.Flags().StringVar(&path, "config", "wpdiag.yaml", "Private source configuration; restart the local server after editing")
	cmd.Flags().StringVar(&address, "listen", "127.0.0.1:8787", "Local loopback host:port (port 0 selects a free port)")
	return cmd
}
