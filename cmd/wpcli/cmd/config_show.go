package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

// fetchAdminJSON fetches a JSON endpoint from a peer and returns the raw bytes.
// Shared by config show/diff and env show/diff.
func fetchAdminJSON(peerURL, path string) ([]byte, error) {
	resp, err := http.Get(peerURL + path)
	if err != nil {
		return nil, wperrors.NewNetworkError(fmt.Sprintf("GET %s: %v", path, err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, wperrors.NewNetworkError(fmt.Sprintf("%s returned status %d", path, resp.StatusCode))
	}
	return io.ReadAll(resp.Body)
}

func newConfigShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <node>",
		Short: "Show the resolved configuration of a single node",
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
			body, err := fetchAdminJSON(r.Client.PeerAdminURL(target), "/admin/config")
			if err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			switch Globals.Output {
			case "json":
				_, _ = w.Write(body)
				return nil
			default: // table, wide, yaml — render as indented YAML which reads well
				var generic any
				if err := json.Unmarshal(body, &generic); err != nil {
					return wperrors.NewNetworkError(fmt.Sprintf("decode config: %v", err))
				}
				enc := yaml.NewEncoder(w)
				enc.SetIndent(2)
				defer enc.Close()
				return enc.Encode(generic)
			}
		},
	}
}
