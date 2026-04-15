package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

// fetchAdminJSON fetches a JSON endpoint from a peer and returns the raw bytes.
// Shared by config show/diff and env show/diff.
func fetchAdminJSON(peerURL, path string) ([]byte, error) {
	client := &http.Client{Timeout: Globals.Timeout}
	resp, err := client.Get(peerURL + path)
	if err != nil {
		return nil, wperrors.NewNetworkError(fmt.Sprintf("GET %s: %v", path, err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, wperrors.NewNetworkError(fmt.Sprintf("%s returned status %d", path, resp.StatusCode))
	}
	return io.ReadAll(resp.Body)
}

// convertJSONNumbers recursively converts json.Number values to int64 or float64
// so that YAML renders them as plain numbers instead of scientific notation.
func convertJSONNumbers(v any) any {
	switch val := v.(type) {
	case json.Number:
		if i, err := strconv.ParseInt(string(val), 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(string(val), 64); err == nil {
			return f
		}
		return string(val)
	case map[string]any:
		for k, v2 := range val {
			val[k] = convertJSONNumbers(v2)
		}
		return val
	case []any:
		for i, v2 := range val {
			val[i] = convertJSONNumbers(v2)
		}
		return val
	default:
		return v
	}
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
				// Use json.Decoder with UseNumber to preserve integer precision.
				// Without this, large integers (e.g. 536870912) become float64
				// and YAML renders them in scientific notation (5.36870912e+08).
				dec := json.NewDecoder(bytes.NewReader(body))
				dec.UseNumber()
				var generic any
				if err := dec.Decode(&generic); err != nil {
					return wperrors.NewNetworkError(fmt.Sprintf("decode config: %v", err))
				}
				generic = convertJSONNumbers(generic)
				enc := yaml.NewEncoder(w)
				enc.SetIndent(2)
				defer enc.Close()
				return enc.Encode(generic)
			}
		},
	}
}
