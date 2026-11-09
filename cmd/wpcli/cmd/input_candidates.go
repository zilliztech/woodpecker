package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/zilliztech/woodpecker/cmd/wpcli/candidates"
	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
)

// Successful operational commands publish independent field suggestions. The
// diagnostic frontend records successful source queries itself, including
// partial scenarios; it must not be recorded again by this hook.
func rememberCommandInputs(cmd *cobra.Command, args []string) {
	path := strings.TrimPrefix(cmd.CommandPath(), cmd.Root().Name()+" ")
	for _, prefix := range []string{"diag", "webui", "shell", "ctx", "help", "completion"} {
		if path == prefix || strings.HasPrefix(path, prefix+" ") {
			return
		}
	}
	values := candidates.Values{}
	rawEtcd := strings.HasPrefix(path, "etcd ")
	for flag, key := range map[string]string{"etcd": "metadata.etcd", "meta-prefix": "metadata.prefix", "bucket": "metadata.bucket", "root": "metadata.root_path", "root-path": "metadata.root_path", "log": "log_id", "log-id": "log_id", "log-name": "log_name", "segment-id": "segment_id", "seg": "segment_id"} {
		if flag == "meta-prefix" && rawEtcd {
			continue
		}
		if f := cmd.Flags().Lookup(flag); f != nil && f.Changed {
			values[key] = append(values[key], f.Value.String())
		}
	}
	if f := cmd.Flags().Lookup("client-parameters-file"); f != nil && f.Changed {
		if info, err := os.Stat(f.Value.String()); err == nil && info.Mode().IsRegular() && info.Size() <= 64<<10 {
			if body, err := os.ReadFile(f.Value.String()); err == nil {
				var params config.EtcdConfig
				if yaml.Unmarshal(body, &params) == nil {
					if len(values["metadata.etcd"]) == 0 {
						values["metadata.etcd"] = []string{strings.Join(params.Endpoints, ",")}
					}
					if !rawEtcd && len(values["metadata.prefix"]) == 0 {
						values["metadata.prefix"] = []string{params.MetaPrefix}
					}
				}
			}
		}
	}
	// Argument names belong to the native command's own contract.
	parts := strings.Fields(cmd.Use)
	if len(parts) == 0 {
		return
	}
	for i, name := range parts[1:] {
		if i >= len(args) {
			break
		}
		name = strings.Trim(name, "<>[]")
		if key := map[string]string{"logName": "log_name", "logId": "log_id", "segmentId": "segment_id", "node": "node_id"}[name]; key != "" {
			values[key] = append(values[key], args[i])
		}
	}
	if err := candidates.Save(values); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), "Warning: could not save temporary input candidates")
	}
}
