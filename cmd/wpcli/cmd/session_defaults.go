package cmd

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

// applySessionCommandDefaults binds shared instance settings once at the root.
// Commands registering the normal storage flags inherit this behavior without
// maintaining command-name lists or loading session files themselves.
func applySessionCommandDefaults(cmd *cobra.Command) error {
	path := os.Getenv(config.SessionConfigEnv)
	if path == "" || cmd.Flags().Lookup("bucket") == nil {
		return nil
	}
	rootFlag := "root-path"
	if cmd.Flags().Lookup(rootFlag) == nil {
		rootFlag = "root"
	}
	if cmd.Flags().Lookup(rootFlag) == nil {
		return nil
	}
	if cmd.Flags().Changed("bucket") || cmd.Flags().Changed(rootFlag) {
		if !cmd.Flags().Changed("bucket") || !cmd.Flags().Changed(rootFlag) {
			return wperrors.NewUsageError("storage overrides require both --bucket and --" + rootFlag)
		}
		bucket, _ := cmd.Flags().GetString("bucket")
		root, _ := cmd.Flags().GetString(rootFlag)
		if err := (config.StorageConfig{Bucket: bucket, RootPath: root}).Validate(); err != nil {
			return wperrors.NewUsageError(err.Error())
		}
		return nil
	}
	if clientMetadataOverride(cmd) {
		return nil // An explicitly selected client must not inherit another client's storage.
	}
	session, err := config.LoadSession(path)
	if err != nil {
		return wperrors.NewConfigError(err.Error())
	}
	if session.ClientStorage == nil {
		return nil
	}
	if err := session.ClientStorage.Validate(); err != nil {
		return wperrors.NewConfigError(err.Error())
	}
	for name, value := range map[string]string{"bucket": session.ClientStorage.Bucket, rootFlag: session.ClientStorage.RootPath} {
		// Preserve Changed: a supplied default is not an explicit target override
		// and must not be recorded as a typed input in the recent-input cache.
		if err := cmd.Flags().Lookup(name).Value.Set(value); err != nil {
			return wperrors.NewConfigError("cannot apply session storage defaults")
		}
	}
	return nil
}

func clientMetadataOverride(cmd *cobra.Command) bool {
	return cmd.Flags().Changed("etcd") || cmd.Flags().Changed("meta-prefix") || cmd.Flags().Changed("client-parameters-file") || cmd.Flags().Changed("standalone")
}
