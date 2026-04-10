package cmd

import (
	"bytes"
	"fmt"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newEnvDiffCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "diff",
		Short: "Compare env / runtime / host / build across all nodes",
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			envs := make(map[string][]byte)
			for _, m := range r.Members.Members {
				b, err := fetchAdminJSON(r.Client.PeerAdminURL(m), "/admin/env")
				if err != nil {
					envs[m.ID] = []byte("<unreachable>")
					continue
				}
				envs[m.ID] = b
			}
			w := cmd.OutOrStdout()
			// Simple byte-compare. A proper implementation would filter noise keys
			// (HOSTNAME, PWD, etc.) — deferred to Phase 1.5.
			var reference []byte
			var refID string
			for _, m := range r.Members.Members {
				reference = envs[m.ID]
				refID = m.ID
				break
			}
			anyDrift := false
			for _, m := range r.Members.Members {
				if m.ID == refID {
					continue
				}
				if bytes.Equal(envs[m.ID], reference) {
					fmt.Fprintf(w, "%s: identical\n", m.ID)
				} else {
					anyDrift = true
					fmt.Fprintf(w, "%s: DIFFERS from %s\n", m.ID, refID)
				}
			}
			if anyDrift {
				return wperrors.NewYellowFindingError("env drift detected")
			}
			return nil
		},
	}
}
