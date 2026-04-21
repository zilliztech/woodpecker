package cmd

import (
	"bytes"
	"fmt"
	"net/http"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newNodeCancelDecommissionCommand() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "cancel-decommission <node>",
		Short: "Cancel an in-progress decommission (decommissioning -> active)",
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
			if !yes && !promptConfirm(cmd, fmt.Sprintf("cancel decommission of %q", target.ID)) {
				return wperrors.NewUserAbortError()
			}
			peerURL := r.Client.PeerAdminURL(target)
			resp, err := http.Post(peerURL+"/admin/node/decommission/cancel", "application/json", bytes.NewReader(nil))
			if err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("POST cancel: %v", err))
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusConflict {
				return wperrors.NewStateConflictError("cannot cancel: node is not decommissioning (may already be decommissioned)")
			}
			if resp.StatusCode != http.StatusOK {
				return wperrors.NewNetworkError(fmt.Sprintf("cancel returned status %d", resp.StatusCode))
			}
			fmt.Fprintf(cmd.OutOrStdout(), "decommission cancelled on %s; returning to active\n", target.ID)
			return nil
		},
	}
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "skip interactive confirmation")
	return cmd
}
