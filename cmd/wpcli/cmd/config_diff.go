package cmd

import (
	"bytes"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newConfigDiffCommand() *cobra.Command {
	var all bool
	var reference string
	cmd := &cobra.Command{
		Use:   "diff [node-a] [node-b]",
		Short: "Compare configurations across nodes",
		Args:  cobra.RangeArgs(0, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}

			// Decide targets.
			var targets []client.Member
			if len(args) == 2 {
				a, okA := r.Members.Resolve(args[0])
				b, okB := r.Members.Resolve(args[1])
				if !okA {
					return wperrors.NewTargetNotFoundError(args[0])
				}
				if !okB {
					return wperrors.NewTargetNotFoundError(args[1])
				}
				targets = []client.Member{a, b}
			} else if all || len(args) == 0 {
				targets = r.Members.Members
			} else {
				return wperrors.NewUsageError("pass two node names, or --all")
			}

			// Fetch each target's config.
			configs := make(map[string][]byte)
			for _, t := range targets {
				b, err := fetchAdminJSON(r.Client.PeerAdminURL(t), "/admin/config")
				if err != nil {
					configs[t.ID] = []byte("<unreachable>")
					continue
				}
				configs[t.ID] = b
			}

			// Reference: first target, or explicit --reference.
			refID := targets[0].ID
			if reference != "" {
				refID = reference
			}
			refBytes, ok := configs[refID]
			if !ok {
				return wperrors.NewTargetNotFoundError(refID)
			}

			w := cmd.OutOrStdout()
			anyDrift := false
			for _, t := range targets {
				if t.ID == refID {
					continue
				}
				if bytes.Equal(configs[t.ID], refBytes) {
					fmt.Fprintf(w, "%s: identical\n", t.ID)
				} else {
					anyDrift = true
					fmt.Fprintf(w, "%s: DRIFT vs %s\n", t.ID, refID)
				}
			}
			if anyDrift {
				return wperrors.NewYellowFindingError("config drift detected")
			}
			if len(targets) == 1 {
				fmt.Fprintln(w, "only one node — nothing to diff")
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&all, "all", false, "diff all nodes in the cluster against the first reachable node")
	cmd.Flags().StringVar(&reference, "reference", "", "explicit reference node name")
	return cmd
}
