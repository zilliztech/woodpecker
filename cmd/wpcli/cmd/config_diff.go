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

			// A node that did not answer is tracked as unreachable, never stored as a value to
			// compare. Storing a sentinel made silence compare equal to silence, so a run that
			// read nothing reported every node identical.
			configs := make(map[string][]byte)
			unreachable := make(map[string]bool)
			for _, t := range targets {
				b, err := fetchAdminJSON(r.Client.PeerAdminURL(t), "/admin/config")
				if err != nil {
					unreachable[t.ID] = true
					continue
				}
				configs[t.ID] = b
			}
			if len(unreachable) == len(targets) {
				return wperrors.NewNetworkError(
					fmt.Sprintf("no node answered: all %d unreachable", len(targets)))
			}
			defer warnIfPartial(cmd.ErrOrStderr(), len(unreachable), len(targets))

			// Reference: explicit --reference, else the first target that answered, which is
			// what --all advertises.
			refID := ""
			if reference != "" {
				refID = reference
				if unreachable[refID] {
					return wperrors.NewNetworkError(
						fmt.Sprintf("reference node %s is unreachable", refID))
				}
			} else {
				for _, t := range targets {
					if !unreachable[t.ID] {
						refID = t.ID
						break
					}
				}
			}
			refBytes, ok := configs[refID]
			if !ok {
				return wperrors.NewTargetNotFoundError(refID)
			}

			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "%s: (reference)\n", refID)
			anyDrift := false
			for _, t := range targets {
				if t.ID == refID {
					continue
				}
				if unreachable[t.ID] {
					fmt.Fprintf(w, "%s: UNREACHABLE (not compared)\n", t.ID)
					continue
				}
				if bytes.Equal(configs[t.ID], refBytes) {
					fmt.Fprintf(w, "%s: identical\n", t.ID)
				} else {
					anyDrift = true
					diffs := jsonDiff(refBytes, configs[t.ID])
					fmt.Fprintf(w, "%s: DRIFT (%d fields differ)\n", t.ID, len(diffs))
					fmt.Fprint(w, renderDiffEntries(diffs, refID, t.ID))
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
