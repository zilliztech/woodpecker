package cmd

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newClusterGossipDiffCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "gossip-diff",
		Short: "Compare memberlist views across nodes to detect partitions",
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}

			// Fan out: each peer is asked for its /admin/memberlist view.
			var urls []string
			for _, m := range r.Members.Members {
				urls = append(urls, r.Client.PeerAdminURL(m))
			}
			f := client.NewFanout(client.FanoutOpts{
				Concurrency: r.Context.Concurrency,
				Timeout:     r.Context.Timeout,
			})
			res := f.Get(urls, "/admin/memberlist", "")

			type viewRow struct {
				NodeID  string
				Members []string
			}
			var rows []viewRow
			for i, m := range r.Members.Members {
				nr := res.Results[i]
				if !nr.OK {
					rows = append(rows, viewRow{NodeID: m.ID, Members: []string{"<unreachable>"}})
					continue
				}
				var ml client.Memberlist
				if err := json.Unmarshal(nr.Body, &ml); err != nil {
					rows = append(rows, viewRow{NodeID: m.ID, Members: []string{"<parse-error>"}})
					continue
				}
				names := make([]string, 0, len(ml.Members))
				for _, sub := range ml.Members {
					names = append(names, sub.ID)
				}
				sort.Strings(names)
				rows = append(rows, viewRow{NodeID: m.ID, Members: names})
			}

			// Compare: consistent iff all row.Members slices are identical.
			w := cmd.OutOrStdout()
			consistent := true
			var reference []string
			for i, row := range rows {
				if i == 0 {
					reference = row.Members
					continue
				}
				if !stringSlicesEqual(reference, row.Members) {
					consistent = false
					break
				}
			}

			if consistent {
				fmt.Fprintln(w, "Gossip View Consistency: CONSISTENT")
				fmt.Fprintf(w, "All %d nodes agree on membership (%d members each).\n",
					len(rows), len(reference))
				return nil
			}

			fmt.Fprintln(w, "Gossip View Consistency: INCONSISTENT")
			fmt.Fprintln(w)
			fmt.Fprintln(w, "Membership Set Agreement")
			for _, row := range rows {
				fmt.Fprintf(w, "  %-12s %d members [%s]\n",
					row.NodeID, len(row.Members), strings.Join(row.Members, ", "))
			}
			return wperrors.NewRedFindingError("gossip views inconsistent")
		},
	}
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
