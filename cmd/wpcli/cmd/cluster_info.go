package cmd

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

type clusterNodeInfo struct {
	member client.Member
	state  string
}

func newClusterInfoCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "info",
		Short: "Show cluster overview (nodes, state, topology)",
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}

			// Fan out to each peer's /admin/node/status to learn each node's state.
			var urls []string
			for _, m := range r.Members.Members {
				urls = append(urls, r.Client.PeerAdminURL(m))
			}
			f := client.NewFanout(client.FanoutOpts{
				Concurrency: r.Context.Concurrency,
				Timeout:     r.Context.Timeout,
				Strict:      r.Context.Strict,
			})
			res := f.Get(urls, "/admin/node/status", "")
			if res.StrictFailure() {
				return wperrors.NewStrictPartialFailureError(res.Unreachable, len(urls))
			}

			// Compute state counts and group by AZ/RG.
			byAZRG := make(map[string]map[string][]clusterNodeInfo) // az -> rg -> nodes
			stateCounts := map[string]int{}
			for i, m := range r.Members.Members {
				state := "UNREACHABLE"
				if res.Results[i].OK {
					var s struct {
						State string `json:"state"`
					}
					_ = json.Unmarshal(res.Results[i].Body, &s)
					if s.State != "" {
						state = s.State
					} else {
						state = "active"
					}
				}
				stateCounts[state]++
				if byAZRG[m.AZ] == nil {
					byAZRG[m.AZ] = make(map[string][]clusterNodeInfo)
				}
				byAZRG[m.AZ][m.RG] = append(byAZRG[m.AZ][m.RG], clusterNodeInfo{member: m, state: state})
			}

			return renderClusterInfo(cmd, r, stateCounts, byAZRG)
		},
	}
}

func renderClusterInfo(cmd *cobra.Command, r *resolved, states map[string]int, byAZRG map[string]map[string][]clusterNodeInfo) error {
	w := cmd.OutOrStdout()

	// Build overview block.
	fmt.Fprintln(w, "Cluster Overview")
	fmt.Fprintf(w, "  Endpoint:    %s\n", r.Context.Endpoint)
	fmt.Fprintf(w, "  Total Nodes: %d\n", len(r.Members.Members))
	fmt.Fprintln(w)
	fmt.Fprintln(w, "By State")
	for _, k := range sortedKeys(states) {
		fmt.Fprintf(w, "  %s: %d\n", k, states[k])
	}
	fmt.Fprintln(w)

	// Build topology tree.
	root := &output.TreeNode{Label: "Topology"}
	azKeys := sortedMapKeys(byAZRG)
	for _, az := range azKeys {
		azNode := &output.TreeNode{Label: az}
		rgKeys := sortedMapKeys(byAZRG[az])
		for _, rg := range rgKeys {
			rgNode := &output.TreeNode{Label: rg}
			for _, ni := range byAZRG[az][rg] {
				rgNode.Children = append(rgNode.Children, &output.TreeNode{
					Label: fmt.Sprintf("%s  %s", ni.member.ID, ni.state),
				})
			}
			azNode.Children = append(azNode.Children, rgNode)
		}
		root.Children = append(root.Children, azNode)
	}
	return output.RenderTree(w, root)
}

func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// sortedMapKeys returns sorted keys for any map[string]V.
func sortedMapKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
