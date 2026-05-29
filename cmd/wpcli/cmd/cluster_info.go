package cmd

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
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

			// Compute state counts and group by region/AZ/RG.
			byRegionAZRG := make(map[string]map[string]map[string][]clusterNodeInfo) // region -> az -> rg -> nodes
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
				if byRegionAZRG[m.Region] == nil {
					byRegionAZRG[m.Region] = make(map[string]map[string][]clusterNodeInfo)
				}
				if byRegionAZRG[m.Region][m.AZ] == nil {
					byRegionAZRG[m.Region][m.AZ] = make(map[string][]clusterNodeInfo)
				}
				byRegionAZRG[m.Region][m.AZ][m.RG] = append(byRegionAZRG[m.Region][m.AZ][m.RG], clusterNodeInfo{member: m, state: state})
			}

			return renderClusterInfo(cmd, r, stateCounts, byRegionAZRG)
		},
	}
}

func renderClusterInfo(cmd *cobra.Command, r *resolved, states map[string]int, byRegionAZRG map[string]map[string]map[string][]clusterNodeInfo) error {
	w := cmd.OutOrStdout()

	// Build overview block.
	clusterName := clusterNameFromMembers(r.Members.Members)
	fmt.Fprintln(w, "Cluster Overview")
	if clusterName != "" {
		fmt.Fprintf(w, "  Cluster:     %s\n", clusterName)
	}
	fmt.Fprintf(w, "  Endpoint:    %s\n", r.Context.Endpoint)
	fmt.Fprintf(w, "  Total Nodes: %d\n", len(r.Members.Members))
	fmt.Fprintln(w)
	fmt.Fprintln(w, "By State")
	for _, k := range sortedKeys(states) {
		fmt.Fprintf(w, "  %s: %d\n", k, states[k])
	}
	fmt.Fprintln(w)

	// Build topology tree.
	treeLabel := "Topology"
	if clusterName != "" {
		treeLabel = clusterName
	}
	root := &output.TreeNode{Label: treeLabel}
	if hasAnyRegion(byRegionAZRG) {
		regionKeys := sortedMapKeys(byRegionAZRG)
		for _, region := range regionKeys {
			regionLabel := region
			if regionLabel == "" {
				regionLabel = "(unknown-region)"
			}
			regionNode := &output.TreeNode{Label: regionLabel}
			appendAZRGNodes(regionNode, byRegionAZRG[region])
			root.Children = append(root.Children, regionNode)
		}
	} else {
		for _, az := range sortedMapKeys(byRegionAZRG[""]) {
			azNode := &output.TreeNode{Label: az}
			appendRGNodes(azNode, byRegionAZRG[""][az])
			root.Children = append(root.Children, azNode)
		}
	}
	return output.RenderTree(w, root)
}

func appendAZRGNodes(parent *output.TreeNode, byAZRG map[string]map[string][]clusterNodeInfo) {
	for _, az := range sortedMapKeys(byAZRG) {
		azNode := &output.TreeNode{Label: az}
		appendRGNodes(azNode, byAZRG[az])
		parent.Children = append(parent.Children, azNode)
	}
}

func appendRGNodes(parent *output.TreeNode, byRG map[string][]clusterNodeInfo) {
	for _, rg := range sortedMapKeys(byRG) {
		rgNode := &output.TreeNode{Label: rg}
		for _, ni := range byRG[rg] {
			rgNode.Children = append(rgNode.Children, &output.TreeNode{
				Label: fmt.Sprintf("%s  %s", ni.member.ID, ni.state),
			})
		}
		parent.Children = append(parent.Children, rgNode)
	}
}

func hasAnyRegion(byRegionAZRG map[string]map[string]map[string][]clusterNodeInfo) bool {
	for region := range byRegionAZRG {
		if region != "" {
			return true
		}
	}
	return false
}

// clusterNameFromMembers extracts the cluster name from the first member that has it.
func clusterNameFromMembers(members []client.Member) string {
	for _, m := range members {
		if clusterName := clusterNameFromMember(m); clusterName != "" {
			return clusterName
		}
	}
	return ""
}

func clusterNameFromMember(m client.Member) string {
	if m.ClusterName != "" {
		return m.ClusterName
	}
	if v, ok := m.Tags["cluster"]; ok && v != "" {
		return v
	}
	return ""
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
