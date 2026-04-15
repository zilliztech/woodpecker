package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newClusterHealthCommand() *cobra.Command {
	var expectedReplicas int
	cmd := &cobra.Command{
		Use:   "health",
		Short: "Red/yellow/green health check for the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			// Phase 1: use cluster-level fallback only. Per-log replica health
			// is Phase 2 (requires scraping /metrics with the new scenario engine).
			N := expectedReplicas

			var urls []string
			for _, m := range r.Members.Members {
				urls = append(urls, r.Client.PeerAdminURL(m))
			}
			f := client.NewFanout(client.FanoutOpts{
				Concurrency: r.Context.Concurrency,
				Timeout:     r.Context.Timeout,
			})
			res := f.Get(urls, "/admin/node/status", "")

			activeCount := 0
			decommissioningCount := 0
			unreachableCount := res.Unreachable
			versions := make(map[string]int)
			for i := range r.Members.Members {
				if !res.Results[i].OK {
					continue
				}
				var s struct {
					State             string `json:"state"`
					Version           string `json:"version"`
					IsDecommissioning bool   `json:"is_decommissioning"`
				}
				_ = json.Unmarshal(res.Results[i].Body, &s)
				if s.IsDecommissioning || s.State == "decommissioning" {
					decommissioningCount++
				} else if s.State == "active" || s.State == "" {
					activeCount++
				}
				if s.Version != "" {
					versions[s.Version]++
				}
			}

			verdict := "GREEN"
			var reasons []string
			// RED conditions.
			if activeCount < N {
				verdict = "RED"
				reasons = append(reasons, fmt.Sprintf("active nodes %d < expected replicas %d", activeCount, N))
			}
			if unreachableCount > 0 {
				verdict = "RED"
				reasons = append(reasons, fmt.Sprintf("%d nodes unreachable", unreachableCount))
			}
			// YELLOW conditions (only if not already RED).
			if verdict != "RED" {
				if decommissioningCount > 0 {
					verdict = "YELLOW"
					reasons = append(reasons, fmt.Sprintf("%d nodes decommissioning", decommissioningCount))
				}
				if len(versions) > 1 {
					verdict = "YELLOW"
					reasons = append(reasons, fmt.Sprintf("version skew: %d distinct versions", len(versions)))
				}
				if activeCount == N {
					verdict = "YELLOW"
					reasons = append(reasons, "zero redundancy (active == N)")
				}
			}

			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "Cluster Health: %s\n", verdict)
			fmt.Fprintf(w, "  expected_replicas: %d (fallback; metric-based check is Phase 2)\n", N)
			fmt.Fprintf(w, "  active: %d  decommissioning: %d  unreachable: %d\n", activeCount, decommissioningCount, unreachableCount)
			for _, rsn := range reasons {
				fmt.Fprintf(w, "  - %s\n", rsn)
			}

			switch verdict {
			case "RED":
				return wperrors.NewRedFindingError("cluster health RED")
			case "YELLOW":
				return wperrors.NewYellowFindingError("cluster health YELLOW")
			}
			return nil
		},
	}
	cmd.Flags().IntVar(&expectedReplicas, "expected-replicas", 3, "expected replica count baseline (default 3)")
	return cmd
}
