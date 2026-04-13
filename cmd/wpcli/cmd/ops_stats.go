package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
)

func newOpsStatsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "stats <node>",
		Short: "Show op registry utilization and eviction statistics",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			res, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			target, ok := res.Members.Resolve(args[0])
			if !ok {
				return wperrors.NewTargetNotFoundError(args[0])
			}
			peerURL := res.Client.PeerAdminURL(target)
			body, err := fetchAdminJSON(peerURL, "/admin/runtime/ops/stats")
			if err != nil {
				return err
			}

			if Globals.Output == "json" || Globals.Output == "yaml" {
				_, _ = cmd.OutOrStdout().Write(body)
				return nil
			}

			var stats struct {
				Capacity     int   `json:"capacity"`
				InUse        int   `json:"in_use"`
				WarnAgeMS    int64 `json:"warn_age_ms"`
				EvictedTotal int64 `json:"evicted_total"`
				EvictedYoung int64 `json:"evicted_young"`
				EvictedOld   int64 `json:"evicted_old"`
			}
			if err := json.Unmarshal(body, &stats); err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("invalid response: %v", err))
			}

			utilPct := float64(0)
			if stats.Capacity > 0 {
				utilPct = float64(stats.InUse) / float64(stats.Capacity) * 100
			}

			oldMarker := ""
			if stats.EvictedOld > 0 {
				oldMarker = " (STALL SIGNAL)"
			}

			sections := []output.Section{
				{Title: "Capacity", Pairs: [][2]string{
					{"capacity", fmt.Sprintf("%d", stats.Capacity)},
					{"in_use", fmt.Sprintf("%d  (%.1f%%)", stats.InUse, utilPct)},
					{"warn_age", fmt.Sprintf("%dms", stats.WarnAgeMS)},
				}},
				{Title: "Eviction Totals", Pairs: [][2]string{
					{"total", fmt.Sprintf("%d", stats.EvictedTotal)},
					{"young", fmt.Sprintf("%d", stats.EvictedYoung)},
					{"old", fmt.Sprintf("%d%s", stats.EvictedOld, oldMarker)},
				}},
			}
			return output.RenderSectionedTable(cmd.OutOrStdout(), sections)
		},
	}
}
