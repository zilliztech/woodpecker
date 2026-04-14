package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
)

// nodeStatusDTO mirrors server.NodeStatus JSON on the wire.
type nodeStatusDTO struct {
	NodeID            string            `json:"node_id"`
	State             string            `json:"state"`
	IsDecommissioning bool              `json:"is_decommissioning"`
	MemberCount       int               `json:"member_count"`
	Address           string            `json:"address"`
	ResourceGroup     string            `json:"resource_group"`
	AZ                string            `json:"az"`
	Tags              map[string]string `json:"tags"`
	StartedAtMS       int64             `json:"started_at_ms"`
	Version           string            `json:"version"`
	LastHealthCheckMS int64             `json:"last_health_check_ms"`
}

func newNodeShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <node>",
		Short: "Show detailed status for a single node",
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

			peerURL := r.Client.PeerAdminURL(target)
			resp, err := http.Get(peerURL + "/admin/node/status")
			if err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("fetch node status: %v", err))
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return wperrors.NewNetworkError(fmt.Sprintf("node status returned %d", resp.StatusCode))
			}
			var dto nodeStatusDTO
			if err := json.NewDecoder(resp.Body).Decode(&dto); err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("decode node status: %v", err))
			}

			return renderNodeShow(cmd, dto, target.Tags["cluster"])
		},
	}
}

func renderNodeShow(cmd *cobra.Command, dto nodeStatusDTO, cluster string) error {
	w := cmd.OutOrStdout()
	switch Globals.Output {
	case "json":
		return output.RenderJSON(w, dto)
	case "yaml":
		return output.RenderYAML(w, dto)
	default:
		startedAt := time.UnixMilli(dto.StartedAtMS).UTC().Format(time.RFC3339)
		uptime := time.Since(time.UnixMilli(dto.StartedAtMS)).Round(time.Second).String()
		sections := []output.Section{
			{
				Title: "Identity",
				Pairs: [][2]string{
					{"node_id", dto.NodeID},
					{"address", dto.Address},
					{"version", dto.Version},
				},
			},
			{
				Title: "Placement",
				Pairs: func() [][2]string {
					pairs := [][2]string{}
					if cluster != "" {
						pairs = append(pairs, [2]string{"cluster", cluster})
					}
					pairs = append(pairs, [2]string{"az", dto.AZ})
					pairs = append(pairs, [2]string{"rg", dto.ResourceGroup})
					return pairs
				}(),
			},
			{
				Title: "Lifecycle",
				Pairs: [][2]string{
					{"state", dto.State},
					{"is_decommissioning", fmt.Sprintf("%v", dto.IsDecommissioning)},
					{"started_at", startedAt},
					{"uptime", uptime},
				},
			},
			{
				Title: "Health",
				Pairs: [][2]string{
					{"member_count", fmt.Sprintf("%d", dto.MemberCount)},
					{"last_health_check", time.UnixMilli(dto.LastHealthCheckMS).UTC().Format(time.RFC3339)},
				},
			},
		}
		return output.RenderSectionedTable(w, sections)
	}
}
