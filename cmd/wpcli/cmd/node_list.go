package cmd

import (
	"encoding/json"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
)

type nodeListRow struct {
	Name    string `json:"name"`
	Cluster string `json:"cluster,omitempty"`
	Region  string `json:"region,omitempty"`
	Addr    string `json:"addr"`
	State   string `json:"state"`
	AZ      string `json:"az"`
	RG      string `json:"rg"`
	Health  string `json:"health"`
}

func newNodeListCommand() *cobra.Command {
	var (
		filterRegion string
		filterAZ     string
		filterRG     string
		filterState  string
	)
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all server nodes in the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}

			// Fan out to every peer and hit /admin/node/status
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

			// Build rows by pairing memberlist entries with fanout results (index-aligned).
			rows := make([]nodeListRow, 0, len(r.Members.Members))
			for i, m := range r.Members.Members {
				row := nodeListRow{
					Name:    m.ID,
					Cluster: clusterNameFromMember(m),
					Region:  m.Region,
					Addr:    m.ServiceAddr,
					AZ:      m.AZ,
					RG:      m.RG,
				}
				nr := res.Results[i]
				if nr.OK {
					var s struct {
						State             string `json:"state"`
						IsDecommissioning bool   `json:"is_decommissioning"`
					}
					_ = json.Unmarshal(nr.Body, &s)
					if s.State == "" {
						s.State = "active"
					}
					row.State = s.State
					row.Health = "OK"
				} else {
					row.State = "UNREACHABLE"
					row.Health = "FAIL"
				}
				// Apply filters.
				if filterRegion != "" && row.Region != filterRegion {
					continue
				}
				if filterAZ != "" && row.AZ != filterAZ {
					continue
				}
				if filterRG != "" && row.RG != filterRG {
					continue
				}
				if filterState != "" && filterState != "all" && row.State != filterState {
					continue
				}
				rows = append(rows, row)
			}

			return renderNodeList(cmd, rows)
		},
	}
	cmd.Flags().StringVar(&filterRegion, "region", "", "filter by region")
	cmd.Flags().StringVar(&filterAZ, "az", "", "filter by availability zone")
	cmd.Flags().StringVar(&filterRG, "rg", "", "filter by resource group")
	cmd.Flags().StringVar(&filterState, "state", "all", "filter by state: active|decommissioning|decommissioned|unreachable|all")
	return cmd
}

func renderNodeList(cmd *cobra.Command, rows []nodeListRow) error {
	w := cmd.OutOrStdout()
	switch Globals.Output {
	case "json":
		return output.RenderJSON(w, rows)
	case "yaml":
		return output.RenderYAML(w, rows)
	case "wide":
		fallthrough
	default:
		// Show topology identity columns only when present for old-server compatibility.
		hasCluster := false
		hasRegion := false
		for _, r := range rows {
			if r.Cluster != "" {
				hasCluster = true
			}
			if r.Region != "" {
				hasRegion = true
			}
		}
		headers := []string{"NAME"}
		if hasCluster {
			headers = append(headers, "CLUSTER")
		}
		if hasRegion {
			headers = append(headers, "REGION")
		}
		headers = append(headers, "ADDR", "STATE", "AZ", "RG", "HEALTH")

		table := make([][]string, len(rows))
		for i, r := range rows {
			row := []string{r.Name}
			if hasCluster {
				row = append(row, r.Cluster)
			}
			if hasRegion {
				row = append(row, r.Region)
			}
			row = append(row, r.Addr, r.State, r.AZ, r.RG, r.Health)
			table[i] = row
		}
		return output.RenderRowTable(w, headers, table)
	}
}
