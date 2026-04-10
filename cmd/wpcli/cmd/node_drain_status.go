package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newNodeDrainStatusCommand() *cobra.Command {
	var (
		watch    bool
		interval time.Duration
		timeout  time.Duration
	)
	cmd := &cobra.Command{
		Use:   "drain-status <node>",
		Short: "Show (or watch) decommission progress for a node",
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

			fetch := func() (*decommissionProgress, error) {
				resp, err := http.Get(peerURL + "/admin/node/decommission/progress")
				if err != nil {
					return nil, wperrors.NewNetworkError(err.Error())
				}
				defer resp.Body.Close()
				var p decommissionProgress
				if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
					return nil, wperrors.NewNetworkError(fmt.Sprintf("decode progress: %v", err))
				}
				return &p, nil
			}

			render := func(p *decommissionProgress) error {
				return output.RenderSectionedTable(cmd.OutOrStdout(), []output.Section{{
					Title: fmt.Sprintf("Drain status for %s", target.ID),
					Pairs: [][2]string{
						{"state", p.State},
						{"remaining_processors", fmt.Sprintf("%d", p.RemainingProcessors)},
						{"has_local_data", fmt.Sprintf("%v", p.HasLocalData)},
						{"safe_to_terminate", fmt.Sprintf("%v", p.SafeToTerminate)},
					},
				}})
			}

			if !watch {
				p, err := fetch()
				if err != nil {
					return err
				}
				return render(p)
			}

			deadline := time.Now().Add(timeout)
			for {
				if time.Now().After(deadline) {
					return wperrors.NewWaitTimeoutError("drain-status watch", int(timeout.Seconds()))
				}
				p, err := fetch()
				if err != nil {
					return err
				}
				_ = render(p)
				if p.SafeToTerminate {
					return nil
				}
				time.Sleep(interval)
			}
		},
	}
	cmd.Flags().BoolVarP(&watch, "watch", "w", false, "continuously watch until safe_to_terminate")
	cmd.Flags().DurationVar(&interval, "interval", 3*time.Second, "watch poll interval")
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Minute, "watch total timeout")
	return cmd
}
