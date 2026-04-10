package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

type decommissionProgress struct {
	State               string `json:"state"`
	RemainingProcessors int    `json:"remaining_processors"`
	HasLocalData        bool   `json:"has_local_data"`
	SafeToTerminate     bool   `json:"safe_to_terminate"`
}

func newNodeDecommissionCommand() *cobra.Command {
	var (
		async             bool
		timeout           time.Duration
		interval          time.Duration
		heartbeatInterval time.Duration
		yes               bool
	)
	cmd := &cobra.Command{
		Use:   "decommission <node>",
		Short: "Trigger graceful node decommission (blocks until safe by default)",
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

			if !yes && !promptConfirm(cmd, fmt.Sprintf("decommission node %q", target.ID)) {
				return wperrors.NewUserAbortError()
			}

			peerURL := r.Client.PeerAdminURL(target)
			// POST /admin/node/decommission
			resp, err := http.Post(peerURL+"/admin/node/decommission", "application/json", bytes.NewReader(nil))
			if err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("POST decommission: %v", err))
			}
			resp.Body.Close()
			if resp.StatusCode == http.StatusConflict {
				return wperrors.NewStateConflictError("node is not in active state")
			}
			if resp.StatusCode != http.StatusOK {
				return wperrors.NewNetworkError(fmt.Sprintf("decommission returned status %d", resp.StatusCode))
			}
			fmt.Fprintf(cmd.OutOrStdout(), "decommission started on %s\n", target.ID)

			if async {
				return nil
			}

			// Blocking wait loop with heartbeat.
			return waitForSafeTermination(cmd, peerURL, timeout, interval, heartbeatInterval)
		},
	}
	cmd.Flags().BoolVar(&async, "async", false, "return immediately instead of waiting for safe_to_terminate")
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Minute, "overall timeout for the wait")
	cmd.Flags().DurationVar(&interval, "interval", 3*time.Second, "progress poll interval")
	cmd.Flags().DurationVar(&heartbeatInterval, "heartbeat-interval", 15*time.Second, "heartbeat line interval when no progress change")
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "skip interactive confirmation")
	return cmd
}

func waitForSafeTermination(cmd *cobra.Command, peerURL string, timeout, interval, heartbeat time.Duration) error {
	w := cmd.OutOrStdout()
	deadline := time.Now().Add(timeout)
	var last decommissionProgress
	lastPrint := time.Now()
	first := true

	for {
		if time.Now().After(deadline) {
			return wperrors.NewWaitTimeoutError("decommission", int(timeout.Seconds()))
		}
		resp, err := http.Get(peerURL + "/admin/node/decommission/progress")
		if err != nil {
			return wperrors.NewNetworkError(fmt.Sprintf("GET progress: %v", err))
		}
		var p decommissionProgress
		_ = json.NewDecoder(resp.Body).Decode(&p)
		resp.Body.Close()

		changed := first || p != last
		heartbeatDue := !first && time.Since(lastPrint) >= heartbeat
		if changed || heartbeatDue {
			suffix := ""
			if !changed && heartbeatDue {
				suffix = "   (heartbeat: no change; see `wp ops list <node>` for in-flight work)"
			}
			fmt.Fprintf(w, "[%s] state=%s remaining_processors=%d has_local_data=%v safe=%v%s\n",
				time.Now().UTC().Format("15:04:05"),
				p.State, p.RemainingProcessors, p.HasLocalData, p.SafeToTerminate, suffix)
			lastPrint = time.Now()
			last = p
			first = false
		}
		if p.SafeToTerminate {
			fmt.Fprintln(w, "Safe to terminate.")
			return nil
		}
		time.Sleep(interval)
	}
}

// promptConfirm reads stdin for y/N. Returns true on "y"/"yes", false otherwise.
func promptConfirm(cmd *cobra.Command, action string) bool {
	fmt.Fprintf(cmd.ErrOrStderr(), "About to %s. Proceed? [y/N]: ", action)
	buf := make([]byte, 8)
	n, _ := cmd.InOrStdin().Read(buf)
	if n == 0 {
		return false
	}
	first := buf[0]
	return first == 'y' || first == 'Y'
}
