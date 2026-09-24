package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
)

// newNodeLogHealthCommand serves /admin/log-health: the node's own observation of how reading and
// writing is going per log, derived from real operation outcomes rather than synthetic probes.
//
// It answers a different question from /healthz. That one is process liveness and decides whether
// Kubernetes keeps routing to the pod; this is data-path health, and a log that has stalled never
// affects it. A node can be perfectly live and serving nothing.
func newNodeLogHealthCommand() *cobra.Command {
	var bucket, rootPath string
	cmd := &cobra.Command{
		Use:   "log-health <node>",
		Short: "Show per-log read/write health observed by a node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			res, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			member, ok := res.Members.Resolve(args[0])
			if !ok {
				return wperrors.NewTargetNotFoundError(args[0])
			}

			// The server ignores a partial filter and answers with every tenant, so send both
			// params or neither rather than silently widening the query.
			if (bucket == "") != (rootPath == "") {
				return wperrors.NewUsageError("--bucket and --root must be given together")
			}
			path := "/admin/log-health"
			if bucket != "" {
				params := url.Values{}
				params.Set("bucket_name", bucket)
				params.Set("root_path", rootPath)
				path += "?" + params.Encode()
			}

			// A 503 here carries the report, not an excuse for one: the server answers it only
			// when every tracked log is Stalled or Failed, and encodes the full body with it.
			body, status, err := fetchAdminJSONWithStatus(res.Client.PeerAdminURL(member), path)
			if err != nil {
				return err
			}
			unhealthy := func() error {
				if status == http.StatusOK {
					return nil
				}
				return wperrors.NewRedFindingError(
					fmt.Sprintf("%s reports log health %s (HTTP %d)", member.ID, reportState(body), status),
				)
			}
			if Globals.Output == "json" || Globals.Output == "yaml" {
				_, _ = cmd.OutOrStdout().Write(body)
				return unhealthy()
			}

			var report struct {
				State       string `json:"state"`
				Reason      string `json:"reason"`
				NodeID      string `json:"node_id"`
				TrackedLogs int    `json:"tracked_logs"`
				HealthyLogs int    `json:"healthy_logs"`
				StalledLogs int    `json:"stalled_logs"`
				FailedLogs  int    `json:"failed_logs"`
				IdleLogs    int    `json:"idle_logs"`
				Logs        []struct {
					LogID     int64  `json:"log_id"`
					Bucket    string `json:"bucket_name"`
					RootPath  string `json:"root_path"`
					Write     string `json:"write_state"`
					Read      string `json:"read_state"`
					LastError string `json:"last_failure_reason,omitempty"`
				} `json:"logs"`
			}
			if jsonErr := json.Unmarshal(body, &report); jsonErr != nil {
				if status != http.StatusOK {
					return wperrors.NewNetworkError(
						fmt.Sprintf("%s returned status %d: %s", path, status, strings.TrimSpace(string(body))))
				}
				return wperrors.NewNetworkError(fmt.Sprintf("invalid response: %v", jsonErr))
			}

			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "Log health on %s: %s\n", member.ID, report.State)
			if report.Reason != "" {
				fmt.Fprintf(w, "  reason: %s\n", report.Reason)
			}
			fmt.Fprintf(w, "  tracked: %d  healthy: %d  stalled: %d  failed: %d  idle: %d\n\n",
				report.TrackedLogs, report.HealthyLogs, report.StalledLogs, report.FailedLogs, report.IdleLogs)

			if len(report.Logs) == 0 {
				return unhealthy()
			}
			headers := []string{"LOG_ID", "BUCKET", "ROOT_PATH", "WRITE", "READ", "LAST_FAILURE"}
			rows := make([][]string, 0, len(report.Logs))
			for _, l := range report.Logs {
				rows = append(rows, []string{
					fmt.Sprintf("%d", l.LogID), l.Bucket, l.RootPath, l.Write, l.Read, l.LastError,
				})
			}
			if tableErr := output.RenderRowTable(w, headers, rows); tableErr != nil {
				return tableErr
			}
			return unhealthy()
		},
	}
	cmd.Flags().StringVar(&bucket, "bucket", "", "Filter to one instance's bucket (requires --root)")
	cmd.Flags().StringVar(&rootPath, "root", "", "Filter to one instance's root path (requires --bucket)")
	return cmd
}

// newNodeHealthzCommand serves /healthz, the readiness probe Kubernetes itself acts on.
//
// `wp cluster health` sounds like it covers this but fans out /admin/node/status, so until now the
// probe that decides whether a pod receives traffic could not be consulted from the CLI. A
// non-200 response exits non-zero: a probe reporting failure is not a successful command.
func newNodeHealthzCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "healthz <node>",
		Short: "Query a node's health probe (the one readiness uses)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			res, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			member, ok := res.Members.Resolve(args[0])
			if !ok {
				return wperrors.NewTargetNotFoundError(args[0])
			}

			req, reqErr := http.NewRequest(http.MethodGet, res.Client.PeerAdminURL(member)+"/healthz", nil)
			if reqErr != nil {
				return wperrors.NewNetworkError(reqErr.Error())
			}
			req.Header.Set("Content-Type", "application/json")
			resp, doErr := (&http.Client{Timeout: res.Context.Timeout}).Do(req)
			if doErr != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("query healthz on %s: %v", member.ID, doErr))
			}
			defer resp.Body.Close()

			raw, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				return wperrors.NewNetworkError(readErr.Error())
			}

			var probe struct {
				State  string `json:"state"`
				Detail []struct {
					Name string `json:"name"`
					Code string `json:"code"`
				} `json:"detail"`
			}
			_ = json.Unmarshal(raw, &probe)

			w := cmd.OutOrStdout()
			if Globals.Output == "json" || Globals.Output == "yaml" {
				// /healthz already answers JSON, so the body passes through for piping.
				_, _ = w.Write(raw)
			} else {
				fmt.Fprintf(w, "Health probe on %s: %s\n", member.ID, probe.State)
				for _, d := range probe.Detail {
					fmt.Fprintf(w, "  %-14s %s\n", d.Name, d.Code)
				}
			}

			if resp.StatusCode != http.StatusOK {
				return wperrors.NewRedFindingError(
					fmt.Sprintf("%s reports unhealthy (HTTP %d): %s", member.ID, resp.StatusCode, probe.State),
				)
			}
			return nil
		},
	}
}

// reportState pulls the overall state out of a log-health body for the error message, so a
// failing status is reported in the server's own words.
func reportState(body []byte) string {
	var r struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(body, &r); err != nil || r.State == "" {
		return "unknown"
	}
	return r.State
}
