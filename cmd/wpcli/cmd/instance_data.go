package cmd

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
)

func newInstanceCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "instance",
		Short: "Inspect the node-local data held for each Woodpecker instance",
	}
	c.AddCommand(newInstanceDataCommand())
	return c
}

// instanceDataReport is the shape of GET /admin/instance/data that wp renders.
type instanceDataReport struct {
	NodeID      string `json:"node_id"`
	StorageMode string `json:"storage_mode"`
	ScanErrors  int    `json:"scan_errors"`
	Instances   []struct {
		BucketName        string `json:"bucket_name"`
		RootPath          string `json:"root_path"`
		LogCount          int    `json:"log_count"`
		SegmentCount      int    `json:"segment_count"`
		LiveSegmentCount  int    `json:"live_segment_count"`
		SizeBytes         int64  `json:"size_bytes"`
		LastModifiedMS    int64  `json:"last_modified_ms"`
		ActiveProcessors  int    `json:"active_processors"`
		DeleteState       string `json:"delete_state"`
		MarkedDeletedAtMS int64  `json:"marked_deleted_at_ms"`
	} `json:"instances"`
}

func newInstanceDataCommand() *cobra.Command {
	var (
		allNodes bool
		bucket   string
		rootPath string
	)
	cmd := &cobra.Command{
		Use:   "data [node]",
		Short: "List the instances holding local data on a node (or all nodes)",
		Long: `List the instances holding node-local data.

Pair this with 'POST /admin/instance/delete' to reclaim an orphaned instance: query
first, decide, then delete only the nodes that reported the instance.

An unreachable node still holds its data, so a node shown as UNREACHABLE — or any
node reporting scan errors — means the picture is incomplete and nothing should be
deleted on the strength of it. Use --strict to turn that into a non-zero exit.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			res, err := resolveAndDiscover()
			if err != nil {
				return err
			}

			// The server ignores a partial filter and answers with every instance, so
			// send both params or neither rather than silently widening the query.
			if (bucket == "") != (rootPath == "") {
				return wperrors.NewUsageError("--bucket and --root must be given together")
			}
			path := "/admin/instance/data"
			if bucket != "" {
				params := url.Values{}
				params.Set("bucket_name", bucket)
				params.Set("root_path", rootPath)
				path += "?" + params.Encode()
			}

			targets := res.Members.Members
			if !allNodes && len(args) == 1 {
				member, ok := res.Members.Resolve(args[0])
				if !ok {
					return wperrors.NewTargetNotFoundError(args[0])
				}
				targets = []client.Member{member}
			}

			urls := make([]string, 0, len(targets))
			for _, m := range targets {
				urls = append(urls, res.Client.PeerAdminURL(m))
			}
			f := client.NewFanout(client.FanoutOpts{
				Concurrency: res.Context.Concurrency,
				Timeout:     res.Context.Timeout,
				Strict:      res.Context.Strict,
			})
			fanRes := f.Get(urls, path, "")
			if fanRes.StrictFailure() {
				return wperrors.NewStrictPartialFailureError(fanRes.Unreachable, len(urls))
			}

			if Globals.Output == "json" || Globals.Output == "yaml" {
				return output.RenderJSON(cmd.OutOrStdout(), buildInstanceDataJSON(targets, fanRes))
			}

			headers := []string{"NODE", "BUCKET", "ROOT_PATH", "LOGS", "SEGMENTS", "LIVE", "SIZE_BYTES", "PROCS", "DELETE_STATE"}
			var rows [][]string
			scanErrorNodes := 0
			for i, m := range targets {
				nr := fanRes.Results[i]
				if !nr.OK {
					rows = append(rows, []string{m.ID, "UNREACHABLE", "", "", "", "", "", "", ""})
					continue
				}
				var report instanceDataReport
				if jsonErr := json.Unmarshal(nr.Body, &report); jsonErr != nil {
					rows = append(rows, []string{m.ID, "BAD_RESPONSE", "", "", "", "", "", "", ""})
					continue
				}
				if report.ScanErrors > 0 {
					scanErrorNodes++
				}
				if len(report.Instances) == 0 {
					rows = append(rows, []string{m.ID, "(none)", "", "0", "0", "0", "0", "0", ""})
					continue
				}
				for _, inst := range report.Instances {
					rows = append(rows, []string{
						m.ID, inst.BucketName, inst.RootPath,
						strconv.Itoa(inst.LogCount),
						strconv.Itoa(inst.SegmentCount),
						strconv.Itoa(inst.LiveSegmentCount),
						strconv.FormatInt(inst.SizeBytes, 10),
						strconv.Itoa(inst.ActiveProcessors),
						inst.DeleteState,
					})
				}
			}
			if err := output.RenderRowTable(cmd.OutOrStdout(), headers, rows); err != nil {
				return err
			}
			if fanRes.Unreachable > 0 {
				fmt.Fprintf(cmd.OutOrStdout(),
					"\nWARNING: %d/%d nodes unreachable — their instances are not listed; do not delete on this view\n",
					fanRes.Unreachable, len(urls))
			}
			if scanErrorNodes > 0 {
				fmt.Fprintf(cmd.OutOrStdout(),
					"\nWARNING: %d node(s) reported scan errors — an absent instance is not proof it has no data there\n",
					scanErrorNodes)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&allNodes, "all", false, "Query every node in the cluster")
	cmd.Flags().StringVar(&bucket, "bucket", "", "Filter to one instance's bucket (requires --root)")
	cmd.Flags().StringVar(&rootPath, "root", "", "Filter to one instance's root path (requires --bucket)")
	return cmd
}

// buildInstanceDataJSON keeps the machine-readable output node-addressable, including
// the nodes that did not answer.
func buildInstanceDataJSON(targets []client.Member, fanRes *client.FanoutResult) []map[string]any {
	out := make([]map[string]any, 0, len(targets))
	for i, m := range targets {
		nr := fanRes.Results[i]
		if !nr.OK {
			out = append(out, map[string]any{"node": m.ID, "ok": false, "error": fmt.Sprintf("%v", nr.Err)})
			continue
		}
		var raw any
		if jsonErr := json.Unmarshal(nr.Body, &raw); jsonErr != nil {
			out = append(out, map[string]any{"node": m.ID, "ok": false, "error": "invalid response"})
			continue
		}
		out = append(out, map[string]any{"node": m.ID, "ok": true, "report": raw})
	}
	return out
}
