package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/cmd/wpcli/diagnosis"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
)

func newDiagnoseCommand() *cobra.Command {
	o := diagnosis.Options{
		Scenario: "all", Mode: "live", Window: 30 * time.Second, Interval: time.Second, Budget: time.Minute,
		Limits: diagnosis.Limits{MaxNodes: 8, MaxSamples: 31, MaxOps: 100, MaxSeries: 500, ResponseBytes: 2 << 20},
	}
	var list, failOnFindings bool
	var logName string
	var segmentID int64
	var metadataFlags metaEtcdFlags
	cmd := &cobra.Command{
		Use:   "diagnose [node]",
		Short: "Diagnose native WP write pressure and read/write stalls with bounded live evidence",
		Long: `Diagnose current WP service state or sample a live window using existing
metrics/Admin APIs. With no node argument, inspect all discovered nodes within
the target budget. Reuses cli.yaml, --context, --endpoint and node mappings.

Use --scenario write for append, flush, disk admission and explicit replica
checks; stuck-write/stuck-read retain their original focused contracts.
Snapshot mode records current admission evidence but cannot establish a sustained
stall or buildup. --window starts sampling now;
historical Prometheus, trace and Loki queries belong to the separate tool.

Exit 0 means evaluation completed, including inconclusive/conflicting findings;
it is not a health assertion. --fail-on-findings returns 8 for warning/critical findings.
Required collection failure returns 1 (all failed) or 6 (partial), cancellation 7,
and exhausted wall-clock budget 5. JSON/YAML evidence is retained on these errors.`,
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) > 1 {
				return wperrors.NewUsageError("diagnose accepts at most one node")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if Globals.Output != "table" && Globals.Output != "wide" && Globals.Output != "json" && Globals.Output != "yaml" {
				return wperrors.NewUsageError("diagnose supports table, wide, json or yaml output")
			}
			if list {
				return renderDiagnoseCatalog(cmd)
			}
			o.RequestTimeout, o.Concurrency = Globals.Timeout, Globals.Concurrency
			if err := o.Validate(); err != nil {
				return wperrors.NewUsageError(err.Error())
			}
			if logName != "" || cmd.Flags().Changed("segment") {
				if !cmd.Flags().Changed("segment") || logName == "" || (o.Scenario != "write" && o.Scenario != "all") || metadataFlags.etcdEndpoints == "" || metadataFlags.metaPrefix == "" {
					return wperrors.NewUsageError("replica diagnosis requires --scenario write (or all), --log-name, --segment, explicit --etcd and --meta-prefix for the WP-client namespace")
				}
				if err := validateMetaTarget("segment", []string{logName, fmt.Sprint(segmentID)}); err != nil {
					return err
				}
			}
			if logName == "" && (cmd.Flags().Changed("etcd") || cmd.Flags().Changed("meta-prefix")) {
				return wperrors.NewUsageError("diagnose metadata flags require --log-name and --segment")
			}
			// Resolve exactly the existing connection precedence, but discovery must
			// share the diagnosis budget and never echo endpoint credentials.
			res, err := resolveConfiguration()
			if err != nil {
				return err
			}
			if _, err := client.ValidateNodeAdminURLs(map[string]string{"seed": res.Context.Endpoint}); err != nil {
				return wperrors.NewUsageError("diagnose requires an HTTP(S) seed origin without credentials, path, query or fragment")
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			ctx, cancel := context.WithTimeout(ctx, o.Budget)
			defer cancel()
			report := diagnosis.NewReport(o, res.ContextName)
			members, err := res.Client.GetMemberlistContext(ctx, o.Limits.ResponseBytes)
			if err != nil {
				report.Status, report.FinishedAt = "failed", time.Now().UTC()
				if errors.Is(ctx.Err(), context.Canceled) {
					report.Status = "cancelled"
				} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					report.Status = "budget_exceeded"
				}
				report.Gaps = append(report.Gaps, diagnosis.Gap{Code: "discovery_failed", Detail: "Cannot read memberlist within the response/time budget; inspect WP connection settings"})
			} else {
				nodes, err := diagnoseNodes(res.Client, members, args, o.Limits.MaxNodes)
				if err != nil {
					return err
				}
				if logName != "" {
					o.MetadataRequired = true
					collectDiagnosisQuorum(ctx, &report, &o, &metadataFlags, res.Client, members, logName, segmentID)
				}
				diagnosis.Collect(ctx, &report, nodes, o)
			}
			if err := renderDiagnosis(cmd, report); err != nil {
				return err
			}
			return diagnosisExit(report, failOnFindings)
		},
	}
	cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error { return wperrors.NewUsageError(err.Error()) })
	f := cmd.Flags()
	f.StringVar(&o.Scenario, "scenario", o.Scenario, "Native scenario: all|write|stuck-write|stuck-read")
	f.StringVar(&o.Mode, "mode", o.Mode, "Observation mode: live|snapshot; no historical retrieval")
	f.DurationVar(&o.Window, "window", o.Window, "Live observation window starting now (max 5m)")
	f.DurationVar(&o.Interval, "interval", o.Interval, "Interval between live sampling rounds")
	f.DurationVar(&o.Budget, "budget", o.Budget, "Whole-command wall-clock budget including discovery (max 10m)")
	f.IntVar(&o.Limits.MaxNodes, "max-nodes", o.Limits.MaxNodes, "Maximum discovered targets; select a node if exceeded")
	f.IntVar(&o.Limits.MaxSamples, "max-samples", o.Limits.MaxSamples, "Maximum sampling rounds per node (2..121)")
	f.IntVar(&o.Limits.MaxOps, "max-ops", o.Limits.MaxOps, "Maximum ops per node per sample (1..1000)")
	f.IntVar(&o.Limits.MaxWriters, "max-writers", 16, "Maximum writer detail requests per node per sample (1..64; write/all)")
	f.StringVar(&logName, "log-name", "", "Explicit WP-client metadata log name for replica checks; requires --segment, --etcd and --meta-prefix")
	f.Int64Var(&segmentID, "segment", 0, "Explicit metadata segment ID for replica checks")
	f.IntVar(&o.Limits.MaxSeries, "max-series", o.Limits.MaxSeries, "Maximum retained completion-counter series per sample")
	f.Int64Var(&o.Limits.ResponseBytes, "max-response-bytes", o.Limits.ResponseBytes, "Maximum bytes per discovery/source response")
	f.BoolVar(&list, "list", false, "List the implemented typed rule contracts without connecting")
	f.BoolVar(&failOnFindings, "fail-on-findings", false, "Return exit 8 for warning/critical findings after rendering the report")
	metadataFlags.register(cmd)
	return cmd
}

func diagnoseNodes(c *client.Client, members *client.Memberlist, args []string, maxNodes int) ([]diagnosis.Node, error) {
	selected := members.Members
	if len(args) > 0 {
		member, ok := c.ResolveMember(members, args[0])
		if !ok {
			return nil, wperrors.NewTargetNotFoundError(args[0])
		}
		selected = []client.Member{member}
	}
	if len(selected) == 0 {
		return nil, wperrors.NewNetworkError("no WP nodes discovered")
	}
	if len(selected) > maxNodes {
		return nil, wperrors.NewUsageError(fmt.Sprintf("discovered %d nodes exceeds --max-nodes=%d; select one node or explicitly raise the budget", len(selected), maxNodes))
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].ID < selected[j].ID })
	nodes := make([]diagnosis.Node, 0, len(selected))
	seen := map[string]bool{}
	for _, member := range selected {
		if member.ID == "" || seen[member.ID] {
			return nil, wperrors.NewNetworkError("memberlist has missing or duplicate node identities")
		}
		seen[member.ID] = true
		identity := "explicit_unverified"
		for _, discovered := range members.Members {
			if discovered.ID == member.ID {
				identity = "discovered"
			}
		}
		nodes = append(nodes, diagnosis.Node{Target: diagnosis.Target{NodeID: member.ID, Cluster: member.ClusterName, Identity: identity}, URL: c.PeerAdminURL(member)})
	}
	return nodes, nil
}

func diagnosisExit(report diagnosis.Report, failOnFindings bool) error {
	switch report.Status {
	case "failed":
		return wperrors.NewNetworkError("diagnosis collection failed; inspect the retained report")
	case "partial":
		return &wperrors.CLIError{Type: wperrors.TypeStrictPartial, Message: "required diagnosis sources were only partially collected; inspect the report"}
	case "cancelled":
		return wperrors.NewUserAbortError()
	case "budget_exceeded":
		return &wperrors.CLIError{Type: wperrors.TypeWaitTimeout, Message: "diagnosis wall-clock budget exhausted; partial evidence retained"}
	}
	if failOnFindings {
		for _, f := range report.Findings {
			if f.Severity == "warning" || f.Severity == "critical" {
				return wperrors.NewYellowFindingError("WP diagnostic finding; inspect supporting evidence and limitations")
			}
		}
	}
	return nil
}

func renderDiagnoseCatalog(cmd *cobra.Command) error {
	if Globals.Output == "json" || Globals.Output == "yaml" {
		value := map[string]any{"schema_version": diagnosis.SchemaVersion, "rules": diagnosis.Rules()}
		if Globals.Output == "yaml" {
			return renderDiagnosisYAML(cmd, value)
		}
		return output.RenderJSON(cmd.OutOrStdout(), value)
	}
	var rows [][]string
	for _, rule := range diagnosis.Rules() {
		rows = append(rows, []string{rule.ID, rule.Scenario, rule.Flow, rule.Stage})
	}
	return output.RenderRowTable(cmd.OutOrStdout(), []string{"RULE", "SCENARIO", "FLOW", "STAGE"}, rows)
}

func renderDiagnosis(cmd *cobra.Command, report diagnosis.Report) error {
	if Globals.Output == "json" {
		return output.RenderJSON(cmd.OutOrStdout(), report)
	}
	if Globals.Output == "yaml" {
		return renderDiagnosisYAML(cmd, report)
	}
	w := cmd.OutOrStdout()
	fmt.Fprintf(w, "WP diagnosis %s — %s (%s)\n", report.RunID, report.Status, report.Mode)
	fmt.Fprintf(w, "Observed %s to %s; requested window %dms. Collection completion is not a health verdict.\n", report.StartedAt.Format(time.RFC3339Nano), report.FinishedAt.Format(time.RFC3339Nano), report.WindowMS)
	for _, f := range report.Findings {
		fmt.Fprintf(w, "\n%s / %s: %s [%s, impact=%s]\n  %s\n", f.Scope.NodeID, f.RuleID, f.Outcome, f.Strength, f.Severity, f.Summary)
		if f.Scope.Operation != "" {
			fmt.Fprintf(w, "  Operation: %s; tenant: %s\n", f.Scope.Operation, f.Scope.Tenant)
		}
		if f.Scope.LogID != nil {
			fmt.Fprintf(w, "  Log: %d\n", *f.Scope.LogID)
		}
		if f.Scope.SegmentID != nil {
			fmt.Fprintf(w, "  Segment: %d\n", *f.Scope.SegmentID)
		}
		fmt.Fprintf(w, "  Evidence references: %d supporting, %d contradicting (IDs in JSON).\n  Unknown: %v\n", len(f.Supporting), len(f.Contradicting), f.Unknown)
		for _, check := range f.NextChecks {
			fmt.Fprintf(w, "  Next: %s\n", check)
		}
	}
	for i, gap := range report.Gaps {
		if i == 10 {
			fmt.Fprintln(w, "  Additional coverage gaps are retained in JSON.")
			break
		}
		fmt.Fprintf(w, "  Gap %s/%s: %s\n", gap.NodeID, gap.Code, gap.Detail)
	}
	fmt.Fprintf(w, "\nEvidence: %d observations; coverage gaps: %d. Use -o json for the complete versioned report.\n", len(report.Observations), len(report.Gaps))
	return nil
}

func renderDiagnosisYAML(cmd *cobra.Command, value any) error {
	// Preserve JSON contract field names in YAML without maintaining a second schema.
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var normalized any
	if err := yaml.Unmarshal(body, &normalized); err != nil {
		return err
	}
	return output.RenderYAML(cmd.OutOrStdout(), normalized)
}
