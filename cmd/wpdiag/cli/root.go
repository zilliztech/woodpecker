// Package cli provides the independent diagnostic command line frontend.
package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/wpcli"
)

func Execute(ctx context.Context, args []string, out, errout io.Writer) int {
	engine, err := d.NewEngine(wpcli.Module{})
	if err != nil {
		fmt.Fprintln(errout, "Cannot initialize diagnostic modules")
		return 1
	}
	code := 0
	root := &cobra.Command{Use: "wpdiag", Short: "Independent, bounded WP/Milvus diagnostic modules", SilenceUsage: true, SilenceErrors: true}
	root.SetOut(out)
	root.SetErr(errout)
	root.SetArgs(args)
	var output string
	root.PersistentFlags().StringVarP(&output, "output", "o", "text", "Output: text|json")
	root.AddCommand(&cobra.Command{Use: "capabilities", Short: "List implemented profiles and planned sources without connecting", Args: cobra.NoArgs, RunE: func(_ *cobra.Command, _ []string) error {
		if output == "json" {
			return json.NewEncoder(out).Encode(engine.Catalog())
		}
		if output != "text" {
			return fmt.Errorf("invalid output")
		}
		for _, source := range engine.Catalog().Sources {
			fmt.Fprintf(out, "%s: %s (#%d)\n", source.Source, source.Status, source.Issue)
			for _, p := range source.Profiles {
				fmt.Fprintf(out, "  %s v%d: scope=%s flow=%s stage=%s modes=%v targets=%v\n", p.ID, p.Version, p.Scope, p.Flow, p.Stage, p.Modes, p.TargetFields)
			}
		}
		return nil
	}})
	for _, source := range []string{"wpcli", "prometheus", "trace", "loki"} {
		short := "Diagnose using " + source
		if source != "wpcli" {
			short += " (not implemented; see capabilities)"
		}
		group := &cobra.Command{Use: source, Short: short}
		group.AddCommand(diagnoseCommand(engine, source, &output, &code, out))
		root.AddCommand(group)
	}
	if root.ExecuteContext(ctx) != nil {
		fmt.Fprintln(errout, "Invalid command or arguments. Run wpdiag --help or wpdiag <source> diagnose --help.")
		return 2
	}
	return code
}

func diagnoseCommand(engine *d.Engine, source string, output *string, code *int, out io.Writer) *cobra.Command {
	r := d.Request{Selection: d.Selection{Sources: []string{source}, Scope: "wp", Window: d.Window{Mode: "live"}}, Limits: d.DefaultLimits()}
	var configPath, from, to string
	var window, interval, budget time.Duration
	cmd := &cobra.Command{Use: "diagnose", Short: "Run a source profile with explicit scope, flow and observation mode", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if *output != "text" && *output != "json" {
			return fmt.Errorf("invalid output")
		}
		if r.Selection.Window.Mode == "live" {
			if !cmd.Flags().Changed("window") {
				window = 30 * time.Second
			}
			if !cmd.Flags().Changed("interval") {
				interval = time.Second
			}
		}
		if window%time.Millisecond != 0 || interval%time.Millisecond != 0 || budget%time.Millisecond != 0 {
			return fmt.Errorf("durations require whole milliseconds")
		}
		r.Selection.Window.DurationMS = window.Milliseconds()
		r.Selection.Window.IntervalMS = interval.Milliseconds()
		r.Limits.BudgetMS = budget.Milliseconds()
		for _, item := range []struct {
			value  string
			target **time.Time
		}{{from, &r.Selection.Window.From}, {to, &r.Selection.Window.To}} {
			if item.value != "" {
				parsed, err := time.Parse(time.RFC3339Nano, item.value)
				if err != nil {
					return fmt.Errorf("invalid timestamp")
				}
				*item.target = &parsed
			}
		}
		config, loadErr := d.LoadConfig(configPath)
		if loadErr != nil {
			config = d.Config{}
		}
		report := engine.Run(cmd.Context(), config, r)
		if loadErr != nil && len(report.Results) > 0 && report.Results[0].Status == "invalid" && report.Results[0].Problems[0].Code == "connection_not_found" {
			report.Results[0] = d.Failure(source, "invalid", "configuration_unavailable", loadErr.Error())
			report.Results[0].Connection = r.Connection
		}
		data, exit, err := d.EncodeReport(report)
		if err != nil {
			return fmt.Errorf("cannot encode report")
		}
		*code = exit
		if *output == "json" {
			_, err = out.Write(data)
			return err
		}
		if err = json.Unmarshal(data, &report); err != nil {
			return err
		}
		fmt.Fprintf(out, "Diagnosis: %s; scope=%s flow=%s stage=%s mode=%s\n", report.Status, report.Selection.Scope, report.Selection.Flow, report.Selection.Stage, report.Selection.Window.Mode)
		fmt.Fprintln(out, "Completion describes execution, not cluster health. Use -o json for evidence and lineage.")
		for _, result := range report.Results {
			for _, problem := range result.Problems {
				fmt.Fprintf(out, "  %s: %s\n", problem.Code, problem.Message)
			}
			for _, finding := range result.Findings {
				fmt.Fprintf(out, "  %s node=%q: %s (strength=%s, impact=%s)\n    %s\n    evidence: %d support / %d contradiction; unknown=%q\n", finding.RuleID, finding.Target.NodeID, finding.Outcome, finding.Strength, finding.Severity, finding.Summary, len(finding.Supporting), len(finding.Contradicting), finding.Unknown)
			}
			fmt.Fprintf(out, "  Evidence: %d observations, %d findings, %d coverage gaps\n", len(result.Observations), len(result.Findings), len(result.Gaps))
		}
		return nil
	}}
	f := cmd.Flags()
	f.StringVar(&configPath, "config", "wpdiag.yaml", "Independent source connections file (does not replace cli.yaml)")
	f.StringVar(&r.Connection, "connection", "", "Named connection in the selected source section")
	f.StringVar(&r.Selection.Scope, "scope", "wp", "Scope: wp|milvus|e2e; inspect capabilities")
	f.StringVar(&r.Selection.Flow, "flow", "", "Required flow: write|read")
	f.StringVar(&r.Selection.Stage, "stage", "", "Optional stage ID; omitted resolves a unique profile")
	f.StringVar(&r.Selection.Profile, "profile", "", "Optional profile ID")
	f.StringVar(&r.Selection.Window.Mode, "mode", "live", "Window: snapshot|live|historical (source must support it)")
	f.DurationVar(&window, "window", 0, "Live duration starting now (default 30s in live mode)")
	f.DurationVar(&interval, "interval", 0, "Live sampling interval (default 1s in live mode)")
	f.StringVar(&from, "from", "", "Historical start, RFC3339; not supported by wpcli")
	f.StringVar(&to, "to", "", "Historical end, RFC3339; not supported by wpcli")
	f.DurationVar(&budget, "budget", time.Minute, "Total execution budget including discovery (max 10m)")
	f.Int64Var(&r.Limits.MaxInputBytes, "max-input-bytes", r.Limits.MaxInputBytes, "Maximum native stdout bytes; stderr separately capped at 64KiB")
	f.Int64Var(&r.Limits.MaxOutputBytes, "max-output-bytes", r.Limits.MaxOutputBytes, "Maximum JSON report bytes; emits compact failure on overflow")
	f.IntVar(&r.Limits.MaxTargets, "max-targets", r.Limits.MaxTargets, "Maximum discovered nodes (1..32)")
	for _, item := range []struct {
		name  string
		value *string
	}{{"deployment", &r.Selection.Target.Deployment}, {"instance", &r.Selection.Target.Instance}, {"node-id", &r.Selection.Target.NodeID}, {"log-id", &r.Selection.Target.LogID}, {"segment-id", &r.Selection.Target.SegmentID}, {"reader-id", &r.Selection.Target.ReaderID}, {"collection", &r.Selection.Target.Collection}, {"channel", &r.Selection.Target.Channel}, {"operation-id", &r.Selection.Target.OperationID}} {
		f.StringVar(item.value, item.name, "", "Explicit target identity; only filters declared by the profile are supported")
	}
	return cmd
}
