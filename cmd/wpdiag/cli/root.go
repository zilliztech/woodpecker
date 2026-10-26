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
	"github.com/zilliztech/woodpecker/diagnostics/jaeger"
	"github.com/zilliztech/woodpecker/diagnostics/loki"
	"github.com/zilliztech/woodpecker/diagnostics/milvusappend"
	"github.com/zilliztech/woodpecker/diagnostics/milvuspersistence"
	"github.com/zilliztech/woodpecker/diagnostics/milvusquery"
	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
	"github.com/zilliztech/woodpecker/diagnostics/wpcli"
)

func Execute(ctx context.Context, args []string, out, errout io.Writer) int {
	engine, err := d.NewEngine(wpcli.Module{}, prometheus.Module{}, jaeger.Module{}, loki.Module{})
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
			catalog := engine.Catalog()
			catalog.Stages = []d.StageDescriptor{milvusquery.Descriptor(), milvusappend.Descriptor(), milvuspersistence.Descriptor()}
			return json.NewEncoder(out).Encode(catalog)
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
		fmt.Fprintln(out, "milvus append: available (#463), source=prometheus, scopes=milvus|e2e, flow=write, stage=append")
		fmt.Fprintln(out, "milvus persistence: available (#465), source=prometheus, scopes=milvus|e2e, flow=write, stage=persistence")
		fmt.Fprintln(out, "milvus query: available (#466), source=prometheus, scopes=milvus|e2e, flow=read, stage=query")
		return nil
	}})
	for _, source := range []string{"wpcli", "prometheus", "trace", "loki"} {
		short := "Diagnose using " + source
		group := &cobra.Command{Use: source, Short: short}
		if source == "trace" {
			group.Aliases = []string{"jaeger"}
		}
		group.AddCommand(diagnoseCommand(engine, source, &output, &code, out))
		root.AddCommand(group)
	}
	milvus := &cobra.Command{Use: "milvus", Short: "Independent Milvus business-stage diagnosis"}
	milvus.AddCommand(diagnosticCommand(engine, "prometheus", &output, &code, out, "query"))
	milvus.AddCommand(diagnosticCommand(engine, "prometheus", &output, &code, out, "append"))
	milvus.AddCommand(diagnosticCommand(engine, "prometheus", &output, &code, out, "persistence"))
	root.AddCommand(milvus)
	if root.ExecuteContext(ctx) != nil {
		fmt.Fprintln(errout, "Invalid command or arguments. Run wpdiag --help or wpdiag <source> diagnose --help.")
		return 2
	}
	return code
}

func diagnoseCommand(engine *d.Engine, source string, output *string, code *int, out io.Writer) *cobra.Command {
	return diagnosticCommand(engine, source, output, code, out, "")
}

func diagnosticCommand(engine *d.Engine, source string, output *string, code *int, out io.Writer, stage string) *cobra.Command {
	r := d.Request{Selection: d.Selection{Sources: []string{source}, Scope: "wp", Window: d.Window{Mode: "live"}}, Limits: d.DefaultLimits()}
	if stage != "" {
		r.Selection.Scope = "milvus"
		r.Selection.Flow = "read"
		r.Selection.Stage = stage
		if stage == "append" || stage == "persistence" {
			r.Selection.Flow = "write"
		}
	}
	selectedSource := source
	var list bool
	var configPath, from, to string
	var window, interval, budget time.Duration
	cmd := &cobra.Command{Use: "diagnose", Short: "Run a source profile with explicit scope, flow and observation mode", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if *output != "text" && *output != "json" {
			return fmt.Errorf("invalid output")
		}
		if stage != "" && list {
			if stage == "append" {
				if *output == "json" {
					return json.NewEncoder(out).Encode(map[string]any{"stage": milvusappend.Descriptor(), "signals": prometheus.AppendCatalog()})
				}
				for _, signal := range prometheus.AppendCatalog() {
					fmt.Fprintf(out, "%s: %s component=%s kind=%s threshold=%.3fms\n", signal.ID, signal.Metric, signal.Component, signal.Kind, signal.ThresholdMS)
				}
				return nil
			}
			if stage == "persistence" {
				if *output == "json" {
					return json.NewEncoder(out).Encode(map[string]any{"stage": milvuspersistence.Descriptor(), "signals": prometheus.PersistenceCatalog()})
				}
				for _, signal := range prometheus.PersistenceCatalog() {
					fmt.Fprintf(out, "%s: %s component=%s kind=%s threshold=%.3fms\n", signal.ID, signal.Metric, signal.Component, signal.Kind, signal.ThresholdMS)
				}
				return nil
			}
			if *output == "json" {
				return json.NewEncoder(out).Encode(map[string]any{"stage": milvusquery.Descriptor(), "signals": prometheus.QueryCatalog()})
			}
			for _, signal := range prometheus.QueryCatalog() {
				threshold := "n/a"
				if signal.Kind == "histogram" {
					threshold = fmt.Sprintf("%.3fms", signal.ThresholdMS)
				}
				fmt.Fprintf(out, "%s: %s component=%s kind=%s threshold=%s\n", signal.ID, signal.Metric, signal.Component, signal.Kind, threshold)
			}
			return nil
		}
		if r.Selection.Window.Mode == "live" {
			if !cmd.Flags().Changed("window") {
				window = 30 * time.Second
			}
			if !cmd.Flags().Changed("interval") {
				interval = time.Second
				if source == "prometheus" {
					interval = 30 * time.Second
				}
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
		var report d.Report
		if stage != "" {
			r.Selection.Sources = []string{selectedSource}
			if stage == "append" {
				report = milvusappend.Run(cmd.Context(), config, r)
			} else if stage == "persistence" {
				report = milvuspersistence.Run(cmd.Context(), config, r)
			} else {
				report = milvusquery.Run(cmd.Context(), config, r)
			}
		} else {
			report = engine.Run(cmd.Context(), config, r)
		}
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
				if stage != "" {
					continue
				}
				fmt.Fprintf(out, "  %s node=%q: %s (strength=%s, impact=%s)\n    %s\n    evidence: %d support / %d contradiction; unknown=%q\n", finding.RuleID, finding.Target.NodeID, finding.Outcome, finding.Strength, finding.Severity, finding.Summary, len(finding.Supporting), len(finding.Contradicting), finding.Unknown)
				if source == "loki" {
					for _, check := range finding.NextChecks {
						fmt.Fprintf(out, "    Next: %s\n", check)
					}
				}
			}
			fmt.Fprintf(out, "  Evidence: %d observations, %d findings, %d coverage gaps\n", len(result.Observations), len(result.Findings), len(result.Gaps))
			if result.Source == "trace" {
				for _, observation := range result.Observations {
					if observation.Kind != "jaeger_trace" {
						continue
					}
					var link struct {
						ID   string `json:"trace_id"`
						Path string `json:"jaeger_ui_path"`
					}
					if json.Unmarshal(observation.Data, &link) == nil && link.ID != "" {
						fmt.Fprintf(out, "  Trace %s: %s (relative to the configured Jaeger endpoint)\n", link.ID, link.Path)
					}
				}
			}
			if stage != "" || result.Source == "trace" {
				unavailable := map[string]bool{}
				for _, o := range result.Observations {
					if o.Status != "observed" {
						unavailable[o.ID] = true
					}
				}
				printed := 0
				for _, gap := range result.Gaps {
					if !unavailable[gap.ObservationID] {
						continue
					}
					if printed == 5 {
						fmt.Fprintln(out, "  Additional source gaps are retained in JSON.")
						break
					}
					fmt.Fprintf(out, "  Source gap %s: %s\n", gap.Code, gap.Detail)
					printed++
				}
			}
		}
		for _, a := range report.Assessments {
			fmt.Fprintf(out, "\nMilvus "+stage+" assessment %s v%d: %s\n", a.Profile, a.Version, a.Status)
			for _, f := range a.Findings {
				fmt.Fprintf(out, "  %s: %s (strength=%s, impact=%s)\n    deployment=%q instance=%q node=%q\n    %s\n", f.RuleID, f.Outcome, f.Strength, f.Severity, f.Target.Deployment, f.Target.Instance, f.Target.NodeID, f.Summary)
				for _, check := range f.NextChecks {
					fmt.Fprintf(out, "    Next: %s\n", check)
				}
			}
			for _, gap := range a.Gaps {
				fmt.Fprintf(out, "  Stage gap %s: %s\n", gap.Code, gap.Detail)
			}
		}
		return nil
	}}
	f := cmd.Flags()
	if stage != "" {
		cmd.Use = stage
		cmd.Short = "Diagnose query steps: completion, visibility, scheduling, execution and return"
		if stage == "append" {
			cmd.Short = "Diagnose append steps: Proxy, producer/quota, WAL/interceptors and separate WP context"
		} else if stage == "persistence" {
			cmd.Short = "Diagnose persistence steps: growing pressure, scheduling, upload, checkpoint and parallel WP maintenance"
		}
		f.StringVar(&selectedSource, "source", "prometheus", "Selected source (only prometheus is implemented; other requested sources fail explicitly)")
		f.BoolVar(&list, "list", false, "List the source metric catalog and stage capabilities without connecting")
	}
	f.StringVar(&configPath, "config", "wpdiag.yaml", "Independent source connections file (does not replace cli.yaml)")
	f.StringVar(&r.Connection, "connection", "", "Named connection in the selected source section")
	f.StringVar(&r.Selection.Scope, "scope", r.Selection.Scope, "Scope: wp|milvus|e2e; inspect capabilities")
	f.StringVar(&r.Selection.Flow, "flow", r.Selection.Flow, "Flow: write|read (Milvus append/persistence=write, query=read)")
	f.StringVar(&r.Selection.Stage, "stage", r.Selection.Stage, "Stage ID; Milvus append/persistence/query commands fix the corresponding stage")
	f.StringVar(&r.Selection.Profile, "profile", "", "Optional profile ID")
	f.StringVar(&r.Selection.Window.Mode, "mode", "live", "Window: snapshot|live|historical (source must support it)")
	f.DurationVar(&window, "window", 0, "Live duration starting now (default 30s in live mode)")
	intervalHelp := "Live sampling interval (default 1s in live mode)"
	if source == "prometheus" {
		intervalHelp = "Maximum tolerated retained-scrape gap (live default 30s); does not reconfigure or resample Prometheus"
	}
	f.DurationVar(&interval, "interval", 0, intervalHelp)
	f.StringVar(&from, "from", "", "Historical start, RFC3339; not supported by wpcli")
	f.StringVar(&to, "to", "", "Historical end, RFC3339; not supported by wpcli")
	f.DurationVar(&budget, "budget", time.Minute, "Total execution budget including discovery (max 10m)")
	f.Int64Var(&r.Limits.MaxInputBytes, "max-input-bytes", r.Limits.MaxInputBytes, "Maximum total source input bytes (native stderr separately capped at 64KiB)")
	f.Int64Var(&r.Limits.MaxOutputBytes, "max-output-bytes", r.Limits.MaxOutputBytes, "Maximum JSON report bytes; emits compact failure on overflow")
	f.IntVar(&r.Limits.MaxTargets, "max-targets", r.Limits.MaxTargets, "Maximum source targets: nodes/exporters/services (1..32)")
	if source == "trace" {
		f.StringVar(&r.Selection.Target.TraceID, "trace-id", "", "Exact nonzero 16/32-digit hexadecimal trace ID; snapshot or time-bounded selection")
		f.StringVar(&r.Selection.Target.OperationID, "request-id", "", "Exact request-ID tag value; configure request_id_tag (alias of trace --operation-id)")
	}
	for _, item := range []struct {
		name  string
		value *string
	}{{"deployment", &r.Selection.Target.Deployment}, {"instance", &r.Selection.Target.Instance}, {"node-id", &r.Selection.Target.NodeID}, {"log-id", &r.Selection.Target.LogID}, {"segment-id", &r.Selection.Target.SegmentID}, {"reader-id", &r.Selection.Target.ReaderID}, {"collection", &r.Selection.Target.Collection}, {"channel", &r.Selection.Target.Channel}, {"operation-id", &r.Selection.Target.OperationID}} {
		f.StringVar(item.value, item.name, "", "Explicit target identity; only filters declared by the profile are supported")
	}
	return cmd
}
