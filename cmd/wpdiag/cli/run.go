package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/integrated"
)

func runCommand(output *string, code *int, out, errout io.Writer) *cobra.Command {
	runner := integrated.Standard()
	q := integrated.Request{Selection: d.Selection{Scope: "e2e", Window: d.Window{Mode: "live"}}, Limits: d.DefaultLimits()}
	var configPath, from, to, traceID string
	var sources, targets, imports []string
	var window, interval, budget time.Duration
	var planOnly, list, progress, all bool
	cmd := &cobra.Command{Use: "run", Short: "Plan and run selected source/stage diagnostics with original evidence and bounded association", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if *output != "text" && *output != "json" {
			return fmt.Errorf("invalid output")
		}
		if list {
			if *output == "json" {
				return json.NewEncoder(out).Encode(map[string]any{"schema_version": "wpdiag.modules/v1", "modules": runner.Catalog()})
			}
			for _, m := range runner.Catalog() {
				fmt.Fprintf(out, "%s: source=%s stage=%s profiles=%d\n", m.ID, m.Source, m.Stage, len(m.Profiles))
			}
			return nil
		}
		if len(sources) == 1 && sources[0] == "auto" {
			sources = nil
		}
		q.Selection.Sources = sources
		if q.Selection.Window.Mode == "live" && !cmd.Flags().Changed("window") {
			window = 30 * time.Second
		}
		if window%time.Millisecond != 0 || interval%time.Millisecond != 0 || budget%time.Millisecond != 0 {
			return fmt.Errorf("whole milliseconds required")
		}
		q.Selection.Window.DurationMS = window.Milliseconds()
		q.Selection.Window.IntervalMS = interval.Milliseconds()
		q.Limits.BudgetMS = budget.Milliseconds()
		for _, v := range []struct {
			raw    string
			target **time.Time
		}{{from, &q.Selection.Window.From}, {to, &q.Selection.Window.To}} {
			if v.raw != "" {
				t, err := time.Parse(time.RFC3339Nano, v.raw)
				if err != nil {
					return fmt.Errorf("invalid timestamp")
				}
				*v.target = &t
			}
		}
		q.Targets = map[string]d.Target{}
		for _, raw := range targets {
			parts := strings.SplitN(raw, "=", 2)
			if len(parts) != 2 {
				return fmt.Errorf("target requires source.field=value")
			}
			key := strings.SplitN(parts[0], ".", 2)
			if len(key) != 2 {
				return fmt.Errorf("target requires source.field=value")
			}
			t := q.Targets[key[0]]
			if !setTarget(&t, key[1], parts[1]) {
				return fmt.Errorf("unknown target field")
			}
			q.Targets[key[0]] = t
		}
		if traceID != "" {
			t := q.Targets["trace"]
			t.TraceID = traceID
			q.Targets["trace"] = t
		}
		if len(imports) > 0 {
			now := time.Now().UTC()
			report := integrated.Report{SchemaVersion: integrated.Schema, RunID: uuid.NewString(), StartedAt: now, FinishedAt: now, Status: "unsupported", Plan: integrated.Plan{Schema: "wpdiag.plan/v1", Request: q, Status: "unsupported", Problems: []d.Problem{{Code: "report_import_not_supported", Message: "Raw report-file import is not supported; this version composes in-process source reports and preserves their lineage"}}}, Summary: integrated.Summarize(cmd.Context(), nil)}
			body, exit, err := integrated.Encode(report)
			*code = exit
			if err != nil {
				return err
			}
			if *output == "json" {
				_, err = out.Write(body)
				return err
			}
			fmt.Fprintln(out, "unsupported: report_import_not_supported; no report files or sources were read")
			return nil
		}
		config, loadErr := d.LoadConfig(configPath)
		if loadErr != nil {
			config = d.Config{}
		}
		if planOnly {
			plan := runner.Plan(config, q)
			if loadErr != nil {
				plan.Problems = append(plan.Problems, d.Problem{Code: "configuration_unavailable", Message: "Cannot load source configuration; check --config (private values withheld)"})
				plan.Status = "invalid"
			}
			*code = d.ExitCode(plan.Status)
			if *output == "json" {
				return json.NewEncoder(out).Encode(plan)
			}
			printPlan(out, plan)
			return nil
		}
		var events chan integrated.Event
		var done chan struct{}
		if progress {
			events = make(chan integrated.Event, 32)
			done = make(chan struct{})
			go func() {
				defer close(done)
				encoder := json.NewEncoder(errout)
				for e := range events {
					_ = encoder.Encode(e)
				}
			}()
		}
		report := runner.Run(cmd.Context(), config, q, events)
		if loadErr != nil {
			report.Plan.Problems = append(report.Plan.Problems, d.Problem{Code: "configuration_unavailable", Message: "Cannot load source configuration; check --config (private values withheld)"})
			report.Status = "invalid"
		}
		if progress {
			close(events)
			<-done
		}
		body, exit, err := integrated.Encode(report)
		*code = exit
		if err != nil {
			return err
		}
		if *output == "json" {
			_, err = out.Write(body)
			return err
		}
		// Render the encoded (possibly compact budget-failure) report.
		if json.Unmarshal(body, &report) != nil {
			return fmt.Errorf("cannot decode run report")
		}
		printPlan(out, report.Plan)
		fmt.Fprintf(out, "\nRun %s: %s; completion is execution status, not cluster health.\n", report.RunID, report.Status)
		views := append([]integrated.View{}, report.Summary.Findings...)
		priority := func(v integrated.View) int {
			f, _ := integrated.Resolve(report, v.Reference)
			switch f.Severity {
			case "critical":
				return 0
			case "warning":
				return 1
			default:
				return 2
			}
		}
		// Keep stage order within each impact class; surface warnings before
		// missing-data findings when the default text view is truncated.
		sort.SliceStable(views, func(i, j int) bool { return priority(views[i]) < priority(views[j]) })
		for _, run := range report.Runs {
			fmt.Fprintf(out, "\n%s: %s (%s; %s window)\n", run.ModuleID, run.Report.Status, run.Role, run.Report.Selection.Window.Mode)
			for _, result := range run.Report.Results {
				for _, p := range result.Problems {
					fmt.Fprintf(out, "  %s: %s\n", p.Code, p.Message)
				}
			}
			printed, others := 0, 0
			for _, view := range views {
				if view.ModuleID == run.ModuleID {
					f, ok := integrated.Resolve(report, view.Reference)
					if !ok {
						continue
					}
					if !all && (printed >= 5 || d.Has([]string{"none", "info"}, f.Severity)) {
						others++
						continue
					}
					fmt.Fprintf(out, "  %s: %s (impact=%s, strength=%s)\n    %s\n    evidence: %d support / %d contradiction; unknowns=%d\n", f.RuleID, f.Outcome, f.Severity, f.Strength, f.Summary, len(f.Supporting), len(f.Contradicting), len(f.Unknown))
					if len(f.Unknown) > 0 {
						fmt.Fprintf(out, "    Uncertainty: %s\n", f.Unknown[0])
					}
					if len(f.NextChecks) > 0 {
						fmt.Fprintf(out, "    Next: %s\n", f.NextChecks[0])
					}
					printed++
				}
			}
			if others > 0 {
				fmt.Fprintf(out, "  %d other findings retained; use --all-findings or -o json.\n", others)
			}
		}
		for _, a := range report.Summary.Associations {
			fmt.Fprintf(out, "\nRelated evidence: %s trace=%s (%d original references; association is not causation)\n", a.Kind, a.Identity, len(a.Observations))
		}
		for _, gap := range report.Summary.Gaps {
			fmt.Fprintf(out, "  Gap %s: %s\n", gap.Code, gap.Detail)
		}
		return nil
	}}
	f := cmd.Flags()
	f.BoolVar(&list, "list", false, "List module IDs/profiles without configuration or backend access")
	f.BoolVar(&planOnly, "plan", false, "Preview locally validated module/profile/window/budget plan; no source calls")
	f.StringSliceVar(&sources, "sources", []string{"auto"}, "Sources: auto or wpcli,prometheus,trace,loki; auto omits unconfigured/disabled/inapplicable sources explicitly")
	f.StringSliceVar(&q.Modules, "modules", nil, "Exact IDs from run --list; mutually exclusive with stages")
	f.StringSliceVar(&q.Stages, "stages", nil, "Milvus metric stages: append,persistence (write) or consume,query (read); source overviews remain context")
	f.StringVar(&q.Selection.Stage, "stage", "", "Single stage alternative; inspect --stages")
	f.StringVar(&q.Selection.Scope, "scope", "e2e", "Scope: wp|milvus|e2e")
	f.StringVar(&q.Selection.Flow, "flow", "", "Flow: write|read (required)")
	f.StringVar(&q.Selection.Profile, "profile", "", "Exact profile; requires one selected module")
	f.StringVar(&configPath, "config", "wpdiag.yaml", "Source-owned private connection settings")
	f.StringVar(&q.Connection, "connection", "", "Shared connection name; otherwise each source must have exactly one connection")
	f.StringToStringVar(&q.Connections, "connections", nil, "Source-local connection overrides, e.g. prometheus=metrics,wpcli=native")
	f.StringArrayVar(&targets, "target", nil, "Complete source-specific target override: source.field=value (repeat for fields)")
	f.StringVar(&traceID, "trace-id", "", "Exact trace ID for trace source only; other sources keep their own target")
	f.StringVar(&q.Selection.Window.Mode, "mode", "live", "Window: snapshot|live|historical; unsupported modules remain explicit")
	f.DurationVar(&window, "window", 0, "Live duration starting now (default 30s)")
	f.DurationVar(&interval, "interval", 0, "Override child interval/gap limit; default native 1s and retained metrics 30s, bounded by window")
	f.StringVar(&from, "from", "", "Historical start, RFC3339")
	f.StringVar(&to, "to", "", "Historical end, RFC3339")
	f.BoolVar(&q.CurrentNative, "current-native", false, "Alongside historical results, explicitly collect a separately labeled current native snapshot")
	f.DurationVar(&budget, "budget", time.Minute, "Shared wall-clock budget, including concurrent live waits and analysis")
	f.Int64Var(&q.Limits.MaxInputBytes, "max-input-bytes", q.Limits.MaxInputBytes, "Total input byte allowance, partitioned across selected modules")
	f.Int64Var(&q.Limits.MaxOutputBytes, "max-output-bytes", q.Limits.MaxOutputBytes, "Total report output bytes; children receive partitioned bounds")
	f.IntVar(&q.Limits.MaxTargets, "max-targets", q.Limits.MaxTargets, "Targets per module (at most eight modules)")
	f.BoolVar(&progress, "progress", false, "Emit bounded advisory progress JSON to stderr")
	f.BoolVar(&all, "all-findings", false, "Print every original finding reference in text mode")
	f.StringArrayVar(&imports, "import", nil, "Reserved: raw report-file import is unsupported and rejected without reading files")
	for _, v := range []struct {
		name  string
		value *string
	}{{"deployment", &q.Selection.Target.Deployment}, {"instance", &q.Selection.Target.Instance}, {"node-id", &q.Selection.Target.NodeID}, {"log-id", &q.Selection.Target.LogID}, {"segment-id", &q.Selection.Target.SegmentID}, {"reader-id", &q.Selection.Target.ReaderID}, {"collection", &q.Selection.Target.Collection}, {"channel", &q.Selection.Target.Channel}, {"operation-id", &q.Selection.Target.OperationID}} {
		f.StringVar(v.value, v.name, "", "Common exact target; unsupported child filters are never silently dropped")
	}
	return cmd
}

func setTarget(t *d.Target, key, value string) bool {
	fields := map[string]*string{"deployment": &t.Deployment, "instance": &t.Instance, "node_id": &t.NodeID, "log_id": &t.LogID, "segment_id": &t.SegmentID, "reader_id": &t.ReaderID, "collection": &t.Collection, "channel": &t.Channel, "operation_id": &t.OperationID, "trace_id": &t.TraceID}
	if target := fields[key]; target != nil {
		*target = value
		return true
	}
	return false
}

func printPlan(out io.Writer, p integrated.Plan) {
	fmt.Fprintf(out, "Plan: %s; scope=%s flow=%s window=%s\n", p.Status, p.Request.Selection.Scope, p.Request.Selection.Flow, p.Request.Selection.Window.Mode)
	for _, problem := range p.Problems {
		fmt.Fprintf(out, "  %s: %s\n", problem.Code, problem.Message)
	}
	for _, e := range p.Entries {
		fmt.Fprintf(out, "  %s: %s; profile=%s v%d scope=%s mode=%s role=%s connection=%q input=%d output=%d targets=%d\n", e.ID, e.Status, e.Profile.ID, e.Profile.Version, e.Request.Selection.Scope, e.Request.Selection.Window.Mode, e.Role, e.Request.Connection, e.Request.Limits.MaxInputBytes, e.Request.Limits.MaxOutputBytes, e.Request.Limits.MaxTargets)
		for _, problem := range e.Problems {
			fmt.Fprintf(out, "    %s: %s\n", problem.Code, problem.Message)
		}
	}
	for _, p := range p.Omitted {
		fmt.Fprintf(out, "  Auto omission %s: %s\n", p.Code, p.Message)
	}
}
