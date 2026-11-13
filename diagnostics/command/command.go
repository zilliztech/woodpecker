package command

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/checks"
)

type Options struct {
	Context func() string
	Output  func() string
}
type Error struct {
	Code    int
	Message string
}

func (e *Error) Error() string { return e.Message }
func (e *Error) ExitCode() int { return e.Code }

type frontend struct {
	metricsRules, traceRules                            string
	options                                             Options
	path, target, from, to, details, traceID, requestID string
	client, server, bindingsFile                        string
	scope                                               checks.Scope
	window, budget                                      time.Duration
	progress, fail                                      bool
}

func NewCommand(options Options) *cobra.Command {
	f := &frontend{options: options}
	c := &cobra.Command{Use: "diag", Short: "Read-only Metrics/Trace checks and WAL scenario summaries", SilenceUsage: true, SilenceErrors: true}
	c.Long = "Run one source-local check, or a fixed list of Metrics/Trace checks grouped by WAL step.\nScenarios summarize independent results without cross-source inference or model calls.\nConnections come from cli.yaml. Open the same capabilities with wp webui."
	p := c.PersistentFlags()
	p.StringVar(&f.metricsRules, "metrics-rules", "", "Metrics rules (default: diag-metrics.yaml beside cli.yaml)")
	p.StringVar(&f.traceRules, "trace-rules", "", "Trace rules (default: diag-trace.yaml beside cli.yaml)")
	p.StringVar(&f.path, "config", "", "Shared cli.yaml (default: normal wp configuration discovery)")
	p.StringVar(&f.client, "client", "", "Deprecated; use explicit request scope")
	p.StringVar(&f.server, "server", "", "Deprecated; use explicit request scope")
	p.StringVar(&f.bindingsFile, "bindings-file", "", "Explicit Client/Server sources and scope as a JSON file; no named target required")
	p.StringVar(&f.target, "target", "", "Deprecated; use context sources and explicit scope")
	for _, name := range []string{"target", "client", "server"} {
		_ = p.MarkHidden(name)
	}
	p.StringVar(&f.scope.LogNamespace, "log-namespace", "", "Optional exact client log_ns label")
	p.StringVar(&f.scope.LogID, "log-id", "", "Optional exact Log ID")
	p.StringVar(&f.scope.ReaderName, "reader-name", "", "Optional reader name")
	p.StringVar(&f.scope.NodeID, "node-id", "", "Exact server node_id when required by a check")
	p.StringVar(&f.scope.Path, "wal-path", "", "Optional exact WAL filesystem path")
	p.StringVar(&f.scope.TraceService, "trace-service", "", "Optional Trace service override")
	p.StringVar(&f.from, "from", "", "Incident start, RFC3339 with timezone; pair with --to")
	p.StringVar(&f.to, "to", "", "Incident end, RFC3339 with timezone; accepts --window for historical lookback")
	p.DurationVar(&f.window, "window", 15*time.Minute, "Retained lookback ending now or at --to; not live sampling")
	p.DurationVar(&f.budget, "budget", time.Minute, "Total execution budget, 1s..10m")
	p.StringVar(&f.details, "details", "abnormal", "Detail: none|abnormal|all; every JSON result retains its basic fields")
	p.StringVar(&f.traceID, "trace-id", "", "Exact Trace ID for Trace collection")
	p.StringVar(&f.requestID, "request-id", "", "Exact request ID; requires a configured Trace request_id_tag")
	p.BoolVar(&f.progress, "progress", false, "Advisory JSON progress on stderr; stdout remains the result")
	p.BoolVar(&f.fail, "fail-on-findings", false, "Exit 8 if a completed run has abnormal checks")
	c.SetFlagErrorFunc(func(cmd *cobra.Command, _ error) error {
		return f.invalidArgs(cmd, "Invalid option; inspect --help for supported parameters")
	})
	c.RunE = func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return cmd.Help()
		}
		return f.invalidArgs(cmd, "Choose metrics check, trace check, scenario or catalog")
	}
	c.AddCommand(f.catalog())
	for _, source := range []string{"metrics", "trace"} {
		g := &cobra.Command{Use: source, Short: "Independent " + source + " checks"}
		g.RunE = func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Help()
			}
			return f.invalidArgs(cmd, "Use check <name> for this source")
		}
		g.AddCommand(f.check(source))
		c.AddCommand(g)
	}
	c.AddCommand(f.scenario())
	return c
}

func (f *frontend) output() string {
	if f.options.Output != nil {
		return f.options.Output()
	}
	return "text"
}

func (f *frontend) load() (d.Config, string, error) {
	name := ""
	if f.options.Context != nil {
		name = f.options.Context()
	}
	if f.target != "" || f.client != "" || f.server != "" {
		return d.Config{}, "", fmt.Errorf("saved diagnostic targets/client profiles are no longer used; select --context and pass scope flags or --bindings-file")
	}

	if f.bindingsFile != "" {
		file, abs, err := LoadDocument(f.path)
		if err != nil {
			return d.Config{}, "", err
		}
		cfg, err := DiagnosticsConfig(file, abs, map[string]string{"metrics": f.metricsRules, "trace": f.traceRules})
		return cfg, "", err
	}
	c, target, err := Load(f.path, name, map[string]string{"metrics": f.metricsRules, "trace": f.traceRules})
	if f.target != "" {
		target = f.target
	}
	return c, target, err
}

func (f *frontend) windowRange(cmd *cobra.Command) (time.Time, time.Time, error) {
	var from, to time.Time
	var err error
	if f.from != "" {
		if f.to == "" || cmd.Flags().Changed("window") {
			return from, to, fmt.Errorf("use --from/--to together, or --window with optional --to")
		}
		from, err = time.Parse(time.RFC3339Nano, f.from)
		if err != nil {
			return from, to, fmt.Errorf("--from must be RFC3339 with timezone")
		}
	}
	if f.to != "" {
		to, err = time.Parse(time.RFC3339Nano, f.to)
		if err != nil {
			return from, to, fmt.Errorf("--to must be RFC3339 with timezone")
		}
	} else {
		to = time.Now().UTC().Add(-time.Second)
	}
	if from.IsZero() {
		if f.window <= 0 {
			return from, to, fmt.Errorf("--window must be positive")
		}
		from = to.Add(-f.window)
	}
	return from.UTC(), to.UTC(), nil
}

func (f *frontend) execute(cmd *cobra.Command, q checks.Request) error {
	q.Details, q.TraceID, q.RequestID, q.BudgetMS = f.details, f.traceID, f.requestID, f.budget.Milliseconds()
	var r checks.Report
	if f.output() != "json" && f.output() != "text" && f.output() != "table" && f.output() != "wide" {
		return &Error{2, "diag supports text (default) or json output"}
	}
	from, to, err := f.windowRange(cmd)
	problem := "invalid_window"
	if err == nil && (f.budget < time.Second || f.budget > 10*time.Minute) {
		err = fmt.Errorf("--budget must be between 1s and 10m")
		problem = "invalid_budget"
	}
	q.From, q.To = from, to
	if err != nil {
		r = checks.ErrorReport(q, problem, err.Error())
	} else {
		cfg, target, loadErr := f.load()
		q.Target = target
		if loadErr == nil {
			if f.target != "" {
				loadErr = fmt.Errorf("use Client/Server bindings or --target, not both")
			} else {
				q.Bindings, loadErr = f.bindings(cfg)
				q.Target = ""
			}
		}
		if loadErr != nil {
			r = checks.ErrorReport(q, "configuration_invalid", loadErr.Error())
		} else {
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			var events chan checks.Event
			var done chan struct{}
			if f.progress {
				events = make(chan checks.Event, 256)
				done = make(chan struct{})
				go func() {
					defer close(done)
					e := json.NewEncoder(cmd.ErrOrStderr())
					for v := range events {
						_ = e.Encode(v)
					}
				}()
			}
			r = checks.Run(ctx, cfg, q, events)
			if cacheErr := checks.RememberInputs(cfg, r); cacheErr != nil {
				fmt.Fprintln(cmd.ErrOrStderr(), "Warning: could not save temporary input candidates")
			}
			if events != nil {
				close(events)
				<-done
			}
		}
	}
	body := checks.Encode(r)
	if json.Unmarshal(body, &r) != nil {
		return &Error{1, "cannot decode bounded diagnostic result"}
	}
	if f.output() == "json" {
		_, err = cmd.OutOrStdout().Write(body)
	} else {
		err = RenderText(cmd.OutOrStdout(), r)
	}
	if err != nil {
		return err
	}
	if code := checks.ExitCode(r, f.fail); code != 0 {
		return &Error{code, "diagnosis " + r.Status + "; inspect the structured result for the cause"}
	}
	return nil
}

func (f *frontend) check(source string) *cobra.Command {
	var threshold float64
	c := &cobra.Command{Use: "check <name>", Short: "Run one check; no implicit execution of other checks", RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return f.invalidArgs(cmd, "check requires exactly one name")
		}
		q := checks.Request{Source: source, Check: args[0]}
		if cmd.Flags().Changed("threshold") {
			q.Threshold = &threshold
		}
		return f.execute(cmd, q)
	}}
	c.Flags().Float64Var(&threshold, "threshold", 0, "Override this check threshold in native units (Trace durations use ms; error counts allow zero)")
	return c
}

func (f *frontend) scenario() *cobra.Command {
	return &cobra.Command{Use: "scenario <name|all>", Short: "Run related Metrics/Trace checks and summarize their unchanged results by WAL step", RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return f.invalidArgs(cmd, "scenario requires one name or all")
		}
		return f.execute(cmd, checks.Request{Scenario: args[0]})
	}}
}

func (f *frontend) invalidArgs(cmd *cobra.Command, message string) error {
	r := checks.ErrorReport(checks.Request{Details: f.details}, "invalid_arguments", message)
	if f.output() == "json" {
		_, _ = cmd.OutOrStdout().Write(checks.Encode(r))
	} else {
		_ = RenderText(cmd.OutOrStdout(), r)
	}
	return &Error{2, message}
}

func (f *frontend) catalog() *cobra.Command {
	var source, name string
	c := &cobra.Command{Use: "catalog", Short: "List implemented checks, formulas, references and scenario manifests; no source/model queries", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		cfg := d.Config{}
		configured := f.path != "" || os.Getenv("WOODPECKER_CLI_CONFIG") != "" || os.Getenv(config.SessionConfigEnv) != "" || f.metricsRules != "" || f.traceRules != ""
		for _, path := range config.DefaultConfigPaths() {
			if _, err := os.Stat(path); err == nil {
				configured = true
				break
			}
		}
		if configured {
			var err error
			cfg, _, err = f.load()
			if err != nil {
				r := checks.ErrorReport(checks.Request{}, "configuration_invalid", err.Error())
				if f.output() == "json" {
					_, _ = cmd.OutOrStdout().Write(checks.Encode(r))
				} else {
					_ = RenderText(cmd.OutOrStdout(), r)
				}
				return &Error{12, err.Error()}
			}
		}
		s, err := checks.LoadSettings(cfg)
		if err != nil {
			return err
		}
		if source != "" && source != "metrics" && source != "trace" {
			return f.invalidArgs(cmd, "catalog source must be metrics or trace")
		}
		list := []checks.Definition{}
		for _, def := range checks.Definitions(s.Policy) {
			if (source == "" || source == def.Source) && (name == "" || name == def.Name || strings.EqualFold(name, def.ID)) {
				list = append(list, def)
			}
		}
		if name != "" && len(list) == 0 {
			return f.invalidArgs(cmd, "unknown check")
		}
		catalog := checks.Catalog{SchemaVersion: "wp.diag.catalog/v1", Checks: list, Scenarios: checks.Scenarios(s.Policy)}
		if f.output() == "json" {
			e := json.NewEncoder(cmd.OutOrStdout())
			e.SetIndent("", "  ")
			return e.Encode(catalog)
		}
		w := cmd.OutOrStdout()
		for _, def := range list {
			fmt.Fprintf(w, "%-4s %-7s %-34s %s\n", def.ID, def.Source, def.Name, def.Step)
			if name != "" {
				fmt.Fprintf(w, "  Meaning: %s\n  Source: %s\n  Formula: %s\n  Rule: %s\n  Limits: %s\n", def.Description.Meaning, strings.Join(def.Description.Metrics, ", "), def.Description.Formula, def.Description.Criterion, def.Limits)
				fmt.Fprintf(w, "  Assessment: %s", def.Assessment.Mode)
				if def.Assessment.Threshold != nil {
					fmt.Fprintf(w, " · %g %s", *def.Assessment.Threshold, def.Assessment.Unit)
				}
				fmt.Fprintf(w, "\n  Rule source: %s\n", def.Assessment.ConfiguredBy)
			}
		}
		if name == "" {
			fmt.Fprintln(w, "\nScenarios:")
			for _, s := range catalog.Scenarios {
				fmt.Fprintf(w, "  %-15s %s\n", s.ID, s.Title)
			}
		}
		return nil
	}}
	c.Flags().StringVar(&source, "source", "", "metrics|trace")
	c.Flags().StringVar(&name, "check", "", "Check name or stable ID")
	return c
}

func RenderText(w io.Writer, r checks.Report) error {
	title := r.Request.Scenario
	if title == "" {
		title = r.Request.Source + " / " + r.Request.Check
	}
	fmt.Fprintf(w, "Diagnosis: %s · %s\nTarget: %s\n", title, r.Status, r.Target.Label)
	if !r.Request.From.IsZero() {
		fmt.Fprintf(w, "Window: %s → %s\n", r.Request.From.Format(time.RFC3339Nano), r.Request.To.Format(time.RFC3339Nano))
	}
	for _, s := range []struct {
		name string
		v    checks.SourceSummary
	}{{"Metrics", r.Summary.Metrics}, {"Trace", r.Summary.Trace}} {
		if r.Request.Source != "" && !strings.EqualFold(r.Request.Source, s.name) {
			continue
		}
		if !s.v.Configured {
			fmt.Fprintf(w, "%s: Not configured — skipped\n", s.name)
			continue
		}
		fmt.Fprintf(w, "%s: %s · %d checks · %d abnormal · %d unassessed · %d failed · %d skipped\n", s.name, s.v.Status, s.v.Checks, s.v.Abnormal, s.v.Inconclusive, s.v.Failed, s.v.Skipped)
		if s.name == "Trace" {
			fmt.Fprintf(w, "  %d matched traces · %d with abnormal spans\n", s.v.MatchedTraces, s.v.AbnormalTraces)
		}
	}
	for _, p := range r.Problems {
		fmt.Fprintf(w, "\n%s: %s\n", p.Code, p.Message)
	}
	for _, item := range r.Checks {
		if r.Request.Details != "all" && (item.Status != "abnormal" && item.Execution == "completed" || item.Code == "source_not_configured") {
			continue
		}
		fmt.Fprintf(w, "\n[%s] %s / %s — %s\n", item.Source, item.Step, item.Name, item.StatusLabel)
		fmt.Fprintf(w, "  Observed: %s\n  Rule: %s\n", item.Value, item.Rule)
		if item.Execution != "completed" || item.Status == "unknown" {
			fmt.Fprintf(w, "  Reason: %s (%s)\n", item.Note, item.Code)
		}
		if item.Inspect != nil && r.Request.Details != "none" {
			if len(item.Inspect.Anomalies) > 0 {
				row := item.Inspect.Anomalies[0]
				if row.FromMS > 0 {
					fmt.Fprintf(w, "  Time: %s → %s\n", time.UnixMilli(row.FromMS).UTC().Format(time.RFC3339Nano), time.UnixMilli(row.ToMS).UTC().Format(time.RFC3339Nano))
				}
			}
			if item.Source == "metrics" {
				metrics := []string{}
				for _, m := range item.Inspect.Metrics {
					metrics = append(metrics, strings.SplitN(m, "{", 2)[0])
				}
				fmt.Fprintf(w, "  Metric: %s\n", strings.Join(metrics, ", "))
			} else {
				for _, span := range item.Inspect.Spans {
					if span.Abnormal || r.Request.Details == "all" {
						fmt.Fprintf(w, "  Trace: %s · %s · %g ms · %s\n", span.TraceID, span.Operation, span.DurationMS, span.From.Format(time.RFC3339Nano))
						break
					}
				}
			}
			if r.Request.Details == "all" {
				fmt.Fprintf(w, "  Meaning: %s\n  Formula: %s\n", item.Inspect.Meaning, item.Inspect.Formula)
				for _, query := range item.Queries {
					fmt.Fprintf(w, "  Query: %s\n", query.Expression)
				}
			}
		}
	}
	for _, gap := range r.Coverage {
		fmt.Fprintf(w, "\nCoverage: %s\n", gap)
	}
	fmt.Fprintln(w, "\nRule matches are initial observations. Missing evidence is not a pass.")
	return nil
}

func (f *frontend) bindings(c d.Config) (*checks.Bindings, error) {
	if f.bindingsFile != "" {
		if f.client != "" || f.server != "" {
			return nil, fmt.Errorf("use --bindings-file or named --client/--server configurations")
		}
		file, err := os.Open(filepath.Clean(f.bindingsFile))
		if err != nil {
			return nil, fmt.Errorf("cannot open bindings JSON: %w", fs.ErrNotExist)
		}
		defer file.Close()
		body, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
		if err != nil || len(body) > 64<<10 {
			return nil, fmt.Errorf("bindings JSON must be at most 64 KiB")
		}
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		var b checks.Bindings
		if dec.Decode(&b) != nil {
			return nil, fmt.Errorf("invalid Client/Server bindings JSON")
		}
		var rest any
		if dec.Decode(&rest) != io.EOF {
			return nil, fmt.Errorf("bindings JSON must contain one object")
		}
		return &b, nil
	}
	settings, err := checks.LoadSettings(c)
	if err != nil {
		return nil, err
	}
	t := settings.Targets[c.DefaultTarget]
	client, server := f.scope, f.scope
	for _, v := range []*checks.Scope{&client, &server} {
		v.Context = t.Context
		v.Metrics = t.Prometheus
		v.Trace = t.Trace
	}
	return &checks.Bindings{Client: &client, Server: &server}, nil
}
