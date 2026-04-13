package cmd

import (
	"context"
	"fmt"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/internal/prom"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
)

// Built-in scenario definitions (embedded as Go structs for Phase 2).
// A future enhancement could load these from YAML files.
type scenarioDef struct {
	Name        string     `json:"name" yaml:"name"`
	Description string     `json:"description" yaml:"description"`
	Metrics     []string   `json:"metrics" yaml:"metrics"`
	Rules       []ruleDef  `json:"rules" yaml:"rules"`
}

type ruleDef struct {
	ID        string   `json:"id" yaml:"id"`
	Severity  string   `json:"severity" yaml:"severity"` // "red" | "yellow"
	Condition string   `json:"condition" yaml:"condition"`
	Hints     []string `json:"hints" yaml:"hints"`
}

type finding struct {
	RuleID   string   `json:"rule_id"`
	Severity string   `json:"severity"`
	Message  string   `json:"message"`
	Hints    []string `json:"hints,omitempty"`
}

var builtinScenarios = map[string]scenarioDef{
	"stuck-flush": {
		Name:        "stuck-flush",
		Description: "Detect flush-path stalls and buildup",
		Metrics:     []string{"woodpecker_server_file_flush_latency", "woodpecker_server_op_registry_evicted_total"},
		Rules: []ruleDef{
			{ID: "flush_latency_high", Severity: "yellow", Condition: "flush_latency_sum > 10000",
				Hints: []string{"wp ops list {node} --type file.flush --longer-than 30000", "wp logstore flush-queue {node}"}},
			{ID: "old_evictions", Severity: "red", Condition: "evicted_old > 0",
				Hints: []string{"wp ops stats {node}", "wp logstore segment-show {node} --log {log} --seg {seg}"}},
		},
	},
	"hot-write": {
		Name: "hot-write", Description: "Cross-node append rate severely imbalanced",
		Metrics: []string{"woodpecker_server_logstore_operations_total"},
		Rules: []ruleDef{
			{ID: "write_imbalance", Severity: "yellow", Condition: "max_rate / min_rate > 10",
				Hints: []string{"wp metrics top --by woodpecker_server_logstore_operations_total"}},
		},
	},
	"slow-write": {
		Name: "slow-write", Description: "Append p99 deviates from baseline",
		Metrics: []string{"woodpecker_server_logstore_operation_latency"},
		Rules: []ruleDef{
			{ID: "write_latency_high", Severity: "yellow", Condition: "p99 > baseline * 2",
				Hints: []string{"wp ops list {node} --type logstore.add_entry --longer-than 5000"}},
		},
	},
	"stuck-write": {
		Name: "stuck-write", Description: "Append throughput near zero with pending ops",
		Metrics: []string{"woodpecker_server_logstore_operations_total"},
		Rules: []ruleDef{
			{ID: "write_stall", Severity: "red", Condition: "rate == 0 && pending > 0",
				Hints: []string{"wp ops list {node} --type logstore.add_entry"}},
		},
	},
	"slow-compact": {
		Name: "slow-compact", Description: "Compact p99 deviates and errors growing",
		Metrics: []string{"woodpecker_server_file_compaction_latency"},
		Rules: []ruleDef{
			{ID: "compact_slow", Severity: "yellow", Condition: "compact_p99 > baseline * 3",
				Hints: []string{"wp logstore segments {node} --writable false"}},
		},
	},
	"fencing": {
		Name: "fencing", Description: "Fence events exceed threshold in window",
		Metrics: []string{"woodpecker_server_logstore_operations_total"},
		Rules: []ruleDef{
			{ID: "fence_burst", Severity: "yellow", Condition: "fence_count > 5 in window",
				Hints: []string{"wp ops list {node} --type logstore.fence"}},
		},
	},
	"slow-read": {
		Name: "slow-read", Description: "Read p99 deviates and read queue growing",
		Metrics: []string{"woodpecker_server_read_batch_latency"},
		Rules: []ruleDef{
			{ID: "read_slow", Severity: "yellow", Condition: "read_p99 > baseline * 2",
				Hints: []string{"wp metrics snapshot {node} --metric woodpecker_server_read_batch_latency"}},
		},
	},
	"stuck-read": {
		Name: "stuck-read", Description: "Read throughput near zero but pending ops exist",
		Metrics: []string{"woodpecker_server_logstore_operations_total"},
		Rules: []ruleDef{
			{ID: "read_stall", Severity: "red", Condition: "read_rate == 0 && pending > 0",
				Hints: []string{"wp ops list {node} --type logstore.get_batch_entries"}},
		},
	},
	"read-amplification": {
		Name: "read-amplification", Description: "Block reads / entry reads ratio anomalous",
		Metrics: []string{"woodpecker_server_read_batch_latency"},
		Rules: []ruleDef{
			{ID: "read_amp_high", Severity: "yellow", Condition: "block_reads / entry_reads > 10",
				Hints: []string{"wp logstore segments {node}"}},
		},
	},
	"quorum-degraded": {
		Name: "quorum-degraded", Description: "Some log active replicas < ensemble size",
		Metrics: []string{"woodpecker_server_logstore_active_segments"},
		Rules: []ruleDef{
			{ID: "quorum_low", Severity: "red", Condition: "active_replicas < ensemble",
				Hints: []string{"wp cluster health"}},
		},
	},
	"under-replication": {
		Name: "under-replication", Description: "Sustained under-replication in window",
		Metrics: []string{"woodpecker_server_logstore_active_segments"},
		Rules: []ruleDef{
			{ID: "under_rep", Severity: "red", Condition: "under_replicated sustained > 50% window",
				Hints: []string{"wp cluster health", "wp node list"}},
		},
	},
	"slow-slot": {
		Name: "slow-slot", Description: "Quorum ensemble member drags overall latency",
		Metrics: []string{"woodpecker_server_logstore_operation_latency"},
		Rules: []ruleDef{
			{ID: "slot_drag", Severity: "yellow", Condition: "max_node_p99 / median_p99 > 5",
				Hints: []string{"wp metrics top --by woodpecker_server_logstore_operation_latency"}},
		},
	},
}

func newMetricsReportCommand() *cobra.Command {
	var scenario string
	var window time.Duration
	var interval time.Duration
	var listScenarios bool
	cmd := &cobra.Command{
		Use:   "report [node]",
		Short: "Run scenario-based metrics analysis",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if listScenarios {
				type scenarioInfo struct {
					Name        string `json:"name"`
					Description string `json:"description"`
				}
				var list []scenarioInfo
				for _, s := range builtinScenarios {
					list = append(list, scenarioInfo{Name: s.Name, Description: s.Description})
				}
				if Globals.Output == "json" || Globals.Output == "yaml" {
					return output.RenderJSON(cmd.OutOrStdout(), list)
				}
				headers := []string{"SCENARIO", "DESCRIPTION"}
				var rows [][]string
				for _, s := range list {
					rows = append(rows, []string{s.Name, s.Description})
				}
				return output.RenderRowTable(cmd.OutOrStdout(), headers, rows)
			}

			if scenario == "" {
				return wperrors.NewUsageError("--scenario is required (use --list to see available scenarios)")
			}
			sc, ok := builtinScenarios[scenario]
			if !ok {
				return wperrors.NewUsageError(fmt.Sprintf("unknown scenario %q (use --list)", scenario))
			}

			res, err := resolveAndDiscover()
			if err != nil {
				return err
			}

			// Pick target node
			var peerURL, nodeID string
			if len(args) > 0 {
				member, ok := res.Members.Resolve(args[0])
				if !ok {
					return wperrors.NewTargetNotFoundError(args[0])
				}
				peerURL = res.Client.PeerAdminURL(member)
				nodeID = member.ID
			} else if len(res.Members.Members) > 0 {
				m := res.Members.Members[0]
				peerURL = res.Client.PeerAdminURL(m)
				nodeID = m.ID
			} else {
				return wperrors.NewNetworkError("no nodes discovered")
			}

			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "Running scenario %q on %s (window=%s, interval=%s)\n", sc.Name, nodeID, window, interval)
			fmt.Fprintf(w, "Description: %s\n\n", sc.Description)

			// Collect samples over the window
			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			type sample struct {
				ts       time.Time
				families map[string]*prom.MetricFamily
			}
			var samples []sample
			deadline := time.After(window)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			// First scrape
			if fam, err := prom.ScrapeNode(peerURL, Globals.Timeout); err == nil {
				samples = append(samples, sample{ts: time.Now(), families: fam})
			}
			fmt.Fprintf(w, "Collecting samples")

		collectLoop:
			for {
				select {
				case <-ctx.Done():
					break collectLoop
				case <-deadline:
					break collectLoop
				case <-ticker.C:
					if fam, err := prom.ScrapeNode(peerURL, Globals.Timeout); err == nil {
						samples = append(samples, sample{ts: time.Now(), families: fam})
						fmt.Fprint(w, ".")
					}
				}
			}
			fmt.Fprintf(w, " %d samples collected\n\n", len(samples))

			// Evaluate rules (simplified: check last sample for threshold conditions)
			var findings []finding
			if len(samples) > 0 {
				last := samples[len(samples)-1]
				for _, rule := range sc.Rules {
					f := evaluateRule(rule, last.families, nodeID)
					if f != nil {
						findings = append(findings, *f)
					}
				}
			}

			if Globals.Output == "json" || Globals.Output == "yaml" {
				return output.RenderJSON(cmd.OutOrStdout(), map[string]any{
					"scenario": sc.Name, "node": nodeID,
					"samples": len(samples), "findings": findings,
				})
			}

			if len(findings) == 0 {
				fmt.Fprintf(w, "Result: OK — no findings for scenario %q\n", sc.Name)
				return nil
			}

			fmt.Fprintf(w, "Findings (%d):\n", len(findings))
			for _, f := range findings {
				marker := "[YELLOW]"
				if f.Severity == "red" {
					marker = "[RED]"
				}
				fmt.Fprintf(w, "  %s %s: %s\n", marker, f.RuleID, f.Message)
				for _, h := range f.Hints {
					hint := strings.ReplaceAll(h, "{node}", nodeID)
					fmt.Fprintf(w, "    -> %s\n", hint)
				}
			}

			// Exit code based on worst severity
			for _, f := range findings {
				if f.Severity == "red" {
					return wperrors.NewRedFindingError(fmt.Sprintf("scenario %q: %d findings", sc.Name, len(findings)))
				}
			}
			return wperrors.NewYellowFindingError(fmt.Sprintf("scenario %q: %d findings", sc.Name, len(findings)))
		},
	}
	cmd.Flags().StringVar(&scenario, "scenario", "", "Scenario name to run")
	cmd.Flags().DurationVar(&window, "window", 30*time.Second, "Collection window duration")
	cmd.Flags().DurationVar(&interval, "interval", 1*time.Second, "Scrape interval within window")
	cmd.Flags().BoolVar(&listScenarios, "list", false, "List available scenarios")
	return cmd
}

// evaluateRule applies a simplified rule evaluation against the scraped families.
// In Phase 2, this is a basic heuristic; the full YAML DSL engine is deferred.
func evaluateRule(rule ruleDef, families map[string]*prom.MetricFamily, nodeID string) *finding {
	// Check for "evicted_old > 0" pattern
	if strings.Contains(rule.Condition, "evicted_old") {
		if mf, ok := families["woodpecker_server_op_registry_evicted_total"]; ok {
			val, found := prom.GetCounterValue(mf, map[string]string{"signal": "old"})
			if found && val > 0 {
				return &finding{
					RuleID:   rule.ID,
					Severity: rule.Severity,
					Message:  fmt.Sprintf("old eviction counter = %.0f (stall signal)", val),
					Hints:    rule.Hints,
				}
			}
		}
	}

	// Check for high latency patterns
	if strings.Contains(rule.Condition, "latency") || strings.Contains(rule.Condition, "p99") {
		for _, mf := range families {
			if mf.GetType().String() == "HISTOGRAM" {
				for _, m := range mf.GetMetric() {
					h := m.GetHistogram()
					if h != nil && h.GetSampleCount() > 0 {
						avgMs := h.GetSampleSum() / float64(h.GetSampleCount())
						if avgMs > 5000 { // 5 second average = suspicious
							labels := prom.LabelMap(m)
							return &finding{
								RuleID:   rule.ID,
								Severity: rule.Severity,
								Message:  fmt.Sprintf("high avg latency %.0fms (node=%s, labels=%v)", avgMs, nodeID, labels),
								Hints:    rule.Hints,
							}
						}
					}
				}
			}
		}
	}

	// Check for rate == 0 patterns (stall detection)
	if strings.Contains(rule.Condition, "rate == 0") {
		// Would need two samples to compute rate — skip in single-sample mode
	}

	return nil
}

