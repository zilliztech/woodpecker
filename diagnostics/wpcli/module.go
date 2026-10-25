// Package wpcli adapts the versioned wp diagnose subprocess protocol. It does
// not import wpcli implementation packages or reimplement its diagnostic rules.
package wpcli

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

type Settings struct {
	Executable   string       `yaml:"executable"`
	Context      string       `yaml:"context"`
	CLIConfig    string       `yaml:"cli_config,omitempty"`
	NativeLimits NativeLimits `yaml:"native_limits,omitempty"`
}

type NativeLimits struct {
	MaxSamples    int   `yaml:"max_samples"`
	MaxOps        int   `yaml:"max_ops"`
	MaxSeries     int   `yaml:"max_series"`
	ResponseBytes int64 `yaml:"max_response_bytes"`
	Concurrency   int   `yaml:"concurrency"`
	TimeoutMS     int64 `yaml:"timeout_ms"`
}

type Module struct{}

func (Module) Descriptor() d.Descriptor {
	return d.Descriptor{Source: "wpcli", Issue: 454, RequiredSettings: []string{"executable", "context"}, Profiles: []d.Profile{
		{ID: "wp.write_stall", Version: 1, Scope: "wp", Flow: "write", Stage: "append", Modes: []string{"snapshot", "live"}, TargetFields: []string{"node_id"}, Description: "Import native write-stall evidence; snapshot is inconclusive for progress"},
		{ID: "wp.read_stall", Version: 1, Scope: "wp", Flow: "read", Stage: "wal_read", Modes: []string{"snapshot", "live"}, TargetFields: []string{"node_id"}, Description: "Import native WAL-read stall evidence; not Milvus visibility or search diagnosis"},
	}}
}

func settings(c d.Connection) (Settings, error) {
	s := Settings{NativeLimits: NativeLimits{MaxSamples: 121, MaxOps: 100, MaxSeries: 500, ResponseBytes: 2 << 20, Concurrency: 8, TimeoutMS: 30000}}
	if err := d.DecodeSettings(c, &s); err != nil {
		return s, err
	}
	l := s.NativeLimits
	if s.Executable == "" || s.Context == "" || len(s.Context) > 256 || strings.ContainsAny(s.Context+s.Executable+s.CLIConfig, "\x00\r\n") || l.MaxSamples < 2 || l.MaxSamples > 121 || l.MaxOps < 1 || l.MaxOps > 1000 || l.MaxSeries < 1 || l.MaxSeries > 5000 || l.ResponseBytes < 1024 || l.ResponseBytes > 8<<20 || l.Concurrency < 1 || l.Concurrency > 32 || l.TimeoutMS < 1 || l.TimeoutMS > 600000 {
		return s, fmt.Errorf("invalid native connection settings")
	}
	if strings.ContainsAny(s.Executable, "/\\") && !filepath.IsAbs(s.Executable) {
		s.Executable = filepath.Join(c.BaseDir, s.Executable)
	}
	if s.CLIConfig != "" && !filepath.IsAbs(s.CLIConfig) {
		s.CLIConfig = filepath.Join(c.BaseDir, s.CLIConfig)
	}
	return s, nil
}

func (Module) ValidateConnection(c d.Connection) error { _, err := settings(c); return err }

// arguments uses literal argv, never a shell. A node beginning with '-' stays
// a positional argument. Context/cli.yaml semantics remain owned by wpcli.
func arguments(ctx context.Context, r d.Request, s Settings) ([]string, error) {
	w := r.Selection.Window
	l := s.NativeLimits
	if w.Mode == "live" && w.DurationMS > 300000 {
		return nil, fmt.Errorf("native live window exceeds 5m")
	}
	samples := 2
	if w.Mode == "live" {
		samples = int((w.DurationMS+w.IntervalMS-1)/w.IntervalMS) + 1
	}
	nodes := r.Limits.MaxTargets
	if r.Selection.Target.NodeID != "" {
		nodes = 1
	}
	if samples > l.MaxSamples || nodes*samples*l.MaxOps > 100000 || nodes*samples*l.MaxSeries > 200000 {
		return nil, fmt.Errorf("native sample/operation/series work budget exceeded")
	}
	budget := time.Duration(r.Limits.BudgetMS) * time.Millisecond
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < budget {
		budget = time.Until(deadline)
	}
	if budget < time.Millisecond {
		return nil, fmt.Errorf("native execution budget exhausted")
	}
	scenario := "stuck-write"
	if r.Selection.Flow == "read" {
		scenario = "stuck-read"
	}
	a := []string{"--context=" + s.Context, "diagnose", "--scenario=" + scenario, "--mode=" + w.Mode, "--budget=" + budget.String(), "--timeout=" + (time.Duration(l.TimeoutMS) * time.Millisecond).String(), "--concurrency=" + strconv.Itoa(l.Concurrency), "--max-nodes=" + strconv.Itoa(nodes), "--max-samples=" + strconv.Itoa(samples), "--max-ops=" + strconv.Itoa(l.MaxOps), "--max-series=" + strconv.Itoa(l.MaxSeries), "--max-response-bytes=" + strconv.FormatInt(l.ResponseBytes, 10), "--output=json"}
	if w.Mode == "live" {
		a = append(a, "--window="+(time.Duration(w.DurationMS)*time.Millisecond).String(), "--interval="+(time.Duration(w.IntervalMS)*time.Millisecond).String())
	}
	if r.Selection.Target.NodeID != "" {
		a = append(a, "--", r.Selection.Target.NodeID)
	}
	return a, nil
}

func (Module) Run(ctx context.Context, r d.Request, c d.Connection) d.Result {
	s, err := settings(c)
	if err != nil {
		return d.Failure("wpcli", "invalid", "invalid_connection_settings", "Check native connection settings")
	}
	args, err := arguments(ctx, r, s)
	if err != nil {
		return d.Failure("wpcli", "invalid", "native_budget_invalid", err.Error())
	}
	output := execute(ctx, s, args, r.Limits.MaxInputBytes)
	if output.Overflow {
		return d.Failure("wpcli", "budget_exceeded", "native_output_limit", "Native stdout/stderr exceeded its byte budget; raw output withheld")
	}
	if output.StartError {
		return d.Failure("wpcli", "failed", "native_start_failed", "Cannot start configured wp executable; raw errors withheld")
	}
	result, err := importReport(output.Data, output.ExitCode, r, s)
	if err != nil {
		return d.Failure("wpcli", "failed", "native_protocol_invalid", err.Error())
	}
	return result
}
