// Package reports consumes immutable diagnostic JSON. It owns optional Agent
// interpretation and report inspection, never source collection or check rules.
package reports

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/checks"
	"github.com/zilliztech/woodpecker/diagnostics/wal"
)

func AgentSettings(c d.Config) (wal.AgentConfig, error) {
	node := c.Agent
	if node.Kind == 0 && c.Workbench.Kind != 0 {
		var legacy map[string]yaml.Node
		if c.Workbench.Decode(&legacy) != nil {
			return wal.AgentConfig{}, fmt.Errorf("invalid Agent envelope")
		}
		node = legacy["agent"]
	}
	var project yaml.Node
	if node.Kind != 0 {
		var value any
		if node.Decode(&value) != nil {
			return wal.AgentConfig{}, fmt.Errorf("invalid Agent settings")
		}
		_ = project.Encode(map[string]any{"agent": value})
	}
	s, err := wal.LoadSettings(d.Config{Workbench: project})
	return s.Agent, err
}

func Inspection(body []byte, id string) (any, error) {
	if len(body) > 16<<20 {
		return nil, fmt.Errorf("report exceeds 16 MiB")
	}
	var header struct {
		Schema string `json:"schema_version"`
	}
	if json.Unmarshal(body, &header) != nil {
		return nil, fmt.Errorf("invalid JSON report")
	}
	if header.Schema == checks.ReportSchema {
		var r checks.Report
		if json.Unmarshal(body, &r) != nil {
			return nil, fmt.Errorf("invalid report")
		}
		for _, c := range r.Checks {
			if c.ID == id || c.Name == id {
				if c.Inspect == nil {
					return nil, fmt.Errorf("inspect detail was omitted; collect with --details all or inspect an abnormal result")
				}
				return c.Inspect, nil
			}
		}
		return nil, fmt.Errorf("check is absent from this report")
	}
	var legacy wal.Report
	if json.Unmarshal(body, &legacy) != nil {
		return nil, fmt.Errorf("invalid report")
	}
	return wal.Inspect(legacy, id)
}

func Packet(body []byte) (wal.Packet, error) {
	if len(body) > 16<<20 {
		return wal.Packet{}, fmt.Errorf("report exceeds 16 MiB")
	}
	// Historical data remains exportable, but retired foreign checks must not
	// become a second diagnostic surface through optional Agent analysis.
	var scope struct {
		Checks []struct {
			ID   string `json:"id"`
			Step string `json:"step"`
		} `json:"checks"`
		Evidence []struct {
			ID string `json:"id"`
		} `json:"evidence"`
	}
	if json.Unmarshal(body, &scope) == nil {
		for _, check := range scope.Checks {
			if strings.HasPrefix(check.ID, "MV") || strings.HasPrefix(check.Step, "milvus") {
				return wal.Packet{}, fmt.Errorf("Milvus diagnostic evidence is retired; export the original JSON for historical reference")
			}
		}
		for _, check := range scope.Evidence {
			if strings.HasPrefix(check.ID, "MV") {
				return wal.Packet{}, fmt.Errorf("Milvus diagnostic evidence is retired; export the original JSON for historical reference")
			}
		}
	}
	var header struct {
		Schema string `json:"schema_version"`
	}
	if json.Unmarshal(body, &header) != nil {
		return wal.Packet{}, fmt.Errorf("invalid JSON report")
	}
	if header.Schema != "wp.diag/v1" {
		return wal.PacketFromBytes(body)
	}
	var r checks.Report
	if json.Unmarshal(body, &r) != nil {
		return wal.Packet{}, fmt.Errorf("invalid report")
	}
	hash := sha256.Sum256(body)
	p := wal.Packet{SchemaVersion: "wpdiag.wal.analysis/v1", Mode: "analyze_supplied_findings_only", ReportSHA256: hex.EncodeToString(hash[:]), RunID: r.RunID, Problem: r.Request.Scenario, Target: r.Target, TimeRange: wal.TimeRange{Mode: "historical", Start: r.Request.From, End: r.Request.To}, Complete: r.Complete, Findings: []wal.PacketEvidence{}, Supporting: []wal.PacketEvidence{}, Gaps: []wal.Evidence{}, Observations: []d.Observation{}, Instructions: []string{"Analyze only supplied evidence. Cite short check IDs. Keep observations separate from hypotheses.", "Metrics and Trace checks are independent. A shared scenario/step does not establish a log, request or causal association.", "No system queries, commands, runtime inspection or repairs. Suggest minimal read-only evidence collection when needed."}}
	scopes := map[string]wal.Target{}
	if r.Request.Bindings != nil {
		for role, scope := range map[string]*checks.Scope{"client": r.Request.Bindings.Client, "server": r.Request.Bindings.Server} {
			if scope != nil {
				t := checks.ScopeTarget(*scope, role)
				t.Milvus = nil
				scopes[role] = t
			}
		}
		p.Instructions = append(p.Instructions, "Component scopes are selected configuration, not evidence of an Admin, metadata, log or causal association.")
	}
	p.Context, _ = json.Marshal(struct {
		Summary  checks.Summary        `json:"summary"`
		Coverage []string              `json:"coverage_gaps"`
		Scopes   map[string]wal.Target `json:"component_scopes,omitempty"`
	}{r.Summary, r.Coverage, scopes})
	for _, c := range r.Checks {
		e := c.Evidence
		e.Facts = nil
		e.Finding = nil
		x := wal.PacketEvidence{Evidence: e, Title: c.Source + " / " + c.Step + " / " + c.Title, Rule: c.Rule, Limits: c.IdentityScope + ": " + join(c.Coverage)}
		if c.Status == "abnormal" {
			p.Findings = append(p.Findings, x)
		} else if c.Status == "unknown" {
			p.Gaps = append(p.Gaps, e)
		} else {
			p.Supporting = append(p.Supporting, x)
		}
	}
	for _, o := range r.Observations {
		o.Data = nil
		p.Observations = append(p.Observations, o)
	}
	data, _ := json.Marshal(p)
	if len(data) > 256<<10 {
		return wal.Packet{}, fmt.Errorf("findings packet exceeds 256 KiB")
	}
	return p, nil
}

func join(xs []string) string {
	out := ""
	for i, x := range xs {
		if i > 0 {
			out += "; "
		}
		out += x
	}
	return out
}
