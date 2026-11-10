package checks

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/evidence"
)

const RuleSchema = "wp.diag.rules/v1"

type AssessmentRule struct {
	Mode         string   `yaml:"mode,omitempty" json:"mode"`
	Threshold    *float64 `yaml:"threshold,omitempty" json:"threshold,omitempty"`
	Unit         string   `yaml:"unit,omitempty" json:"unit,omitempty"`
	ConfiguredBy string   `yaml:"-" json:"configured_by"`
}
type RuleDefaults struct {
	LatencyMS           *float64 `yaml:"latency_ms,omitempty"`
	MinObservations     *int     `yaml:"min_observations,omitempty"`
	SustainMS           *int64   `yaml:"sustain_ms,omitempty"`
	MaxGapMS            *int64   `yaml:"max_gap_ms,omitempty"`
	CalculationWindowMS *int64   `yaml:"calculation_window_ms,omitempty"`
}
type RuleDocument struct {
	Schema   string                    `yaml:"schema_version"`
	Source   string                    `yaml:"source"`
	Defaults RuleDefaults              `yaml:"defaults,omitempty"`
	Evidence map[string]AssessmentRule `yaml:"evidence"`
}

func ruleMode(def Definition) string {
	if def.ID == "S06" {
		return "runtime_watermarks"
	}
	if def.ID == "W05" {
		return "sustained_growth"
	}
	if def.Kind == "p99" || def.Kind == "duration" || def.ID == "H01" || def.ID == "H03" {
		return "threshold"
	}
	if (def.Kind == "errors" || def.Kind == "increase") && !has([]string{"B01", "R09", "OR01", "OW01", "R10"}, def.ID) {
		return "threshold"
	}
	return "observe"
}

func ruleUnit(def Definition) string {
	if def.Source == "trace" {
		if def.Kind == "duration" {
			return "ms"
		}
		return "count"
	}
	return rawDefinition(def.ID).Unit
}
func numericRule(def Definition) bool { return ruleMode(def) == "threshold" }
func countRule(def Definition) bool {
	return numericRule(def) && (def.Kind == "errors" || def.Kind == "increase")
}

func RuleFor(def Definition, p evidence.Policy) AssessmentRule {
	r := AssessmentRule{Mode: ruleMode(def), Unit: ruleUnit(def), ConfiguredBy: "built-in"}
	if source := p.RuleSources[def.ID]; source != "" {
		r.ConfiguredBy = source
	}
	if !numericRule(def) {
		return r
	}
	v := 0.0
	if def.Kind == "duration" {
		v = p.TraceLatencyMS
		if v == 0 {
			v = 1000
		}
	} else if !countRule(def) {
		v = p.LatencyMS
		if v == 0 {
			v = 100
		}
		if r.Unit == "seconds" {
			v /= 1000
		}
		if def.ID == "H01" {
			v = 90
		}
		if def.ID == "H03" {
			v = .9
		}
	}
	if n, ok := p.Thresholds[def.ID]; ok {
		v = n
	} else if n, ok := p.Thresholds[def.Name]; ok {
		v = n
	}
	r.Threshold = &v
	return r
}

// RuleOverrides resolves explicit relative paths beside cli.yaml. Missing default
// files use built-ins; an explicitly selected missing file is a configuration error.
func RuleOverrides(base string, overrides map[string]string) map[string]string {
	out := map[string]string{}
	for _, source := range []string{"metrics", "trace"} {
		if path := overrides[source]; path != "" {
			if !filepath.IsAbs(path) {
				path = filepath.Join(base, path)
			}
			out[source] = filepath.Clean(path)
		}
	}
	return out
}

func WithRuleFiles(c d.Config, base string, overrides map[string]string) (d.Config, error) {
	s, err := LoadSettings(c)
	if err != nil {
		return c, err
	}
	p := s.Policy
	p.Thresholds = copyThresholds(p.Thresholds)
	p.RuleSources = map[string]string{}
	p.RuleFiles = nil
	paths := RuleOverrides(base, overrides)
	c.RulePaths = paths
	for _, source := range []string{"metrics", "trace"} {
		path := paths[source]
		explicit := path != ""
		if !explicit {
			path = filepath.Join(base, "diag-"+source+".yaml")
		}
		path, err = filepath.Abs(path)
		if err != nil {
			return c, fmt.Errorf("cannot resolve diagnostic rule file")
		}
		state := evidence.RuleFile{Source: source, Path: path}
		f, openErr := os.Open(path)
		if os.IsNotExist(openErr) && !explicit {
			p.RuleFiles = append(p.RuleFiles, state)
			continue
		}
		if openErr != nil {
			return c, fmt.Errorf("cannot read %s rules: %s", source, path)
		}
		body, readErr := io.ReadAll(io.LimitReader(f, (256<<10)+1))
		_ = f.Close()
		if readErr != nil || len(body) > 256<<10 {
			return c, fmt.Errorf("%s rules are unreadable or exceed 256 KiB: %s", source, path)
		}
		var doc RuleDocument
		decoder := yaml.NewDecoder(bytes.NewReader(body))
		decoder.KnownFields(true)
		if decoder.Decode(&doc) != nil || decoder.Decode(new(any)) != io.EOF {
			return c, fmt.Errorf("invalid %s rule YAML: %s", source, path)
		}
		if doc.Schema != RuleSchema || doc.Source != source {
			return c, fmt.Errorf("%s requires schema_version %s and source %s", path, RuleSchema, source)
		}
		if source == "trace" && (doc.Defaults.MinObservations != nil || doc.Defaults.SustainMS != nil || doc.Defaults.MaxGapMS != nil || doc.Defaults.CalculationWindowMS != nil) {
			return c, fmt.Errorf("%s: Trace defaults only support latency_ms", path)
		}
		if n := doc.Defaults.LatencyMS; n != nil {
			if source == "trace" {
				p.TraceLatencyMS = *n
			} else {
				p.LatencyMS = *n
			}
		}
		if n := doc.Defaults.MinObservations; n != nil {
			p.MinObservations = *n
		}
		if n := doc.Defaults.SustainMS; n != nil {
			p.SustainMS = *n
		}
		if n := doc.Defaults.MaxGapMS; n != nil {
			p.MaxGapMS = *n
		}
		if n := doc.Defaults.CalculationWindowMS; n != nil {
			p.CalculationWindowMS = *n
		}
		definitions := map[string]Definition{}
		for _, def := range Definitions(p) {
			if def.Source == source {
				definitions[def.ID] = def
				definitions[def.Name] = def
				p.RuleSources[def.ID] = path
			}
		}
		seen := map[string]bool{}
		for id, rule := range doc.Evidence {
			def, ok := definitions[id]
			if !ok {
				def, ok = definitions[strings.ToUpper(id)]
			}
			if !ok {
				return c, fmt.Errorf("%s: unknown %s evidence %q", path, source, id)
			}
			if seen[def.ID] {
				return c, fmt.Errorf("%s: duplicate evidence key %s (including legacy aliases)", path, def.Name)
			}
			seen[def.ID] = true
			if rule.Mode != "" && rule.Mode != ruleMode(def) {
				return c, fmt.Errorf("%s: %s requires mode %s", path, def.Name, ruleMode(def))
			}
			if rule.Unit != "" && rule.Unit != ruleUnit(def) {
				return c, fmt.Errorf("%s: %s requires unit %s", path, def.Name, ruleUnit(def))
			}
			if n := rule.Threshold; n != nil {
				if !validRuleThreshold(def, *n) {
					return c, fmt.Errorf("%s: %s has an invalid %s threshold or no numeric threshold", path, def.Name, ruleUnit(def))
				}
				p.Thresholds[def.ID] = *n
				delete(p.Thresholds, def.Name)
			}
		}
		state.Loaded = true
		sum := sha256.Sum256(body)
		state.SHA256 = fmt.Sprintf("%x", sum)
		p.RuleFiles = append(p.RuleFiles, state)
	}
	if err = c.Policy.Encode(p); err != nil {
		return c, err
	}
	if _, err = LoadSettings(c); err != nil {
		return c, err
	}
	return c, nil
}

func validRuleThreshold(def Definition, value float64) bool {
	if !numericRule(def) || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 86400000 {
		return false
	}
	if !countRule(def) && value == 0 {
		return false
	}
	if countRule(def) && math.Trunc(value) != value {
		return false
	}
	if def.ID == "H01" && value > 100 || def.ID == "H03" && value > 1 {
		return false
	}
	return true
}

// RuleTemplate lists every implemented evidence and its actual native unit.
func RuleTemplate(source string, p evidence.Policy) (string, error) {
	if source != "metrics" && source != "trace" {
		return "", fmt.Errorf("choose metrics or trace")
	}
	doc := RuleDocument{Schema: RuleSchema, Source: source, Evidence: map[string]AssessmentRule{}}
	if source == "trace" {
		doc.Defaults.LatencyMS = &p.TraceLatencyMS
	} else {
		doc.Defaults = RuleDefaults{LatencyMS: &p.LatencyMS, MinObservations: &p.MinObservations, SustainMS: &p.SustainMS, MaxGapMS: &p.MaxGapMS, CalculationWindowMS: &p.CalculationWindowMS}
	}
	for _, def := range Definitions(p) {
		if def.Source == source {
			r := RuleFor(def, p)
			r.ConfiguredBy = ""
			doc.Evidence[def.Name] = r
		}
	}
	body, err := yaml.Marshal(doc)
	header := "# Evidence keys match the CLI check name and WebUI selector exactly.\n# Use lowercase letters, digits and single hyphens; legacy IDs remain readable.\n# Threshold values use the stated native unit; ms and seconds are different.\n# observe: retain measurements without a workload-health threshold.\n# runtime_watermarks: compare with the same node's exported protection watermarks.\n# sustained_growth: use the Metrics sustain_ms setting.\n"
	return header + strings.TrimSpace(string(body)) + "\n", err
}
