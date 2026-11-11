package checks

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/evidence"
)

type Settings struct {
	Targets map[string]evidence.Target
	Clients map[string]Scope
	Servers map[string]Scope
	Policy  evidence.Policy
}

func strictNode(n yaml.Node, out any) error {
	b, err := yaml.Marshal(&n)
	if err != nil {
		return fmt.Errorf("invalid diagnostic configuration")
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if dec.Decode(out) != nil {
		return fmt.Errorf("invalid diagnostic configuration (values withheld)")
	}
	return nil
}

func LoadSettings(c d.Config) (Settings, error) {
	s := Settings{Targets: map[string]evidence.Target{}, Policy: evidence.Policy{LatencyMS: 100, TraceLatencyMS: 1000, MinObservations: 100, SustainMS: 180000, MaxGapMS: 30000, CalculationWindowMS: 60000, Thresholds: map[string]float64{}}}
	// Read only target/policy nodes from a legacy workbench envelope. Agent/UI
	// settings are deliberately not decoded or validated by the check engine.
	var legacy map[string]yaml.Node
	if c.Workbench.Kind != 0 && c.Workbench.Decode(&legacy) != nil {
		return s, fmt.Errorf("invalid legacy workbench envelope")
	}
	targets, policy := c.Targets, c.Policy
	if old, ok := legacy["targets"]; ok {
		if targets.Kind != 0 {
			return s, fmt.Errorf("targets is configured in two places; use diagnostics.targets")
		}
		targets = old
	}
	if old, ok := legacy["policy"]; ok {
		if policy.Kind != 0 {
			return s, fmt.Errorf("policy is configured in two places; use diagnostics.policy")
		}
		policy = old
	}
	if targets.Kind != 0 {
		if err := strictNode(targets, &s.Targets); err != nil {
			return s, err
		}
	}
	if policy.Kind != 0 {
		if err := strictNode(policy, &s.Policy); err != nil {
			return s, err
		}
	}
	p := s.Policy
	positive := func(x float64) bool { return x > 0 && !math.IsNaN(x) && !math.IsInf(x, 0) }
	if !positive(p.LatencyMS) || !positive(p.TraceLatencyMS) || p.LatencyMS > 3600000 || p.TraceLatencyMS > 86400000 || p.MinObservations < 1 || p.MinObservations > 1000000 || p.CalculationWindowMS < 1000 || p.CalculationWindowMS > 3600000 || p.SustainMS < p.CalculationWindowMS || p.SustainMS > 86400000 || p.MaxGapMS < 1 || p.MaxGapMS > 600000 {
		return s, fmt.Errorf("invalid check policy limits")
	}
	definitions := Definitions(p)
	for key, v := range p.Thresholds {
		valid := false
		for _, def := range definitions {
			if key == def.ID || key == def.Name {
				valid = validRuleThreshold(def, v)
			}
		}
		if !valid {
			return s, fmt.Errorf("invalid threshold for %s: use a supported numeric check and its native unit", key)
		}
	}
	// Normalize named threshold overrides into stable IDs for the shared evaluator.
	for _, def := range definitions {
		if v, ok := s.Policy.Thresholds[def.Name]; ok {
			if old, exists := s.Policy.Thresholds[def.ID]; exists && old != v {
				return s, fmt.Errorf("conflicting check thresholds")
			}
			s.Policy.Thresholds[def.ID] = v
		}
	}
	s.Clients, s.Servers = map[string]Scope{}, map[string]Scope{}
	for _, item := range []struct {
		node yaml.Node
		out  *map[string]Scope
	}{{c.Clients, &s.Clients}, {c.Servers, &s.Servers}} {
		if item.node.Kind != 0 {
			if err := strictNode(item.node, item.out); err != nil {
				return s, err
			}
		}
		if len(*item.out) > 32 {
			return s, fmt.Errorf("at most 32 client or server configurations")
		}
		for name, scope := range *item.out {
			if scope.Name == "" {
				scope.Name = name
			}
			if err := validateScope(scope); err != nil {
				return s, err
			}
			(*item.out)[name] = scope
		}
	}
	if len(s.Targets) == 0 && len(s.Clients) == 0 && len(s.Servers) == 0 {
		for name := range c.Sources["prometheus"].Connections {
			s.Targets[name] = evidence.Target{Label: name + " / configured metric populations", Prometheus: name}
		}
		if len(s.Targets) == 0 {
			for name := range c.Sources["trace"].Connections {
				s.Targets[name] = evidence.Target{Label: name + " / configured trace service", Trace: name}
			}
		}
	}
	if len(s.Targets) > 32 {
		return s, fmt.Errorf("at most 32 diagnostic targets")
	}
	for id, t := range s.Targets {
		if t.Metrics != "" {
			if t.Prometheus != "" && t.Prometheus != t.Metrics {
				return s, fmt.Errorf("target metrics and prometheus references differ")
			}
			t.Prometheus = t.Metrics
		}
		values := []string{id, t.Label, t.Context, t.Prometheus, t.Trace, t.TraceService, t.LogNamespace, t.LogID, t.ReaderName, t.NodeID, t.Path, t.LogName, t.Metadata.Etcd, t.Metadata.Prefix}
		if t.Milvus != nil {
			return s, fmt.Errorf("Milvus diagnostic associations are no longer supported")
		}
		for _, v := range values {
			if len(v) > 512 || strings.ContainsAny(v, "\x00\r\n") {
				return s, fmt.Errorf("invalid diagnostic target identity")
			}
		}
		if id == "" {
			return s, fmt.Errorf("target name cannot be empty")
		}
		if t.LogID != "" {
			n, err := strconv.ParseInt(t.LogID, 10, 64)
			if err != nil || n < 0 || strconv.FormatInt(n, 10) != t.LogID {
				return s, fmt.Errorf("log_id must be a canonical decimal string")
			}
		}
		t.ID = id
		if t.Label == "" {
			t.Label = id
		}
		t.MetadataReady = t.LogName != "" && t.Metadata.Etcd != "" && t.Metadata.Prefix != ""
		s.Targets[id] = t
	}
	return s, nil
}

func PublicTargets(s Settings) []evidence.Target {
	keys := []string{}
	for id := range s.Targets {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	out := []evidence.Target{}
	for _, id := range keys {
		t := s.Targets[id]
		t.Metadata = evidence.MetadataBinding{}
		t.Milvus = nil
		t.Loki = ""
		t.WPCLI = ""
		out = append(out, t)
	}
	return out
}
