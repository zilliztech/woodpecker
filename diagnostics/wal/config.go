package wal

import (
	"bytes"
	"fmt"
	"math"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func LoadSettings(c d.Config) (Settings, error) {
	s := Settings{Targets: map[string]Target{}, Policy: Policy{LatencyMS: 100, MinObservations: 100, SustainMS: 180000, MaxGapMS: 30000, Thresholds: map[string]float64{}}, Agent: AgentConfig{TimeoutMS: 60000, MaxOutputBytes: 1 << 20}}
	if c.Workbench.Kind != 0 {
		b, e := yaml.Marshal(c.Workbench)
		if e != nil {
			return s, fmt.Errorf("invalid workbench settings")
		}
		dec := yaml.NewDecoder(bytes.NewReader(b))
		dec.KnownFields(true)
		if dec.Decode(&s) != nil {
			return s, fmt.Errorf("invalid workbench settings (values withheld)")
		}
	}
	if s.Targets == nil {
		s.Targets = map[string]Target{}
	}
	if s.Links.Grafana != "" {
		u, err := url.Parse(s.Links.Grafana)
		if err != nil || len(s.Links.Grafana) > 2048 || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || strings.ContainsAny(s.Links.Grafana, "\x00\r\n") {
			return s, fmt.Errorf("Grafana link must be an HTTP(S) URL without embedded credentials")
		}
	}
	p := s.Policy
	if math.IsNaN(p.LatencyMS) || math.IsInf(p.LatencyMS, 0) || p.LatencyMS <= 0 || p.LatencyMS > 3600000 || p.MinObservations < 1 || p.MinObservations > 1000000 || p.SustainMS < 60000 || p.SustainMS > 86400000 || p.MaxGapMS < 1 || p.MaxGapMS > 600000 {
		return s, fmt.Errorf("invalid workbench policy limits")
	}
	ids := map[string]bool{}
	for _, x := range Definitions().Evidence {
		ids[x.ID] = x.Kind == "p99" || x.ID == "H01" || x.ID == "H03"
	}
	for id, v := range p.Thresholds {
		if !ids[id] || v <= 0 || v > 3600000 || math.IsNaN(v) || math.IsInf(v, 0) {
			return s, fmt.Errorf("invalid workbench evidence threshold")
		}
	}
	for id, t := range s.Targets {
		t.ID = id
		if t.Label == "" {
			t.Label = id
		}
		values := []string{id, t.Label, t.Context, t.Prometheus, t.Trace, t.Loki, t.WPCLI, t.LogNamespace, t.LogID, t.ReaderName, t.NodeID, t.Path, t.LogName, t.Metadata.Etcd, t.Metadata.Prefix}
		if t.Milvus != nil {
			values = append(values, t.Milvus.Prometheus, t.Milvus.Channel, t.Milvus.NodeID)
		}
		for _, v := range values {
			if len(v) > 512 || strings.ContainsAny(v, "\x00\r\n") {
				return s, fmt.Errorf("invalid workbench target identity")
			}
		}
		if t.LogID != "" {
			v, e := strconv.ParseInt(t.LogID, 10, 64)
			if e != nil || v < 0 || strconv.FormatInt(v, 10) != t.LogID {
				return s, fmt.Errorf("log_id must be a canonical decimal string")
			}
		}
		if t.LogName != "" && strings.HasPrefix(t.LogName, "-") {
			return s, fmt.Errorf("log_name cannot begin with a dash")
		}
		t.MetadataReady = t.WPCLI != "" && t.LogName != "" && t.Metadata.Etcd != "" && t.Metadata.Prefix != ""
		t.MilvusReady = t.Milvus != nil && t.Milvus.Prometheus != "" && t.Milvus.Channel != ""
		s.Targets[id] = t
	}
	if len(s.Targets) == 0 {
		for name := range c.Sources["prometheus"].Connections {
			s.Targets[name] = Target{ID: name, Label: name + " / matching log populations", Prometheus: name}
		}
	}
	if len(s.Targets) > 32 {
		return s, fmt.Errorf("at most 32 workbench targets")
	}
	s.Agent = agentDefaults(s.Agent)
	a := s.Agent
	if a.Provider != "claude" && a.Provider != "codex" {
		return s, fmt.Errorf("Agent provider must be claude or codex")
	}
	if a.Auth != "api_key" && !(a.Provider == "codex" && a.Auth == "cli") {
		return s, fmt.Errorf("Agent auth must be api_key, or cli for Codex")
	}
	if a.Auth == "cli" && a.APIKeyEnv != "" {
		return s, fmt.Errorf("api_key_env is only used with auth: api_key")
	}
	if a.TimeoutMS < 1000 || a.TimeoutMS > 300000 || a.MaxOutputBytes < 1024 || a.MaxOutputBytes > 2<<20 || len(a.Model) > 256 || strings.ContainsAny(a.Executable+a.Model+a.APIKeyEnv, "\x00\r\n") {
		return s, fmt.Errorf("invalid optional Agent configuration")
	}
	if a.Auth == "api_key" && !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(a.APIKeyEnv) {
		return s, fmt.Errorf("Agent api_key_env must name a backend environment variable")
	}
	if a.Enabled && a.Executable == "" {
		return s, fmt.Errorf("configure the optional Agent executable")
	}
	if strings.ContainsAny(a.Executable, "/\\") && !filepath.IsAbs(a.Executable) {
		return s, fmt.Errorf("Agent executable must be absolute or a PATH command")
	}
	return s, nil
}

func PublicTargets(s Settings) []Target {
	keys := []string{}
	for id := range s.Targets {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	out := []Target{}
	for _, id := range keys {
		t := s.Targets[id]
		t.Metadata = MetadataBinding{}
		t.Milvus = nil
		out = append(out, t)
	}
	return out
}

func ValidateRequest(s Settings, q Request) error {
	p, ok := ProblemByID(q.Problem)
	if !ok {
		return fmt.Errorf("unknown problem")
	}
	if _, ok = s.Targets[q.Target]; !ok {
		return fmt.Errorf("select a configured target")
	}
	if _, err := SelectedChecks(q); err != nil {
		return err
	}
	if p.ID != "metadata" {
		if q.From.IsZero() || !q.From.Before(q.To) || q.To.After(time.Now()) || q.To.Sub(q.From) > 24*time.Hour {
			return fmt.Errorf("choose a past interval with from < to, at most 24 hours")
		}
	}
	return nil
}
