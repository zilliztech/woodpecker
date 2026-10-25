// Package prometheus implements bounded source-only metric diagnosis. It does
// not fetch WP runtime registries or combine evidence from other backends.
package prometheus

import (
	"fmt"
	"math"
	"net/url"
	"os"
	"regexp"
	"strings"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

type Settings struct {
	Endpoint       string            `yaml:"endpoint"`
	Deployment     string            `yaml:"deployment"`
	MatchLabels    map[string]string `yaml:"match_labels"`
	BearerTokenEnv string            `yaml:"bearer_token_env,omitempty"`
	BasicAuth      *BasicAuth        `yaml:"basic_auth,omitempty"`
	Limits         Limits            `yaml:"limits,omitempty"`
	Policy         Policy            `yaml:"policy,omitempty"`
}

type BasicAuth struct {
	Username    string `yaml:"username"`
	PasswordEnv string `yaml:"password_env"`
}

type Limits struct {
	MaxQueries       int   `yaml:"max_queries"`
	MaxSeries        int   `yaml:"max_series"`
	MaxSamples       int   `yaml:"max_samples"`
	MaxResponseBytes int64 `yaml:"max_response_bytes"`
	MaxWindowMS      int64 `yaml:"max_window_ms"`
	TimeoutMS        int64 `yaml:"timeout_ms"`
}

type Policy struct {
	MaxGapMS            int64              `yaml:"max_gap_ms" json:"max_gap_ms"`
	MinObservations     int                `yaml:"min_observations" json:"min_observations"`
	LatencyMS           map[string]float64 `yaml:"latency_ms" json:"latency_ms"`
	ThroughputDropRatio float64            `yaml:"throughput_drop_ratio" json:"throughput_drop_ratio"`
}

var (
	labelName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	envName   = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

func settings(c d.Connection) (Settings, error) {
	s := Settings{Limits: Limits{MaxQueries: 8, MaxSeries: 500, MaxSamples: 50000, MaxResponseBytes: 2 << 20, MaxWindowMS: 3600000, TimeoutMS: 10000}, Policy: Policy{MaxGapMS: 30000, MinObservations: 20, ThroughputDropRatio: 0.5, LatencyMS: map[string]float64{"wp.append": 250, "wp.wal_read": 250, "milvus.append": 1000, "milvus.query": 1000, "milvus.wal_append": 500}}}
	if d.DecodeSettings(c, &s) != nil {
		return s, fmt.Errorf("invalid Prometheus connection settings (values withheld)")
	}
	u, err := url.Parse(s.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || s.Deployment == "" || len(s.Deployment) > 256 || len(s.MatchLabels) == 0 || len(s.MatchLabels) > 16 {
		return s, fmt.Errorf("endpoint must be an HTTP(S) base URL without credentials/query/fragment, with an explicit deployment and match_labels")
	}
	for k, v := range s.MatchLabels {
		if !labelName.MatchString(k) || strings.HasPrefix(k, "__") || d.Has([]string{"instance", "node_id", "le", "operation", "status", "component"}, k) || v == "" || len(v) > 256 || strings.ContainsAny(v, "\x00\r\n") {
			return s, fmt.Errorf("match_labels must contain bounded deployment selectors, not reserved per-signal labels")
		}
	}
	l, p := s.Limits, s.Policy
	if l.MaxQueries < 1 || l.MaxQueries > 32 || l.MaxSeries < 1 || l.MaxSeries > 5000 || l.MaxSamples < 2 || l.MaxSamples > 200000 || l.MaxResponseBytes < 1024 || l.MaxResponseBytes > 8<<20 || l.MaxWindowMS < 1000 || l.MaxWindowMS > 86400000 || l.TimeoutMS < 1 || l.TimeoutMS > 60000 || p.MaxGapMS < 1 || p.MaxGapMS > 600000 || p.MinObservations < 1 || p.MinObservations > 1000000 || math.IsNaN(p.ThroughputDropRatio) || p.ThroughputDropRatio <= 0 || p.ThroughputDropRatio >= 1 {
		return s, fmt.Errorf("invalid Prometheus query/data/policy budgets")
	}
	if len(p.LatencyMS) != 5 {
		return s, fmt.Errorf("latency policy must name only the five supported signal IDs")
	}
	for _, id := range []string{"wp.append", "wp.wal_read", "milvus.append", "milvus.query", "milvus.wal_append"} {
		value := p.LatencyMS[id]
		if math.IsNaN(value) || value <= 0 || value > 3600000 {
			return s, fmt.Errorf("latency thresholds must be positive milliseconds")
		}
	}
	if s.BearerTokenEnv != "" && (!envName.MatchString(s.BearerTokenEnv) || s.BasicAuth != nil) {
		return s, fmt.Errorf("select one authentication mode using environment references")
	}
	if s.BasicAuth != nil && (s.BasicAuth.Username == "" || len(s.BasicAuth.Username) > 256 || strings.ContainsAny(s.BasicAuth.Username, ":\x00\r\n") || !envName.MatchString(s.BasicAuth.PasswordEnv)) {
		return s, fmt.Errorf("invalid basic_auth environment reference")
	}
	return s, nil
}

func auth(s Settings) (string, string, string, error) {
	if s.BearerTokenEnv != "" {
		token := os.Getenv(s.BearerTokenEnv)
		if token == "" || strings.ContainsAny(token, "\r\n") {
			return "", "", "", fmt.Errorf("authentication secret is unavailable")
		}
		return token, "", "", nil
	}
	if s.BasicAuth != nil {
		password, ok := os.LookupEnv(s.BasicAuth.PasswordEnv)
		if !ok || password == "" {
			return "", "", "", fmt.Errorf("authentication secret is unavailable")
		}
		return "", s.BasicAuth.Username, password, nil
	}
	return "", "", "", nil
}
