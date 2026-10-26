// Package jaeger implements bounded source-only diagnosis of retained Jaeger
// traces. It does not fetch runtime registries or other monitoring backends.
package jaeger

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
	Services       map[string]string `yaml:"services"` // Exact service name -> component role.
	MatchTags      map[string]string `yaml:"match_tags,omitempty"`
	Search         Search            `yaml:"search,omitempty"`
	RequestIDTag   string            `yaml:"request_id_tag,omitempty"`
	BearerTokenEnv string            `yaml:"bearer_token_env,omitempty"`
	BasicAuth      *BasicAuth        `yaml:"basic_auth,omitempty"`
	Limits         Limits            `yaml:"limits,omitempty"`
	Policy         Policy            `yaml:"policy,omitempty"`
}

type Search struct {
	Service   string `yaml:"service"`
	Operation string `yaml:"operation,omitempty"`
}
type BasicAuth struct {
	Username    string `yaml:"username"`
	PasswordEnv string `yaml:"password_env"`
}
type Limits struct {
	MaxTraces        int   `yaml:"max_traces"`
	MaxSpans         int   `yaml:"max_spans"`
	MaxQueries       int   `yaml:"max_queries"`
	MaxResponseBytes int64 `yaml:"max_response_bytes"`
	MaxWindowMS      int64 `yaml:"max_window_ms"`
	TimeoutMS        int64 `yaml:"timeout_ms"`
	ExportDelayMS    int64 `yaml:"export_delay_ms"`
}
type Policy struct {
	SlowSpanMS  float64 `yaml:"slow_span_ms" json:"slow_span_ms"`
	ClockSkewMS int64   `yaml:"clock_skew_ms" json:"clock_skew_ms"`
}

var (
	envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	tagName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,127}$`)
)

func bounded(s string) bool { return len(s) <= 256 && !strings.ContainsAny(s, "\x00\r\n") }

func settings(c d.Connection) (Settings, error) {
	s := Settings{Limits: Limits{MaxTraces: 20, MaxSpans: 2000, MaxQueries: 8, MaxResponseBytes: 2 << 20, MaxWindowMS: 3600000, TimeoutMS: 10000}, Policy: Policy{SlowSpanMS: 1000, ClockSkewMS: 100}}
	if d.DecodeSettings(c, &s) != nil {
		return s, fmt.Errorf("invalid Jaeger settings (values withheld)")
	}
	u, err := url.Parse(s.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || s.Deployment == "" || !bounded(s.Deployment) || len(s.Services) == 0 || len(s.Services) > 32 || len(s.MatchTags) > 16 {
		return s, fmt.Errorf("Jaeger requires an HTTP(S) query base without credentials/query/fragment, a deployment and exact service bindings")
	}
	for name, role := range s.Services {
		if name == "" || !bounded(name) || !d.Has([]string{"proxy", "streamingnode", "querynode", "segcore", "wp_client", "wp_server", "other"}, role) {
			return s, fmt.Errorf("invalid service/component binding")
		}
	}
	for key, value := range s.MatchTags {
		if !d.Has([]string{"service.namespace", "deployment.environment", "deployment.environment.name", "cluster", "namespace", "woodpecker_id"}, key) || value == "" || !bounded(value) {
			return s, fmt.Errorf("invalid bounded deployment tag selector")
		}
	}
	if s.Search.Service != "" && s.Services[s.Search.Service] == "" || !bounded(s.Search.Operation) || s.RequestIDTag != "" && !tagName.MatchString(s.RequestIDTag) {
		return s, fmt.Errorf("search service must be explicitly bound and request ID tag must be a literal tag name")
	}
	l, p := s.Limits, s.Policy
	if l.MaxTraces < 1 || l.MaxTraces > 100 || l.MaxSpans < 1 || l.MaxSpans > 20000 || l.MaxQueries < 1 || l.MaxQueries > 32 || l.MaxResponseBytes < 1024 || l.MaxResponseBytes > 8<<20 || l.MaxWindowMS < 1 || l.MaxWindowMS > 86400000 || l.TimeoutMS < 1 || l.TimeoutMS > 60000 || l.ExportDelayMS < 0 || l.ExportDelayMS > 30000 || math.IsNaN(p.SlowSpanMS) || math.IsInf(p.SlowSpanMS, 0) || p.SlowSpanMS <= 0 || p.SlowSpanMS > 86400000 || p.ClockSkewMS < 0 || p.ClockSkewMS > 60000 {
		return s, fmt.Errorf("invalid Jaeger collection or policy budgets")
	}
	if s.BearerTokenEnv != "" && (!envName.MatchString(s.BearerTokenEnv) || s.BasicAuth != nil) {
		return s, fmt.Errorf("select one authentication mode using environment references")
	}
	if s.BasicAuth != nil && (s.BasicAuth.Username == "" || !bounded(s.BasicAuth.Username) || strings.Contains(s.BasicAuth.Username, ":") || !envName.MatchString(s.BasicAuth.PasswordEnv)) {
		return s, fmt.Errorf("invalid basic-auth environment reference")
	}
	return s, nil
}

func auth(s Settings) (string, string, string, error) {
	if s.BearerTokenEnv != "" {
		token := os.Getenv(s.BearerTokenEnv)
		if token == "" || strings.ContainsAny(token, "\r\n") {
			return "", "", "", fmt.Errorf("authentication secret unavailable")
		}
		return token, "", "", nil
	}
	if s.BasicAuth != nil {
		password := os.Getenv(s.BasicAuth.PasswordEnv)
		if password == "" {
			return "", "", "", fmt.Errorf("authentication secret unavailable")
		}
		return "", s.BasicAuth.Username, password, nil
	}
	return "", "", "", nil
}
