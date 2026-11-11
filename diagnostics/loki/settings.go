// Package loki implements bounded, source-only diagnosis of known log messages.
package loki

import (
	"fmt"
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
	TenantEnv      string            `yaml:"tenant_env,omitempty"`
	Limits         Limits            `yaml:"limits,omitempty"`
}
type BasicAuth struct {
	Username    string `yaml:"username"`
	PasswordEnv string `yaml:"password_env"`
}
type Limits struct {
	MaxQueries       int   `yaml:"max_queries"`
	PageSize         int   `yaml:"page_size"`
	MaxLines         int   `yaml:"max_lines"`
	MaxResponseBytes int64 `yaml:"max_response_bytes"`
	MaxWindowMS      int64 `yaml:"max_window_ms"`
	TimeoutMS        int64 `yaml:"timeout_ms"`
}

var labelName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func settings(c d.Connection) (Settings, error) {
	s := Settings{Limits: Limits{MaxQueries: 8, PageSize: 500, MaxLines: 2000, MaxResponseBytes: 2 << 20, MaxWindowMS: 3600000, TimeoutMS: 10000}}
	if d.DecodeSettings(c, &s) != nil {
		return s, fmt.Errorf("invalid Loki settings (values withheld)")
	}
	u, e := url.Parse(s.Endpoint)
	if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || s.Deployment == "" || len(s.Deployment) > 256 || strings.ContainsAny(s.Deployment, "\x00\r\n") || s.MatchLabels["namespace"] == "" || len(s.MatchLabels) > 16 {
		return s, fmt.Errorf("require HTTP(S) base endpoint without credentials/query/fragment, deployment and match_labels.namespace")
	}
	for k, v := range s.MatchLabels {
		if !labelName.MatchString(k) || strings.HasPrefix(k, "__") || d.Has([]string{"component", "pod", "container"}, k) || v == "" || len(v) > 256 || strings.ContainsAny(v, "\x00\r\n") {
			return s, fmt.Errorf("invalid deployment label binding")
		}
	}
	l := s.Limits
	if l.MaxQueries < 1 || l.MaxQueries > 32 || l.PageSize < 1 || l.PageSize > 5000 || l.MaxLines < 1 || l.MaxLines > 20000 || l.MaxResponseBytes < 1024 || l.MaxResponseBytes > 8<<20 || l.MaxWindowMS < 1 || l.MaxWindowMS > 86400000 || l.TimeoutMS < 1 || l.TimeoutMS > 60000 {
		return s, fmt.Errorf("invalid Loki query/line/byte/time budgets")
	}
	if s.BearerTokenEnv != "" && (!labelName.MatchString(s.BearerTokenEnv) || s.BasicAuth != nil) || s.TenantEnv != "" && !labelName.MatchString(s.TenantEnv) {
		return s, fmt.Errorf("invalid authentication environment reference")
	}
	if s.BasicAuth != nil && (s.BasicAuth.Username == "" || strings.ContainsAny(s.BasicAuth.Username, ":\x00\r\n") || len(s.BasicAuth.Username) > 256 || !labelName.MatchString(s.BasicAuth.PasswordEnv)) {
		return s, fmt.Errorf("invalid basic authentication reference")
	}
	return s, nil
}

func secrets(s Settings) (token, user, password, tenant string, err error) {
	read := func(name string) string {
		if name == "" {
			return ""
		}
		v := os.Getenv(name)
		if v == "" || strings.ContainsAny(v, "\x00\r\n") {
			err = fmt.Errorf("authentication environment unavailable")
		}
		return v
	}
	token = read(s.BearerTokenEnv)
	tenant = read(s.TenantEnv)
	if s.BasicAuth != nil {
		user = s.BasicAuth.Username
		password = read(s.BasicAuth.PasswordEnv)
	}
	return
}
