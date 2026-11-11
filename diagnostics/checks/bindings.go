package checks

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/evidence"
	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

// Bindings separates operational identities from retained evidence sources.
// Metadata and Context describe the operator's selection; checks never dial etcd
// or Admin, or infer a telemetry association from those addresses.
type Bindings struct {
	Client *Scope `json:"client,omitempty" yaml:"client,omitempty"`
	Server *Scope `json:"server,omitempty" yaml:"server,omitempty"`
}
type ClientMetadata struct {
	Etcd     string `json:"etcd,omitempty" yaml:"etcd,omitempty"`
	Prefix   string `json:"prefix,omitempty" yaml:"prefix,omitempty"`
	Bucket   string `json:"bucket,omitempty" yaml:"bucket,omitempty"`
	RootPath string `json:"root_path,omitempty" yaml:"root_path,omitempty"`
}
type Scope struct {
	Name            string                  `json:"name,omitempty" yaml:"name,omitempty"`
	Context         string                  `json:"context,omitempty" yaml:"context,omitempty"`
	Metadata        ClientMetadata          `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	Metrics         string                  `json:"metrics,omitempty" yaml:"metrics,omitempty"`
	Trace           string                  `json:"trace,omitempty" yaml:"trace,omitempty"`
	MetricsEndpoint string                  `json:"metrics_endpoint,omitempty" yaml:"metrics_endpoint,omitempty"`
	TraceEndpoint   string                  `json:"trace_endpoint,omitempty" yaml:"trace_endpoint,omitempty"`
	Deployment      string                  `json:"deployment,omitempty" yaml:"deployment,omitempty"` // Legacy input; Context takes precedence.
	MetricLabels    map[string]string       `json:"metric_labels,omitempty" yaml:"metric_labels,omitempty"`
	TraceService    string                  `json:"trace_service,omitempty" yaml:"trace_service,omitempty"`
	LogNamespace    string                  `json:"log_namespace,omitempty" yaml:"log_namespace,omitempty"`
	LogID           string                  `json:"log_id,omitempty" yaml:"log_id,omitempty"`
	ReaderName      string                  `json:"reader_name,omitempty" yaml:"reader_name,omitempty"`
	NodeID          string                  `json:"node_id,omitempty" yaml:"node_id,omitempty"`
	Path            string                  `json:"path,omitempty" yaml:"path,omitempty"`
	Milvus          *evidence.MilvusBinding `json:"milvus,omitempty" yaml:"milvus,omitempty"`
}

func validateScope(s Scope) error {
	for _, v := range []string{s.Name, s.Context, s.Metadata.Etcd, s.Metadata.Prefix, s.Metadata.Bucket, s.Metadata.RootPath, s.Metrics, s.Trace, s.MetricsEndpoint, s.TraceEndpoint, s.Deployment, s.TraceService, s.LogNamespace, s.LogID, s.ReaderName, s.NodeID, s.Path} {
		if len(v) > 512 || strings.ContainsAny(v, "\x00\r\n") {
			return fmt.Errorf("client/server fields must be bounded single-line values")
		}
	}
	for _, endpoint := range []string{s.MetricsEndpoint, s.TraceEndpoint} {
		if endpoint != "" {
			u, err := url.Parse(endpoint)
			if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
				return fmt.Errorf("use HTTP(S) observability endpoints without credentials or query parameters")
			}
		}
	}
	if s.Metrics != "" && s.MetricsEndpoint != "" || s.Trace != "" && s.TraceEndpoint != "" {
		return fmt.Errorf("choose a saved source or a manual endpoint, not both")
	}
	if s.LogID != "" {
		n, e := strconv.ParseInt(s.LogID, 10, 64)
		if e != nil || n < 0 || strconv.FormatInt(n, 10) != s.LogID {
			return fmt.Errorf("Log ID must be a nonnegative decimal string")
		}
	}
	if len(s.MetricLabels) > 16 || !prometheus.ValidateWALMatch(s.MetricLabels) {
		return fmt.Errorf("invalid metric population labels")
	}
	for _, key := range []string{"status", "operation", "le", "reason", "from_scope", "to_scope"} {
		if _, ok := s.MetricLabels[key]; ok {
			return fmt.Errorf("metric population labels cannot override check-owned outcome/operation filters")
		}
	}
	if s.MetricsEndpoint != "" && len(s.MetricLabels) == 0 {
		return fmt.Errorf("a manual Prometheus endpoint needs at least one population label")
	}
	if s.TraceEndpoint != "" && s.TraceService == "" {
		return fmt.Errorf("a manual Trace endpoint needs a Trace service")
	}
	if s.Milvus != nil {
		return fmt.Errorf("Milvus diagnostic associations are no longer supported")
	}
	return nil
}
func CheckComponent(def Definition) string {
	if def.Role == "wp_client" || rawDefinition(def.ID).Client {
		return "client"
	}
	return "server"
}
func (b *Bindings) scope(component string) *Scope {
	if component == "client" {
		return b.Client
	}
	return b.Server
}
func ScopeTarget(s Scope, component string) evidence.Target {
	return evidence.Target{ID: component, Label: s.Name, Context: s.Context, Metrics: s.Metrics, Prometheus: s.Metrics, Trace: s.Trace, TraceService: s.TraceService, LogNamespace: s.LogNamespace, LogID: s.LogID, ReaderName: s.ReaderName, NodeID: s.NodeID, Path: s.Path}
}

// ResolveBindings clones source maps and source-owned YAML settings. Per-run
// filters/endpoints never change the shared configuration or another run.
func ResolveBindings(c d.Config, b *Bindings) (d.Config, map[string]evidence.Target, error) {
	targets := map[string]evidence.Target{}
	if b == nil || b.Client == nil && b.Server == nil {
		return c, targets, fmt.Errorf("select Client and/or Server checks")
	}
	sources := map[string]d.SourceConfig{}
	for name, g := range c.Sources {
		cp := d.SourceConfig{Connections: map[string]d.Connection{}}
		for id, conn := range g.Connections {
			cp.Connections[id] = conn
		}
		sources[name] = cp
	}
	c.Sources = sources
	for _, role := range []string{"client", "server"} {
		selected := b.scope(role)
		if selected == nil {
			continue
		}
		s := *selected
		if err := validateScope(s); err != nil {
			return c, targets, err
		}
		for _, source := range []string{"prometheus", "trace"} {
			ref, endpoint := s.Metrics, s.MetricsEndpoint
			if source == "trace" {
				ref, endpoint = s.Trace, s.TraceEndpoint
			}
			if ref == "" && endpoint == "" {
				continue
			}
			conn, ok := c.Sources[source].Connections[ref]
			if endpoint == "" && !ok {
				return c, targets, fmt.Errorf("select an existing %s source or enter its endpoint", source)
			}
			change := endpoint != "" || source == "prometheus" && len(s.MetricLabels) > 0
			if !change {
				continue
			}
			settings := map[string]any{}
			if endpoint != "" {
				settings["endpoint"], settings["deployment"] = endpoint, s.Deployment
				if s.Context != "" {
					settings["deployment"] = s.Context
				}
				if source == "trace" {
					settings["services"] = map[string]string{s.TraceService: "wp_" + role}
					settings["search"] = map[string]string{"service": s.TraceService}
				}
			} else if conn.Settings.Decode(&settings) != nil {
				return c, targets, fmt.Errorf("invalid saved source configuration")
			}
			if source == "prometheus" {
				labels := map[string]string{}
				if raw, ok := settings["match_labels"].(map[string]interface{}); ok {
					for k, v := range raw {
						if value, ok := v.(string); ok {
							labels[k] = value
						}
					}
				}
				for k, v := range s.MetricLabels {
					labels[k] = v
				}
				settings["match_labels"] = labels
			}
			if err := conn.Settings.Encode(settings); err != nil {
				return c, targets, fmt.Errorf("cannot prepare source configuration")
			}
			id := "selection-" + role + "-" + source
			g := c.Sources[source]
			if g.Connections == nil {
				g.Connections = map[string]d.Connection{}
			}
			g.Connections[id] = conn
			c.Sources[source] = g
			if source == "prometheus" {
				s.Metrics = id
			} else {
				s.Trace = id
			}
		}
		t := ScopeTarget(s, role)
		if t.Label == "" {
			t.Label = strings.Title(role)
		}
		targets[role] = t
	}
	return c, targets, nil
}
