package webui

import (
	"net/url"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/checks"
	"github.com/zilliztech/woodpecker/diagnostics/jaeger"
	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

func publicObservationConnection(name, source string, c d.Connection) connectionView {
	out := connectionView{Name: name, Enabled: c.IsEnabled()}
	endpoint := ""
	if source == "metrics" {
		var s prometheus.Settings
		if d.DecodeSettings(c, &s) == nil {
			endpoint = s.Endpoint
			out.Deployment = s.Deployment
			out.MetricLabels = s.MatchLabels
		}
	} else {
		var s jaeger.Settings
		if d.DecodeSettings(c, &s) == nil {
			endpoint = s.Endpoint
			out.Deployment = s.Deployment
			out.TraceServices = s.Services
			out.TraceService = s.Search.Service
			out.IdentityTags = s.IdentityTags
			out.RequestIDTag = s.RequestIDTag
		}
	}
	if u, err := url.Parse(endpoint); err == nil && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == "" {
		out.Endpoint = endpoint
	}
	return out
}

func componentProfiles(s checks.Settings, f *config.File, session browserSession) (map[string]checks.Scope, map[string]checks.Scope, string, string) {
	clients, servers := map[string]checks.Scope{}, map[string]checks.Scope{}
	for name, ctx := range f.Contexts {
		scope := checks.Scope{Name: name, Context: name, Metrics: ctx.Observability.MetricsReference(), Trace: ctx.Observability.Trace}
		clients[name] = scope
		servers[name] = scope
	}
	return clients, servers, session.Context, session.Context
}

// Context defaults contain connections only, never tenant/check inputs.
func contextConnectionDefaults(s checks.Settings, f *config.File, cfg d.Config) map[string]any {
	out := map[string]any{}
	for name, ctx := range f.Contexts {
		scope := checks.Scope{Name: name, Context: name, Metrics: ctx.Observability.MetricsReference(), Trace: ctx.Observability.Trace}
		out[name] = map[string]any{"client": scope, "server": scope}
	}
	return out
}

func publicServerContext(ctx config.Context) map[string]any {
	endpoint := ""
	if _, err := client.ValidateNodeAdminURLs(map[string]string{"seed": ctx.Endpoint}); err == nil {
		endpoint = ctx.Endpoint
	}
	mappings := map[string]string{}
	for node, value := range ctx.NodeAdminURLs {
		if validated, err := client.ValidateNodeAdminURLs(map[string]string{node: value}); err == nil {
			mappings[node] = validated[node]
		}
	}
	grafana := ""
	if u, err := url.Parse(ctx.Observability.Grafana); err == nil && u.Host != "" && u.User == nil && (u.Scheme == "http" || u.Scheme == "https") {
		grafana = ctx.Observability.Grafana
	}
	return map[string]any{"endpoint": endpoint, "admin_port": func() int {
		if ctx.AdminPort == 0 {
			return 9091
		}
		return ctx.AdminPort
	}(), "node_admin_urls": mappings, "prometheus": ctx.Observability.MetricsReference(), "trace": ctx.Observability.Trace, "grafana": grafana}
}

// Public client selectors contain no passwords, TLS key paths or authentication
// material. The native CLI resolves client_meta by name from its private file.
type clientMetaView struct {
	Name         string          `json:"name"`
	Metadata     metadataSession `json:"metadata"`
	LogNamespace string          `json:"log_namespace,omitempty"`
	LogID        string          `json:"log_id,omitempty"`
	ReaderName   string          `json:"reader_name,omitempty"`
	TraceService string          `json:"trace_service,omitempty"`
}

func publicClientMetas(f *config.File) map[string]clientMetaView { return map[string]clientMetaView{} }
