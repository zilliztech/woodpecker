package checks

import (
	"context"

	"github.com/zilliztech/woodpecker/cmd/wpcli/candidates"
	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/jaeger"
	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

// ProbeConnections uses only source connection fields, not check scope or time.
func ProbeConnections(ctx context.Context, c d.Config, source string, b *Bindings) []d.ConnectionCheck {
	out := []d.ConnectionCheck{}
	if source != "metrics" && source != "trace" {
		return []d.ConnectionCheck{d.FailedConnection("invalid_source", "Choose Metrics or Trace.", "Select one evidence source.")}
	}
	for _, role := range []string{"client", "server"} {
		if b == nil || b.scope(role) == nil {
			continue
		}
		v := b.scope(role)
		s := Scope{Deployment: v.Deployment}
		key := "prometheus"
		if source == "metrics" {
			s.Metrics = v.Metrics
			s.MetricsEndpoint = v.MetricsEndpoint
			s.MetricLabels = v.MetricLabels
		} else {
			key = "trace"
			s.Trace = v.Trace
			s.TraceEndpoint = v.TraceEndpoint
			s.TraceService = v.TraceService
		}
		selected := &Bindings{}
		if role == "client" {
			selected.Client = &s
		} else {
			selected.Server = &s
		}
		cfg, targets, err := ResolveBindings(c, selected)
		result := d.ConnectionCheck{}
		if err != nil {
			result = d.FailedConnection("invalid_connection_settings", err.Error(), "Review the source address and connection settings.")
		} else {
			target := targets[role]
			ref := target.Prometheus
			if source == "trace" {
				ref = target.Trace
			}
			conn, ok := connection(cfg, key, ref)
			if !ok {
				result = d.FailedConnection("connection_not_configured", "This evidence source is missing or disabled.", "Enter an address or configure an enabled source in the server context.")
			} else if source == "metrics" {
				result = prometheus.CheckConnection(ctx, conn)
			} else {
				result = jaeger.CheckConnection(ctx, conn)
			}
		}
		result.Source = source
		result.Component = role
		out = append(out, result)
	}
	if len(out) == 0 {
		out = append(out, d.FailedConnection("connection_not_configured", "No connection was selected.", "Enter a source address or select a server context."))
	}
	return out
}

// RememberProbedConnections records only addresses with a successful API response.
func RememberProbedConnections(c d.Config, b *Bindings, source string, results []d.ConnectionCheck) error {
	values := candidates.Values{}
	for _, result := range results {
		if result.Status != "ready" && result.Status != "warning" {
			continue
		}
		if b == nil || b.scope(result.Component) == nil {
			continue
		}
		s := b.scope(result.Component)
		ref, endpoint, key := s.Metrics, s.MetricsEndpoint, "prometheus"
		if source == "trace" {
			ref, endpoint, key = s.Trace, s.TraceEndpoint, "trace"
		}
		if endpoint == "" {
			var settings struct {
				Endpoint string `yaml:"endpoint"`
			}
			conn := c.Sources[key].Connections[ref]
			_ = conn.Settings.Decode(&settings)
			endpoint = settings.Endpoint
		}
		if endpoint != "" {
			values[source+"_endpoint"] = append(values[source+"_endpoint"], endpoint)
		}
	}
	if len(values) == 0 {
		return nil
	}
	return candidates.Save(values)
}
