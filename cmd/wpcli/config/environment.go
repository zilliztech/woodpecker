package config

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// SourceReference is an internal identifier, never a tenant binding.
func SourceReference(context, source string) string {
	h := sha256.Sum256([]byte(context + "\x00" + source))
	return fmt.Sprintf("context-%x", h[:8])
}

func (o *ObservabilityConfig) UnmarshalYAML(n *yaml.Node) error {
	var fields map[string]yaml.Node
	if err := n.Decode(&fields); err != nil {
		return err
	}
	o.Connections = map[string]yaml.Node{}
	for key, node := range fields {
		switch key {
		case "grafana":
			if err := node.Decode(&o.Grafana); err != nil {
				return err
			}
		case "prometheus", "metrics", "trace":
			if node.Kind == yaml.MappingNode {
				o.Connections[key] = node
				continue
			}
			var ref string
			if err := node.Decode(&ref); err != nil {
				return err
			}
			if key == "trace" {
				o.Trace = ref
			} else if key == "metrics" {
				o.Metrics = ref
			} else {
				o.Prometheus = ref
			}
		default:
			// Provider-owned objects are transported without a central list of
			// fields or providers. The consuming backend validates its settings.
			// Scalar references remain limited to the legacy named sources above.
			if node.Kind != yaml.MappingNode {
				return fmt.Errorf("observability connection %q must be an object", key)
			}
			o.Connections[key] = node
		}
	}
	return nil
}

func (o ObservabilityConfig) MarshalYAML() (any, error) {
	out := map[string]any{}
	for key, value := range o.Connections {
		node := value
		out[key] = &node
	}
	for key, value := range map[string]string{"prometheus": o.Prometheus, "metrics": o.Metrics, "trace": o.Trace} {
		if _, ok := out[key]; !ok && value != "" {
			out[key] = value
		}
	}
	if o.Grafana != "" {
		out["grafana"] = o.Grafana
	}
	return out, nil
}

// Legacy source definitions can still supply connection settings. Legacy client
// profiles/targets are deliberately never imported into runtime defaults.
func (f *File) normalizeConnections() {
	var legacy struct {
		Sources map[string]struct {
			Connections map[string]yaml.Node `yaml:"connections"`
		} `yaml:"sources"`
	}
	_ = f.Diagnostics.Decode(&legacy)
	for name, ctx := range f.Contexts {
		o := &ctx.Observability
		if o.Connections == nil {
			o.Connections = map[string]yaml.Node{}
		}
		for _, source := range []string{"prometheus", "trace"} {
			ref := o.MetricsReference()
			if source == "trace" {
				ref = o.Trace
			}
			node, ok := o.Connections[source]
			if source == "prometheus" && !ok {
				node, ok = o.Connections["metrics"]
				delete(o.Connections, "metrics")
			}
			if !ok && ref != "" {
				group := legacy.Sources[source]
				if source == "prometheus" && len(group.Connections) == 0 {
					group = legacy.Sources["metrics"]
				}
				node, ok = group.Connections[ref]
				if !ok && (strings.HasPrefix(ref, "http://") || strings.HasPrefix(ref, "https://")) {
					_ = node.Encode(map[string]string{"endpoint": ref})
					ok = true
				}
			}
			if ok {
				o.Connections[source] = node
				ref = SourceReference(name, source)
				if source == "trace" {
					o.Trace = ref
				} else {
					o.Prometheus = ref
					o.Metrics = ""
				}
			}
		}
		if ctx.Agent.Kind == 0 {
			ctx.Agent = f.Agent
		}
		f.Contexts[name] = ctx
	}
}

// MarshalYAML writes environment settings only. Client parameters never belong
// in cli.yaml, including legacy fields accepted while reading older files.
func (f File) MarshalYAML() (any, error) {
	contexts := map[string]any{}
	for name, ctx := range f.Contexts {
		type environment Context
		copy := ctx
		copy.Etcd = EtcdConfig{}
		copy.DiagnosticTarget = ""
		contexts[name] = environment(copy)
	}
	return struct {
		Current  string         `yaml:"current-context"`
		Contexts map[string]any `yaml:"contexts"`
		Defaults Defaults       `yaml:"defaults,omitempty"`
	}{f.CurrentContext, contexts, f.Defaults}, nil
}
