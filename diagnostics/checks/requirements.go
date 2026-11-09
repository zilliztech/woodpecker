package checks

// Requirements describe query dependencies, not an inference or health rule.
// Catalogs, forms and execution use this same source-owned contract.
type CheckRequirements struct {
	Component             string            `json:"component"`
	Connections           []string          `json:"connections"`
	Parameters            []string          `json:"parameters"`
	OptionalParameters    []string          `json:"optional_parameters,omitempty"`
	ConditionalParameters map[string]string `json:"conditional_parameters,omitempty"`
}

func RequirementsFor(def Definition) CheckRequirements {
	r := CheckRequirements{Component: CheckComponent(def), Connections: []string{def.Source}, Parameters: []string{"window"}}
	if def.Source == "trace" {
		r.OptionalParameters = []string{"trace_id", "trace_service"}
		r.ConditionalParameters = map[string]string{"request_id": "request_id_tag", "log_namespace": "identity_tags.log_ns", "log_id": "identity_tags.log_id", "node_id": "identity_tags.node_id", "reader_name": "identity_tags.reader_name"}
		return r
	}
	if def.Source == "metrics" {
		catalog := metricCatalog
		supported := map[string]bool{}
		nodeRequired := false
		for _, name := range def.Metrics {
			labels := catalog.Registry[name].Labels
			node, log := false, false
			for _, label := range labels {
				supported[label] = true
				node = node || label == "node_id"
				log = log || label == "log_id"
			}
			if node && !log {
				nodeRequired = true
			}
		}
		if nodeRequired {
			r.Parameters = append(r.Parameters, "node_id")
		}
		for _, field := range []struct{ parameter, label string }{{"log_namespace", "log_ns"}, {"log_id", "log_id"}, {"reader_name", "reader_name"}, {"node_id", "node_id"}, {"path", "path"}} {
			if supported[field.label] && !(field.parameter == "node_id" && nodeRequired) {
				r.OptionalParameters = append(r.OptionalParameters, field.parameter)
			}
		}
	}
	return r
}

// Ignore irrelevant fields from a reusable form. A Trace-only operation must not
// fail because an unused Metrics draft is incomplete, or query that draft.
func BindingsForChecks(b *Bindings, defs []Definition) *Bindings {
	if b == nil {
		return nil
	}
	used := map[string]map[string]bool{}
	parameters := map[string]map[string]bool{}
	for _, def := range defs {
		role := CheckComponent(def)
		if used[role] == nil {
			used[role] = map[string]bool{}
		}
		used[role][def.Source] = true
		if parameters[role] == nil {
			parameters[role] = map[string]bool{}
		}
		requirements := RequirementsFor(def)
		for _, name := range append(requirements.Parameters, requirements.OptionalParameters...) {
			parameters[role][name] = true
		}
		for name := range requirements.ConditionalParameters {
			parameters[role][name] = true
		}
	}
	out := &Bindings{}
	for _, role := range []string{"client", "server"} {
		original := b.scope(role)
		if original == nil || used[role] == nil {
			continue
		}
		s := *original
		s.Metadata = ClientMetadata{}
		s.Milvus = nil // Retired request fields never propagate to collectors.
		for name, value := range map[string]*string{"log_namespace": &s.LogNamespace, "log_id": &s.LogID, "reader_name": &s.ReaderName, "node_id": &s.NodeID, "path": &s.Path} {
			if !parameters[role][name] {
				*value = ""
			}
		}
		if !used[role]["metrics"] {
			s.Metrics = ""
			s.MetricsEndpoint = ""
			s.MetricLabels = nil
			s.Path = ""
			s.Milvus = nil
		}
		if !used[role]["trace"] {
			s.Trace = ""
			s.TraceEndpoint = ""
			s.TraceService = ""
		}
		if role == "client" {
			out.Client = &s
		} else {
			out.Server = &s
		}
	}
	return out
}
