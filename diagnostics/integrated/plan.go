package integrated

import (
	"sort"
	"strings"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func problem(code, message string) d.Problem { return d.Problem{Code: code, Message: message} }

func connectionName(c d.Config, q Request, source string) string {
	if q.Connections[source] != "" {
		return q.Connections[source]
	}
	if q.Connection != "" {
		return q.Connection
	}
	names := []string{}
	for name := range c.Sources[source].Connections {
		names = append(names, name)
	}
	if len(names) == 1 {
		return names[0]
	}
	return ""
}

// Plan performs only local validation. It never queries a backend or starts wp.
func (r *Runner) Plan(c d.Config, q Request) Plan {
	p := Plan{Schema: "wpdiag.plan/v1", Request: q, Status: "invalid", Entries: []Entry{}, Omitted: []d.Problem{}, Problems: []d.Problem{}, Semantics: []string{
		"Milvus stage selection controls metric assessors; native/trace/log overviews remain separately labeled source context with their declared profiles",
		"Wall-clock budget is shared; input/output byte budgets are partitioned across modules; max_targets applies per module (at most eight modules)",
		"Live modules start concurrently and retain their own actual sample windows; a current native snapshot never represents a historical interval",
		"Only observed matching identities with compatible actual event times may be associated; shared deployment/time or numeric IDs do not establish a Milvus/WP metadata mapping",
	}}
	invalid := func(code, msg string) Plan { p.Problems = append(p.Problems, problem(code, msg)); return p }
	if d.ValidateLimits(q.Limits) != nil {
		return invalid("invalid_limits", "Use the shared bounded run limits")
	}
	for _, name := range append([]string{q.Connection}, connectionNames(q.Connections)...) {
		if len(name) > 256 || strings.ContainsAny(name, "\x00\r\n") {
			return invalid("invalid_connection_name", "Connection names must be single-line values of at most 256 bytes")
		}
	}
	for source := range q.Connections {
		if !d.Has([]string{"wpcli", "prometheus", "trace", "loki"}, source) {
			return invalid("invalid_connection_source", "Connection overrides must name known sources")
		}
	}
	for source := range q.Targets {
		if !d.Has([]string{"wpcli", "prometheus", "trace", "loki"}, source) {
			return invalid("invalid_target_source", "Target overrides must name known sources")
		}
	}
	if len(q.Modules) > 8 {
		return invalid("module_budget_invalid", "Select at most eight modules")
	}
	if len(q.Modules) > 0 && (len(q.Stages) > 0 || q.Selection.Stage != "") {
		return invalid("ambiguous_module_selection", "Use either explicit modules or stage selection")
	}
	// Normalize the common request before resolving automatic source selection.
	base := q.Selection
	if len(base.Sources) == 0 {
		base.Sources = []string{"prometheus"}
	}
	autoInterval := base.Window.Mode == "live" && base.Window.IntervalMS == 0
	if autoInterval {
		base.Window.IntervalMS = min(base.Window.DurationMS, 30000)
	}
	var err error
	base, err = d.Normalize(base)
	if err != nil {
		return invalid("invalid_selection", err.Error())
	}
	q.Selection = base
	if len(p.Request.Selection.Sources) == 0 {
		q.Selection.Sources = nil
	}
	if q.CurrentNative && q.Selection.Window.Mode != "historical" {
		return invalid("current_native_requires_historical", "Current native context is only a separate snapshot alongside a historical incident")
	}
	if len(q.Modules) > 0 {
		chosen := []string{}
		for _, id := range q.Modules {
			def := r.definition(id)
			if def == nil {
				return invalid("unknown_module", "Inspect run --list for module IDs")
			}
			if !d.Has(chosen, def.Source) {
				chosen = append(chosen, def.Source)
			}
		}
		if len(q.Selection.Sources) > 0 {
			for _, s := range append(append([]string{}, chosen...), q.Selection.Sources...) {
				if !d.Has(chosen, s) || !d.Has(q.Selection.Sources, s) {
					return invalid("module_source_mismatch", "Explicit modules must match the selected source set")
				}
			}
		}
		q.Selection.Sources = chosen
	}
	if len(q.Selection.Sources) == 0 {
		for _, source := range []string{"wpcli", "prometheus", "trace", "loki"} {
			group := c.Sources[source]
			if len(group.Connections) == 0 {
				p.Omitted = append(p.Omitted, problem("source_not_configured", source+": no connection configured"))
				continue
			}
			name := connectionName(c, q, source)
			connection, found := group.Connections[name]
			if found && !connection.IsEnabled() {
				p.Omitted = append(p.Omitted, problem("connection_disabled", source+": disabled connection omitted by auto plan"))
				continue
			}
			if source == "wpcli" && (q.Selection.Scope == "milvus" || q.Selection.Window.Mode == "historical" && !q.CurrentNative) {
				p.Omitted = append(p.Omitted, problem("native_scope_or_time_unavailable", "wpcli: only current WP runtime is available"))
				continue
			}
			if q.Selection.Window.Mode == "snapshot" && d.Has([]string{"prometheus", "loki"}, source) {
				p.Omitted = append(p.Omitted, problem("snapshot_not_supported", source+": retained interval required"))
				continue
			}
			q.Selection.Sources = append(q.Selection.Sources, source)
		}
	}
	if len(q.Selection.Sources) == 0 {
		return invalid("no_selected_modules", "Configure an enabled source connection or explicitly select sources/modules")
	}
	if q.CurrentNative && (!d.Has(q.Selection.Sources, "wpcli") || len(q.Selection.Sources) < 2 || q.Selection.Scope == "milvus") {
		return invalid("current_native_requires_incident_source", "Select wpcli with at least one historical source and wp/e2e scope; a current snapshot alone cannot represent an incident")
	}
	for source := range q.Connections {
		if !d.Has(q.Selection.Sources, source) {
			return invalid("unused_connection_override", "Connection override names an unselected source")
		}
	}
	for source := range q.Targets {
		if !d.Has(q.Selection.Sources, source) {
			return invalid("unused_target_override", "Target override names an unselected source")
		}
	}
	stages := append([]string{}, q.Stages...)
	if q.Selection.Stage != "" && q.Selection.Scope != "wp" {
		stages = append(stages, q.Selection.Stage)
	}
	if len(stages) > 0 && (q.Selection.Scope == "wp" || !d.Has(q.Selection.Sources, "prometheus")) {
		return invalid("stage_selection_requires_milvus_metrics", "Milvus stage selection requires Prometheus and milvus/e2e scope; use modules for source-only runs")
	}
	if len(stages) == 0 {
		if q.Selection.Flow == "write" {
			stages = []string{"append", "persistence"}
		} else {
			stages = []string{"consume", "query"}
		}
	}
	for _, stage := range stages {
		if q.Selection.Flow == "write" && !d.Has([]string{"append", "persistence"}, stage) || q.Selection.Flow == "read" && !d.Has([]string{"consume", "query"}, stage) {
			return invalid("invalid_stages", "Select stages compatible with the flow")
		}
	}
	ids := append([]string{}, q.Modules...)
	if len(ids) == 0 {
		for _, source := range q.Selection.Sources {
			if source == "prometheus" && q.Selection.Scope != "wp" {
				for _, stage := range stages {
					ids = append(ids, "milvus."+stage)
				}
			} else {
				ids = append(ids, source)
			}
		}
	}
	unique := []string{}
	for _, id := range ids {
		if !d.Has(unique, id) {
			unique = append(unique, id)
		}
	}
	if len(unique) > 8 || q.Limits.MaxInputBytes/int64(len(unique)) < 1024 || q.Limits.MaxOutputBytes/int64(len(unique)) < 4096 {
		return invalid("module_budget_invalid", "Byte budgets must fit all selected modules (at least 1KiB input / 4KiB output each)")
	}
	if q.Selection.Profile != "" && len(unique) != 1 {
		return invalid("profile_requires_single_module", "An exact profile selector requires one module")
	}
	for _, id := range unique {
		def := r.definition(id)
		if def == nil {
			return invalid("unknown_module", "Inspect run --list")
		}
		child := d.Request{Selection: q.Selection, Connection: connectionName(c, q, def.Source), Limits: q.Limits}
		child.Selection.Sources = []string{def.Source}
		child.Selection.Stage = def.Stage
		if q.Selection.Scope == "wp" {
			child.Selection.Stage = q.Selection.Stage
		}
		if t, ok := q.Targets[def.Source]; ok {
			child.Selection.Target = t
		}
		role := "source"
		if def.Assess != nil {
			role = "stage"
		} else if q.Selection.Scope != "wp" {
			role = "source_context"
		}
		if def.Source == "wpcli" && q.Selection.Scope == "e2e" {
			child.Selection.Scope = "wp"
		}
		if def.Source == "wpcli" && q.CurrentNative {
			child.Selection.Window = d.Window{Mode: "snapshot"}
			role = "current_context"
		}
		if autoInterval && child.Selection.Window.Mode == "live" && def.Source == "wpcli" {
			child.Selection.Window.IntervalMS = min(child.Selection.Window.DurationMS, 1000)
		}
		child.Limits.MaxInputBytes /= int64(len(unique))
		child.Limits.MaxOutputBytes /= int64(len(unique))
		e := Entry{ID: id, Source: def.Source, Role: role, Request: child, Status: "ready", Problems: []d.Problem{}}
		s, err := d.Normalize(child.Selection)
		if err != nil {
			e.Status = "invalid"
			e.Problems = append(e.Problems, problem("invalid_module_selection", err.Error()))
		} else {
			e.Request.Selection = s
			profiles := []d.Profile{}
			for _, profile := range def.Profiles {
				if profile.Scope == s.Scope && profile.Flow == s.Flow && (s.Stage == "" || profile.Stage == s.Stage) && (s.Profile == "" || s.Profile == profile.ID) && d.Has(profile.Modes, s.Window.Mode) {
					profiles = append(profiles, profile)
				}
			}
			if len(profiles) != 1 {
				e.Status = "unsupported"
				e.Problems = append(e.Problems, problem("profile_not_supported", "Selected module cannot provide this scope/flow/window/profile"))
			} else {
				e.Profile = profiles[0]
				e.Request.Selection.Profile = e.Profile.ID
				e.Request.Selection.Stage = e.Profile.Stage
				for _, field := range d.SelectedTargetFields(s.Target) {
					if !d.Has(e.Profile.TargetFields, field) {
						e.Status = "unsupported"
						e.Problems = append(e.Problems, problem("target_filter_not_supported", "Module cannot establish requested target field: "+field))
					}
				}
			}
		}
		if e.Status == "ready" {
			connection, ok := c.Sources[def.Source].Connections[e.Request.Connection]
			switch {
			case !ok:
				e.Status = "invalid"
				e.Problems = append(e.Problems, problem("connection_not_found", "Select an unambiguous named connection for this source"))
			case !connection.IsEnabled():
				e.Status = "disabled"
				e.Problems = append(e.Problems, problem("connection_disabled", "Selected connection is disabled"))
			case def.Module.ValidateConnection(connection) != nil:
				e.Status = "invalid"
				e.Problems = append(e.Problems, problem("invalid_connection_settings", "Check source-owned settings; values withheld"))
			}
		}
		p.Entries = append(p.Entries, e)
	}
	sort.SliceStable(p.Entries, func(i, j int) bool { return moduleOrder(p.Entries[i].ID) < moduleOrder(p.Entries[j].ID) })
	p.Request = q
	statuses := []string{}
	for _, e := range p.Entries {
		statuses = append(statuses, e.Status)
	}
	p.Status = aggregate(statuses)
	return p
}

func connectionNames(overrides map[string]string) []string {
	names := make([]string, 0, len(overrides))
	for _, name := range overrides {
		names = append(names, name)
	}
	return names
}

func moduleOrder(id string) int {
	order := []string{"milvus.append", "milvus.persistence", "milvus.consume", "milvus.query", "prometheus", "wpcli", "trace", "loki"}
	for i, x := range order {
		if id == x {
			return i
		}
	}
	return 100
}

func aggregate(statuses []string) string {
	if len(statuses) == 0 {
		return "invalid"
	}
	good := 0
	unique := map[string]bool{}
	for _, s := range statuses {
		if s == "completed" || s == "ready" {
			good++
		}
		unique[s] = true
	}
	if good == len(statuses) {
		return "completed"
	}
	if good > 0 || unique["partial"] {
		return "partial"
	}
	for _, s := range strings.Fields("cancelled budget_exceeded failed invalid unsupported disabled") {
		if unique[s] {
			return s
		}
	}
	return "failed"
}
