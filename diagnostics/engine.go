package diagnostics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Engine struct{ modules map[string]Module }

func NewEngine(modules ...Module) (*Engine, error) {
	e := &Engine{modules: map[string]Module{}}
	for _, module := range modules {
		d := module.Descriptor()
		if !Has(knownSources, d.Source) || e.modules[d.Source] != nil || len(d.Profiles) == 0 {
			return nil, fmt.Errorf("invalid/duplicate module registration")
		}
		seen := map[string]bool{}
		for _, p := range d.Profiles {
			if p.ID == "" || p.Version < 1 || seen[p.ID] || !Has([]string{"wp", "milvus", "e2e"}, p.Scope) || !Has([]string{"read", "write"}, p.Flow) || len(p.Modes) == 0 {
				return nil, fmt.Errorf("invalid module profile")
			}
			seen[p.ID] = true
		}
		e.modules[d.Source] = module
	}
	return e, nil
}

func (e *Engine) Catalog() Catalog {
	c := Catalog{SchemaVersion: CatalogSchema, Sources: []Descriptor{}}
	issues := map[string]int{"wpcli": 451, "prometheus": 460, "trace": 461, "loki": 462}
	for _, source := range knownSources {
		d := Descriptor{Source: source, Status: "not_implemented", Issue: issues[source], Profiles: []Profile{}}
		if module := e.modules[source]; module != nil {
			d = module.Descriptor()
			d.Status = "available"
		}
		c.Sources = append(c.Sources, d)
	}
	return c
}

// Run executes one module. Multi-source orchestration belongs to #467.
func (e *Engine) Run(ctx context.Context, config Config, request Request) Report {
	r := Report{SchemaVersion: ReportSchema, RunID: uuid.NewString(), Selection: request.Selection, Limits: request.Limits, StartedAt: time.Now().UTC(), Results: []Result{}}
	finish := func(result Result) Report {
		result.Connection = request.Connection
		r.Results = append(r.Results, result)
		r.Status = result.Status
		r.FinishedAt = time.Now().UTC()
		return r
	}
	selection, err := Normalize(request.Selection)
	if err != nil {
		return finish(Failure("", "invalid", "invalid_selection", err.Error()))
	}
	r.Selection, request.Selection = selection, selection
	if err := ValidateLimits(request.Limits); err != nil {
		return finish(Failure("", "invalid", "invalid_limits", err.Error()))
	}
	if len(selection.Sources) != 1 {
		return finish(Failure("", "unsupported", "multi_source_not_implemented", "Select one source; integrated orchestration is tracked in #467"))
	}
	source := selection.Sources[0]
	module := e.modules[source]
	if module == nil {
		return finish(Failure(source, "unsupported", "module_not_implemented", "Inspect capabilities for the source implementation issue"))
	}
	var profiles []Profile
	for _, p := range module.Descriptor().Profiles {
		if p.Scope == selection.Scope && p.Flow == selection.Flow && (selection.Stage == "" || p.Stage == selection.Stage) && (selection.Profile == "" || p.ID == selection.Profile) && Has(p.Modes, selection.Window.Mode) {
			profiles = append(profiles, p)
		}
	}
	if len(profiles) != 1 {
		return finish(Failure(source, "unsupported", "profile_not_supported", "Select a supported scope/flow/stage/window/profile from capabilities"))
	}
	profile := profiles[0]
	for _, field := range SelectedTargetFields(selection.Target) {
		if !Has(profile.TargetFields, field) {
			return finish(Failure(source, "unsupported", "target_filter_not_supported", "This profile cannot establish the requested target filter"))
		}
	}
	request.Profile = profile
	request.Selection.Stage = profile.Stage
	request.Selection.Profile = profile.ID
	r.Selection = request.Selection
	group := config.Sources[source]
	connection, ok := group.Connections[request.Connection]
	if !ok {
		return finish(Failure(source, "invalid", "connection_not_found", "Select a named connection for this source in --config"))
	}
	if !connection.IsEnabled() {
		return finish(Failure(source, "disabled", "connection_disabled", "The selected source connection is disabled"))
	}
	if module.ValidateConnection(connection) != nil {
		return finish(Failure(source, "invalid", "invalid_connection_settings", "Check the selected source connection's documented settings"))
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(request.Limits.BudgetMS)*time.Millisecond)
	defer cancel()
	if ctx.Err() != nil {
		return finish(Failure(source, "cancelled", "cancelled", "Run cancelled before collection"))
	}
	result := module.Run(ctx, request, connection)
	result.Source = source
	result.Profile = profile.ID
	if ctx.Err() != nil {
		result.Status = "cancelled"
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			result.Status = "budget_exceeded"
		}
		result.Problems = append(result.Problems, Problem{Code: result.Status, Message: "Module interrupted; any valid returned evidence remains available"})
	}
	return finish(result)
}

func ExitCode(status string) int {
	switch status {
	case "completed":
		return 0
	case "invalid":
		return 2
	case "unsupported", "disabled":
		return 3
	case "budget_exceeded":
		return 5
	case "partial":
		return 6
	case "cancelled":
		return 7
	default:
		return 1
	}
}

// EncodeReport emits either a complete report or a small budget failure, never
// a truncated JSON object. Private configuration is not part of the envelope.
func EncodeReport(report Report) ([]byte, int, error) {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, 1, err
	}
	limit := report.Limits.MaxOutputBytes
	if limit < 4096 || limit > 64<<20 {
		limit = DefaultLimits().MaxOutputBytes
	}
	if int64(len(data))+1 > limit {
		report.Status = "budget_exceeded"
		report.Selection = Selection{Sources: []string{}}
		report.Assessments = nil
		report.Results = []Result{Failure("", "budget_exceeded", "report_output_limit", "Result exceeded the output budget; evidence was not emitted as truncated JSON")}
		data, err = json.MarshalIndent(report, "", "  ")
	}
	return append(data, '\n'), ExitCode(report.Status), err
}
