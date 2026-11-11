package wal

import "fmt"

// RequestForCheck chooses a catalog scenario, retaining the same rules and
// dependency closure as a scenario run. No alternative per-check engine exists.
func RequestForCheck(id string) (Request, error) {
	for _, p := range Definitions().Problems {
		if contains(p.Evidence, id) {
			return Request{Problem: p.ID, Check: id}, nil
		}
		if contains(p.Extension, id) {
			return Request{Problem: p.ID, Check: id, IncludeMilvus: true}, nil
		}
	}
	return Request{}, fmt.Errorf("unknown check; use wp diag catalog")
}

// SelectedChecks includes supporting checks explicitly declared by the catalog.
// Dependencies stay visible in the report; missing evidence is never silently
// substituted with a clear verdict when running a point on its own.
func SelectedChecks(q Request) ([]string, error) {
	p, ok := ProblemByID(q.Problem)
	if !ok {
		return nil, fmt.Errorf("unknown scenario")
	}
	ids := append([]string{}, p.Evidence...)
	if q.IncludeMilvus {
		ids = append(ids, p.Extension...)
	}
	if q.Check == "" {
		return ids, nil
	}
	if !contains(ids, q.Check) {
		return nil, fmt.Errorf("check does not belong to this scenario or requires --include-milvus")
	}
	by := map[string]Definition{}
	for _, def := range Definitions().Evidence {
		by[def.ID] = def
	}
	selected := map[string]bool{}
	var visit func(string) error
	visit = func(id string) error {
		if selected[id] {
			return nil
		}
		def, ok := by[id]
		if !ok {
			return fmt.Errorf("unknown catalog dependency")
		}
		selected[id] = true
		for _, dep := range def.Dependencies {
			if err := visit(dep); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(q.Check); err != nil {
		return nil, err
	}
	out := []string{}
	for _, def := range Definitions().Evidence {
		if selected[def.ID] {
			out = append(out, def.ID)
		}
	}
	return out, nil
}
