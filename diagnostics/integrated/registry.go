package integrated

import (
	"fmt"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/jaeger"
	"github.com/zilliztech/woodpecker/diagnostics/loki"
	"github.com/zilliztech/woodpecker/diagnostics/milvusappend"
	"github.com/zilliztech/woodpecker/diagnostics/milvusconsume"
	"github.com/zilliztech/woodpecker/diagnostics/milvuspersistence"
	"github.com/zilliztech/woodpecker/diagnostics/milvusquery"
	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
	"github.com/zilliztech/woodpecker/diagnostics/wpcli"
)

type Runner struct{ definitions []Definition }

func Standard() *Runner {
	r, _ := New(
		Definition{ID: "wpcli", Source: "wpcli", Module: wpcli.Module{}},
		Definition{ID: "prometheus", Source: "prometheus", Module: prometheus.Module{}},
		Definition{ID: "trace", Source: "trace", Module: jaeger.Module{}},
		Definition{ID: "loki", Source: "loki", Module: loki.Module{}},
		Definition{ID: "milvus.append", Source: "prometheus", Stage: "append", Module: prometheus.AppendModule{}, Assess: milvusappend.Assess},
		Definition{ID: "milvus.persistence", Source: "prometheus", Stage: "persistence", Module: prometheus.PersistenceModule{}, Assess: milvuspersistence.Assess},
		Definition{ID: "milvus.consume", Source: "prometheus", Stage: "consume", Module: prometheus.ConsumeModule{}, Assess: milvusconsume.Assess},
		Definition{ID: "milvus.query", Source: "prometheus", Stage: "query", Module: prometheus.QueryModule{}, Assess: milvusquery.Assess},
	)
	return r
}

func New(definitions ...Definition) (*Runner, error) {
	seen := map[string]bool{}
	if len(definitions) == 0 || len(definitions) > 16 {
		return nil, fmt.Errorf("register 1..16 definitions")
	}
	definitions = append([]Definition{}, definitions...)
	for i := range definitions {
		x := &definitions[i]
		if x.Module == nil || x.ID == "" || seen[x.ID] || x.Module.Descriptor().Source != x.Source {
			return nil, fmt.Errorf("invalid/duplicate integrated module")
		}
		if _, err := d.NewEngine(x.Module); err != nil {
			return nil, err
		}
		x.Profiles = copyProfiles(x.Module.Descriptor().Profiles)
		seen[x.ID] = true
	}
	return &Runner{definitions: append([]Definition{}, definitions...)}, nil
}

func (r *Runner) Catalog() []Definition {
	definitions := append([]Definition{}, r.definitions...)
	for i := range definitions {
		definitions[i].Profiles = copyProfiles(definitions[i].Profiles)
	}
	return definitions
}

func copyProfiles(profiles []d.Profile) []d.Profile {
	out := append([]d.Profile{}, profiles...)
	for i := range out {
		out[i].Modes = append([]string{}, out[i].Modes...)
		out[i].TargetFields = append([]string{}, out[i].TargetFields...)
	}
	return out
}

func (r *Runner) definition(id string) *Definition {
	for _, x := range r.definitions {
		if x.ID == id {
			return &x
		}
	}
	return nil
}
