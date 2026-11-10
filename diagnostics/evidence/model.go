// Package evidence owns source-local metric rules, evidence contracts and inspection.
// It has no CLI, WebUI, Agent or runtime-operation dependency.
package evidence

import (
	"embed"
	"encoding/json"
	"time"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

//go:embed catalog.json
var files embed.FS

type Definition struct {
	ID            string            `json:"id"`
	Title         string            `json:"title"`
	Group         string            `json:"group"`
	Source        string            `json:"source"`
	Metrics       []string          `json:"metrics"`
	Kind          string            `json:"kind"`
	Unit          string            `json:"unit"`
	Scope         string            `json:"scope"`
	Preconditions string            `json:"preconditions"`
	Rule          string            `json:"rule"`
	Limits        string            `json:"limits"`
	Availability  string            `json:"availability"`
	Match         map[string]string `json:"match"`
	Dependencies  []string          `json:"dependencies"`
	References    []string          `json:"references"`
	Client        bool              `json:"client,omitempty"`
	Branch        string            `json:"branch,omitempty"`
	Disposition   string            `json:"disposition"`
	Owner         string            `json:"owner"`
	RuleVersion   int               `json:"ruleVersion"`
}
type Problem struct {
	ID          string   `json:"id"`
	Title       string   `json:"title"`
	Icon        string   `json:"icon"`
	Sub         string   `json:"sub"`
	Description string   `json:"description"`
	Evidence    []string `json:"evidence"`
	Path        []string `json:"path"`
	Extension   []string `json:"extension"`
}
type Metric struct {
	Labels     []string `json:"labels"`
	Definition string   `json:"definition"`
}
type Catalog struct {
	Version  string            `json:"version"`
	Evidence []Definition      `json:"evidence"`
	Problems []Problem         `json:"problems"`
	Registry map[string]Metric `json:"registry"`
}

func Definitions() Catalog {
	b, _ := files.ReadFile("catalog.json")
	var c Catalog
	if json.Unmarshal(b, &c) != nil {
		panic("invalid embedded WAL catalog")
	}
	return c
}

func ProblemByID(id string) (Problem, bool) {
	for _, p := range Definitions().Problems {
		if p.ID == id {
			return p, true
		}
	}
	return Problem{}, false
}

type Target struct {
	ID            string          `yaml:"-" json:"id"`
	Label         string          `yaml:"label" json:"label"`
	Context       string          `yaml:"context,omitempty" json:"context,omitempty"`
	Prometheus    string          `yaml:"prometheus,omitempty" json:"prometheus,omitempty"`
	Metrics       string          `yaml:"metrics,omitempty" json:"metrics,omitempty"`
	Trace         string          `yaml:"trace,omitempty" json:"trace,omitempty"`
	TraceService  string          `yaml:"trace_service,omitempty" json:"trace_service,omitempty"`
	Loki          string          `yaml:"loki,omitempty" json:"loki,omitempty"`
	WPCLI         string          `yaml:"wpcli,omitempty" json:"wpcli,omitempty"`
	LogNamespace  string          `yaml:"log_namespace,omitempty" json:"log_namespace,omitempty"`
	LogID         string          `yaml:"log_id,omitempty" json:"log_id,omitempty"`
	NodeID        string          `yaml:"node_id,omitempty" json:"node_id,omitempty"`
	Path          string          `yaml:"path,omitempty" json:"path,omitempty"`
	ReaderName    string          `yaml:"reader_name,omitempty" json:"reader_name,omitempty"`
	LogName       string          `yaml:"log_name,omitempty" json:"log_name,omitempty"`
	Metadata      MetadataBinding `yaml:"metadata,omitempty" json:"-"`
	Milvus        *MilvusBinding  `yaml:"milvus,omitempty" json:"-"`
	MetadataReady bool            `yaml:"-" json:"metadata_ready,omitempty"`
	MilvusReady   bool            `yaml:"-" json:"milvus_ready,omitempty"`
}
type MetadataBinding struct {
	Etcd   string `yaml:"etcd"`
	Prefix string `yaml:"prefix"`
}
type MilvusBinding struct {
	Prometheus string `yaml:"prometheus"`
	Channel    string `yaml:"channel"`
	NodeID     string `yaml:"node_id,omitempty"`
}
type Policy struct {
	LatencyMS           float64            `yaml:"latency_ms" json:"latency_ms"`
	MinObservations     int                `yaml:"min_observations" json:"min_observations"`
	SustainMS           int64              `yaml:"sustain_ms" json:"sustain_ms"`
	MaxGapMS            int64              `yaml:"max_gap_ms" json:"max_gap_ms"`
	Thresholds          map[string]float64 `yaml:"thresholds,omitempty" json:"thresholds,omitempty"`
	TraceLatencyMS      float64            `yaml:"trace_latency_ms,omitempty" json:"trace_latency_ms,omitempty"`
	CalculationWindowMS int64              `yaml:"calculation_window_ms,omitempty" json:"calculation_window_ms,omitempty"`
	RuleSources         map[string]string  `yaml:"rule_sources,omitempty" json:"rule_sources,omitempty"`
	RuleFiles           []RuleFile         `yaml:"rule_files,omitempty" json:"rule_files,omitempty"`
}

// RuleFile identifies the independent configuration used by a diagnostic run.
type RuleFile struct {
	Source string `yaml:"source" json:"source"`
	Path   string `yaml:"path" json:"path"`
	Loaded bool   `yaml:"loaded" json:"loaded"`
	SHA256 string `yaml:"sha256,omitempty" json:"sha256,omitempty"`
}
type TimeRange struct {
	Mode       string     `json:"mode"`
	Start      time.Time  `json:"start,omitempty"`
	End        time.Time  `json:"end,omitempty"`
	ObservedAt *time.Time `json:"observed_at,omitempty"`
}
type Evidence struct {
	ID             string               `json:"id"`
	Status         string               `json:"status"`
	DataState      string               `json:"data_state,omitempty"`
	StatusLabel    string               `json:"status_label,omitempty"`
	Value          string               `json:"value"`
	Note           string               `json:"note"`
	Code           string               `json:"code,omitempty"`
	Facts          []prometheus.WALFact `json:"facts,omitempty"`
	ObservationIDs []string             `json:"observation_ids"`
	Finding        *d.Finding           `json:"finding,omitempty"`
}
type Report struct {
	SchemaVersion string          `json:"schema_version"`
	RunID         string          `json:"run_id"`
	Problem       string          `json:"problem"`
	Check         string          `json:"check,omitempty"`
	Target        Target          `json:"target"`
	TimeRange     TimeRange       `json:"time_range"`
	Policy        Policy          `json:"policy"`
	Status        string          `json:"status"`
	Complete      bool            `json:"complete"`
	StartedAt     time.Time       `json:"started_at"`
	FinishedAt    time.Time       `json:"finished_at"`
	Evidence      []Evidence      `json:"evidence"`
	Observations  []d.Observation `json:"observations"`
	Problems      []d.Problem     `json:"problems"`
}

func unknown(id, code, note string) Evidence {
	return Evidence{ID: id, Status: "unknown", Code: code, Value: "—", Note: note, ObservationIDs: []string{}}
}
func Unknown(id, code, note string) Evidence { return unknown(id, code, note) }
