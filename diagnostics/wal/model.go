// Package wal owns the problem-oriented WAL check catalog and deterministic
// evidence rules. Source I/O stays in the existing source packages.
package wal

import (
	"context"
	"fmt"
	"time"

	"github.com/zilliztech/woodpecker/diagnostics/evidence"
)

type (
	Definition      = evidence.Definition
	Problem         = evidence.Problem
	Metric          = evidence.Metric
	Catalog         = evidence.Catalog
	Target          = evidence.Target
	MetadataBinding = evidence.MetadataBinding
	MilvusBinding   = evidence.MilvusBinding
	Policy          = evidence.Policy
	TimeRange       = evidence.TimeRange
	Evidence        = evidence.Evidence
	Report          = evidence.Report
)

func Definitions() Catalog                  { return evidence.Definitions() }
func ProblemByID(id string) (Problem, bool) { return evidence.ProblemByID(id) }

type AgentConfig struct {
	Provider       string `yaml:"provider,omitempty"`
	Auth           string `yaml:"auth,omitempty"`
	Enabled        bool   `yaml:"enabled"`
	Executable     string `yaml:"executable,omitempty"`
	Model          string `yaml:"model,omitempty"`
	APIKeyEnv      string `yaml:"api_key_env,omitempty"`
	TimeoutMS      int64  `yaml:"timeout_ms,omitempty"`
	MaxOutputBytes int64  `yaml:"max_output_bytes,omitempty"`
}
type Settings struct {
	Targets map[string]Target `yaml:"targets,omitempty"`
	Policy  Policy            `yaml:"policy,omitempty"`
	Agent   AgentConfig       `yaml:"agent,omitempty"`
	Links   Links             `yaml:"links,omitempty"`
}
type Links struct {
	Grafana string `yaml:"grafana,omitempty" json:"grafana,omitempty"`
}
type Request struct {
	Problem       string    `json:"problem"`
	Check         string    `json:"check,omitempty"`
	Target        string    `json:"target"`
	From          time.Time `json:"from,omitempty"`
	To            time.Time `json:"to,omitempty"`
	IncludeMilvus bool      `json:"include_milvus,omitempty"`
}
type Event struct {
	EvidenceID string    `json:"evidence_id,omitempty"`
	Phase      string    `json:"phase"`
	Message    string    `json:"message"`
	At         time.Time `json:"at"`
}

func emit(ctx context.Context, ch chan<- Event, id, phase, message string) {
	if ch != nil {
		select {
		case ch <- Event{id, phase, message, time.Now().UTC()}:
		case <-ctx.Done():
		default:
		}
	}
}

func unknown(id, code, note string) Evidence {
	return Evidence{ID: id, Status: "unknown", Code: code, Value: "—", Note: note, ObservationIDs: []string{}}
}
func observationCode(id string) string { return "wal." + id + ".v1" }
func evidenceError(id string, err error) Evidence {
	return unknown(id, "source_unavailable", fmt.Sprintf("Evidence unavailable: %s", err))
}
