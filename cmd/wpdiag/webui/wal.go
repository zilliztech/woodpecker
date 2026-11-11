package webui

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/zilliztech/woodpecker/diagnostics/wal"
)

func (s *Server) walCatalog(w http.ResponseWriter, r *http.Request) {
	settings, err := wal.LoadSettings(s.config)
	if err != nil {
		fail(w, 422, "invalid_workbench_config", err.Error())
		return
	}
	send(w, 200, map[string]any{"schema_version": "wpdiag.wal.ui/v1", "frontend": s.config.Frontend, "default_target": s.config.DefaultTarget, "links": settings.Links, "catalog": wal.Definitions(), "descriptions": wal.Descriptions(settings.Policy), "targets": wal.PublicTargets(settings), "policy": settings.Policy, "agent": wal.AgentAvailability(r.Context(), settings.Agent), "limits": map[string]int{"max_active": maxActive, "max_history": maxJobs}, "sources": []string{"Existing Prometheus connections are reused; configured does not mean reachable.", "Trace/log correlation and historical readable-tail capture are explicitly deferred where no exact identity is established.", "Metadata uses existing read-only wpcli views and meta check."}})
}

func decodeBody(w http.ResponseWriter, r *http.Request, target any) bool {
	if r.Header.Get("Content-Type") != "application/json" {
		fail(w, 415, "json_required", "Send application/json")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if dec.Decode(target) != nil || dec.Decode(new(any)) != io.EOF {
		fail(w, 400, "invalid_request", "Invalid or oversized request")
		return false
	}
	return true
}

func (s *Server) newWALJob(w http.ResponseWriter, kind, parent string, q *wal.Request) (*job, context.Context, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.ctx.Err() != nil {
		fail(w, 503, "server_stopping", "The local server is stopping")
		return nil, nil, false
	}
	if s.active >= maxActive {
		fail(w, 429, "active_run_limit", "Two runs are active; cancel or wait")
		return nil, nil, false
	}
	if parent != "" {
		if source := s.jobs[parent]; source == nil || !source.view.ReportReady {
			fail(w, 404, "parent_run_missing", "The original diagnostic run expired; no analysis was started")
			return nil, nil, false
		}
	}
	s.evictLocked(maxJobs-1, maxHistoryBytes, parent)
	ctx, cancel := context.WithCancel(s.ctx)
	j := &job{view: jobView{ID: uuid.NewString(), Kind: kind, ParentID: parent, WALRequest: q, Status: "running", CreatedAt: time.Now().UTC(), WALEvents: []wal.Event{}}, cancel: cancel}
	if q != nil {
		j.view.WALChecks, _ = wal.SelectedChecks(*q)
	}
	s.jobs[j.view.ID] = j
	s.order = append(s.order, j.view.ID)
	s.active++
	s.wg.Add(1)
	return j, ctx, true
}

func (s *Server) finishWALJob(j *job, body []byte, status string) {
	defer s.wg.Done()
	defer j.cancel()
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	j.view.Status = status
	j.view.FinishedAt = &now
	j.view.ReportReady = true
	j.body = body
	s.active--
	s.retained += len(body)
	s.evictLocked(maxJobs, maxHistoryBytes, j.view.ID)
}

func (s *Server) walStart(w http.ResponseWriter, r *http.Request) {
	var q wal.Request
	if !decodeBody(w, r, &q) {
		return
	}
	settings, e := wal.LoadSettings(s.config)
	if e != nil {
		fail(w, 422, "invalid_workbench_config", e.Error())
		return
	}
	if e = wal.ValidateRequest(settings, q); e != nil {
		fail(w, 422, "invalid_selection", e.Error())
		return
	}
	j, ctx, ok := s.newWALJob(w, "wal", "", &q)
	if !ok {
		return
	}
	view := j.view
	go func() {
		ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		events := make(chan wal.Event, 64)
		drained := make(chan struct{})
		go func() {
			defer close(drained)
			for event := range events {
				s.mu.Lock()
				if len(j.view.WALEvents) < 64 {
					j.view.WALEvents = append(j.view.WALEvents, event)
				}
				s.mu.Unlock()
			}
		}()
		report := wal.Run(ctx, s.config, q, events)
		close(events)
		<-drained
		body := wal.Encode(report)
		var state struct {
			Status string `json:"status"`
		}
		_ = json.Unmarshal(body, &state)
		s.finishWALJob(j, body, state.Status)
	}()
	send(w, 202, view)
}

func (s *Server) walPacket(w http.ResponseWriter, r *http.Request) {
	body := s.reportBody(w, r)
	if body == nil {
		return
	}
	packet, e := wal.PacketFromBytes(body)
	if e != nil {
		fail(w, 422, "packet_unavailable", e.Error())
		return
	}
	send(w, 200, packet)
}

func (s *Server) walObservation(w http.ResponseWriter, r *http.Request) {
	body := s.reportBody(w, r)
	if body == nil {
		return
	}
	var report wal.Report
	if json.Unmarshal(body, &report) != nil || report.SchemaVersion != "wpdiag.wal/v1" {
		fail(w, 422, "invalid_report", "Not a WAL evidence report")
		return
	}
	for _, o := range report.Observations {
		if o.ID == r.PathValue("observation") {
			data, err := json.MarshalIndent(o, "", "  ")
			if err != nil {
				fail(w, 500, "invalid_observation", "Original observation could not be encoded")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(data)
			return
		}
	}
	fail(w, 404, "observation_not_found", "No original observation has this ID")
}

func (s *Server) walAnalyze(w http.ResponseWriter, r *http.Request) {
	var empty struct{}
	if !decodeBody(w, r, &empty) {
		return
	}
	body := s.reportBody(w, r)
	if body == nil {
		return
	}
	packet, e := wal.PacketFromBytes(body)
	if e != nil {
		fail(w, 422, "packet_unavailable", e.Error())
		return
	}
	settings, e := wal.LoadSettings(s.config)
	if e != nil {
		fail(w, 422, "invalid_agent_config", "Check private backend Agent configuration")
		return
	}
	if availability := wal.AgentAvailability(r.Context(), settings.Agent); !availability.Ready {
		fail(w, 422, availability.Code, availability.Message)
		return
	}
	if len(packet.Findings) == 0 {
		fail(w, 422, "no_abnormal_findings", "No abnormal findings to analyze; the full report remains exportable")
		return
	}
	j, ctx, ok := s.newWALJob(w, "analysis", r.PathValue("id"), nil)
	if !ok {
		return
	}
	view := j.view
	go func() {
		result := wal.Analyze(ctx, settings.Agent, packet, func(message string) {
			s.mu.Lock()
			if len(j.view.WALEvents) < 64 {
				j.view.WALEvents = append(j.view.WALEvents, wal.Event{Phase: "analysis", Message: message, At: time.Now().UTC()})
			}
			s.mu.Unlock()
		})
		data, _ := json.Marshal(result)
		s.finishWALJob(j, data, result.Status)
	}()
	send(w, 202, view)
}

func (s *Server) walInspect(w http.ResponseWriter, r *http.Request) {
	body := s.reportBody(w, r)
	if body == nil {
		return
	}
	var report wal.Report
	if json.Unmarshal(body, &report) != nil {
		fail(w, 422, "invalid_report", "Invalid stored WAL report")
		return
	}
	detail, err := wal.Inspect(report, r.PathValue("evidence"))
	if err != nil {
		fail(w, 404, "evidence_not_found", "Evidence point is not in this stored report")
		return
	}
	send(w, 200, detail)
}
