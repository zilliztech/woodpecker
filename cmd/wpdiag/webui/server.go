// Package webui presents the existing integrated runner over a local HTTP API.
// It owns no collection, threshold, diagnosis or evidence-association rules.
package webui

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/integrated"
)

//go:embed assets/*
var assets embed.FS

const (
	maxRequestBytes = 64 << 10
	maxJobs         = 8
	maxActive       = 2
	maxHistoryBytes = 64 << 20
)

type connectionView struct {
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`
}

type jobView struct {
	ID          string             `json:"id"`
	Status      string             `json:"status"`
	CreatedAt   time.Time          `json:"created_at"`
	FinishedAt  *time.Time         `json:"finished_at,omitempty"`
	Plan        integrated.Plan    `json:"plan"`
	Events      []integrated.Event `json:"events"`
	ReportReady bool               `json:"report_ready"`
}

type job struct {
	view   jobView
	cancel context.CancelFunc
	body   []byte // Immutable, exact bytes from integrated.Encode, including its output bound.
}

// Server keeps private configuration in memory and retains bounded run history.
// Its context owns runs; an HTTP disconnect does not cancel a diagnostic run.
type Server struct {
	ctx      context.Context
	cancel   context.CancelFunc
	config   d.Config
	runner   *integrated.Runner
	token    string
	mux      *http.ServeMux
	mu       sync.Mutex
	jobs     map[string]*job
	order    []string
	active   int
	retained int
	closed   bool
	wg       sync.WaitGroup
}

func New(ctx context.Context, config d.Config, runner *integrated.Runner) *Server {
	ctx, cancel := context.WithCancel(ctx)
	s := &Server{ctx: ctx, cancel: cancel, config: config, runner: runner, token: uuid.NewString(), jobs: map[string]*job{}, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /api/catalog", s.catalog)
	s.mux.HandleFunc("POST /api/plan", s.plan)
	s.mux.HandleFunc("POST /api/runs", s.start)
	s.mux.HandleFunc("GET /api/runs", s.list)
	s.mux.HandleFunc("GET /api/runs/{id}", s.status)
	s.mux.HandleFunc("POST /api/runs/{id}/cancel", s.stop)
	s.mux.HandleFunc("GET /api/runs/{id}/report", s.report)
	s.mux.HandleFunc("GET /api/runs/{id}/evidence", s.evidence)
	s.mux.HandleFunc("GET /", s.asset)
	return s
}

func (s *Server) Close() {
	s.mu.Lock()
	s.closed = true
	s.cancel()
	s.mu.Unlock()
	s.wg.Wait()
}

func loopbackHost(host string) bool {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return host == "localhost" || net.ParseIP(strings.Trim(host, "[]")).IsLoopback()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
	if !loopbackHost(r.Host) || (r.Header.Get("Origin") != "" && r.Header.Get("Origin") != "http://"+r.Host) || strings.HasPrefix(r.URL.Path, "/api/") && r.Header.Get("Sec-Fetch-Site") == "cross-site" {
		fail(w, http.StatusForbidden, "local_origin_required", "Use the printed local URL from this browser")
		return
	}
	if r.Method == http.MethodPost && r.Header.Get("X-WPDiag-Session") != s.token {
		fail(w, http.StatusForbidden, "session_required", "Reload the local page before starting or cancelling a run")
		return
	}
	s.mux.ServeHTTP(w, r)
}

func send(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func fail(w http.ResponseWriter, status int, code, message string) {
	send(w, status, map[string]any{"error": d.Problem{Code: code, Message: message}})
}

func (s *Server) request(w http.ResponseWriter, r *http.Request) (integrated.Request, bool) {
	var q integrated.Request
	if r.Header.Get("Content-Type") != "application/json" {
		fail(w, http.StatusUnsupportedMediaType, "json_required", "Send application/json")
		return q, false
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(&q) != nil || decoder.Decode(new(any)) != io.EOF {
		fail(w, http.StatusBadRequest, "invalid_request", "Invalid or oversized request; inspect the published request schema")
		return q, false
	}
	return q, true
}

func (s *Server) catalog(w http.ResponseWriter, _ *http.Request) {
	connections := map[string][]connectionView{}
	for source, group := range s.config.Sources {
		for name, c := range group.Connections {
			connections[source] = append(connections[source], connectionView{Name: name, Enabled: c.IsEnabled()})
		}
		sort.Slice(connections[source], func(i, j int) bool { return connections[source][i].Name < connections[source][j].Name })
	}
	send(w, http.StatusOK, map[string]any{
		"schema_version": "wpdiag.webui/v1", "modules": s.runner.Catalog(), "connections": connections, "default_limits": d.DefaultLimits(),
		"history": map[string]int{"max_runs": maxJobs, "max_active": maxActive, "max_report_bytes": maxHistoryBytes},
	})
}

func (s *Server) plan(w http.ResponseWriter, r *http.Request) {
	if q, ok := s.request(w, r); ok {
		send(w, http.StatusOK, s.runner.Plan(s.config, q))
	}
}

func (s *Server) start(w http.ResponseWriter, r *http.Request) {
	q, ok := s.request(w, r)
	if !ok {
		return
	}
	p := s.runner.Plan(s.config, q)
	ready := false
	for _, entry := range p.Entries {
		ready = ready || entry.Status == "ready"
	}
	if len(p.Problems) > 0 || !ready {
		send(w, http.StatusUnprocessableEntity, map[string]any{"error": d.Problem{Code: "plan_not_executable", Message: "Inspect the plan: no executable module or invalid selection"}, "plan": p})
		return
	}
	s.mu.Lock()
	if s.closed || s.ctx.Err() != nil {
		s.mu.Unlock()
		fail(w, http.StatusServiceUnavailable, "server_stopping", "The local server is stopping")
		return
	}
	if s.active >= maxActive {
		s.mu.Unlock()
		fail(w, http.StatusTooManyRequests, "active_run_limit", "Two runs are already active; cancel or wait for one")
		return
	}
	s.evictLocked(maxJobs-1, maxHistoryBytes, "")
	ctx, cancel := context.WithCancel(s.ctx)
	j := &job{view: jobView{ID: uuid.NewString(), Status: "running", CreatedAt: time.Now().UTC(), Plan: p, Events: []integrated.Event{}}, cancel: cancel}
	s.jobs[j.view.ID] = j
	s.order = append(s.order, j.view.ID)
	s.active++
	s.wg.Add(1)
	view := j.view
	s.mu.Unlock()
	go s.execute(ctx, q, j)
	send(w, http.StatusAccepted, view)
}

func (s *Server) execute(ctx context.Context, q integrated.Request, j *job) {
	defer s.wg.Done()
	defer j.cancel()
	events := make(chan integrated.Event, 32)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for e := range events {
			s.mu.Lock()
			if len(j.view.Events) < 64 {
				j.view.Events = append(j.view.Events, e)
			}
			s.mu.Unlock()
		}
	}()
	report := s.runner.Run(ctx, s.config, q, events)
	close(events)
	<-drained
	body, _, err := integrated.Encode(report)
	status := struct {
		Status string `json:"status"`
	}{Status: report.Status}
	if err != nil || json.Unmarshal(body, &status) != nil {
		body = []byte(`{"schema_version":"wpdiag.run/v1","status":"failed","plan":{"problems":[{"code":"report_encoding_failed","message":"Report could not be encoded"}]},"runs":[],"summary":{"findings":[],"associations":[],"coverage_gaps":[]}}`)
		status.Status = "failed"
	}
	now := time.Now().UTC()
	s.mu.Lock()
	j.view.Status = status.Status
	j.view.FinishedAt = &now
	j.view.ReportReady = true
	j.body = body
	s.active--
	s.retained += len(body)
	s.evictLocked(maxJobs, maxHistoryBytes, j.view.ID)
	s.mu.Unlock()
}

// Only completed reports are evictable; active collection is never evicted.
func (s *Server) evictLocked(count, bytes int, keep string) {
	for len(s.order) > count || s.retained > bytes {
		index := -1
		for i, id := range s.order {
			if id != keep && s.jobs[id].view.ReportReady {
				index = i
				break
			}
		}
		if index < 0 {
			return
		}
		id := s.order[index]
		s.retained -= len(s.jobs[id].body)
		delete(s.jobs, id)
		s.order = append(s.order[:index], s.order[index+1:]...)
	}
}

func (s *Server) list(w http.ResponseWriter, _ *http.Request) {
	type brief struct {
		ID        string    `json:"id"`
		Status    string    `json:"status"`
		CreatedAt time.Time `json:"created_at"`
	}
	s.mu.Lock()
	rows := []brief{}
	for i := len(s.order) - 1; i >= 0; i-- {
		j := s.jobs[s.order[i]]
		rows = append(rows, brief{ID: j.view.ID, Status: j.view.Status, CreatedAt: j.view.CreatedAt})
	}
	s.mu.Unlock()
	send(w, http.StatusOK, rows)
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	j, ok := s.jobs[r.PathValue("id")]
	var v jobView
	if ok {
		v = j.view
		v.Events = append([]integrated.Event{}, j.view.Events...)
	}
	s.mu.Unlock()
	if !ok {
		fail(w, http.StatusNotFound, "run_not_found", "Run expired or was not found in this local session")
		return
	}
	send(w, http.StatusOK, v)
}

func (s *Server) stop(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	j, ok := s.jobs[r.PathValue("id")]
	if ok && !j.view.ReportReady {
		j.view.Status = "cancelling"
		j.cancel()
	}
	s.mu.Unlock()
	if !ok {
		fail(w, http.StatusNotFound, "run_not_found", "Run expired or was not found in this local session")
		return
	}
	s.status(w, r)
}

func (s *Server) reportBody(w http.ResponseWriter, r *http.Request) []byte {
	s.mu.Lock()
	j, ok := s.jobs[r.PathValue("id")]
	var body []byte
	if ok {
		body = j.body
	}
	s.mu.Unlock()
	if !ok {
		fail(w, http.StatusNotFound, "run_not_found", "Run expired or was not found in this local session")
		return nil
	}
	if body == nil {
		fail(w, http.StatusConflict, "report_pending", "Collection is still running; poll its status")
		return nil
	}
	return body
}

func (s *Server) report(w http.ResponseWriter, r *http.Request) {
	if body := s.reportBody(w, r); body != nil {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("download") == "1" {
			w.Header().Set("Content-Disposition", `attachment; filename="wpdiag-run.json"`)
		}
		_, _ = w.Write(body)
	}
}

func (s *Server) evidence(w http.ResponseWriter, r *http.Request) {
	body := s.reportBody(w, r)
	if body == nil {
		return
	}
	var report integrated.Report
	if json.Unmarshal(body, &report) != nil {
		fail(w, http.StatusInternalServerError, "invalid_report", "Stored report could not be decoded")
		return
	}
	for _, run := range report.Runs {
		if run.ModuleID != r.URL.Query().Get("module_id") {
			continue
		}
		for _, result := range run.Report.Results {
			for _, o := range result.Observations {
				if o.ID == r.URL.Query().Get("observation_id") {
					data, err := json.MarshalIndent(o, "", "  ")
					if err != nil {
						fail(w, http.StatusInternalServerError, "invalid_observation", "Observation could not be encoded")
						return
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write(data)
					return
				}
			}
		}
	}
	fail(w, http.StatusNotFound, "observation_not_found", "No original observation has this module/observation identity")
}

func (s *Server) asset(w http.ResponseWriter, r *http.Request) {
	name, ctype := "", ""
	switch r.URL.Path {
	case "/":
		name = "index.html"
		ctype = "text/html; charset=utf-8"
	case "/app.js":
		name = "app.js"
		ctype = "text/javascript; charset=utf-8"
	case "/styles.css":
		name = "styles.css"
		ctype = "text/css; charset=utf-8"
	default:
		http.NotFound(w, r)
		return
	}
	data, err := assets.ReadFile("assets/" + name)
	if err != nil {
		http.Error(w, "UI asset unavailable", http.StatusInternalServerError)
		return
	}
	if name == "index.html" {
		data = []byte(strings.ReplaceAll(string(data), "SESSION_TOKEN", s.token))
	}
	w.Header().Set("Content-Type", ctype)
	_, _ = w.Write(data)
}

// Serve binds only a loopback address. Configuration is fixed for this session;
// source settings are edited through the existing private YAML file, not HTTP.
func Serve(ctx context.Context, address string, config d.Config, out io.Writer) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || !loopbackHost(host) {
		return fmt.Errorf("--listen must be a loopback host:port, for example 127.0.0.1:8787")
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 0 || n > 65535 {
		return fmt.Errorf("--listen requires a port in 0..65535")
	}
	if host == "localhost" {
		host = "127.0.0.1"
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(host, port))
	if err != nil {
		return fmt.Errorf("cannot listen on the requested local address; check the port")
	}
	app := New(ctx, config, integrated.Standard())
	defer app.Close()
	server := &http.Server{Handler: app, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: time.Minute, MaxHeaderBytes: 16 << 10, ErrorLog: log.New(io.Discard, "", 0)}
	fmt.Fprintf(out, "WP diagnosis WebUI: http://%s\nConnections are loaded from --config; Ctrl+C stops the server and active runs.\n", listener.Addr())
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err = <-done:
		if err != http.ErrServerClosed {
			return fmt.Errorf("local HTTP server stopped unexpectedly")
		}
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}
	return nil
}
