package webui

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/zilliztech/woodpecker/cmd/wpcli/candidates"
	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/checks"
	diagcommand "github.com/zilliztech/woodpecker/diagnostics/command"
	"github.com/zilliztech/woodpecker/diagnostics/evidence"
	"github.com/zilliztech/woodpecker/diagnostics/reports"
	"github.com/zilliztech/woodpecker/diagnostics/wal"
	"github.com/zilliztech/woodpecker/diagnostics/wpcli"
)

type (
	sessionKey     struct{}
	browserSession struct {
		ID        string           `json:"-"`
		Context   string           `json:"context"`
		Target    string           `json:"target"`
		Metadata  *metadataSession `json:"metadata,omitempty"`
		UpdatedAt time.Time        `json:"-"`
	}
)

type metadataSession struct {
	AuthorityID string `json:"authority_id,omitempty"`
	Target      string `json:"target,omitempty"`
	Label       string `json:"label"`
	Etcd        string `json:"etcd"`
	Prefix      string `json:"prefix"`
	LogName     string `json:"log_name"`
	ClientMeta  string `json:"client_meta,omitempty"`
	Bucket      string `json:"bucket,omitempty"`
	RootPath    string `json:"root_path,omitempty"`
}
type workspace struct {
	initialTargets     map[string]string
	rulePaths          map[string]string
	executable         string
	path, startContext string
	file               *config.File
	mu                 sync.Mutex
	sessions           map[string]browserSession
}

func workspaceRoute(path, method string) bool {
	if strings.HasPrefix(path, "/api/workspace") || strings.HasPrefix(path, "/api/diag/") || strings.HasPrefix(path, "/api/ops/") {
		return true
	}
	if path == "/api/runs" {
		return method == "GET"
	}
	return strings.HasPrefix(path, "/api/runs/")
}

func (w *workspace) requestSession(out http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	id := ""
	if cookie, err := r.Cookie("wp-session"); err == nil {
		id = cookie.Value
	}
	session, ok := w.sessions[id]
	if !ok {
		if r.Method != "GET" {
			fail(out, 403, "browser_session_required", "Reload the workspace before starting an operation")
			return r, false
		}
		if len(w.sessions) >= 32 {
			old := ""
			var oldest time.Time
			for key, value := range w.sessions {
				if old == "" || value.UpdatedAt.Before(oldest) {
					old, oldest = key, value.UpdatedAt
				}
			}
			delete(w.sessions, old)
		}
		id = uuid.NewString()
		name := w.startContext
		if name == "" {
			name = w.file.CurrentContext
		}
		if _, err := w.file.ResolveContext(name); err != nil {
			name = ""
		}
		if name == "" && len(w.file.Contexts) == 1 {
			for configured := range w.file.Contexts {
				name = configured
			}
		}
		target := w.initialTargets[name]
		session = browserSession{ID: id, Context: name, Target: target}
		w.sessions[id] = session
		http.SetCookie(out, &http.Cookie{Name: "wp-session", Value: id, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
	}
	session.UpdatedAt = time.Now().UTC()
	w.sessions[id] = session
	return r.WithContext(context.WithValue(r.Context(), sessionKey{}, id)), true
}

func (s *Server) owns(r *http.Request, j *job) bool {
	return s.workspace == nil || j.owner != "" && j.owner == r.Context().Value(sessionKey{})
}

func (s *Server) session(r *http.Request) browserSession {
	id, _ := r.Context().Value(sessionKey{}).(string)
	s.workspace.mu.Lock()
	defer s.workspace.mu.Unlock()
	return s.workspace.sessions[id]
}

func (s *Server) storeSelection(v browserSession, action string) {
	s.workspace.mu.Lock()
	defer s.workspace.mu.Unlock()
	current := s.workspace.sessions[v.ID]
	switch action {
	case "use-context":
		current.Context = v.Context
	case "use-target":
		current.Target = v.Target
	case "connect-meta", "disconnect-meta":
		current.Metadata = v.Metadata
	}
	s.workspace.sessions[v.ID] = current
}

func (s *Server) workspaceConfig(r *http.Request) (d.Config, browserSession, error) {
	session := s.session(r)
	cfg, err := diagcommand.DiagnosticsConfig(s.workspace.file, s.workspace.path, s.workspace.rulePaths)
	return cfg, session, err
}

func (s *Server) workspaceAgentConfig(r *http.Request) d.Config {
	cfg := d.Config{Agent: s.workspace.file.Agent}
	if ctx, ok := s.workspace.file.Contexts[s.session(r).Context]; ok && ctx.Agent.Kind != 0 {
		cfg.Agent = ctx.Agent
	}
	if cfg.Agent.Kind == 0 {
		var legacy map[string]any
		if s.workspace.file.Diagnostics.Decode(&legacy) == nil && legacy["workbench"] != nil {
			_ = cfg.Workbench.Encode(legacy["workbench"])
		}
	}
	return cfg
}

func (s *Server) setupWorkspace(path, name string, overrides ...map[string]string) error {
	file, abs, err := diagcommand.LoadDocument(path)
	if err != nil {
		return err
	}
	if name != "" {
		if _, err = file.ResolveContext(name); err != nil {
			return fmt.Errorf("selected WP context is absent from cli.yaml")
		}
	}
	var paths map[string]string
	if len(overrides) > 0 {
		paths = overrides[0]
	}
	s.workspace = &workspace{rulePaths: checks.RuleOverrides(filepath.Dir(abs), paths), path: abs, startContext: name, file: file, sessions: map[string]browserSession{}}

	// Resolve immutable context defaults once, outside the browser-session lock.
	s.workspace.initialTargets = map[string]string{}
	if cfg, loadErr := diagcommand.DiagnosticsConfig(file, abs, s.workspace.rulePaths); loadErr == nil {
		if settings, settingsErr := checks.LoadSettings(cfg); settingsErr == nil {
			for name, ctx := range file.Contexts {
				target := ctx.DiagnosticTarget
				if _, ok := settings.Targets[target]; !ok {
					target = ""
				}
				if target == "" && len(settings.Targets) == 1 {
					for id := range settings.Targets {
						target = id
					}
				}
				s.workspace.initialTargets[name] = target
			}
		}
	}
	s.workspace.executable, err = os.Executable()
	if err != nil {
		return fmt.Errorf("cannot locate wp executable")
	}
	s.mux.HandleFunc("POST /api/ops/commands", s.commandStart)
	s.mux.HandleFunc("POST /api/ops/commands/{id}/input", s.commandInput)

	s.mux.HandleFunc("GET /api/workspace", s.workspaceInfo)
	s.mux.HandleFunc("GET /api/workspace/agent", s.workspaceAgentStatus)
	s.mux.HandleFunc("GET /api/workspace/rules/{source}", s.workspaceRulesPreview)
	s.mux.HandleFunc("GET /api/workspace/config", s.workspaceConfigPreview)
	s.mux.HandleFunc("GET /api/workspace/candidates", s.inputCandidates)
	s.mux.HandleFunc("GET /api/workspace/candidates/config", s.recentInputConfig)
	s.mux.HandleFunc("POST /api/workspace/candidates", s.forgetInputCandidate)
	s.mux.HandleFunc("POST /api/workspace/session", s.workspaceSelect)
	s.mux.HandleFunc("GET /api/diag/catalog", s.checkCatalog)
	s.mux.HandleFunc("POST /api/diag/runs", s.checkStart)
	s.mux.HandleFunc("POST /api/workspace/connections/check", s.connectionCheck)
	s.mux.HandleFunc("GET /api/diag/runs/{id}/inspect/{check}", s.checkInspect)
	s.mux.HandleFunc("GET /api/diag/runs/{id}/packet", s.checkPacket)
	s.mux.HandleFunc("GET /api/diag/runs/{id}/observation/{observation}", s.checkObservation)
	s.mux.HandleFunc("POST /api/diag/runs/{id}/analyze", s.checkAnalyze)
	s.mux.HandleFunc("GET /api/ops/cluster", s.clusterView)
	s.mux.HandleFunc("GET /api/ops/metadata/{view}", s.metadataView)
	return nil
}

func ServeWorkspace(ctx context.Context, address, path, name string, out io.Writer, overrides ...map[string]string) error {
	file, abs, err := diagcommand.LoadDocument(path)
	if err != nil {
		return err
	}
	cfg, err := diagcommand.DiagnosticsConfig(file, abs, overrides...)
	if err != nil {
		cfg = d.Config{}
	} // Page-specific errors are shown in the workspace.
	return serve(ctx, address, cfg, out, func(s *Server) error { return s.setupWorkspace(path, name, overrides...) })
}

func (s *Server) workspaceInfo(w http.ResponseWriter, r *http.Request) {
	cfg, session, configErr := s.workspaceConfig(r)
	settings, settingsErr := checks.LoadSettings(cfg)
	diagnosticError := ""
	if configErr != nil {
		diagnosticError = configErr.Error()
	} else if settingsErr != nil {
		diagnosticError = settingsErr.Error()
	}
	if diagnosticError != "" {
		settings, _ = checks.LoadSettings(d.Config{})
	}
	if s.workspace.file.Agent.Kind != 0 {
		cfg.Agent = s.workspace.file.Agent
	}
	// Public connection defaults only: credentials remain in the private cli.yaml snapshot.
	profiles := map[string]any{}
	for name, ctx := range s.workspace.file.Contexts {
		profiles[name] = map[string]any{
			"k8s":    map[string]any{"namespace": ctx.K8s.Namespace, "cluster": ctx.K8s.Cluster, "kube_context": ctx.K8s.KubeContext, "kubeconfig": ctx.K8s.Kubeconfig},
			"server": publicServerContext(ctx),
		}
	}
	names := []string{}
	availableContexts := []string{}
	for name := range s.workspace.file.Contexts {
		names = append(names, name)
		if ctx, err := s.workspace.file.ResolveContext(name); err == nil {
			if _, err = client.ValidateNodeAdminURLs(map[string]string{"seed": ctx.Endpoint}); err == nil {
				availableContexts = append(availableContexts, name)
			}
		}
	}
	sort.Strings(availableContexts)
	sort.Strings(names)
	targets := []evidence.Target{}
	authorities, recent, historyError := []metadataChoice{}, []metadataRecent{}, ""
	targets = append(targets, checks.PublicTargets(settings)...)
	clients, servers, defaultClient, defaultServer := componentProfiles(settings, s.workspace.file, session)
	agent, err := reports.AgentSettings(s.workspaceAgentConfig(r))
	state := wal.AgentState{Message: "Agent configuration is invalid", Code: "agent_configuration_invalid"}
	if err == nil {
		state = wal.AgentConfigurationState(agent)
	}
	connections := map[string][]connectionView{"metrics": {}, "trace": {}}
	for _, source := range []string{"metrics", "trace"} {
		key := source
		if source == "metrics" {
			key = "prometheus"
		}
		for name, c := range cfg.Sources[key].Connections {
			connections[source] = append(connections[source], publicObservationConnection(name, source, c))
		}
		sort.Slice(connections[source], func(i, j int) bool { return connections[source][i].Name < connections[source][j].Name })
	}
	links := map[string]string{}
	for key, value := range s.workspace.file.Links {
		u, err := url.Parse(value)
		if err == nil && u.Host != "" && u.User == nil && (u.Scheme == "http" || u.Scheme == "https") {
			links[key] = value
		}
	}
	send(w, 200, map[string]any{"schema_version": "wp.workspace/v1", "contexts": names, "connection_profiles": profiles, "available_contexts": availableContexts, "session": session, "targets": targets, "client_configs": clients, "client_meta_profiles": publicClientMetas(s.workspace.file), "server_configs": servers, "default_client": defaultClient, "default_server": defaultServer, "context_defaults": contextConnectionDefaults(settings, s.workspace.file, cfg), "metadata_authorities": authorities, "metadata_history": recent, "metadata_history_error": historyError, "metadata_history_path": candidates.Directory(), "candidate_path": candidates.Directory(), "diagnostics_error": diagnosticError, "connections": connections, "links": links, "agent": state, "config_path": s.workspace.path, "metadata_boundary": "Current WP-client metadata; authority is independent of the WP service. Not historical evidence.", "sources_note": "Configured does not mean reachable. Runs record actual query failures."})
}

func (s *Server) workspaceSelect(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Action      string `json:"action"`
		AuthorityID string `json:"authority_id"`
		Context     string `json:"context"`
		Target      string `json:"target"`
		Etcd        string `json:"etcd"`
		Prefix      string `json:"prefix"`
		LogName     string `json:"log_name"`
		Bucket      string `json:"bucket,omitempty"`
		RootPath    string `json:"root_path,omitempty"`
	}
	if !decodeBody(w, r, &q) {
		return
	}
	session := s.session(r)
	switch q.Action {
	case "use-context":
		if q.Context != "" {
			if _, err := s.workspace.file.ResolveContext(q.Context); err != nil {
				fail(w, 422, "context_missing", "Choose a configured WP context")
				return
			}
		}
		session.Context = q.Context
	case "use-target":
		cfg, err := diagcommand.DiagnosticsConfig(s.workspace.file, s.workspace.path, s.workspace.rulePaths)
		if err != nil {
			fail(w, 422, "configuration_invalid", err.Error())
			return
		}
		settings, err := checks.LoadSettings(cfg)
		if err != nil {
			fail(w, 422, "configuration_invalid", err.Error())
			return
		}
		_, ok := settings.Targets[q.Target]
		if !ok {
			fail(w, 422, "target_missing", "Choose a configured diagnostic target")
			return
		}
		session.Target = q.Target
	case "disconnect-meta":
		session.Metadata = nil
	case "connect-meta":
		meta := metadataSession{Label: "Explicit client metadata", Etcd: q.Etcd, Prefix: q.Prefix, LogName: q.LogName, Bucket: q.Bucket, RootPath: q.RootPath}
		if q.AuthorityID != "" || q.Target != "" {
			fail(w, 422, "explicit_metadata_required", "Enter etcd endpoints and full metadata prefix explicitly; suggestions only fill individual fields")
			return
		}

		normalized, err := normalizeMetadata(meta)
		if err != nil {
			fail(w, 422, "invalid_metadata_endpoint", err.Error())
			return
		}
		meta = normalized
		if meta.AuthorityID == "" {
			meta.AuthorityID = metadataHistoryID(meta)
		}
		session.Metadata = &meta
	default:
		fail(w, 400, "invalid_action", "Unknown session selection")
		return
	}
	s.storeSelection(session, q.Action)
	s.workspaceInfo(w, r)
}

func (s *Server) checkCatalog(w http.ResponseWriter, r *http.Request) {
	cfg, _, err := s.workspaceConfig(r)
	if err != nil {
		fail(w, 422, "configuration_invalid", err.Error())
		return
	}
	settings, err := checks.LoadSettings(cfg)
	if err != nil {
		fail(w, 422, "configuration_invalid", err.Error())
		return
	}
	send(w, 200, checks.Catalog{SchemaVersion: "wp.diag.catalog/v1", Checks: checks.Definitions(settings.Policy), Scenarios: checks.Scenarios(settings.Policy)})
}

func (s *Server) checkStart(w http.ResponseWriter, r *http.Request) {
	var q checks.Request
	if !decodeBody(w, r, &q) {
		return
	}
	cfg, session, err := s.workspaceConfig(r)
	if err != nil {
		fail(w, 422, "configuration_invalid", err.Error())
		return
	}
	if q.Bindings == nil {
		if q.Target == "" {
			q.Target = session.Target
		}
		settings, err := checks.LoadSettings(cfg)
		if err != nil {
			fail(w, 422, "configuration_invalid", err.Error())
			return
		}
		if _, ok := settings.Targets[q.Target]; !ok {
			fail(w, 422, "target_missing", "Choose Client/Server observability sources")
			return
		}
	} else {
		settings, settingsErr := checks.LoadSettings(cfg)
		if settingsErr != nil {
			fail(w, 422, "configuration_invalid", settingsErr.Error())
			return
		}
		defs, _, selectionErr := checks.Selection(q, settings.Policy)
		if selectionErr != nil {
			fail(w, 422, "invalid_selection", selectionErr.Error())
			return
		}
		q.Bindings = checks.BindingsForChecks(q.Bindings, defs)
		if _, _, err := checks.ResolveBindings(cfg, q.Bindings); err != nil {
			fail(w, 422, "invalid_bindings", err.Error())
			return
		}
	}
	if q.Details == "" {
		q.Details = "all"
	}
	j, ctx, ok := s.newWALJob(w, "diag", "", nil)
	if !ok {
		return
	}
	s.mu.Lock()
	j.owner = session.ID
	j.view.DiagRequest = &q
	view := j.view
	s.mu.Unlock()
	go func() {
		events := make(chan checks.Event, 256)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for event := range events {
				s.mu.Lock()
				if len(j.view.DiagEvents) < 256 {
					j.view.DiagEvents = append(j.view.DiagEvents, event)
				}
				s.mu.Unlock()
			}
		}()
		report := checks.Run(ctx, cfg, q, events)
		_ = checks.RememberInputs(cfg, report)
		close(events)
		<-done
		s.mu.Lock()
		j.view.DiagSummary = &report.Summary
		j.view.DiagTarget = report.Target.Label
		s.mu.Unlock()
		body := checks.Encode(report)
		var state struct {
			Status string `json:"status"`
		}
		_ = json.Unmarshal(body, &state)
		s.finishWALJob(j, body, state.Status)
	}()
	send(w, 202, view)
}

func (s *Server) checkInspect(w http.ResponseWriter, r *http.Request) {
	body := s.reportBody(w, r)
	if body == nil {
		return
	}
	detail, err := reports.Inspection(body, r.PathValue("check"))
	if err != nil {
		fail(w, 422, "inspection_unavailable", err.Error())
		return
	}
	send(w, 200, detail)
}

func (s *Server) checkPacket(w http.ResponseWriter, r *http.Request) {
	body := s.reportBody(w, r)
	if body == nil {
		return
	}
	packet, err := reports.Packet(body)
	if err != nil {
		fail(w, 422, "packet_unavailable", err.Error())
		return
	}
	send(w, 200, packet)
}

func (s *Server) checkObservation(w http.ResponseWriter, r *http.Request) {
	body := s.reportBody(w, r)
	if body == nil {
		return
	}
	var report checks.Report
	if json.Unmarshal(body, &report) != nil {
		fail(w, 422, "invalid_report", "Invalid report")
		return
	}
	for _, o := range report.Observations {
		if o.ID == r.PathValue("observation") {
			send(w, 200, o)
			return
		}
	}
	fail(w, 404, "observation_missing", "Observation is absent")
}

func (s *Server) checkAnalyze(w http.ResponseWriter, r *http.Request) {
	var q struct {
		PreviousAnalysisID string `json:"previous_analysis_id,omitempty"`
		Question           string `json:"question,omitempty"`
	}
	if !decodeBody(w, r, &q) {
		return
	}
	body := s.reportBody(w, r)
	if body == nil {
		return
	}
	packet, err := reports.Packet(body)
	if err != nil {
		fail(w, 422, "packet_unavailable", err.Error())
		return
	}
	if len(packet.Findings) == 0 {
		fail(w, 422, "no_abnormal_findings", "No abnormal findings; no model request was made")
		return
	}
	var previous *wal.Analysis
	if q.PreviousAnalysisID != "" {
		s.mu.Lock()
		prior := s.jobs[q.PreviousAnalysisID]
		valid := prior != nil && s.owns(r, prior) && prior.view.Kind == "analysis" && prior.view.ParentID == r.PathValue("id") && prior.view.ReportReady
		var data []byte
		if valid {
			data = prior.body
		}
		s.mu.Unlock()
		if !valid {
			fail(w, 404, "analysis_not_found", "Choose a retained analysis for this diagnostic run")
			return
		}
		previous = &wal.Analysis{}
		if json.Unmarshal(data, previous) != nil {
			fail(w, 422, "invalid_analysis", "The stored analysis cannot be used as context")
			return
		}
	}
	packet, err = reports.AnalysisPacket(packet, q.PreviousAnalysisID, previous, q.Question)
	if err != nil {
		fail(w, 422, "analysis_context_invalid", err.Error())
		return
	}
	session := s.session(r)
	agent, err := reports.AgentSettings(s.workspaceAgentConfig(r))
	if err != nil {
		fail(w, 422, "agent_configuration_invalid", err.Error())
		return
	}
	state := wal.AgentAvailability(r.Context(), agent)
	if !state.Ready {
		fail(w, 422, state.Code, state.Message)
		return
	}
	j, ctx, ok := s.newWALJob(w, "analysis", r.PathValue("id"), nil)
	if !ok {
		return
	}
	s.mu.Lock()
	j.owner = session.ID
	j.view.PreviousAnalysisID = q.PreviousAnalysisID
	view := j.view
	s.mu.Unlock()
	go func() {
		result := wal.Analyze(ctx, agent, packet, nil)
		data, _ := json.Marshal(result)
		s.finishWALJob(j, append(data, '\n'), result.Status)
	}()
	send(w, 202, view)
}

func (s *Server) clusterView(w http.ResponseWriter, r *http.Request) {
	session := s.session(r)
	c, err := s.workspace.file.ResolveContext(session.Context)
	if err != nil {
		fail(w, 422, "context_missing", "Select a WP service context")
		return
	}
	urls, err := client.ValidateNodeAdminURLs(map[string]string{"seed": c.Endpoint})
	if err != nil {
		fail(w, 422, "endpoint_invalid", "Configure a valid Admin origin in this context")
		return
	}
	mappings, err := client.ValidateNodeAdminURLs(c.NodeAdminURLs)
	if err != nil {
		fail(w, 422, "node_mapping_invalid", "Configure valid Admin mappings")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), c.Timeout)
	defer cancel()
	api := client.New(urls["seed"], client.ClientOpts{Timeout: c.Timeout, AdminPort: c.AdminPort, NodeAdminURLs: mappings})
	members, err := api.GetMemberlistContext(ctx, 2<<20)
	if err != nil {
		fail(w, 502, "admin_unavailable", "Cannot read memberlist from this context; check its Admin access and port forwarding")
		return
	}
	send(w, 200, map[string]any{"context": session.Context, "observed_at": time.Now().UTC(), "memberlist": members})
}

func (s *Server) metadataView(w http.ResponseWriter, r *http.Request) {
	session := s.session(r)
	if session.Metadata == nil {
		fail(w, 422, "metadata_not_attached", "Attach an explicit WP-client metadata authority first")
		return
	}
	id := r.PathValue("view")
	if id != "M01" && id != "M02" && id != "M03" && id != "M04" {
		fail(w, 400, "unsupported_view", "Only existing read-only metadata views/checks are available")
		return
	}
	conn, err := diagcommand.NativeConnection(s.workspace.path, "")
	if err != nil {
		fail(w, 422, "native_cli_unavailable", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Minute)
	defer cancel()
	meta := session.Metadata
	logName := meta.LogName
	if r.URL.Query().Has("log_name") {
		logName = r.URL.Query().Get("log_name")
	}
	if id != "M03" && (logName == "" || len(logName) > 512 || strings.ContainsAny(logName, "/\x00\r\n") || strings.HasPrefix(logName, "-")) {
		fail(w, 422, "log_name_required", "Enter a valid log name for this metadata view")
		return
	}
	observation, err := wpcli.ReadMetadata(ctx, conn, wpcli.MetadataRequest{ID: id, LogName: logName, Etcd: meta.Etcd, Prefix: meta.Prefix}, 2<<20)
	if err != nil {
		fail(w, 502, "metadata_unavailable", err.Error())
		return
	}
	// The native command records its successful request in the shared candidate store.
	send(w, 200, observation)
}

// Agent authentication may spawn a local CLI. It never blocks workspace setup.
func (s *Server) workspaceAgentStatus(w http.ResponseWriter, r *http.Request) {
	agent, err := reports.AgentSettings(s.workspaceAgentConfig(r))
	if err != nil {
		send(w, 200, wal.AgentState{Code: "agent_configuration_invalid", Message: "Agent configuration is invalid"})
		return
	}
	send(w, 200, wal.AgentAvailability(r.Context(), agent))
}

func (s *Server) connectionCheck(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Source   string           `json:"source"`
		Bindings *checks.Bindings `json:"bindings"`
	}
	if !decodeBody(w, r, &q) {
		return
	}
	if q.Source != "metrics" && q.Source != "trace" {
		fail(w, 400, "invalid_source", "Choose Metrics or Trace")
		return
	}
	cfg, err := diagcommand.EnvironmentConfig(s.workspace.file, s.workspace.path)
	if err != nil {
		fail(w, 422, "configuration_invalid", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 6*time.Second)
	defer cancel()
	results := checks.ProbeConnections(ctx, cfg, q.Source, q.Bindings)
	warning := ""
	if err := checks.RememberProbedConnections(cfg, q.Bindings, q.Source, results); err != nil {
		warning = "Connection results are available, but recent input history could not be saved."
	}
	send(w, 200, map[string]any{"source": q.Source, "results": results, "history_warning": warning})
}
