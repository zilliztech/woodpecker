package webui

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/zilliztech/woodpecker/diagnostics/checks"
)

type commandFlag struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Global   bool   `json:"global"`
	Required bool   `json:"required"`
}
type operationDefinition struct {
	Name       string `json:"name"`
	Connection string `json:"connection"`
	Execution  string `json:"execution"`
	Arguments  []struct {
		Name     string `json:"name"`
		Optional bool   `json:"optional"`
	} `json:"arguments"`
	Flags []commandFlag `json:"flags"`
}
type commandRequest struct {
	Metadata          *metadataSession           `json:"metadata,omitempty"`
	Bindings          *checks.Bindings           `json:"bindings,omitempty"`
	MetadataAuthority string                     `json:"metadata_authority"`
	Name              string                     `json:"name"`
	Arguments         []string                   `json:"arguments"`
	Flags             map[string]json.RawMessage `json:"flags"`
	Context           string                     `json:"context"`
	UseMetadata       bool                       `json:"use_metadata"`
	BudgetMS          int64                      `json:"budget_ms"`
}
type commandResult struct {
	Bindings      *checks.Bindings `json:"bindings,omitempty"`
	SchemaVersion string           `json:"schema_version"`
	Name          string           `json:"name"`
	Status        string           `json:"status"`
	Context       string           `json:"context,omitempty"`
	Args          []string         `json:"argv"`
	Directory     string           `json:"working_directory"`
	Stdout        string           `json:"stdout"`
	Stderr        string           `json:"stderr"`
	ExitCode      *int             `json:"exit_code,omitempty"`
	StartedAt     time.Time        `json:"started_at"`
	FinishedAt    *time.Time       `json:"finished_at,omitempty"`
	InputOpen     bool             `json:"input_open"`
	OutputLimited bool             `json:"output_limited"`
	BudgetMS      int64            `json:"budget_ms"`
}

func commandDefinition(name string) (operationDefinition, error) {
	body, err := assets.ReadFile("assets/commands.json")
	if err != nil {
		return operationDefinition{}, fmt.Errorf("command catalog is unavailable")
	}
	var catalog struct {
		Commands []operationDefinition `json:"commands"`
	}
	if json.Unmarshal(body, &catalog) != nil {
		return operationDefinition{}, fmt.Errorf("command catalog is invalid")
	}
	for _, c := range catalog.Commands {
		if c.Name == name {
			return c, nil
		}
	}
	return operationDefinition{}, fmt.Errorf("unknown command; choose an operation from Command Explorer")
}

func commandArguments(q commandRequest, def operationDefinition, session browserSession) ([]string, error) {
	if def.Execution != "process" {
		return nil, fmt.Errorf("this operation requires a terminal or is not implemented")
	}
	if len(q.Arguments) > 64 || len(q.Flags) > 128 {
		return nil, fmt.Errorf("too many arguments or options")
	}
	if q.BudgetMS < 1000 || q.BudgetMS > 1800000 {
		return nil, fmt.Errorf("execution budget must be 1s..30m")
	}
	rootFlag := ""
	hasBucket := false
	for _, flag := range def.Flags {
		if flag.Name == "bucket" {
			hasBucket = true
		}
		if flag.Name == "root" || flag.Name == "root-path" {
			rootFlag = flag.Name
		}
	}
	if hasBucket && rootFlag != "" {
		var bucket, root string
		_ = json.Unmarshal(q.Flags["bucket"], &bucket)
		_ = json.Unmarshal(q.Flags[rootFlag], &root)
		physical := def.Name == "segment probe" || def.Name == "segment inspect" || def.Name == "log scan"
		if (bucket == "") != (root == "") || physical && bucket == "" {
			return nil, fmt.Errorf("provide bucket and storage root path together for this physical inspection")
		}
	}
	minimum := 0
	variadic := false
	for _, a := range def.Arguments {
		if !a.Optional {
			minimum++
		}
		variadic = variadic || strings.Contains(a.Name, "...")
	}
	if def.Name == "log skip-range remove" && string(q.Flags["log-id"]) != "" {
		minimum = 1
		if len(q.Arguments) != 1 {
			return nil, fmt.Errorf("Log ID mode requires one Segment ID positional argument")
		}
	}
	if len(q.Arguments) < minimum || (!variadic && len(q.Arguments) > len(def.Arguments)) {
		return nil, fmt.Errorf("fill the command's required positional parameters")
	}
	for _, a := range q.Arguments {
		if len(a) > 4096 || strings.ContainsRune(a, 0) {
			return nil, fmt.Errorf("invalid or oversized positional argument")
		}
	}
	allowed := map[string]commandFlag{}
	for _, f := range def.Flags {
		allowed[f.Name] = f
	}
	flags := map[string]json.RawMessage{}
	for name, value := range q.Flags {
		if _, ok := allowed[name]; !ok {
			return nil, fmt.Errorf("unsupported option for this command")
		}
		if name == "context" || name == "config" || name == "endpoint" || name == "node-admin-url" || name == "kubectl" {
			return nil, fmt.Errorf("select connections through the workspace settings")
		}
		flags[name] = value
	}
	for _, f := range def.Flags {
		if f.Required && (len(flags[f.Name]) == 0 || string(flags[f.Name]) == `""`) {
			return nil, fmt.Errorf("provide required option --%s", f.Name)
		}
	}
	if q.Metadata != nil {
		if q.Metadata.ClientMeta != "" {
			return nil, fmt.Errorf("client meta profiles are no longer used; provide explicit etcd endpoints and metadata prefix")
		}
		if def.Connection != "metadata" && def.Connection != "mixed" && def.Connection != "etcd" {
			return nil, fmt.Errorf("this operation does not use client metadata")
		}
		explicit := *q.Metadata
		// Storage is an explicit command flag pair, not metadata connection input.
		explicit.Bucket, explicit.RootPath = "", ""
		if def.Connection == "etcd" && explicit.Prefix == "" {
			explicit.Prefix = "raw-keys"
		}
		meta, err := normalizeMetadata(explicit)
		if err != nil {
			return nil, err
		}
		for key, value := range map[string]string{"etcd": meta.Etcd, "meta-prefix": meta.Prefix} {
			if _, ok := allowed[key]; ok {
				flags[key], _ = json.Marshal(value)
			}
		}
		if _, ok := allowed["standalone"]; ok {
			flags["standalone"] = json.RawMessage("true")
		}
	} else if q.UseMetadata {
		if session.Metadata == nil {
			return nil, fmt.Errorf("connect to client metadata before using this binding")
		}
		if q.MetadataAuthority != "" && q.MetadataAuthority != metadataKey(*session.Metadata) {
			return nil, fmt.Errorf("metadata selection changed; review the connection and run again")
		}
		if _, ok := allowed["etcd"]; !ok {
			return nil, fmt.Errorf("this command does not accept a client metadata binding")
		}
		for key, value := range map[string]string{"etcd": session.Metadata.Etcd, "meta-prefix": session.Metadata.Prefix} {
			if _, present := flags[key]; !present {
				flags[key], _ = json.Marshal(value)
			}
		}
		if _, ok := allowed["standalone"]; ok {
			flags["standalone"] = json.RawMessage("true")
		}
	}
	if q.Bindings != nil {
		if def.Connection != "telemetry" {
			return nil, fmt.Errorf("Client/Server bindings are only used by diagnostic checks")
		}
	}
	args := []string{}
	if q.Context != "" {
		args = append(args, "--context", q.Context)
	}
	args = append(args, strings.Split(def.Name, " ")...)
	keys := []string{}
	for name := range flags {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	for _, key := range keys {
		flag, value := allowed[key], flags[key]
		if flag.Type == "bool" {
			var b bool
			if json.Unmarshal(value, &b) != nil {
				return nil, fmt.Errorf("boolean option requires true or false")
			}
			args = append(args, fmt.Sprintf("--%s=%t", key, b))
			continue
		}
		var values []string
		if flag.Type == "stringArray" || flag.Type == "stringSlice" {
			if json.Unmarshal(value, &values) != nil {
				var one string
				if json.Unmarshal(value, &one) != nil {
					return nil, fmt.Errorf("option requires a string or string array")
				}
				values = []string{one}
			}
		} else {
			var one string
			if json.Unmarshal(value, &one) != nil {
				return nil, fmt.Errorf("option requires a string value")
			}
			values = []string{one}
		}
		if len(values) > 32 {
			return nil, fmt.Errorf("too many repeated option values")
		}
		for _, one := range values {
			if len(one) > 4096 || strings.ContainsRune(one, 0) {
				return nil, fmt.Errorf("invalid or oversized option value")
			}
			args = append(args, "--"+key+"="+one)
		}
	}
	// A positional value beginning with '-' must never become an option.
	if len(q.Arguments) > 0 {
		args = append(args, "--")
		args = append(args, q.Arguments...)
	}
	return args, nil
}

func redactedArguments(args []string) []string {
	out := append([]string{}, args...)
	for i, a := range out {
		if strings.HasPrefix(a, "--") && (strings.Contains(a, "password=") || strings.Contains(a, "token=") || strings.Contains(a, "secret=")) {
			key, _, _ := strings.Cut(a, "=")
			out[i] = key + "=[redacted]"
		}
	}
	return out
}

func (s *Server) commandStart(w http.ResponseWriter, r *http.Request) {
	var q commandRequest
	if !decodeBody(w, r, &q) {
		return
	}
	def, err := commandDefinition(q.Name)
	if err != nil {
		fail(w, 422, "invalid_command", err.Error())
		return
	}
	session := s.session(r)
	if q.Context != "" {
		if _, err := s.workspace.file.ResolveContext(q.Context); err != nil {
			fail(w, 422, "context_missing", "choose a configured WP context")
			return
		}
	}
	if q.BudgetMS == 0 {
		q.BudgetMS = 120000
	}
	listOnly := def.Name == "diagnose" && string(q.Flags["list"]) == "true"
	if (def.Connection == "service" || def.Connection == "mixed") && !listOnly && q.Context == "" {
		fail(w, 422, "context_missing", "select a WP context for this command")
		return
	}
	if q.Bindings != nil {
		cfg, _, cfgErr := s.workspaceConfig(r)
		if cfgErr != nil {
			fail(w, 422, "configuration_invalid", cfgErr.Error())
			return
		}
		settings, settingsErr := checks.LoadSettings(cfg)
		if settingsErr != nil {
			fail(w, 422, "configuration_invalid", settingsErr.Error())
			return
		}
		query := checks.Request{}
		if len(q.Arguments) > 0 {
			if q.Name == "diag scenario" {
				query.Scenario = q.Arguments[0]
			} else if q.Name == "diag metrics check" {
				query.Source, query.Check = "metrics", q.Arguments[0]
			} else if q.Name == "diag trace check" {
				query.Source, query.Check = "trace", q.Arguments[0]
			}
		}
		defs, _, selectionErr := checks.Selection(query, settings.Policy)
		if selectionErr != nil {
			fail(w, 422, "invalid_selection", selectionErr.Error())
			return
		}
		q.Bindings = checks.BindingsForChecks(q.Bindings, defs)
		if _, _, bindErr := checks.ResolveBindings(cfg, q.Bindings); bindErr != nil {
			fail(w, 422, "invalid_bindings", bindErr.Error())
			return
		}
	}
	if q.Metadata != nil && q.Metadata.ClientMeta != "" {
		fail(w, 422, "explicit_metadata_required", "Pass explicit client metadata parameters; cli.yaml contains environment contexts only")
		return
	}

	args, err := commandArguments(q, def, session)
	if err != nil {
		fail(w, 422, "command_parameters_invalid", err.Error())
		return
	}
	j, ctx, ok := s.newWALJob(w, "command", "", nil)
	if !ok {
		return
	}
	view := commandResult{Bindings: q.Bindings, SchemaVersion: "wp.command/v1", Name: q.Name, Status: "running", Context: q.Context, Args: redactedArguments(args), Directory: filepath.Dir(s.workspace.path), BudgetMS: q.BudgetMS, StartedAt: time.Now().UTC()}
	s.mu.Lock()
	j.owner = session.ID
	j.view.Command = &view
	initial := j.view
	s.mu.Unlock()
	go s.executeCommand(ctx, j, args, view)
	send(w, 202, initial)
}

type commandWriter struct {
	s      *Server
	j      *job
	stderr bool
	cancel context.CancelFunc
}

func (out commandWriter) Write(p []byte) (int, error) {
	n := len(p)
	out.s.mu.Lock()
	result := *out.j.view.Command
	limit := 2 << 20
	current := result.Stdout
	if out.stderr {
		current = result.Stderr
		limit = 128 << 10
	}
	remaining := limit - len(current)
	if len(p) > remaining {
		p = p[:remaining]
		result.OutputLimited = true
	}
	if out.stderr {
		result.Stderr += string(p)
	} else {
		result.Stdout += string(p)
	}
	out.j.view.Command = &result
	out.s.mu.Unlock()
	if result.OutputLimited {
		out.cancel()
	}
	return n, nil
}

func (s *Server) executeCommand(parent context.Context, j *job, args []string, initial commandResult) {
	ctx, cancel := context.WithTimeout(parent, time.Duration(initial.BudgetMS)*time.Millisecond)
	defer cancel()
	configPath := s.workspace.path
	if strings.HasPrefix(initial.Name, "diag ") {
		for _, source := range []string{"metrics", "trace"} {
			if path := s.workspace.rulePaths[source]; path != "" {
				args = append(args, "--"+source+"-rules", path)
			}
		}
	}
	if initial.Name != "ctx use" {
		snapshot := *s.workspace.file
		if initial.Context == "" && (strings.HasPrefix(initial.Name, "k8s ") || strings.HasPrefix(initial.Name, "etcd ")) {
			// The form explicitly chose kubectl defaults / manual etcd. Do not
			// inherit the unrelated WP current-context's connection defaults.
			snapshot.CurrentContext = ""
		}
		if initial.Bindings != nil {
			snapshot.CurrentContext = ""
		}

		body, err := yaml.Marshal(&snapshot)
		if err == nil && initial.Bindings != nil {
			var params *os.File
			params, err = os.CreateTemp("", "wp-check-inputs-*.json")
			if err == nil {
				defer os.Remove(params.Name())
				err = json.NewEncoder(params).Encode(initial.Bindings)
				closeErr := params.Close()
				if err == nil {
					err = closeErr
				}
				if err == nil {
					at := len(args)
					for i, arg := range args {
						if arg == "--" {
							at = i
							break
						}
					}
					tail := append([]string{}, args[at:]...)
					args = append(args[:at], "--bindings-file", params.Name())
					args = append(args, tail...)
				}
			}
		}

		var file *os.File
		if err == nil {
			file, err = os.CreateTemp(filepath.Dir(s.workspace.path), ".wp-command-*.yaml")
		}
		if err == nil {
			_, err = file.Write(body)
			closeErr := file.Close()
			if err == nil {
				err = closeErr
			}
		}
		if err != nil {
			if file != nil {
				os.Remove(file.Name())
			}
			failed := initial
			failed.Status = "failed"
			failed.Stderr = "Cannot create a private cli.yaml snapshot beside the configuration file.\n"
			code := -1
			failed.ExitCode = &code
			now := time.Now().UTC()
			failed.FinishedAt = &now
			s.mu.Lock()
			j.view.Command = &failed
			s.mu.Unlock()
			body, _ := json.Marshal(failed)
			s.finishWALJob(j, body, failed.Status)
			return
		}
		configPath = file.Name()
		defer os.Remove(configPath)
	}
	cmd := exec.CommandContext(ctx, s.workspace.executable, args...) // #nosec G204 -- fixed local executable, catalog-validated literal argv.
	cmd.Dir = initial.Directory
	cmd.Env = []string{}
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		if key != "WOODPECKER_ENDPOINT" && key != "WOODPECKER_CLI_CONFIG" {
			cmd.Env = append(cmd.Env, value)
		}
	}
	cmd.Env = append(cmd.Env, "WOODPECKER_CLI_CONFIG="+configPath)
	cmd.WaitDelay = time.Second
	configureCommandProcess(cmd)
	cmd.Stdout = commandWriter{s: s, j: j, cancel: cancel}
	cmd.Stderr = commandWriter{s: s, j: j, stderr: true, cancel: cancel}
	input, err := cmd.StdinPipe()
	if err == nil {
		err = cmd.Start()
	}
	if err == nil {
		s.mu.Lock()
		j.stdin = input
		view := *j.view.Command
		view.InputOpen = true
		j.view.Command = &view
		s.mu.Unlock()
		err = cmd.Wait()
	}
	if input != nil {
		input.Close()
	}
	code := 0
	if err != nil {
		code = -1
		if cmd.ProcessState != nil {
			code = cmd.ProcessState.ExitCode()
		}
	}
	finished := time.Now().UTC()
	s.mu.Lock()
	result := *j.view.Command
	result.ExitCode = &code
	result.FinishedAt = &finished
	result.InputOpen = false
	result.Status = "completed"
	if err != nil {
		result.Status = "failed"
		if cmd.Process == nil {
			result.Stderr += "Cannot start the local wp executable.\n"
		}
	}
	if parent.Err() != nil {
		result.Status = "cancelled"
	} else if ctx.Err() == context.DeadlineExceeded {
		result.Status = "timed_out"
	}
	if result.OutputLimited {
		result.Status = "output_limit"
	}
	j.view.Command = &result
	s.mu.Unlock()
	body, _ := json.MarshalIndent(result, "", "  ")
	s.finishWALJob(j, append(body, '\n'), result.Status)
}

func (s *Server) commandInput(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Text string `json:"text"`
	}
	if !decodeBody(w, r, &q) {
		return
	}
	if len(q.Text) > 512 || strings.ContainsRune(q.Text, 0) {
		fail(w, 422, "input_invalid", "send at most 512 characters")
		return
	}
	s.mu.Lock()
	j, ok := s.jobs[r.PathValue("id")]
	if ok {
		ok = s.owns(r, j) && j.view.Kind == "command" && !j.view.ReportReady && j.stdin != nil
	}
	var input interface{ Write([]byte) (int, error) }
	if ok {
		input = j.stdin
	}
	s.mu.Unlock()
	if !ok {
		fail(w, 404, "input_unavailable", "command input is not available")
		return
	}
	if file, ok := input.(*os.File); ok {
		_ = file.SetWriteDeadline(time.Now().Add(time.Second))
	}
	if _, err := input.Write([]byte(q.Text + "\n")); err != nil {
		fail(w, 409, "input_closed", "command input was closed")
		return
	}
	send(w, 200, map[string]bool{"sent": true})
}
