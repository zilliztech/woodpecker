package webui

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPhysicalCommandsRequireExplicitStorageNamespace(t *testing.T) {
	for _, name := range []string{"segment probe", "segment inspect", "log scan"} {
		def, err := commandDefinition(name)
		require.NoError(t, err)
		arguments := []string{"my-log"}
		if name != "log scan" {
			arguments = append(arguments, "1")
		}
		q := commandRequest{Name: name, Context: "dev", Arguments: arguments, BudgetMS: 1000, Metadata: &metadataSession{Etcd: "http://etcd:2379", Prefix: "tenant/wp"}, Flags: map[string]json.RawMessage{}}
		_, err = commandArguments(q, def, browserSession{})
		require.ErrorContains(t, err, "bucket and storage root path")
		q.Flags["bucket"] = json.RawMessage(`"bucket-a"`)
		_, err = commandArguments(q, def, browserSession{})
		require.Error(t, err)
		q.Flags["root-path"] = json.RawMessage(`"wal-root"`)
		args, err := commandArguments(q, def, browserSession{})
		require.NoError(t, err)
		require.Contains(t, strings.Join(args, " "), "--bucket=bucket-a")
		require.Contains(t, strings.Join(args, " "), "--root-path=wal-root")
	}
}

func awaitCommand(t *testing.T, app *Server, id string) commandResult {
	t.Helper()
	var result commandResult
	require.Eventually(t, func() bool {
		app.mu.Lock()
		defer app.mu.Unlock()
		j := app.jobs[id]
		if j == nil || !j.view.ReportReady {
			return false
		}
		result = *j.view.Command
		return true
	}, 5*time.Second, 10*time.Millisecond)
	return result
}

func TestCommandLiteralArgumentsAndSessionOwnership(t *testing.T) {
	app, srv, _, _ := workspaceFixture(t)
	file := filepath.Join(t.TempDir(), "wp")
	require.NoError(t, os.WriteFile(file, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\"\nprintf 'CFG=%s\\n' \"$WOODPECKER_CLI_CONFIG\"\n"), 0o700))
	app.workspace.executable = file
	c := browser(t, srv.URL)
	value := "node;$(touch should-never-exist)"
	code, data := wsCall(t, c, srv.URL, app.token, "POST", "/api/ops/commands", commandRequest{Name: "ops list", Context: "a", Arguments: []string{value}, Flags: map[string]json.RawMessage{"log": json.RawMessage(`"7"`)}})
	require.Equal(t, 202, code, string(data))
	var view jobView
	require.NoError(t, json.Unmarshal(data, &view))
	result := awaitCommand(t, app, view.ID)
	require.Equal(t, "completed", result.Status)
	require.Contains(t, result.Stdout, "--log=7\n--\n"+value)
	configPath := strings.TrimSpace(strings.Split(result.Stdout, "CFG=")[1])
	require.Eventually(t, func() bool { _, err := os.Stat(configPath); return os.IsNotExist(err) }, time.Second, 10*time.Millisecond)
	other := browser(t, srv.URL)
	code, _ = wsCall(t, other, srv.URL, app.token, "GET", "/api/runs/"+view.ID, nil)
	require.Equal(t, 404, code)
	code, _ = wsCall(t, other, srv.URL, app.token, "POST", "/api/ops/commands/"+view.ID+"/input", map[string]string{"text": "y"})
	require.Equal(t, 404, code)
}

func TestCommandInputAndCancellation(t *testing.T) {
	app, srv, _, _ := workspaceFixture(t)
	file := filepath.Join(t.TempDir(), "wp")
	require.NoError(t, os.WriteFile(file, []byte("#!/bin/sh\nprintf 'Proceed? [y/N]: ' >&2\nIFS= read -r answer\nprintf 'answer=%s\\n' \"$answer\"\n"), 0o700))
	app.workspace.executable = file
	c := browser(t, srv.URL)
	start := func() string {
		code, data := wsCall(t, c, srv.URL, app.token, "POST", "/api/ops/commands", commandRequest{Name: "node cancel-decommission", Context: "a", Arguments: []string{"node-1"}})
		require.Equal(t, 202, code, string(data))
		var view jobView
		require.NoError(t, json.Unmarshal(data, &view))
		return view.ID
	}
	id := start()
	require.Eventually(t, func() bool {
		app.mu.Lock()
		defer app.mu.Unlock()
		return strings.Contains(app.jobs[id].view.Command.Stderr, "Proceed?")
	}, time.Second, 10*time.Millisecond)
	code, data := wsCall(t, c, srv.URL, app.token, "POST", "/api/ops/commands/"+id+"/input", map[string]string{"text": "n"})
	require.Equal(t, 200, code, string(data))
	result := awaitCommand(t, app, id)
	require.Contains(t, result.Stdout, "answer=n")
	id = start()
	code, _ = wsCall(t, c, srv.URL, app.token, "POST", "/api/runs/"+id+"/cancel", map[string]string{})
	require.Equal(t, 200, code)
	result = awaitCommand(t, app, id)
	require.Equal(t, "cancelled", result.Status)
}

func TestCommandValidatesCatalogAndMetadataAuthority(t *testing.T) {
	def, err := commandDefinition("ops list")
	require.NoError(t, err)
	for _, q := range []commandRequest{
		{Name: "ops list", Arguments: []string{"n"}, BudgetMS: 1000, Flags: map[string]json.RawMessage{"invented": json.RawMessage(`"x"`)}},
		{Name: "ops list", Arguments: []string{"n"}, BudgetMS: 1000, Flags: map[string]json.RawMessage{"endpoint": json.RawMessage(`"http://other"`)}},
		{Name: "ops list", BudgetMS: 1000},
	} {
		_, err := commandArguments(q, def, browserSession{})
		require.Error(t, err)
	}
	_, err = commandDefinition("sh -c touch")
	require.Error(t, err)
	def, err = commandDefinition("meta check")
	require.NoError(t, err)
	_, err = commandArguments(commandRequest{Name: "meta check", Arguments: []string{"log"}, BudgetMS: 1000, UseMetadata: true, MetadataAuthority: "other"}, def, browserSession{Metadata: &metadataSession{Etcd: "http://e:2379", Prefix: "client/wp"}})
	require.ErrorContains(t, err, "selection changed")
	def, err = commandDefinition("webui")
	require.NoError(t, err)
	_, err = commandArguments(commandRequest{BudgetMS: 1000}, def, browserSession{})
	require.Error(t, err)
}

func TestCommandOutputBound(t *testing.T) {
	app, srv, _, _ := workspaceFixture(t)
	file := filepath.Join(t.TempDir(), "wp")
	require.NoError(t, os.WriteFile(file, []byte("#!/bin/sh\nhead -c 3000000 /dev/zero\n"), 0o700))
	app.workspace.executable = file
	c := browser(t, srv.URL)
	code, data := wsCall(t, c, srv.URL, app.token, "POST", "/api/ops/commands", commandRequest{Name: "version"})
	require.Equal(t, 202, code, string(data))
	var view jobView
	require.NoError(t, json.Unmarshal(data, &view))
	result := awaitCommand(t, app, view.ID)
	require.Equal(t, "output_limit", result.Status)
	require.Len(t, result.Stdout, 2<<20)
	require.True(t, result.OutputLimited, fmt.Sprint(result.Status))
}

// These catalog branches must remain usable without a WP Admin context. The
// fake executable records argv and the private config; no etcd/kubectl is run.
func TestCommandIndependentConnectionBoundaries(t *testing.T) {
	app, srv, _, _ := workspaceFixture(t)
	file := filepath.Join(t.TempDir(), "wp")
	require.NoError(t, os.WriteFile(file, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\"\ncat \"$WOODPECKER_CLI_CONFIG\"\n"), 0o700))
	app.workspace.executable = file
	c := browser(t, srv.URL)
	for _, name := range []string{"log truncate", "log skip-range list", "log skip-range remove"} {
		def, err := commandDefinition(name)
		require.NoError(t, err)
		require.Equal(t, "metadata", def.Connection)
	}
	for _, q := range []commandRequest{
		{Name: "diag catalog"},
		{Name: "diagnose", Flags: map[string]json.RawMessage{"list": json.RawMessage("true")}},
		{Name: "k8s status"},
		{Name: "etcd list", Flags: map[string]json.RawMessage{"etcd": json.RawMessage(`"http://raw:2379"`), "standalone": json.RawMessage("true")}},
	} {
		code, data := wsCall(t, c, srv.URL, app.token, "POST", "/api/ops/commands", q)
		require.Equal(t, 202, code, string(data))
		var view jobView
		require.NoError(t, json.Unmarshal(data, &view))
		result := awaitCommand(t, app, view.ID)
		require.Equal(t, "completed", result.Status)
		if strings.HasPrefix(q.Name, "k8s ") || strings.HasPrefix(q.Name, "etcd ") {
			require.Contains(t, result.Stdout, `current-context: ""`)
		}
	}
}

func TestCommandRequiredFlagsAndAlternativeLogIdentity(t *testing.T) {
	def, err := commandDefinition("logstore segment-show")
	require.NoError(t, err)
	_, err = commandArguments(commandRequest{BudgetMS: 1000, Arguments: []string{"node"}}, def, browserSession{})
	require.ErrorContains(t, err, "--log")
	def, err = commandDefinition("log skip-range remove")
	require.NoError(t, err)
	args, err := commandArguments(commandRequest{BudgetMS: 1000, Arguments: []string{"9"}, Flags: map[string]json.RawMessage{"log-id": json.RawMessage(`"7"`)}}, def, browserSession{})
	require.NoError(t, err)
	require.Equal(t, []string{"log", "skip-range", "remove", "--log-id=7", "--", "9"}, args)
	_, err = commandArguments(commandRequest{BudgetMS: 1000, Arguments: []string{"name", "9"}, Flags: map[string]json.RawMessage{"log-id": json.RawMessage(`"7"`)}}, def, browserSession{})
	require.ErrorContains(t, err, "one Segment ID")
}

func TestExplicitMetadataFormOverridesAttachmentWithoutServerCredentials(t *testing.T) {
	def, err := commandDefinition("meta log list")
	require.NoError(t, err)
	q := commandRequest{Name: def.Name, Context: "b", BudgetMS: 1000, Metadata: &metadataSession{Etcd: "http://form-etcd:2379", Prefix: "form-client/wp"}}
	args, err := commandArguments(q, def, browserSession{Metadata: &metadataSession{Etcd: "http://session-etcd:2379", Prefix: "session/wp"}})
	require.NoError(t, err)
	joined := strings.Join(args, " ")
	require.Contains(t, joined, "--context b")
	require.Contains(t, joined, "--etcd=http://form-etcd:2379")
	require.Contains(t, joined, "--meta-prefix=form-client/wp")
	require.Contains(t, joined, "--standalone=true")
	require.NotContains(t, joined, "session-etcd")
	q.Metadata.ClientMeta = "tenant-a"
	_, err = commandArguments(q, def, browserSession{})
	require.ErrorContains(t, err, "client meta profiles are no longer used")
	def, err = commandDefinition("etcd list")
	require.NoError(t, err)
	q.Name = def.Name
	q.Context = ""
	q.Metadata.ClientMeta = ""
	q.Metadata.Prefix = ""
	args, err = commandArguments(q, def, browserSession{})
	require.NoError(t, err)
	joined = strings.Join(args, " ")
	require.Contains(t, joined, "--standalone=true")
	require.NotContains(t, joined, "--meta-prefix")
}
