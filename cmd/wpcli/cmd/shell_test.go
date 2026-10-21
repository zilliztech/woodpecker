package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chzyer/readline"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

type scriptedShellInput struct {
	lines   []string
	err     error
	prompts []string
}

func (in *scriptedShellInput) Read(prompt string) (string, error) {
	in.prompts = append(in.prompts, prompt)
	if len(in.lines) == 0 {
		if in.err != nil {
			return "", in.err
		}
		return "", io.EOF
	}
	line := in.lines[0]
	in.lines = in.lines[1:]
	return line, nil
}

func (*scriptedShellInput) Remember(string) {}
func (*scriptedShellInput) Close() error    { return nil }

func TestShellContextSelector(t *testing.T) {
	for _, tc := range []struct {
		name, current string
		contexts      []string
		lines         []string
		want          string
		promptCount   int
	}{
		{"number", "prod", []string{"prod", "dev"}, []string{"1"}, "dev", 1},
		{"name", "prod", []string{"prod", "dev"}, []string{"dev"}, "dev", 1},
		{"default", "prod", []string{"prod", "dev"}, []string{""}, "prod", 1},
		{"no current", "", []string{"prod", "dev"}, []string{"", "0", "bad", "2"}, "prod", 4},
		{"stale current", "missing", []string{"prod", "dev"}, []string{"", "dev"}, "dev", 2},
		{"single", "", []string{"only"}, []string{"1"}, "only", 1},
		{"out of range", "prod", []string{"prod", "dev"}, []string{"-1", "3", "prod"}, "prod", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &config.File{CurrentContext: tc.current, Contexts: map[string]config.Context{}}
			for _, name := range tc.contexts {
				f.Contexts[name] = config.Context{}
			}
			input := &scriptedShellInput{lines: tc.lines}
			var out bytes.Buffer
			name, err := selectShellContext(f, "/example/cli.yaml", input, &out)
			require.NoError(t, err)
			require.Equal(t, tc.want, name)
			require.Len(t, input.prompts, tc.promptCount)
			require.Contains(t, out.String(), "/example/cli.yaml")
			if len(tc.contexts) > 1 {
				require.Less(t, strings.Index(out.String(), "1) dev"), strings.Index(out.String(), "2) prod"))
			}
			if tc.current == "prod" {
				require.Contains(t, out.String(), "[current]")
			}
		})
	}
	for _, abort := range []error{io.EOF, readline.ErrInterrupt} {
		input := &scriptedShellInput{err: abort}
		name, err := selectShellContext(&config.File{Contexts: map[string]config.Context{"prod": {}}}, "cli.yaml", input, io.Discard)
		require.ErrorIs(t, err, abort)
		require.Empty(t, name)
	}
}

func newTestShell(t *testing.T) (*shellSession, *bytes.Buffer) {
	t.Helper()
	original := Globals
	t.Cleanup(func() { Globals = original })
	p := writeTempConfig(t, goodYAML)
	f, err := config.Load(p)
	require.NoError(t, err)
	// These fields must never implicitly attach a tenant.
	c := f.Contexts["prod"]
	c.Etcd = config.EtcdConfig{Endpoints: []string{"old-etcd:2379"}, MetaPrefix: "old/woodpecker", Password: "old-secret"}
	f.Contexts["prod"] = c
	out := &bytes.Buffer{}
	s := &shellSession{file: f, configPath: p, in: strings.NewReader(""), out: out, errOut: out}
	s.checkTenant = func(context.Context, *shellTenant) error { return nil }
	require.NoError(t, s.use("prod"))
	return s, out
}

func shellExecute(t *testing.T, s *shellSession, args ...string) error {
	t.Helper()
	done, err := s.execute(context.Background(), args)
	require.False(t, done)
	return err
}

func TestShellStartup(t *testing.T) {
	p := writeTempConfig(t, goodYAML)
	t.Setenv("WOODPECKER_CLI_CONFIG", p)
	t.Setenv("WOODPECKER_ENDPOINT", "http://wrong-cluster:9091")
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"explicit bypass", []string{"shell", "--context", "dev"}, ""},
		{"unknown context", []string{"shell", "--context", "missing"}, "not found"},
		{"empty context", []string{"shell", "--context="}, "must name"},
		{"batch needs explicit context", []string{"shell"}, "requires --context"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := NewRootCommand()
			var out bytes.Buffer
			root.SetIn(strings.NewReader("status\nexit\n"))
			root.SetOut(&out)
			root.SetErr(&out)
			root.SetArgs(tc.args)
			err := root.Execute()
			if tc.want == "" {
				require.NoError(t, err)
				require.Contains(t, out.String(), "Service context: dev")
				require.NotContains(t, out.String(), "Select context")
				require.NotContains(t, out.String(), "wrong-cluster")
			} else {
				require.ErrorContains(t, err, tc.want)
			}
			contents, err := os.ReadFile(p)
			require.NoError(t, err)
			require.Equal(t, goodYAML, string(contents))
		})
	}
	t.Setenv("WOODPECKER_CLI_CONFIG", writeTempConfig(t, "contexts: {}\n"))
	root := NewRootCommand()
	root.SetIn(strings.NewReader(""))
	root.SetErr(io.Discard)
	root.SetArgs([]string{"shell", "--context", "prod"})
	require.ErrorContains(t, root.Execute(), "WOODPECKER_CLI_CONFIG")
}

func TestShellSessionTransitions(t *testing.T) {
	s, out := newTestShell(t)
	require.Nil(t, s.tenant)
	require.Empty(t, s.service.Etcd.Endpoints)
	require.NoError(t, shellExecute(t, s, "connect", "tenant-a", "--etcd", "localhost:2379", "--meta-prefix", "a/woodpecker", "--bucket", "a-bucket", "--root-path", "a/root", "--etcd-username", "alice", "--etcd-password", "secret"))
	require.Equal(t, "tenant-a", s.tenantName())
	require.NoError(t, shellExecute(t, s, "status"))
	require.NotContains(t, out.String(), "secret")
	require.Contains(t, out.String(), "credentials redacted")
	old := s.tenant
	s.checkTenant = func(context.Context, *shellTenant) error { return errors.New("etcd authentication failed") }
	require.ErrorContains(t, shellExecute(t, s, "connect", "bad", "--etcd", "localhost:1234", "--meta-prefix", "bad/woodpecker"), "active tenant unchanged: tenant-a")
	require.Same(t, old, s.tenant)
	require.Error(t, shellExecute(t, s, "use", "missing"))
	require.Same(t, old, s.tenant)
	require.Equal(t, "prod", s.contextName)
	s.checkTenant = func(context.Context, *shellTenant) error { return nil }
	require.NoError(t, shellExecute(t, s, "connect", "tenant-b", "--etcd", "localhost:42379", "--meta-prefix", "b/woodpecker"))
	require.Empty(t, s.tenant.etcd.Password)
	require.Empty(t, s.tenant.etcd.Username)
	require.Empty(t, s.tenant.bucket)
	require.Empty(t, s.tenant.rootPath)
	require.NoError(t, shellExecute(t, s, "disconnect"))
	require.Nil(t, s.tenant)
	require.Equal(t, "prod", s.contextName)
	require.NoError(t, shellExecute(t, s, "connect", "tenant-b", "--etcd", "localhost:42379", "--meta-prefix", "b/woodpecker"))
	require.NoError(t, shellExecute(t, s, "use", "dev"))
	require.Nil(t, s.tenant)
	require.Equal(t, "dev", s.contextName)
	require.Equal(t, "prod", s.file.CurrentContext)
	data, err := os.ReadFile(s.configPath)
	require.NoError(t, err)
	require.Equal(t, goodYAML, string(data))
}

func TestShellPrerequisitesAndStorageInheritance(t *testing.T) {
	s, _ := newTestShell(t)
	var calls [][]string
	var transient string
	s.runChild = func(_ context.Context, args []string, path string) error {
		calls = append(calls, append([]string(nil), args...))
		transient = path
		file, err := config.Load(path)
		require.NoError(t, err)
		c, err := file.ResolveContext("")
		require.NoError(t, err)
		if s.tenant == nil {
			require.Empty(t, c.Etcd.Endpoints)
		} else {
			require.Equal(t, s.tenant.etcd, c.Etcd)
		}
		stat, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), stat.Mode().Perm())
		return nil
	}
	for _, args := range [][]string{{"meta", "log", "list"}, {"log", "readers", "a"}, {"logstore", "lac", "a", "0"}, {"etcd", "list"}, {"segment", "inspect", "a", "0"}} {
		require.ErrorContains(t, shellExecute(t, s, args...), "no tenant metadata connection configured")
	}
	require.Empty(t, calls)
	require.NoError(t, shellExecute(t, s, "node", "list"))
	require.NoError(t, shellExecute(t, s, "meta", "log", "list", "--help"))
	require.NoError(t, shellExecute(t, s, "connect", "a", "--etcd", "localhost:2379", "--meta-prefix", "a/woodpecker"))
	require.NoError(t, shellExecute(t, s, "meta", "log", "list"))
	require.ErrorContains(t, shellExecute(t, s, "segment", "probe", "a", "0"), "physical inspection requires")
	require.NoError(t, shellExecute(t, s, "segment", "inspect", "a", "0", "--bucket", "explicit", "--root-path", "explicit/root"))
	require.NoError(t, shellExecute(t, s, "connect", "a", "--etcd", "localhost:2379", "--meta-prefix", "a/woodpecker", "--bucket", "a-bucket", "--root-path", "a/root"))
	require.NoError(t, shellExecute(t, s, "log", "scan", "a", "--mode", "raw"))
	require.Contains(t, calls[len(calls)-1], "--bucket=a-bucket")
	require.Contains(t, calls[len(calls)-1], "--root-path=a/root")
	require.NoError(t, shellExecute(t, s, "log", "scan", "a", "--bucket=override", "--root-path=override/root"))
	require.NotContains(t, calls[len(calls)-1], "--bucket=a-bucket")
	require.NotContains(t, calls[len(calls)-1], "--root-path=a/root")
	require.Equal(t, "a-bucket", s.tenant.bucket)
	require.Equal(t, "a/root", s.tenant.rootPath)
	require.ErrorContains(t, shellExecute(t, s, "log", "readers", "a", "--etcd="), "must not be empty")
	require.ErrorContains(t, shellExecute(t, s, "log", "readers", "a", "--meta-prefix="), "must not be empty")
	require.ErrorContains(t, shellExecute(t, s, "node", "list", "--context=dev"), "use 'use <context>'")
	_, err := os.Stat(transient)
	require.True(t, os.IsNotExist(err), "transient credentials must be removed after command execution")
}

func TestShellEtcdAuthenticationFailureRetainsTenant(t *testing.T) {
	cli, endpoint := startShellTestEtcd(t)
	s, _ := newTestShell(t)
	s.checkTenant = nil
	s.service.Timeout = time.Second
	require.NoError(t, shellExecute(t, s, "connect", "a", "--etcd", endpoint, "--meta-prefix", "a/woodpecker"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.UserAdd(ctx, "root", "test-password")
	require.NoError(t, err)
	_, err = cli.UserGrantRole(ctx, "root", "root")
	require.NoError(t, err)
	_, err = cli.AuthEnable(ctx)
	require.NoError(t, err)
	require.ErrorContains(t, shellExecute(t, s, "connect", "bad", "--etcd", endpoint, "--meta-prefix", "b/woodpecker", "--etcd-username", "root", "--etcd-password", "wrong-password"), "active tenant unchanged: a")
	require.Equal(t, "a", s.tenantName())
	require.Empty(t, s.tenant.etcd.Password)
	require.NoError(t, shellExecute(t, s, "connect", "b", "--etcd", endpoint, "--meta-prefix", "b/woodpecker", "--etcd-username", "root", "--etcd-password", "test-password"))
	require.Equal(t, "b", s.tenantName())
}

// This subprocess entry runs the actual CLI tree while isolating process-global
// state exactly as the production shell dispatcher does.
func TestShellChildProcess(t *testing.T) {
	if os.Getenv("WP_SHELL_TEST_HELPER") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			root := NewRootCommand()
			root.SetArgs(os.Args[i+1:])
			err := root.Execute()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
			os.Exit(wperrors.ExitCodeFor(err))
		}
	}
	os.Exit(99)
}

func shellRealChild(t *testing.T, s *shellSession) {
	t.Helper()
	executable, err := os.Executable()
	require.NoError(t, err)
	s.runChild = func(ctx context.Context, args []string, path string) error {
		args = append([]string{"-test.run=^TestShellChildProcess$", "--"}, args...)
		child := exec.CommandContext(ctx, executable, args...)
		child.Env = append(shellChildEnvironment(path), "WP_SHELL_TEST_HELPER=1")
		child.Stdin, child.Stdout, child.Stderr = s.in, s.out, s.errOut
		return runShellProcess(ctx, child)
	}
}

func TestShellRealDispatchIsolation(t *testing.T) {
	s, out := newTestShell(t)
	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
			_, _ = io.WriteString(w, `{}`)
			return
		}
		if r.URL.Path == "/admin/memberlist" {
			_, _ = io.WriteString(w, `{"members":[]}`)
			return
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()
	s.service.Endpoint = server.URL
	s.service.Timeout = 150 * time.Millisecond
	t.Setenv("WOODPECKER_ENDPOINT", "http://must-not-be-contacted:9091")
	shellRealChild(t, s)
	require.NoError(t, shellExecute(t, s, "version", "--output=json"))
	require.Contains(t, out.String(), `"version"`)
	out.Reset()
	require.NoError(t, shellExecute(t, s, "version"))
	require.Contains(t, out.String(), "wp version")
	require.NotContains(t, out.String(), `"version"`)
	require.NoError(t, shellExecute(t, s, "node", "list"))
	// The first command is explicitly confirmed. The next must return its normal
	// refusal rather than inherit --yes or issue another POST.
	require.NoError(t, shellExecute(t, s, "logstore", "fence", strings.TrimPrefix(server.URL, "http://"), "--log=1", "--seg=0", "--reason=test", "--yes"))
	require.EqualValues(t, 1, posts.Load())
	require.Error(t, shellExecute(t, s, "logstore", "fence", strings.TrimPrefix(server.URL, "http://"), "--log=1", "--seg=0", "--reason=test"))
	require.EqualValues(t, 1, posts.Load())
	// An explicit endpoint override does not alter the next command's target.
	require.Error(t, shellExecute(t, s, "node", "list", "--endpoint=http://127.0.0.1:1", "--timeout=50ms"))
	require.NoError(t, shellExecute(t, s, "node", "list"))
}

func TestShellEtcdValidationAndRealMetadata(t *testing.T) {
	cli, endpoints := startShellTestEtcd(t)
	s, out := newTestShell(t)
	s.checkTenant = nil
	s.service.Timeout = 1500 * time.Millisecond
	_, err := cli.Put(context.Background(), "a/woodpecker/test", "present")
	require.NoError(t, err)
	require.NoError(t, shellExecute(t, s, "connect", "a", "--etcd", endpoints, "--meta-prefix", "a/woodpecker"))
	shellRealChild(t, s)
	out.Reset()
	require.NoError(t, shellExecute(t, s, "etcd", "list", "a/woodpecker"))
	require.Contains(t, out.String(), "a/woodpecker/test")
	require.NoError(t, shellExecute(t, s, "meta", "log", "list"))
	// --apply must not leak into a following preview, including after a failed
	// apply. Both commands use the real metadata handlers in child processes.
	kb := meta.NewKeyBuilder("a/woodpecker")
	logBytes, err := pb.Marshal(&proto.LogMeta{LogId: 42, TruncatedSegmentId: -1, TruncatedEntryId: -1})
	require.NoError(t, err)
	written, err := cli.Put(context.Background(), kb.BuildLogKey("a"), string(logBytes))
	require.NoError(t, err)
	patch := filepath.Join(t.TempDir(), "patch.json")
	require.NoError(t, os.WriteFile(patch, []byte(`{"maxSegmentRollSizeBytes":"4096"}`), 0o600))
	require.Error(t, shellExecute(t, s, "meta", "log", "patch", "a", "--patch-file", patch, "--apply"))
	out.Reset()
	require.NoError(t, shellExecute(t, s, "meta", "log", "patch", "a", "--patch-file", patch))
	require.Contains(t, out.String(), `"applied": false`)
	unchanged, err := cli.Get(context.Background(), kb.BuildLogKey("a"))
	require.NoError(t, err)
	require.Equal(t, written.Header.Revision, unchanged.Kvs[0].ModRevision)
	require.ErrorContains(t, shellExecute(t, s, "connect", "bad", "--etcd", "127.0.0.1:1", "--meta-prefix", "b/woodpecker", "--etcd-cacert", filepath.Join(t.TempDir(), "missing.pem")), "active tenant unchanged: a")
	s.service.Timeout = 100 * time.Millisecond
	require.ErrorContains(t, shellExecute(t, s, "connect", "bad", "--etcd", "127.0.0.1:1", "--meta-prefix", "b/woodpecker"), "active tenant unchanged: a")
	require.Equal(t, "a", s.tenantName())
}

func startShellTestEtcd(t *testing.T) (*clientv3.Client, string) {
	t.Helper()
	u, err := url.Parse("http://127.0.0.1:0")
	require.NoError(t, err)
	cfg := embed.NewConfig()
	cfg.Dir, cfg.LogLevel = t.TempDir(), "error"
	cfg.ListenClientUrls, cfg.AdvertiseClientUrls = []url.URL{*u}, []url.URL{*u}
	cfg.ListenPeerUrls, cfg.AdvertisePeerUrls = []url.URL{*u}, []url.URL{*u}
	cfg.InitialCluster = cfg.Name + "=" + u.String()
	server, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(server.Close)
	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		t.Fatal("test etcd did not become ready")
	}
	endpoint := "http://" + server.Clients[0].Addr().String()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	return cli, endpoint
}

func TestShellCancelOperationRetainsSession(t *testing.T) {
	s, _ := newTestShell(t)
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	s.service.Endpoint = server.URL
	shellRealChild(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := s.execute(ctx, []string{"node", "list"})
		finished <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("child operation did not start")
	}
	cancel()
	select {
	case err := <-finished:
		require.ErrorContains(t, err, "operation canceled; session retained")
	case <-time.After(5 * time.Second):
		t.Fatal("child operation did not stop")
	}
	require.Equal(t, "prod", s.contextName)
	require.NoError(t, shellExecute(t, s, "version"))
}

func TestShellStatusOfflineAndBoundedChecks(t *testing.T) {
	s, out := newTestShell(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		<-r.Context().Done()
	}))
	defer server.Close()
	s.service.Endpoint = server.URL
	s.service.Timeout = 50 * time.Millisecond
	require.NoError(t, shellExecute(t, s, "status"))
	require.Zero(t, requests.Load())
	require.ErrorContains(t, shellExecute(t, s, "status", "--check"), "configuration retained")
	require.EqualValues(t, 1, requests.Load())
	require.Contains(t, out.String(), "Admin check: failed")
	require.Contains(t, out.String(), "Etcd check: skipped")
	require.Equal(t, "prod", s.contextName)
	require.Equal(t, "https://REDACTED@example.com:2379?REDACTED", safeShellEndpoint("https://user:password@example.com:2379?token=secret#secret"))
	require.Equal(t, "127.0.0.1:2379", safeShellEndpoint("127.0.0.1:2379"))
	require.Equal(t, "[::1]:2379", safeShellEndpoint("[::1]:2379"))
}

func TestShellCompletionHistoryAndBatchInput(t *testing.T) {
	s, _ := newTestShell(t)
	for text, want := range map[string]string{"no": "de ", "node li": "st ", "node list --out": "put ", "use d": "ev ", "connect --etcd-p": "assword "} {
		matches, _ := (shellCompleter{s}).Do([]rune(text), len([]rune(text)))
		var values []string
		for _, match := range matches {
			values = append(values, string(match))
		}
		require.Contains(t, values, want, text)
	}
	for _, line := range []string{"connect a --etcd x:2379 --meta-prefix a --etcd-password secret", "etcd list --etcd-password=secret", "node list --endpoint=https://user:secret@host"} {
		require.False(t, shellHistorySafe(line, strings.Fields(line)))
	}
	require.True(t, shellHistorySafe("node list", []string{"node", "list"}))
	in := batchShellInput{strings.NewReader("one\r\ntwo\nthree")}
	for _, expected := range []string{"one", "two", "three"} {
		line, err := in.Read("")
		require.NoError(t, err)
		require.Equal(t, expected, line)
	}
	_, err := in.Read("")
	require.ErrorIs(t, err, io.EOF)
}

func TestShellQuotedArgumentsAndBatchFailures(t *testing.T) {
	s, out := newTestShell(t)
	s.runChild = func(_ context.Context, args []string, _ string) error {
		require.Contains(t, args, "value with spaces")
		return nil
	}
	input := &scriptedShellInput{lines: []string{"meta log list", "connect a --etcd localhost:2379 --meta-prefix a", `etcd get 'value with spaces'`, "exit"}}
	require.ErrorContains(t, s.loop(context.Background(), input, false), "no tenant metadata")
	require.Contains(t, out.String(), "no tenant metadata")
	input = &scriptedShellInput{lines: []string{"'unclosed", "status", "exit"}}
	require.NoError(t, s.loop(context.Background(), input, true))
}
