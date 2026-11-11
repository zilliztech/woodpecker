package checks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/cmd/wpcli/candidates"
	d "github.com/zilliztech/woodpecker/diagnostics"
)

func TestConnectionProbesUseQueryAPIsAndIgnoreUnrelatedScope(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		require.Equal(t, "GET", r.Method)
		switch r.URL.Path {
		case "/api/v1/query":
			require.Equal(t, "vector(1)", r.URL.Query().Get("query"))
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"1"]}]}}`)
		case "/api/services":
			fmt.Fprint(w, `{"data":["wp"]}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c := configuration(t, server.URL, true, true)
	b := &Bindings{Server: &Scope{Metrics: "local", Trace: "local", LogID: "bad unrelated log", Metadata: ClientMetadata{Etcd: "not used", Prefix: "not used"}}}
	before, _ := json.Marshal(c)
	for _, source := range []string{"metrics", "trace"} {
		r := ProbeConnections(context.Background(), c, source, b)
		require.Len(t, r, 1)
		require.Equal(t, "ready", r[0].Status, r)
		require.Equal(t, 200, r[0].HTTPStatus)
		require.Equal(t, "server", r[0].Component)
		require.NotZero(t, r[0].CheckedAt)
	}
	after, _ := json.Marshal(c)
	require.Equal(t, string(before), string(after))
	require.EqualValues(t, 2, calls.Load())
}

func TestManualConnectionsNeedNoDeployment(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/query":
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"1"]}]}}`)
		case "/api/services":
			fmt.Fprint(w, `{"data":["wp"]}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	for _, contextName := range []string{"", "selected-context"} {
		b := &Bindings{Server: &Scope{Context: contextName, MetricsEndpoint: server.URL, TraceEndpoint: server.URL, MetricLabels: map[string]string{"cluster": "fixture"}, TraceService: "wp"}}
		cfg, targets, err := ResolveBindings(d.Config{}, b)
		require.NoError(t, err)
		for source, ref := range map[string]string{"prometheus": targets["server"].Metrics, "trace": targets["server"].Trace} {
			var settings map[string]any
			conn := cfg.Sources[source].Connections[ref]
			require.NoError(t, conn.Settings.Decode(&settings))
			require.Equal(t, contextName, settings["deployment"])
		}
		for _, source := range []string{"metrics", "trace"} {
			result := ProbeConnections(context.Background(), d.Config{}, source, b)
			require.Len(t, result, 1)
			require.Equal(t, "ready", result[0].Status, result)
		}
		b.Server.MetricLabels = nil
		_, _, err = ResolveBindings(d.Config{}, b)
		require.ErrorContains(t, err, "population label")
		b.Server.MetricLabels = map[string]string{"cluster": "fixture"}
		b.Server.TraceService = ""
		_, _, err = ResolveBindings(d.Config{}, b)
		require.ErrorContains(t, err, "Trace service")
	}
}

func TestConnectionProbeFailureReasons(t *testing.T) {
	for _, tc := range []struct {
		name, body, code string
		status           int
	}{{"unauthorized", "private error", "authentication_failed", 401}, {"forbidden", "private error", "authentication_failed", 403}, {"wrong-path", "private error", "api_not_found", 404}, {"redirect", "private error", "redirect_rejected", 302}, {"html", "<html>private error</html>", "invalid_response", 200}, {"wrong-api", `{"data":[]}`, "invalid_prometheus_response", 200}} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Location", "/private-redirect")
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer s.Close()
			r := ProbeConnections(context.Background(), configuration(t, s.URL, true, false), "metrics", &Bindings{Server: &Scope{Metrics: "local"}})
			require.Equal(t, tc.code, r[0].Code)
			require.Equal(t, "failed", r[0].Status)
			require.NotContains(t, r[0].Message, "private error")
			require.EqualValues(t, 1, calls.Load())
		})
	}
}

func TestProbeUsesSavedAuthenticationAndTLS(t *testing.T) {
	t.Setenv("WP_TEST_PROBE_PASSWORD", "fake-secret")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		require.True(t, ok)
		require.Equal(t, "tester", user)
		require.Equal(t, "fake-secret", password)
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"1"]}]}}`)
	}))
	defer server.Close()
	c := configuration(t, server.URL, true, false)
	g := c.Sources["metrics"]
	key := "metrics"
	if len(g.Connections) == 0 {
		g = c.Sources["prometheus"]
		key = "prometheus"
	}
	conn := g.Connections["local"]
	var settings map[string]any
	require.NoError(t, conn.Settings.Decode(&settings))
	settings["basic_auth"] = map[string]string{"username": "tester", "password_env": "WP_TEST_PROBE_PASSWORD"}
	require.NoError(t, conn.Settings.Encode(settings))
	g.Connections["local"] = conn
	c.Sources[key] = g
	r := ProbeConnections(context.Background(), c, "metrics", &Bindings{Server: &Scope{Metrics: "local"}})
	require.Equal(t, "ready", r[0].Status, r)
	t.Setenv("WP_TEST_PROBE_PASSWORD", "")
	r = ProbeConnections(context.Background(), c, "metrics", &Bindings{Server: &Scope{Metrics: "local"}})
	require.Equal(t, "authentication_unavailable", r[0].Code)
	tls := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("untrusted TLS must not reach the handler") }))
	defer tls.Close()
	r = ProbeConnections(context.Background(), configuration(t, tls.URL, true, false), "metrics", &Bindings{Server: &Scope{Metrics: "local"}})
	require.Equal(t, "tls_verification_failed", r[0].Code, r)
}

func TestProbeMissingTraceServiceAndSuccessfulHistoryOnly(t *testing.T) {
	t.Setenv("WOODPECKER_RUN_CACHE_DIR", t.TempDir())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"data":[]}`) }))
	defer server.Close()
	c := configuration(t, server.URL, false, true)
	b := &Bindings{Server: &Scope{Trace: "local"}}
	r := ProbeConnections(context.Background(), c, "trace", b)
	require.Equal(t, "warning", r[0].Status)
	require.Equal(t, "service_not_listed", r[0].Code)
	require.NoError(t, RememberProbedConnections(c, b, "trace", r))
	entries, err := candidates.List()
	require.NoError(t, err)
	require.Contains(t, fmt.Sprint(entries), server.URL)
	b.Server.Trace = ""
	b.Server.TraceEndpoint = "http://unreachable.invalid:1"
	require.NoError(t, RememberProbedConnections(c, b, "trace", []d.ConnectionCheck{{Status: "failed", Component: "server"}}))
	entries, err = candidates.List()
	require.NoError(t, err)
	require.False(t, strings.Contains(fmt.Sprint(entries), "unreachable.invalid"))
}
