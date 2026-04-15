package http

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
)

func resetGlobals() {
	metricsServer = nil
	server = nil
}

func TestRegister_HandlerFunc(t *testing.T) {
	resetGlobals()
	t.Setenv(PprofEnableEnvKey, "false")

	called := false
	Register(&Handler{
		Path: "/test",
		HandlerFunc: func(w http.ResponseWriter, r *http.Request) {
			called = true
		},
	})
	assert.NotNil(t, metricsServer)
	assert.False(t, called) // just registered, not invoked
}

func TestRegister_Handler(t *testing.T) {
	resetGlobals()
	t.Setenv(PprofEnableEnvKey, "false")

	Register(&Handler{
		Path:    "/test2",
		Handler: http.NotFoundHandler(),
	})
	assert.NotNil(t, metricsServer)
}

func TestRegister_PprofEnabled(t *testing.T) {
	resetGlobals()
	t.Setenv(PprofEnableEnvKey, "true")

	Register(&Handler{
		Path:    "/test-pprof-enabled",
		Handler: http.NotFoundHandler(),
	})
	assert.Equal(t, http.DefaultServeMux, metricsServer)
}

func TestRegister_PprofDefault(t *testing.T) {
	resetGlobals()
	t.Setenv(PprofEnableEnvKey, "")

	Register(&Handler{
		Path:    "/test-pprof-default",
		Handler: http.NotFoundHandler(),
	})
	// Empty env defaults to pprof enabled → DefaultServeMux
	assert.Equal(t, http.DefaultServeMux, metricsServer)
}

func TestStop_NilServer(t *testing.T) {
	resetGlobals()
	err := Stop()
	assert.NoError(t, err)
}

func TestStartAndStop(t *testing.T) {
	resetGlobals()
	t.Setenv(PprofEnableEnvKey, "false")
	t.Setenv(ListenPortEnvKey, "19091") // use non-default port to avoid conflicts

	cfg, _ := config.NewConfiguration()
	err := Start(cfg, AdminCallbacks{
		GetMemberlistStatus: func() string { return "memberlist status" },
	})
	assert.NoError(t, err)
	assert.NotNil(t, server)

	// Give server time to start
	time.Sleep(50 * time.Millisecond)

	// Hit the log level endpoint to cover the handler closure
	resp, httpErr := http.Get("http://127.0.0.1:19091" + LogLevelRouterPath)
	if httpErr == nil {
		resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}

	// Hit the memberlist endpoint
	resp, httpErr = http.Get("http://127.0.0.1:19091" + AdminMemberlistPath)
	if httpErr == nil {
		resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}

	err = Stop()
	assert.NoError(t, err)
}

func TestStartAndStop_WithLifecycleEndpoints(t *testing.T) {
	resetGlobals()
	t.Setenv(PprofEnableEnvKey, "false")
	t.Setenv(ListenPortEnvKey, "19092")

	cfg, _ := config.NewConfiguration()

	decommissioned := false
	err := Start(cfg, AdminCallbacks{
		GetMemberlistStatus: func() string { return "ok" },
		GetNodeStatus: func() any {
			return map[string]string{"state": "active"}
		},
		Decommission: func() error {
			decommissioned = true
			return nil
		},
		GetDecommissionProgress: func() any {
			return map[string]any{"safe_to_terminate": decommissioned}
		},
	})
	assert.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// Test GET /admin/node/status
	resp, httpErr := http.Get("http://127.0.0.1:19092" + AdminNodeStatusPath)
	if httpErr == nil {
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	}

	// Test POST /admin/node/decommission
	resp, httpErr = http.Post("http://127.0.0.1:19092"+AdminNodeDecommissionPath, "", nil)
	if httpErr == nil {
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	}
	assert.True(t, decommissioned)

	// Test GET /admin/node/decommission/progress
	resp, httpErr = http.Get("http://127.0.0.1:19092" + AdminNodeDecommissionProgressPath)
	if httpErr == nil {
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	}

	err = Stop()
	assert.NoError(t, err)
}

func TestMemberlistHandler_ContentNegotiation(t *testing.T) {
	callbacks := AdminCallbacks{
		GetMemberlistStatus: func() string { return "Total Members: 1\n" },
		GetMemberlistJSON:   func() []byte { return []byte(`{"members":[{"id":"node-1"}]}`) },
	}

	mux := http.NewServeMux()
	mux.HandleFunc(AdminMemberlistPath, newMemberlistHandler(callbacks))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Default (no Accept header) → text
	resp, err := http.Get(srv.URL + AdminMemberlistPath)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Contains(t, string(body), "Total Members")
	require.Equal(t, "text/plain; charset=utf-8", resp.Header.Get("Content-Type"))

	// Accept: application/json → JSON
	req, _ := http.NewRequest("GET", srv.URL+AdminMemberlistPath, nil)
	req.Header.Set("Accept", "application/json")
	resp2, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	require.Contains(t, string(body2), `"members"`)
	require.Equal(t, "application/json", resp2.Header.Get("Content-Type"))
}
