package http

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

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
	err := Start(cfg, func() string { return "memberlist status" })
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
