package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewEnvHandler_Shape(t *testing.T) {
	handler := NewEnvHandler()

	req := httptest.NewRequest(http.MethodGet, "/admin/env", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var got struct {
		Env     map[string]string `json:"env"`
		Runtime struct {
			GoVersion    string `json:"go_version"`
			GoMaxProcs   int    `json:"gomaxprocs"`
			NumCPU       int    `json:"num_cpu"`
			NumGoroutine int    `json:"num_goroutine"`
		} `json:"runtime"`
		Host struct {
			Hostname string `json:"hostname"`
			OS       string `json:"os"`
			Arch     string `json:"arch"`
		} `json:"host"`
		Build struct {
			Version   string `json:"version"`
			Commit    string `json:"commit"`
			BuildTime string `json:"build_time"`
			GoVersion string `json:"go_version"`
		} `json:"build"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.NotEmpty(t, got.Runtime.GoVersion)
	require.NotEmpty(t, got.Host.OS)
	require.NotEmpty(t, got.Build.Version)
	// PATH is almost always set on a test runner.
	_, hasPath := got.Env["PATH"]
	require.True(t, hasPath, "expected PATH in env section")
}

func TestNewEnvHandler_WrongMethod(t *testing.T) {
	handler := NewEnvHandler()
	req := httptest.NewRequest(http.MethodPost, "/admin/env", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}
