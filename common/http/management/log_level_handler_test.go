package management

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/logger"
)

func TestLogLevelHandler_Get(t *testing.T) {
	handler := NewLogLevelHandler()

	req := httptest.NewRequest(http.MethodGet, "/log/level", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"level"`)
}

func TestLogLevelHandler_Post(t *testing.T) {
	handler := NewLogLevelHandler()
	// Note: SetLevel modifies global state. We restore to warn level
	// which is the fallback used when _globalLogger is uninitialized.
	defer func() { _ = logger.SetLevel("warn") }()

	body := strings.NewReader(`{"level":"debug"}`)
	req := httptest.NewRequest(http.MethodPost, "/log/level", body)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"debug"`)
	assert.Equal(t, "debug", logger.GetLevel())
}

func TestLogLevelHandler_PostInvalidLevel(t *testing.T) {
	handler := NewLogLevelHandler()

	body := strings.NewReader(`{"level":"trace"}`)
	req := httptest.NewRequest(http.MethodPost, "/log/level", body)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid log level")
}

func TestLogLevelHandler_PostBadJSON(t *testing.T) {
	handler := NewLogLevelHandler()

	body := strings.NewReader(`not json`)
	req := httptest.NewRequest(http.MethodPost, "/log/level", body)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestLogLevelHandler_MethodNotAllowed(t *testing.T) {
	handler := NewLogLevelHandler()

	req := httptest.NewRequest(http.MethodDelete, "/log/level", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}
