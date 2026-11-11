package management

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpsListHandler_Success(t *testing.T) {
	handler := NewOpsListHandler(func(params map[string]string) any {
		return map[string]any{"ops": []any{}, "count": 0}
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/runtime/ops", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "ops")
}

func TestOpsListHandler_WithParams(t *testing.T) {
	var gotParams map[string]string
	handler := NewOpsListHandler(func(params map[string]string) any {
		gotParams = params
		return nil
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/runtime/ops?type=file.flush&longer_than_ms=30000", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "file.flush", gotParams["type"])
	assert.Equal(t, "30000", gotParams["longer_than_ms"])
}

func TestOpsGetHandler_Success(t *testing.T) {
	handler := NewOpsGetHandler(func(opID string) any {
		return map[string]string{"op_id": opID, "op_type": "file.flush"}
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/runtime/ops?op_id=fl-000001", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "fl-000001")
}

func TestOpsGetHandler_NotFound(t *testing.T) {
	handler := NewOpsGetHandler(func(opID string) any {
		return nil
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/runtime/ops?op_id=nonexistent", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestOpsGetHandler_MissingID(t *testing.T) {
	handler := NewOpsGetHandler(nil)
	req := httptest.NewRequest(http.MethodGet, "/admin/runtime/ops", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestOpsStatsHandler_Success(t *testing.T) {
	handler := NewOpsStatsHandler(func() any {
		return map[string]any{"capacity": 1024, "in_use": 42}
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/runtime/ops/stats", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "1024")
}

func TestOpsStatsHandler_MethodNotAllowed(t *testing.T) {
	handler := NewOpsStatsHandler(nil)
	req := httptest.NewRequest(http.MethodPost, "/admin/runtime/ops/stats", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}
