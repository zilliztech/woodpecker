package management

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewConfigHandler_Success(t *testing.T) {
	handler := NewConfigHandler(func() any {
		return map[string]any{
			"etcd":  map[string]any{"endpoints": []string{"etcd:2379"}},
			"minio": map[string]any{"bucketName": "woodpecker"},
		}
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/config", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	require.Contains(t, w.Body.String(), "etcd")
	require.Contains(t, w.Body.String(), "bucketName")
}

func TestNewConfigHandler_WrongMethod(t *testing.T) {
	handler := NewConfigHandler(func() any { return nil })
	req := httptest.NewRequest(http.MethodPost, "/admin/config", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}
