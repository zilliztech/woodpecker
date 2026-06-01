package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogHealthHandler_NoFilterReturnsAll(t *testing.T) {
	var gotBucket, gotRoot string
	h := NewLogHealthHandler(func(_ context.Context, b, r string) (any, int) {
		gotBucket, gotRoot = b, r
		return map[string]string{"state": "Healthy"}, http.StatusOK
	})
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/admin/log-health", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Empty(t, gotBucket)
	require.Empty(t, gotRoot)
}

func TestLogHealthHandler_PartialFilterIgnored(t *testing.T) {
	var gotBucket, gotRoot string
	h := NewLogHealthHandler(func(_ context.Context, b, r string) (any, int) {
		gotBucket, gotRoot = b, r
		return map[string]string{}, http.StatusOK
	})
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/admin/log-health?bucket_name=b", nil))
	require.Empty(t, gotBucket)
	require.Empty(t, gotRoot)
}

func TestLogHealthHandler_FilterPassedThrough(t *testing.T) {
	var gotBucket, gotRoot string
	h := NewLogHealthHandler(func(_ context.Context, b, r string) (any, int) {
		gotBucket, gotRoot = b, r
		return map[string]string{}, http.StatusServiceUnavailable
	})
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/admin/log-health?bucket_name=b&root_path=r", nil))
	require.Equal(t, "b", gotBucket)
	require.Equal(t, "r", gotRoot)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestLogHealthHandler_RejectsNonGet(t *testing.T) {
	h := NewLogHealthHandler(func(context.Context, string, string) (any, int) { return nil, http.StatusOK })
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodPost, "/admin/log-health", nil))
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestLogHealthHandler_EncodesJSON(t *testing.T) {
	h := NewLogHealthHandler(func(context.Context, string, string) (any, int) {
		return map[string]any{"state": "Healthy", "tracked_logs": 0}, http.StatusOK
	})
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/admin/log-health", nil))
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "Healthy", body["state"])
}
