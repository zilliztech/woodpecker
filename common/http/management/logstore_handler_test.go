package management

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogstoreSegmentsHandler_Success(t *testing.T) {
	handler := NewLogstoreSegmentsHandler(func(logID *int64, writable *bool) []any {
		return []any{
			map[string]any{"log_id": 42, "segment_id": 7, "backend": "stagedstorage"},
		}
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/logstore/segments", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "segments")
	assert.Contains(t, w.Body.String(), "42")
}

func TestLogstoreSegmentsHandler_WithFilter(t *testing.T) {
	var gotLogID *int64
	handler := NewLogstoreSegmentsHandler(func(logID *int64, writable *bool) []any {
		gotLogID = logID
		return nil
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/logstore/segments?log_id=42", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, gotLogID)
	assert.Equal(t, int64(42), *gotLogID)
}

func TestLogstoreSegmentsHandler_MethodNotAllowed(t *testing.T) {
	handler := NewLogstoreSegmentsHandler(nil)
	req := httptest.NewRequest(http.MethodPost, "/admin/logstore/segments", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestLogstoreSegmentShowHandler_Success(t *testing.T) {
	handler := NewLogstoreSegmentShowHandler(func(logID, segmentID int64) (any, error) {
		return map[string]any{"log_id": logID, "segment_id": segmentID, "detailed": true}, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/logstore/segments?log_id=42&segment_id=7", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "42")
}

func TestLogstoreSegmentShowHandler_MissingParams(t *testing.T) {
	handler := NewLogstoreSegmentShowHandler(nil)
	req := httptest.NewRequest(http.MethodGet, "/admin/logstore/segments?log_id=42", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestLogstoreFlushHandler_Success(t *testing.T) {
	handler := NewLogstoreFlushHandler(func(logID, segmentID int64) error {
		return nil
	})

	body := strings.NewReader(`{"log_id":42,"segment_id":7}`)
	req := httptest.NewRequest(http.MethodPost, "/admin/logstore/flush", body)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "flush completed")
}

func TestLogstoreFenceHandler_Success(t *testing.T) {
	var gotReason string
	handler := NewLogstoreFenceHandler(func(logID, segmentID int64, reason string) error {
		gotReason = reason
		return nil
	})

	body := strings.NewReader(`{"log_id":42,"segment_id":7,"reason":"test fence"}`)
	req := httptest.NewRequest(http.MethodPost, "/admin/logstore/fence", body)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "test fence", gotReason)
}

func TestLogstoreCompactHandler_MethodNotAllowed(t *testing.T) {
	handler := NewLogstoreCompactHandler(nil)
	req := httptest.NewRequest(http.MethodGet, "/admin/logstore/compact", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}
