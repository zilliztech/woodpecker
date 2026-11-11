package management

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type inspectCall struct {
	bucket, rootPath      string
	logID, segmentID      int64
	fromBlock, maxBlocks  int64
	coverageOnly, reached bool
}

func inspectHandlerCapturing(got *inspectCall) http.HandlerFunc {
	return NewLogstoreSegmentInspectHandler(func(_ context.Context, bucket, rootPath string, logID, segmentID, fromBlock, maxBlocks int64, coverageOnly bool) (any, error) {
		*got = inspectCall{bucket, rootPath, logID, segmentID, fromBlock, maxBlocks, coverageOnly, true}
		return map[string]any{"survey": map[string]any{"stop_reason": "end_of_segment"}}, nil
	})
}

func inspectRequest(t *testing.T, query string, got *inspectCall) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/admin/logstore/segment/inspect?log_id=7&segment_id=3"+query, nil)
	w := httptest.NewRecorder()
	inspectHandlerCapturing(got).ServeHTTP(w, req)
	return w
}

// TestSegmentInspectHandler_VerifiesUnlessToldNotTo pins the default. Asking a node to inspect a
// segment means checking it; the cheap pass that checks nothing has to be asked for, or a sweep's
// unverified answer could be mistaken for a clean bill of health.
func TestSegmentInspectHandler_VerifiesUnlessToldNotTo(t *testing.T) {
	var got inspectCall
	require.Equal(t, http.StatusOK, inspectRequest(t, "", &got).Code)
	assert.False(t, got.coverageOnly, "an omitted verify must verify")

	require.Equal(t, http.StatusOK, inspectRequest(t, "&verify=true", &got).Code)
	assert.False(t, got.coverageOnly)

	require.Equal(t, http.StatusOK, inspectRequest(t, "&verify=false", &got).Code)
	assert.True(t, got.coverageOnly, "verify=false is the coverage pass")
}

// TestSegmentInspectHandler_RejectsAnUnparseableVerify keeps a typo from silently choosing a mode.
func TestSegmentInspectHandler_RejectsAnUnparseableVerify(t *testing.T) {
	var got inspectCall
	w := inspectRequest(t, "&verify=maybe", &got)

	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.False(t, got.reached, "the node must not be asked when the request is not understood")
}

// TestSegmentInspectHandler_PassesTheBoundsThrough covers the two params that decide how much work
// each node does.
func TestSegmentInspectHandler_PassesTheBoundsThrough(t *testing.T) {
	var got inspectCall
	require.Equal(t, http.StatusOK, inspectRequest(t, "&from_block=10&max_blocks=8&bucket_name=bkt&root_path=inst", &got).Code)

	assert.Equal(t, int64(10), got.fromBlock)
	assert.Equal(t, int64(8), got.maxBlocks)
	assert.Equal(t, "bkt", got.bucket)
	assert.Equal(t, "inst", got.rootPath)
}

// TestSegmentInspectHandler_PartialInstanceIsRefused covers half a tenant filter: quietly widening
// it can answer for a different tenant than the caller named.
func TestSegmentInspectHandler_PartialInstanceIsRefused(t *testing.T) {
	for _, query := range []string{"&bucket_name=bkt", "&root_path=inst"} {
		var got inspectCall
		w := inspectRequest(t, query, &got)

		require.Equal(t, http.StatusBadRequest, w.Code, "query %q", query)
		assert.False(t, got.reached, "query %q reached the node", query)
	}
}

// TestSegmentInspectHandler_ReadOnly keeps a survey from being invoked as a mutation.
func TestSegmentInspectHandler_ReadOnly(t *testing.T) {
	var got inspectCall
	req := httptest.NewRequest(http.MethodPost, "/admin/logstore/segment/inspect?log_id=7&segment_id=3", nil)
	w := httptest.NewRecorder()
	inspectHandlerCapturing(&got).ServeHTTP(w, req)

	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
	assert.False(t, got.reached)
}
