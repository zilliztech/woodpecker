package management

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type probeCall struct {
	bucket, rootPath      string
	logID, segmentID      int64
	fromEntry, maxEntries int64
}

func probeHandlerCapturing(got *probeCall, err error) http.HandlerFunc {
	return NewLogstoreSegmentProbeHandler(func(_ context.Context, bucket, rootPath string, logID, segmentID, fromEntry, maxEntries int64) (any, error) {
		*got = probeCall{bucket, rootPath, logID, segmentID, fromEntry, maxEntries}
		if err != nil {
			return nil, err
		}
		return map[string]any{"last_entry": 4821, "outcome": "error"}, nil
	})
}

// TestSegmentProbeHandler_PassesThroughWhatWasAsked covers the parameters, including the two that
// decide how much work the node does.
func TestSegmentProbeHandler_PassesThroughWhatWasAsked(t *testing.T) {
	var got probeCall
	req := httptest.NewRequest(http.MethodGet,
		"/admin/logstore/segment/probe?log_id=7&segment_id=3&from_entry=4000&max_entries=50&bucket_name=bkt&root_path=inst", nil)
	w := httptest.NewRecorder()
	probeHandlerCapturing(&got, nil).ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, probeCall{"bkt", "inst", 7, 3, 4000, 50}, got)
	assert.Contains(t, w.Body.String(), "4821")
}

// TestSegmentProbeHandler_OmittedBoundsAreLeftToTheNode keeps the default in one place: the node
// owns it, so a caller that omits the bound gets the node's, not a second one invented here.
func TestSegmentProbeHandler_OmittedBoundsAreLeftToTheNode(t *testing.T) {
	var got probeCall
	req := httptest.NewRequest(http.MethodGet, "/admin/logstore/segment/probe?log_id=7&segment_id=3", nil)
	w := httptest.NewRecorder()
	probeHandlerCapturing(&got, nil).ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Zero(t, got.maxEntries, "an omitted bound is passed as zero for the node to fill in")
	assert.Zero(t, got.fromEntry)
}

// TestSegmentProbeHandler_RequiresTheSegment covers the two params without which there is no
// question to answer.
func TestSegmentProbeHandler_RequiresTheSegment(t *testing.T) {
	for _, query := range []string{"", "?log_id=7", "?segment_id=3", "?log_id=x&segment_id=3", "?log_id=7&segment_id=y"} {
		var got probeCall
		req := httptest.NewRequest(http.MethodGet, "/admin/logstore/segment/probe"+query, nil)
		w := httptest.NewRecorder()
		probeHandlerCapturing(&got, nil).ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code, "query %q", query)
		assert.Zero(t, got.logID, "query %q reached the node", query)
	}
}

// TestSegmentProbeHandler_NodeThatHoldsNothingSaysSo covers a node that cannot answer about this
// segment. That is a per-node state the caller has to see per node, not a failed request.
func TestSegmentProbeHandler_NodeThatHoldsNothingSaysSo(t *testing.T) {
	var got probeCall
	req := httptest.NewRequest(http.MethodGet, "/admin/logstore/segment/probe?log_id=7&segment_id=3", nil)
	w := httptest.NewRecorder()
	probeHandlerCapturing(&got, fmt.Errorf("this node holds no local data for log 7 segment 3")).ServeHTTP(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "no local data", "the node's own reason is what makes this actionable")
}

// TestSegmentProbeHandler_ReadOnly keeps a diagnostic from being invoked as a mutation.
func TestSegmentProbeHandler_ReadOnly(t *testing.T) {
	var got probeCall
	req := httptest.NewRequest(http.MethodPost, "/admin/logstore/segment/probe?log_id=7&segment_id=3", nil)
	w := httptest.NewRecorder()
	probeHandlerCapturing(&got, nil).ServeHTTP(w, req)

	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

// TestSegmentProbeHandler_PartialInstanceIsRefused covers half a tenant filter. Quietly widening it
// to "whichever instance this node finds" can answer for a different tenant than the caller named.
func TestSegmentProbeHandler_PartialInstanceIsRefused(t *testing.T) {
	for _, query := range []string{"&bucket_name=bkt", "&root_path=inst"} {
		var got probeCall
		req := httptest.NewRequest(http.MethodGet,
			"/admin/logstore/segment/probe?log_id=7&segment_id=3"+query, nil)
		w := httptest.NewRecorder()
		probeHandlerCapturing(&got, nil).ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code, "query %q", query)
		assert.Zero(t, got.logID, "query %q reached the node", query)
	}
}

// TestSegmentProbeHandler_CarriesTheCallersContext pins that a caller giving up stops the read: the
// scan is the expensive half, and nobody is left waiting for its report.
func TestSegmentProbeHandler_CarriesTheCallersContext(t *testing.T) {
	var gotCtx context.Context
	handler := NewLogstoreSegmentProbeHandler(func(ctx context.Context, _, _ string, _, _, _, _ int64) (any, error) {
		gotCtx = ctx
		return map[string]any{"outcome": "cap_reached"}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/admin/logstore/segment/probe?log_id=7&segment_id=3", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, gotCtx)
	cancel()
	require.Error(t, gotCtx.Err(), "the read is given the request's context, not a detached one")
}
