package health

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockIndicator struct {
	name   string
	status string
}

func (m *mockIndicator) GetName() string                 { return m.name }
func (m *mockIndicator) Health(_ context.Context) string { return m.status }

func TestHandler(t *testing.T) {
	h := Handler()
	assert.NotNil(t, h)
	assert.Equal(t, &defaultHandler, h)
}

func TestHealthHandler_NoIndicators(t *testing.T) {
	handler := &HealthHandler{}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "OK", w.Body.String())
}

func TestHealthHandler_AllHealthy(t *testing.T) {
	handler := &HealthHandler{
		indicators: []Indicator{
			&mockIndicator{name: "comp1", status: Healthy},
			&mockIndicator{name: "comp2", status: Healthy},
		},
		indicatorNum:      2,
		unregisteredRoles: map[string]struct{}{},
	}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "OK", w.Body.String())
}

func TestHealthHandler_StandByIsHealthy(t *testing.T) {
	handler := &HealthHandler{
		indicators: []Indicator{
			&mockIndicator{name: "comp1", status: StandBy},
		},
		indicatorNum:      1,
		unregisteredRoles: map[string]struct{}{},
	}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHealthHandler_Unhealthy(t *testing.T) {
	handler := &HealthHandler{
		indicators: []Indicator{
			&mockIndicator{name: "comp1", status: Healthy},
			&mockIndicator{name: "comp2", status: Abnormal},
		},
		indicatorNum:      2,
		unregisteredRoles: map[string]struct{}{},
	}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "Not all components are healthy")
}

func TestHealthHandler_UnregisteredRole(t *testing.T) {
	handler := &HealthHandler{
		indicators: []Indicator{
			&mockIndicator{name: "comp1", status: Abnormal},
		},
		indicatorNum:      1,
		unregisteredRoles: map[string]struct{}{"comp1": {}},
	}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	// comp1 is unregistered so skipped — result is OK
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHealthHandler_JSONResponse(t *testing.T) {
	handler := &HealthHandler{
		indicators: []Indicator{
			&mockIndicator{name: "comp1", status: Healthy},
		},
		indicatorNum:      1,
		unregisteredRoles: map[string]struct{}{},
	}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set(ContentTypeHeader, ContentTypeJSON)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, ContentTypeJSON, w.Header().Get(ContentTypeHeader))

	var resp HealthResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "OK", resp.State)
	assert.Len(t, resp.Detail, 1)
	assert.Equal(t, "comp1", resp.Detail[0].Name)
	assert.Equal(t, Healthy, resp.Detail[0].Code)
}

func TestHealthHandler_JSONResponse_Unhealthy(t *testing.T) {
	handler := &HealthHandler{
		indicators: []Indicator{
			&mockIndicator{name: "comp1", status: Initializing},
		},
		indicatorNum:      1,
		unregisteredRoles: map[string]struct{}{},
	}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set(ContentTypeHeader, ContentTypeJSON)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)

	var resp HealthResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Contains(t, resp.State, "Not all components are healthy")
}
