package management

import (
	"encoding/json"
	"net/http"
)

// NewOpsListHandler handles GET /admin/runtime/ops.
// Query params: ?type=file.flush&log_id=42&longer_than_ms=30000&limit=100
func NewOpsListHandler(list func(params map[string]string) any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		params := make(map[string]string)
		for key := range r.URL.Query() {
			params[key] = r.URL.Query().Get(key)
		}
		result := list(params)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	}
}

// NewOpsGetHandler handles GET /admin/runtime/ops?op_id=xxx.
func NewOpsGetHandler(get func(opID string) any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		opID := r.URL.Query().Get("op_id")
		if opID == "" {
			http.Error(w, `{"error":"op_id required"}`, http.StatusBadRequest)
			return
		}
		result := get(opID)
		if result == nil {
			http.Error(w, `{"error":"op not found"}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	}
}

// NewOpsStatsHandler handles GET /admin/runtime/ops/stats.
func NewOpsStatsHandler(stats func() any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		result := stats()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	}
}
