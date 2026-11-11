package management

import (
	"context"
	"encoding/json"
	"net/http"
)

// LogHealthCallback returns the node's log health, optionally filtered to one
// (bucketName, rootPath) instance. Empty strings mean "all tenants".
type LogHealthCallback func(ctx context.Context, bucketName, rootPath string) (any, int)

// NewLogHealthHandler serves the node-wide log health endpoint.
// Optional query params: ?bucket_name=<bucket>&root_path=<root>.
// A partial filter (only one of the two) is ignored — all tenants are returned.
func NewLogHealthHandler(get LogHealthCallback) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		bucketName, rootPath := tenantFilter(r)

		result, statusCode := get(r.Context(), bucketName, rootPath)
		if statusCode == 0 {
			statusCode = http.StatusOK
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_ = json.NewEncoder(w).Encode(result)
	}
}
