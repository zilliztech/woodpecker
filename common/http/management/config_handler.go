package management

import (
	"encoding/json"
	"net/http"
)

// NewConfigHandler returns a handler for GET /admin/config.
// getConfig must return a JSON-serializable snapshot of the loaded configuration.
// Per spec §2.E.2 there is NO redaction — this CLI is an admin tool and the admin
// port is assumed to be on an internal network (spec §3.7 security model).
func NewConfigHandler(getConfig func() any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(getConfig())
	}
}
