package management

import (
	"encoding/json"
	"net/http"
	"strings"
)

// secretFields are the configuration keys whose values must never leave the
// process. Matching is case-insensitive so both YAML-style and Go field names
// are covered.
var secretFields = map[string]bool{
	"secretaccesskey":   true,
	"gcpcredentialjson": true,
	"tlskey":            true,
	"password":          true,
}

// NewConfigHandler returns a handler for GET /admin/config.
// getConfig must return a JSON-serializable snapshot of the loaded configuration.
// Secret fields are masked after serialisation so credentials never reach the
// client, while every non-secret field remains visible.
func NewConfigHandler(getConfig func() any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		data, err := json.Marshal(getConfig())
		if err != nil {
			http.Error(w, "config encode failed", http.StatusInternalServerError)
			return
		}
		var snapshot any
		if err := json.Unmarshal(data, &snapshot); err != nil {
			http.Error(w, "config decode failed", http.StatusInternalServerError)
			return
		}
		maskSecrets(snapshot)
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(snapshot)
	}
}

func maskSecrets(v any) {
	switch node := v.(type) {
	case map[string]any:
		for key, value := range node {
			if secretFields[strings.ToLower(key)] {
				node[key] = "[REDACTED]"
				continue
			}
			maskSecrets(value)
		}
	case []any:
		for _, value := range node {
			maskSecrets(value)
		}
	}
}
