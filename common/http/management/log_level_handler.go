package management

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/zilliztech/woodpecker/common/logger"
)

// NewLogLevelHandler returns a handler for GET + POST /log/level.
// GET returns {"level": "<current>"}.
// POST expects {"level": "<new>"} and sets the log level.
func NewLogLevelHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"level": logger.GetLevel()})
		case http.MethodPost:
			var body struct {
				Level string `json:"level"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				w.Header().Set("Content-Type", "application/json")
				http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
				return
			}
			if err := logger.SetLevel(body.Level); err != nil {
				w.Header().Set("Content-Type", "application/json")
				http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"level": logger.GetLevel()})
		default:
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
	}
}
