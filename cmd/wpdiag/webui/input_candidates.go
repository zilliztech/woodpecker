package webui

import (
	"net/http"

	"github.com/zilliztech/woodpecker/cmd/wpcli/candidates"
)

func (s *Server) inputCandidates(w http.ResponseWriter, r *http.Request) {
	values, err := candidates.List()
	if err != nil {
		fail(w, 500, "candidate_cache_unavailable", "Cannot read temporary input candidates")
		return
	}
	send(w, 200, map[string]any{"values": values, "path": candidates.Directory()})
}

func (s *Server) forgetInputCandidate(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if !decodeBody(w, r, &q) {
		return
	}
	if err := candidates.Forget(q.Key, q.Value); err != nil {
		fail(w, 422, "candidate_delete_failed", "Cannot remove this candidate value")
		return
	}
	s.inputCandidates(w, r)
}

func (s *Server) recentInputConfig(w http.ResponseWriter, r *http.Request) {
	path, body, err := candidates.RecentConfig()
	if err != nil {
		fail(w, 500, "candidate_config_unavailable", "Cannot read the temporary input candidate file")
		return
	}
	send(w, 200, map[string]any{"path": path, "content": body, "read_only": true})
}
