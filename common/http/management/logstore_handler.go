package management

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// NewLogstoreSegmentsHandler handles GET /admin/logstore/segments.
// Query params: ?log_id=42&writable=true
func NewLogstoreSegmentsHandler(list func(logID *int64, writable *bool) []any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var logID *int64
		if v := r.URL.Query().Get("log_id"); v != "" {
			id, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				http.Error(w, `{"error":"invalid log_id"}`, http.StatusBadRequest)
				return
			}
			logID = &id
		}

		var writable *bool
		if v := r.URL.Query().Get("writable"); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				http.Error(w, `{"error":"invalid writable"}`, http.StatusBadRequest)
				return
			}
			writable = &b
		}

		segments := list(logID, writable)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"segments": segments})
	}
}

// NewLogstoreSegmentShowHandler handles GET /admin/logstore/segments?log_id=X&segment_id=Y&detailed=true.
func NewLogstoreSegmentShowHandler(get func(logID, segmentID int64) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		logIDStr := r.URL.Query().Get("log_id")
		segIDStr := r.URL.Query().Get("segment_id")
		if logIDStr == "" || segIDStr == "" {
			http.Error(w, `{"error":"log_id and segment_id required"}`, http.StatusBadRequest)
			return
		}
		logID, err := strconv.ParseInt(logIDStr, 10, 64)
		if err != nil {
			http.Error(w, `{"error":"invalid log_id"}`, http.StatusBadRequest)
			return
		}
		segID, err := strconv.ParseInt(segIDStr, 10, 64)
		if err != nil {
			http.Error(w, `{"error":"invalid segment_id"}`, http.StatusBadRequest)
			return
		}

		seg, err := get(logID, segID)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(seg)
	}
}

// NewLogstoreFlushHandler handles POST /admin/logstore/flush.
// Body: {"log_id": 42, "segment_id": 7} or {} for all.
func NewLogstoreFlushHandler(flush func(logID, segmentID int64) error) http.HandlerFunc {
	return logstorePostHandler("flush", func(logID, segmentID int64, _ string) error {
		return flush(logID, segmentID)
	})
}

// NewLogstoreFenceHandler handles POST /admin/logstore/fence.
// Body: {"log_id": 42, "segment_id": 7, "reason": "..."}
func NewLogstoreFenceHandler(fence func(logID, segmentID int64, reason string) error) http.HandlerFunc {
	return logstorePostHandler("fence", fence)
}

// NewLogstoreCompactHandler handles POST /admin/logstore/compact.
// Body: {"log_id": 42, "segment_id": 7}
func NewLogstoreCompactHandler(compact func(logID, segmentID int64) error) http.HandlerFunc {
	return logstorePostHandler("compact", func(logID, segmentID int64, _ string) error {
		return compact(logID, segmentID)
	})
}

func logstorePostHandler(op string, fn func(logID, segmentID int64, reason string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			LogID     int64  `json:"log_id"`
			SegmentID int64  `json:"segment_id"`
			Reason    string `json:"reason,omitempty"`
		}
		if r.Body != nil {
			_ = json.NewDecoder(r.Body).Decode(&body)
		}
		if err := fn(body.LogID, body.SegmentID, body.Reason); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": op + " completed"})
	}
}
