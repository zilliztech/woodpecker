package management

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// NewLogstoreSegmentProbeHandler handles GET /admin/logstore/segment/probe.
//
// Query params: log_id and segment_id (required), from_entry and max_entries (optional, zero leaves
// the node's own bounds in force), plus the shared bucket_name/root_path tenant filter for a caller
// that knows which instance it means.
//
// The node attempts a bounded read of its own copy and reports how far it got. It answers for
// itself and asks no peer; assembling the quorum's view is the caller's job.
func NewLogstoreSegmentProbeHandler(probe func(bucketName, rootPath string, logID, segmentID, fromEntry, maxEntries int64) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		logID, ok := requiredInt64(w, r, "log_id")
		if !ok {
			return
		}
		segmentID, ok := requiredInt64(w, r, "segment_id")
		if !ok {
			return
		}
		fromEntry, ok := optionalInt64(w, r, "from_entry")
		if !ok {
			return
		}
		maxEntries, ok := optionalInt64(w, r, "max_entries")
		if !ok {
			return
		}
		bucketName, rootPath := tenantFilter(r)

		report, err := probe(bucketName, rootPath, logID, segmentID, fromEntry, maxEntries)
		if err != nil {
			// A node that cannot answer about this segment is a per-node state the caller has to
			// see per node, carrying the node's own reason.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(report)
	}
}

func requiredInt64(w http.ResponseWriter, r *http.Request, name string) (int64, bool) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		http.Error(w, `{"error":"log_id and segment_id required"}`, http.StatusBadRequest)
		return 0, false
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		http.Error(w, `{"error":"invalid `+name+`"}`, http.StatusBadRequest)
		return 0, false
	}
	return v, true
}

// optionalInt64 leaves an omitted bound at zero, which the node reads as "use your own".
func optionalInt64(w http.ResponseWriter, r *http.Request, name string) (int64, bool) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return 0, true
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		http.Error(w, `{"error":"invalid `+name+`"}`, http.StatusBadRequest)
		return 0, false
	}
	return v, true
}
