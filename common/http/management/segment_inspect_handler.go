package management

import (
	"context"
	"encoding/json"
	"net/http"
)

// NewLogstoreSegmentInspectHandler handles GET /admin/logstore/segment/inspect.
//
// Query params: log_id and segment_id (required), from_block and max_blocks (optional, zero leaves
// the node's own bounds in force), plus bucket_name and root_path, which name an instance together.
//
// The node walks its own copy of the segment block by block and reports what it found at each one,
// continuing past a block it could not read. It answers for itself and asks no peer.
func NewLogstoreSegmentInspectHandler(inspect func(ctx context.Context, bucketName, rootPath string, logID, segmentID, fromBlock, maxBlocks int64) (any, error)) http.HandlerFunc {
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
		fromBlock, ok := optionalInt64(w, r, "from_block")
		if !ok {
			return
		}
		maxBlocks, ok := optionalInt64(w, r, "max_blocks")
		if !ok {
			return
		}
		bucketName := r.URL.Query().Get("bucket_name")
		rootPath := r.URL.Query().Get("root_path")
		if (bucketName == "") != (rootPath == "") {
			http.Error(w, `{"error":"bucket_name and root_path must be given together"}`, http.StatusBadRequest)
			return
		}

		report, err := inspect(r.Context(), bucketName, rootPath, logID, segmentID, fromBlock, maxBlocks)
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
