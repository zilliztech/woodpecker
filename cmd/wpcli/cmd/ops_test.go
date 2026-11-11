package cmd

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func opsTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"members": []map[string]any{
				{"id": "node-1", "gossip_addr": "127.0.0.1:17946", "service_addr": "127.0.0.1:18080"},
			},
		})
	})
	mux.HandleFunc("/admin/runtime/ops", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"op_id": "fl-000001", "op_type": "file.flush", "trace_id": "abc123", "log_id": 42, "segment_id": 7},
		})
	})
	mux.HandleFunc("/admin/runtime/ops/get", func(w http.ResponseWriter, r *http.Request) {
		opID := r.URL.Query().Get("op_id")
		if opID == "fl-000001" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"op_id": "fl-000001", "op_type": "file.flush",
				"trace_id": "abc123", "span_id": "def456",
				"log_id": 42, "segment_id": 7,
			})
		} else {
			http.Error(w, `{"error":"op not found"}`, http.StatusNotFound)
		}
	})
	mux.HandleFunc("/admin/runtime/ops/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"capacity": 1024, "in_use": 42, "warn_age_ms": 30000,
			"evicted_total": 100, "evicted_young": 95, "evicted_old": 5,
		})
	})
	return httptest.NewServer(mux)
}

func TestOpsList(t *testing.T) {
	srv := opsTestServer(t)
	defer srv.Close()
	out, err := runLogstoreCmd(t, srv, "ops", "list", "node-1")
	require.NoError(t, err)
	assert.Contains(t, out, "file.flush")
	assert.Contains(t, out, "fl-000001")
}

func TestOpsShow(t *testing.T) {
	srv := opsTestServer(t)
	defer srv.Close()
	out, err := runLogstoreCmd(t, srv, "ops", "show", "node-1", "--op-id", "fl-000001")
	require.NoError(t, err)
	assert.Contains(t, out, "file.flush")
	assert.Contains(t, out, "abc123")
}

func TestOpsShow_MissingOpID(t *testing.T) {
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"--endpoint", "http://localhost:9091", "ops", "show", "node-1"})
	err := root.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--op-id is required")
}

func TestOpsStats(t *testing.T) {
	srv := opsTestServer(t)
	defer srv.Close()
	out, err := runLogstoreCmd(t, srv, "ops", "stats", "node-1")
	require.NoError(t, err)
	assert.Contains(t, out, "1024")
	assert.Contains(t, out, "STALL SIGNAL")
}
