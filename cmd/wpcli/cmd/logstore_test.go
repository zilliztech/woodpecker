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

func logstoreTestServer(t *testing.T) *httptest.Server {
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
	mux.HandleFunc("/admin/logstore/segments", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"segments": []map[string]any{
				{
					"log_id": 42, "segment_id": 7, "backend": "stagedstorage",
					"writable": true, "fenced": false, "finalized": false,
					"entry_count": 100, "block_count": 5,
					"buffer_bytes": float64(2048), "buffer_entries": float64(10),
					"flush_queue_depth": float64(3), "flush_queue_capacity": float64(100),
				},
			},
		})
	})
	mux.HandleFunc("/admin/logstore/segments/detail", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"log_id": 42, "segment_id": 7, "detailed": true,
		})
	})
	mux.HandleFunc("/admin/logstore/flush", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "flush completed"})
	})
	mux.HandleFunc("/admin/logstore/fence", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "fence completed"})
	})
	mux.HandleFunc("/admin/logstore/compact", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "compact completed"})
	})
	return httptest.NewServer(mux)
}

func runLogstoreCmd(t *testing.T, srv *httptest.Server, args ...string) (string, error) {
	t.Helper()
	withCliYAML(t, srv.URL)
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	fullArgs := append([]string{"--admin-port", extractPort(t, srv.URL)}, args...)
	root.SetArgs(fullArgs)
	err := root.Execute()
	return buf.String(), err
}

func TestLogstoreSegments(t *testing.T) {
	srv := logstoreTestServer(t)
	defer srv.Close()
	out, err := runLogstoreCmd(t, srv, "logstore", "segments", "node-1")
	require.NoError(t, err)
	assert.Contains(t, out, "42")
	assert.Contains(t, out, "stagedstorage")
}

func TestLogstoreSegmentShow(t *testing.T) {
	srv := logstoreTestServer(t)
	defer srv.Close()
	out, err := runLogstoreCmd(t, srv, "logstore", "segment-show", "node-1", "--log", "42", "--seg", "7")
	require.NoError(t, err)
	assert.Contains(t, out, "42")
}

func TestLogstoreBuffer(t *testing.T) {
	srv := logstoreTestServer(t)
	defer srv.Close()
	out, err := runLogstoreCmd(t, srv, "logstore", "buffer", "node-1")
	require.NoError(t, err)
	assert.Contains(t, out, "42:7")
	assert.Contains(t, out, "2048")
}

func TestLogstoreFlushQueue(t *testing.T) {
	srv := logstoreTestServer(t)
	defer srv.Close()
	out, err := runLogstoreCmd(t, srv, "logstore", "flush-queue", "node-1")
	require.NoError(t, err)
	assert.Contains(t, out, "42:7")
}

func TestLogstoreForceFlush(t *testing.T) {
	srv := logstoreTestServer(t)
	defer srv.Close()
	out, err := runLogstoreCmd(t, srv, "logstore", "force-flush", "node-1")
	require.NoError(t, err)
	assert.Contains(t, out, "flush completed")
}

func TestLogstoreFence_RequiresReason(t *testing.T) {
	srv := logstoreTestServer(t)
	defer srv.Close()
	_, err := runLogstoreCmd(t, srv, "logstore", "fence", "node-1", "--log", "42", "--seg", "7")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--reason is required")
}

func TestLogstoreFence_WithYes(t *testing.T) {
	srv := logstoreTestServer(t)
	defer srv.Close()
	out, err := runLogstoreCmd(t, srv, "logstore", "fence", "node-1", "--log", "42", "--seg", "7", "--reason", "test", "-y")
	require.NoError(t, err)
	assert.Contains(t, out, "fence completed")
}

func TestLogstoreCompact(t *testing.T) {
	srv := logstoreTestServer(t)
	defer srv.Close()
	out, err := runLogstoreCmd(t, srv, "logstore", "compact", "node-1", "--log", "42", "--seg", "7")
	require.NoError(t, err)
	assert.Contains(t, out, "compact completed")
}
