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

func TestLoggingGetLevel_SingleNode(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"members": []map[string]any{
				{"id": "node-1", "gossip_addr": "127.0.0.1:17946", "service_addr": "127.0.0.1:18080"},
			},
		})
	})
	mux.HandleFunc("/log/level", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"level": "info"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"logging", "get-level", "node-1", "--admin-port", extractPort(t, srv.URL)})
	err := root.Execute()
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "info")
}

func TestLoggingSetLevel_SingleNode(t *testing.T) {
	var gotBody map[string]string
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"members": []map[string]any{
				{"id": "node-1", "gossip_addr": "127.0.0.1:17946", "service_addr": "127.0.0.1:18080"},
			},
		})
	})
	mux.HandleFunc("/log/level", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			_ = json.NewDecoder(r.Body).Decode(&gotBody)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"level": gotBody["level"]})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"level": "info"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"logging", "set-level", "node-1", "--level", "debug", "--admin-port", extractPort(t, srv.URL)})
	err := root.Execute()
	require.NoError(t, err)
	assert.Equal(t, "debug", gotBody["level"])
	assert.Contains(t, buf.String(), "debug")
}

func TestLoggingSetLevel_MissingLevel(t *testing.T) {
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"--endpoint", "http://localhost:9091", "logging", "set-level", "node-1"})
	err := root.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--level is required")
}
