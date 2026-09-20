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

// instanceTestServer serves a memberlist plus /admin/instance/data. gotQuery, when
// non-nil, receives the raw query string of each inventory request.
func instanceTestServer(t *testing.T, members []map[string]any, report map[string]any, gotQuery *string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"members": members})
	})
	mux.HandleFunc("/admin/instance/data", func(w http.ResponseWriter, r *http.Request) {
		if gotQuery != nil {
			*gotQuery = r.URL.RawQuery
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(report)
	})
	return httptest.NewServer(mux)
}

func oneMember(id string) map[string]any {
	return map[string]any{"id": id, "gossip_addr": "127.0.0.1:17946", "service_addr": "127.0.0.1:18080"}
}

func sampleReport(scanErrors int) map[string]any {
	return map[string]any{
		"node_id": "woodpecker-0", "storage_mode": "service",
		"storage_root": "/var/lib/woodpecker", "timestamp_ms": 1758300000123,
		"instance_count": 1, "total_size_bytes": 1234567, "scan_errors": scanErrors,
		"instances": []map[string]any{{
			"bucket_name": "a-bucket", "root_path": "in01-orphan",
			"log_count": 12, "segment_count": 37, "live_segment_count": 30,
			"size_bytes": 1234567, "last_modified_ms": 1758299000456,
			"active_processors": 0, "delete_state": "", "marked_deleted_at_ms": 0,
		}},
	}
}

func runInstanceCmd(t *testing.T, srv *httptest.Server, args ...string) (string, error) {
	t.Helper()
	withCliYAML(t, srv.URL)
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs(append([]string{"--admin-port", extractPort(t, srv.URL)}, args...))
	err := root.Execute()
	return buf.String(), err
}

func TestInstanceData_SingleNodeRendersRows(t *testing.T) {
	srv := instanceTestServer(t, []map[string]any{oneMember("node-1")}, sampleReport(0), nil)
	defer srv.Close()

	out, err := runInstanceCmd(t, srv, "instance", "data", "node-1")

	require.NoError(t, err)
	assert.Contains(t, out, "a-bucket")
	assert.Contains(t, out, "in01-orphan")
	assert.Contains(t, out, "12")
}

func TestInstanceData_AllNodesFansOut(t *testing.T) {
	members := []map[string]any{oneMember("node-1"), oneMember("node-2")}
	srv := instanceTestServer(t, members, sampleReport(0), nil)
	defer srv.Close()

	out, err := runInstanceCmd(t, srv, "instance", "data", "--all")

	require.NoError(t, err)
	assert.Contains(t, out, "node-1")
	assert.Contains(t, out, "node-2")
}

// TestInstanceData_FilterBecomesQueryParams: the CLI must send both filter params or
// neither, because the server ignores a partial filter and answers with everything.
func TestInstanceData_FilterBecomesQueryParams(t *testing.T) {
	var gotQuery string
	srv := instanceTestServer(t, []map[string]any{oneMember("node-1")}, sampleReport(0), &gotQuery)
	defer srv.Close()

	_, err := runInstanceCmd(t, srv, "instance", "data", "node-1",
		"--bucket", "a-bucket", "--root", "in01-orphan")

	require.NoError(t, err)
	assert.Contains(t, gotQuery, "bucket_name=a-bucket")
	assert.Contains(t, gotQuery, "root_path=in01-orphan")
}

// TestInstanceData_ScanErrorsAreSurfaced: a silent scan error would let an operator read
// "instance absent" as "instance clean" — the one conclusion this output must not invite.
func TestInstanceData_ScanErrorsAreSurfaced(t *testing.T) {
	srv := instanceTestServer(t, []map[string]any{oneMember("node-1")}, sampleReport(3), nil)
	defer srv.Close()

	out, err := runInstanceCmd(t, srv, "instance", "data", "node-1")

	require.NoError(t, err)
	assert.Contains(t, out, "scan errors")
}

// TestInstanceData_UnreachableNodeIsVisible: an unreachable node must appear as such
// rather than being dropped, so its instances are never assumed absent.
func TestInstanceData_UnreachableNodeIsVisible(t *testing.T) {
	dead := oneMember("node-dead")
	dead["tags"] = map[string]string{"admin_port": "1"}
	srv := instanceTestServer(t, []map[string]any{oneMember("node-1"), dead}, sampleReport(0), nil)
	defer srv.Close()

	out, err := runInstanceCmd(t, srv, "instance", "data", "--all")

	require.NoError(t, err)
	assert.Contains(t, out, "node-dead")
	assert.Contains(t, out, "UNREACHABLE")
}

func TestInstanceData_StrictFailsOnUnreachableNode(t *testing.T) {
	dead := oneMember("node-dead")
	dead["tags"] = map[string]string{"admin_port": "1"}
	srv := instanceTestServer(t, []map[string]any{oneMember("node-1"), dead}, sampleReport(0), nil)
	defer srv.Close()

	_, err := runInstanceCmd(t, srv, "--strict", "instance", "data", "--all")

	require.Error(t, err)
}
