package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfile_DownloadsFile(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	// Fake pprof bytes — not a real gzipped profile but the test just checks save.
	pprofBody := []byte{0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00}

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(ml))
	})
	mux.HandleFunc("/debug/pprof/heap", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(pprofBody)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	outFile := filepath.Join(t.TempDir(), "heap.pb.gz")

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{
		"profile", "node-1",
		"--type", "heap",
		"--output-file", outFile,
		"--admin-port", extractPort(t, srv.URL),
	})
	require.NoError(t, root.Execute())

	data, err := os.ReadFile(outFile)
	require.NoError(t, err)
	require.Equal(t, pprofBody, data)
}
