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
	for _, tc := range []struct {
		profileType   string
		path          string
		seconds       string
		defaultOutput bool
	}{
		{profileType: "heap", path: "/debug/pprof/heap"},
		{profileType: "cpu", path: "/debug/pprof/profile", seconds: "10"},
		{profileType: "trace", path: "/debug/pprof/trace", seconds: "10"},
		{profileType: "trace", path: "/debug/pprof/trace", seconds: "10", defaultOutput: true},
	} {
		t.Run(tc.profileType, func(t *testing.T) {
			ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
			// Fake pprof bytes — not a real gzipped profile but the test just checks save.
			pprofBody := []byte{0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00}

			mux := http.NewServeMux()
			mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(ml))
			})
			mux.HandleFunc(tc.path, func(w http.ResponseWriter, r *http.Request) {
				if got := r.URL.Query().Get("seconds"); got != tc.seconds {
					t.Errorf("seconds = %q; want %q", got, tc.seconds)
				}
				_, _ = w.Write(pprofBody)
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()
			withCliYAML(t, srv.URL)

			outFile := filepath.Join(t.TempDir(), tc.profileType+".pb.gz")
			if tc.defaultOutput {
				t.Chdir(t.TempDir())
			}

			root := NewRootCommand()
			buf := new(bytes.Buffer)
			root.SetOut(buf)
			root.SetErr(buf)
			args := []string{
				"profile", "node-1",
				"--type", tc.profileType,
				"--seconds", "10",
				"--admin-port", extractPort(t, srv.URL),
			}
			if !tc.defaultOutput {
				args = append(args, "--output-file", outFile)
			}
			root.SetArgs(args)
			require.NoError(t, root.Execute())
			if tc.defaultOutput {
				files, err := filepath.Glob("node-1-trace-*.trace")
				require.NoError(t, err)
				require.Len(t, files, 1)
				outFile = "./" + files[0]
			}

			data, err := os.ReadFile(outFile)
			require.NoError(t, err)
			require.Equal(t, pprofBody, data)
			tool := "pprof"
			if tc.profileType == "trace" {
				tool = "trace"
			}
			require.Contains(t, buf.String(), "To analyze: go tool "+tool+" -http=127.0.0.1:8081 "+outFile)
		})
	}
}
