package cmd

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func TestClusterGossipDiff_Consistent(t *testing.T) {
	ml := `{"members":[
		{"id":"node-1","gossip_addr":"127.0.0.1:17946"},
		{"id":"node-2","gossip_addr":"127.0.0.1:17946"}
	]}`
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(ml))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"cluster", "gossip-diff", "--admin-port", extractPort(t, srv.URL)})
	require.NoError(t, root.Execute())
	require.Contains(t, buf.String(), "CONSISTENT")
}

func TestClusterGossipDiff_Inconsistent(t *testing.T) {
	// node-1's view has both nodes; node-2's view has only itself.
	// Since the fanout resolves to the same httptest server we distinguish
	// by request count: first call is the seed discovery, subsequent are fan-out.
	var callCount int
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		callCount++
		// First call (seed discovery) returns both members.
		if callCount == 1 {
			_, _ = fmt.Fprint(w, `{"members":[
				{"id":"node-1","gossip_addr":"127.0.0.1:17946"},
				{"id":"node-2","gossip_addr":"127.0.0.1:17946"}
			]}`)
			return
		}
		// Subsequent fanout calls alternate between two views.
		if callCount%2 == 0 {
			_, _ = fmt.Fprint(w, `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`)
		} else {
			_, _ = fmt.Fprint(w, `{"members":[
				{"id":"node-1","gossip_addr":"127.0.0.1:17946"},
				{"id":"node-2","gossip_addr":"127.0.0.1:17946"}
			]}`)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"cluster", "gossip-diff", "--admin-port", extractPort(t, srv.URL)})

	err := root.Execute()
	require.Error(t, err)
	require.Equal(t, 9, wperrors.ExitCodeFor(err))
	require.Contains(t, buf.String(), "INCONSISTENT")
}
