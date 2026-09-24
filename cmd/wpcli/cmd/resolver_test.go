package cmd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResolveEndpoint(t *testing.T) {
	const flag, env, ctx = "http://flag:9091", "http://env:9091", "http://ctx:9091"
	tests := []struct {
		name           string
		flag, env, ctx string
		want           string
	}{
		{"flag beats env and ctx", flag, env, ctx, flag},
		{"flag beats env (no ctx)", flag, env, "", flag},
		{"flag beats ctx (no env)", flag, "", ctx, flag},
		{"flag only", flag, "", "", flag},
		{"env beats ctx (no flag)", "", env, ctx, env},
		{"env only", "", env, "", env},
		{"ctx only", "", "", ctx, ctx},
		{"all empty -> empty", "", "", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, resolveEndpoint(tt.flag, tt.env, tt.ctx))
		})
	}
}

// TestSourceLine_NamesTheContextActuallyUsed guards the line's whole purpose. The context in
// effect comes from ResolveContext, which falls back to cli.yaml's current-context when no
// --context is given, so printing the flag's value reports a cluster the run is not talking to.
func TestSourceLine_NamesTheContextActuallyUsed(t *testing.T) {
	srv := spinTestServer(t, `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`, nil)
	defer srv.Close()
	withCliYAML(t, srv.URL) // writes current-context: prod

	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 3 * time.Second} // no --context

	res, err := resolveAndDiscover()
	require.NoError(t, err)

	require.Contains(t, res.SourceLine(), "context prod",
		"the line must name the context the request actually uses, not the empty flag")
	require.NotContains(t, res.SourceLine(), "context default")
}
