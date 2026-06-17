package cmd

import (
	"testing"

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
