package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoad_GoodFile(t *testing.T) {
	cfg, err := Load("testdata/good.yaml")
	require.NoError(t, err)
	require.Equal(t, "prod", cfg.CurrentContext)
	require.Len(t, cfg.Contexts, 2)
	require.Equal(t, "http://prod.wp.svc:9091", cfg.Contexts["prod"].Endpoint)
	require.Equal(t, "table", cfg.Defaults.Output)
}

func TestLoad_BadYAML(t *testing.T) {
	_, err := Load("testdata/bad-yaml.yaml")
	require.Error(t, err)
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load("testdata/does-not-exist.yaml")
	require.Error(t, err)
}

func TestResolveContext_Explicit(t *testing.T) {
	cfg, _ := Load("testdata/good.yaml")
	ctx, err := cfg.ResolveContext("dev")
	require.NoError(t, err)
	require.Equal(t, "http://localhost:9091", ctx.Endpoint)
}

func TestResolveContext_Current(t *testing.T) {
	cfg, _ := Load("testdata/good.yaml")
	ctx, err := cfg.ResolveContext("")
	require.NoError(t, err)
	require.Equal(t, "http://prod.wp.svc:9091", ctx.Endpoint)
}

func TestResolveContext_MissingName(t *testing.T) {
	cfg, _ := Load("testdata/missing-context.yaml")
	_, err := cfg.ResolveContext("")
	require.Error(t, err)
}

func TestResolveContext_Defaults(t *testing.T) {
	cfg, _ := Load("testdata/good.yaml")
	ctx, _ := cfg.ResolveContext("dev")
	require.Equal(t, 9091, ctx.AdminPort)
	require.Equal(t, 30*time.Second, ctx.Timeout)
}
