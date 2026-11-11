package wal

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func fakeCodex(t *testing.T, body string) AgentConfig {
	t.Helper()
	path := filepath.Join(t.TempDir(), "codex")
	script := "#!/bin/sh\nif [ \"$1 $2\" = 'login status' ]; then exit 0; fi\n" + body
	require.NoError(t, os.WriteFile(path, []byte(script), 0o700))
	return AgentConfig{Enabled: true, Provider: "codex", Auth: "cli", Executable: path, TimeoutMS: 2000, MaxOutputBytes: 4096}
}

func codexPacket() Packet {
	return Packet{Mode: "analyze_supplied_findings_only", Findings: []PacketEvidence{{Evidence: Evidence{ID: "S06", Status: "abnormal"}}}}
}

func TestCodexCLIAuthDoesNotInheritModelKeysOrSystemTools(t *testing.T) {
	t.Setenv("CODEX_API_KEY", "do-not-inherit")
	t.Setenv("OPENAI_API_KEY", "do-not-inherit")
	t.Setenv("UNRELATED_SECRET", "do-not-inherit")
	a := fakeCodex(t, `
[ -z "$CODEX_API_KEY$OPENAI_API_KEY$UNRELATED_SECRET" ] || exit 11
case "$*" in *'--ignore-user-config'*'--ignore-rules'*) ;; *) exit 12;; esac
case "$*" in *'features.shell_tool=false'*) ;; *) exit 13;; esac
case "$*" in *'features.view_image=false'*) ;; *) exit 14;; esac
case "$*" in *'mcp_servers={}'*) ;; *) exit 15;; esac
input=$(cat)
case "$input" in *'analyze_supplied_findings_only'*) ;; *) exit 16;; esac
printf '%s\n' '{"type":"thread.started","thread_id":"t"}' '{"type":"turn.started"}' '{"type":"item.completed","item":{"type":"agent_message","text":"S06: filesystem reached the watermark; cause unconfirmed."}}' '{"type":"turn.completed","usage":{"input_tokens":100,"output_tokens":10}}'
`)
	availability := AgentAvailability(context.Background(), a)
	require.True(t, availability.Ready)
	require.Equal(t, "cli", availability.Auth)
	progress := 0
	result := Analyze(context.Background(), a, codexPacket(), func(string) { progress++ })
	require.Equal(t, "completed", result.Status)
	require.Equal(t, "codex", result.Provider)
	require.Contains(t, result.Text, "S06")
	require.Equal(t, 1, progress)
	a.Auth = "api_key"
	a.APIKeyEnv = "WPDIAG_CODEX_TEST_KEY"
	t.Setenv(a.APIKeyEnv, "backend-test-key")
	require.Contains(t, codexEnvironment(a), "CODEX_API_KEY=backend-test-key")
	require.NotContains(t, codexEnvironment(a), "OPENAI_API_KEY=do-not-inherit")
}

func TestCodexFailuresWithholdAnyPartialConclusion(t *testing.T) {
	for _, tc := range []struct{ name, script, code string }{
		{"tool", `printf '%s\n' '{"type":"turn.started"}' '{"type":"item.started","item":{"type":"command_execution","command":"touch forbidden"}}'; exec sleep 10`, "agent_tool_request_rejected"},
		{"interactive", `printf '%s\n' '{"type":"turn.started"}' '{"type":"item.started","item":{"type":"request_user_input"}}'; exec sleep 10`, "agent_tool_request_rejected"},
		{"failed", `printf '%s\n' '{"type":"turn.started"}' '{"type":"item.completed","item":{"type":"agent_message","text":"partial"}}' '{"type":"turn.failed","error":{"message":"secret-provider-error"}}'; exec sleep 10`, "agent_reported_error"},
		{"invalid", `printf '%s\n' 'not-json'; exec sleep 10`, "invalid_agent_stream"},
		{"unfinished", `printf '%s\n' '{"type":"turn.started"}' '{"type":"item.completed","item":{"type":"agent_message","text":"partial"}}'`, "codex_process_failed"},
		{"overflow", `awk 'BEGIN {for(i=0;i<10000;i++) printf "x"}'`, "output_budget_or_stream_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := fakeCodex(t, tc.script)
			result := Analyze(context.Background(), a, codexPacket(), nil)
			require.Equal(t, "failed", result.Status)
			require.Equal(t, tc.code, result.Code)
			require.Empty(t, result.Text)
		})
	}
	a := fakeCodex(t, "exec sleep 10\n")
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(25 * time.Millisecond); cancel() }()
	result := Analyze(ctx, a, codexPacket(), nil)
	require.Equal(t, "cancelled", result.Status)
	require.Empty(t, result.Text)
	a.TimeoutMS = 25
	result = Analyze(context.Background(), a, codexPacket(), nil)
	require.Equal(t, "timeout", result.Status)
}

func TestCodexConfigurationAndLocalReadiness(t *testing.T) {
	for _, tc := range []struct{ yaml, provider, auth, executable, key string }{
		{"workbench: {agent: {provider: codex}}\n", "codex", "cli", "codex", ""},
		{"workbench: {agent: {provider: codex, auth: api_key}}\n", "codex", "api_key", "codex", "CODEX_API_KEY"},
		{"workbench: {agent: {}}\n", "claude", "api_key", "claude", "ANTHROPIC_API_KEY"},
	} {
		s, err := LoadSettings(config(t, tc.yaml))
		require.NoError(t, err)
		require.Equal(t, tc.provider, s.Agent.Provider)
		require.Equal(t, tc.auth, s.Agent.Auth)
		require.Equal(t, tc.executable, s.Agent.Executable)
		require.Equal(t, tc.key, s.Agent.APIKeyEnv)
	}
	for _, bad := range []string{"provider: unsupported", "provider: claude, auth: cli", "provider: codex, auth: cli, api_key_env: SOMETHING"} {
		_, err := LoadSettings(config(t, "workbench: {agent: {"+bad+"}}\n"))
		require.Error(t, err)
	}
	a := fakeCodex(t, "exit 12\n")
	a.Executable = filepath.Join(t.TempDir(), "missing")
	require.Equal(t, "agent_executable_unavailable", AgentAvailability(context.Background(), a).Code)
	result := Analyze(context.Background(), fakeCodex(t, "exit 12\n"), Packet{}, nil)
	require.Equal(t, "no_abnormal_findings", result.Code)
}
