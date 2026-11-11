package wal

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func fakeAgent(t *testing.T, body string) AgentConfig {
	t.Helper()
	path := filepath.Join(t.TempDir(), "agent")
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o700))
	t.Setenv("WPDIAG_TEST_KEY", "backend-only-sentinel")
	return AgentConfig{Enabled: true, Executable: path, APIKeyEnv: "WPDIAG_TEST_KEY", TimeoutMS: 2000, MaxOutputBytes: 4096}
}

func TestAgentUsesToolsDisabledAndOnlySuppliedPacket(t *testing.T) {
	a := fakeAgent(t, `
[ "$ANTHROPIC_API_KEY" = backend-only-sentinel ] || exit 11
[ -z "$WPDIAG_UNRELATED_SECRET" ] || exit 12
found=no
while [ "$#" -gt 0 ]; do
 if [ "$1" = --tools ]; then shift; [ -z "$1" ] || exit 13; found=yes; fi
 shift
done
[ "$found" = yes ] || exit 14
input=$(cat)
case "$input" in *analyze_supplied_findings_only*) ;; *) exit 15;; esac
printf '%s\n' '{"type":"assistant","message":{"content":[{"type":"text","text":"working"}]}}' '{"type":"result","is_error":false,"result":"W01 is elevated; cause is not confirmed."}'
`)
	t.Setenv("WPDIAG_UNRELATED_SECRET", "must-not-be-inherited")
	n := 0
	r := Analyze(context.Background(), a, Packet{Mode: "analyze_supplied_findings_only", Findings: []PacketEvidence{{Evidence: Evidence{ID: "W01", Status: "abnormal"}}}}, func(string) { n++ })
	require.Equal(t, "completed", r.Status)
	require.Contains(t, r.Text, "not confirmed")
	require.Equal(t, 1, n)
	require.NotContains(t, r.Text, "backend-only")
}

func TestAgentFailureAndCancellationNeverPublishPartialConclusion(t *testing.T) {
	packet := Packet{Findings: []PacketEvidence{{Evidence: Evidence{ID: "W01", Status: "abnormal"}}}}
	for _, tc := range []struct{ name, script, code string }{
		{"tool", `printf '%s\n' '{"type":"assistant","message":{"content":[{"type":"tool_use"}]}}'; exec sleep 10`, "agent_tool_request_rejected"},
		{"error", `printf '%s\n' '{"type":"result","is_error":true,"result":"secret failure"}'; exec sleep 10`, "agent_reported_error"},
		{"invalid", `printf '%s\n' 'not JSON'; exec sleep 10`, "invalid_agent_stream"},
		{"overflow", `awk 'BEGIN {for(i=0;i<10000;i++) printf "x"}'`, "output_budget_or_stream_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := fakeAgent(t, tc.script)
			start := time.Now()
			r := Analyze(context.Background(), a, packet, nil)
			require.Less(t, time.Since(start), 1500*time.Millisecond)
			require.Equal(t, "failed", r.Status)
			require.Equal(t, tc.code, r.Code)
			require.Empty(t, r.Text)
		})
	}
	a := fakeAgent(t, "exec sleep 10\n")
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	r := Analyze(ctx, a, packet, nil)
	require.Equal(t, "cancelled", r.Status)
	require.Empty(t, r.Text)
}
