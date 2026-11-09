package wal

import (
	"context"
	"io"
	"os"
	"os/exec"
	"time"
)

func agentDefaults(a AgentConfig) AgentConfig {
	if a.Provider == "" {
		a.Provider = "claude"
	}
	if a.Auth == "" {
		a.Auth = "api_key"
		if a.Provider == "codex" {
			a.Auth = "cli"
		}
	}
	if a.Executable == "" {
		a.Executable = "claude"
		if a.Provider == "codex" {
			a.Executable = "codex"
		}
	}
	if a.APIKeyEnv == "" && a.Auth == "api_key" {
		a.APIKeyEnv = "ANTHROPIC_API_KEY"
		if a.Provider == "codex" {
			a.APIKeyEnv = "CODEX_API_KEY"
		}
	}
	return a
}

type AgentState struct {
	Enabled  bool   `json:"enabled"`
	Provider string `json:"provider"`
	Auth     string `json:"auth"`
	Ready    bool   `json:"ready"`
	Code     string `json:"code,omitempty"`
	Message  string `json:"message"`
	Mode     string `json:"mode"`
}

// AgentConfigurationState checks configuration only, without starting a process.
func AgentConfigurationState(a AgentConfig) AgentState {
	return agentState(context.Background(), a, false)
}

// AgentAvailability does a bounded local login-status check, never a model call.
// Credentials and raw CLI output are never returned to the browser.
func AgentAvailability(ctx context.Context, a AgentConfig) AgentState {
	return agentState(ctx, a, true)
}

func agentState(ctx context.Context, a AgentConfig, probe bool) AgentState {
	a = agentDefaults(a)
	out := AgentState{Enabled: a.Enabled, Provider: a.Provider, Auth: a.Auth, Mode: "analyze_supplied_findings_only"}
	if !a.Enabled {
		out.Code = "agent_disabled"
		out.Message = "Agent is disabled."
		return out
	}
	if a.Provider != "claude" && a.Provider != "codex" {
		out.Code = "unsupported_provider"
		out.Message = "Select claude or codex."
		return out
	}
	if _, err := exec.LookPath(a.Executable); err != nil {
		out.Code = "agent_executable_unavailable"
		out.Message = "The configured Agent executable is unavailable."
		return out
	}
	if a.Auth == "api_key" {
		if os.Getenv(a.APIKeyEnv) == "" {
			out.Code = "model_credentials_unavailable"
			out.Message = "The configured API-key environment variable is unavailable."
			return out
		}
	} else if a.Auth == "cli" && a.Provider == "codex" {
		if !probe {
			out.Code = "agent_status_pending"
			out.Message = "Checking local Agent login…"
			return out
		}
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, a.Executable, "login", "status") // #nosec G204 -- private backend executable; fixed argv.
		cmd.Env = codexEnvironment(a)
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		cmd.WaitDelay = 250 * time.Millisecond
		if cmd.Run() != nil {
			out.Code = "codex_login_unavailable"
			out.Message = "Codex login is unavailable. Run codex login status, then codex login if needed, as the user running wpdiag."
			return out
		}
	} else {
		out.Code = "unsupported_auth"
		out.Message = "Unsupported Agent authentication mode."
		return out
	}
	out.Ready = true
	out.Message = "Local executable and authentication are available. No model request has been made."
	return out
}
func AgentReady(a AgentConfig) bool { return AgentAvailability(context.Background(), a).Ready }

func codexEnvironment(a AgentConfig) []string {
	env := []string{}
	for _, key := range []string{"PATH", "HOME", "CODEX_HOME", "TMPDIR", "LANG", "LC_ALL", "SYSTEMROOT", "HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY", "NO_PROXY", "CODEX_CA_CERTIFICATE", "SSL_CERT_FILE", "SSL_CERT_DIR"} {
		if value, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+value)
		}
	}
	if a.Auth == "api_key" {
		env = append(env, "CODEX_API_KEY="+os.Getenv(a.APIKeyEnv))
	}
	return env
}
