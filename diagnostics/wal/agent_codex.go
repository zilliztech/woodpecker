package wal

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// codexArguments is intentionally independent of user/project config. The CLI
// keeps its normal auth storage, but no source endpoints or tools are supplied.
// Validated with Codex CLI 0.166.0 and a local fake Responses endpoint.
func codexArguments(a AgentConfig, instructions string) []string {
	args := []string{"exec", "--json", "--strict-config", "--ephemeral", "--ignore-user-config", "--ignore-rules", "--skip-git-repo-check", "--sandbox", "read-only", "--color", "never"}
	settings := []string{
		`approval_policy="never"`, `agents.enabled=false`, `web_search="disabled"`,
		`features.shell_tool=false`, `features.view_image=false`, `features.apps=false`,
		`features.plugins=false`, `features.memories=false`, `features.hooks=false`,
		`features.request_permissions_tool=false`, `features.computer_use=false`,
		`features.browser_use=false`, `features.image_generation=false`, `features.code_mode=false`,
		`features.code_mode_host=false`, `features.goals=false`, `features.sleep_tool=false`,
		`features.skill_search=false`, `features.tool_suggest=false`, `features.skip_host_skill_discovery=true`,
		`skills.bundled.enabled=false`, `skills.include_instructions=false`, `mcp_servers={}`,
		`project_doc_max_bytes=0`, `history.persistence="none"`, `suppress_unstable_features_warning=true`,
	}
	settings = append(settings, "model_instructions_file="+strconv.Quote(instructions))
	for _, setting := range settings {
		args = append(args, "-c", setting)
	}
	if a.Model != "" {
		args = append(args, "--model", a.Model)
	}
	return append(args, "-")
}

func analyzeCodex(ctx context.Context, a AgentConfig, packet Packet, progress func(string)) Analysis {
	out := Analysis{Status: "failed", StartedAt: time.Now().UTC()}
	finish := func(code string) Analysis { out.Code = code; out.FinishedAt = time.Now().UTC(); return out }
	if !a.Enabled {
		return finish("agent_disabled")
	}
	if len(packet.Findings) == 0 {
		return finish("no_abnormal_findings")
	}
	if a.Auth == "api_key" && os.Getenv(a.APIKeyEnv) == "" {
		return finish("model_credentials_unavailable")
	}
	body, err := json.Marshal(packet)
	if err != nil || len(body) > 256<<10 {
		return finish("input_budget_exceeded")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(a.TimeoutMS)*time.Millisecond)
	defer cancel()
	dir, err := os.MkdirTemp("", "wpdiag-codex-")
	if err != nil {
		return finish("workspace_unavailable")
	}
	defer os.RemoveAll(dir)
	instructions := filepath.Join(dir, "instructions.md")
	if os.WriteFile(instructions, []byte(agentSystem+"\n"), 0o600) != nil {
		return finish("workspace_unavailable")
	}
	cmd := exec.CommandContext(ctx, a.Executable, codexArguments(a, instructions)...) // #nosec G204 -- private backend executable and fixed flags.
	cmd.Dir = dir
	cmd.Env = codexEnvironment(a)
	cmd.Stdin = bytes.NewReader(body)
	cmd.Stderr = io.Discard
	cmd.WaitDelay = time.Second
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return finish("agent_start_failed")
	}
	if cmd.Start() != nil {
		return finish("agent_start_failed")
	}
	reader := bufio.NewScanner(io.LimitReader(stdout, a.MaxOutputBytes+1))
	reader.Buffer(make([]byte, 4096), int(a.MaxOutputBytes)+1)
	size := int64(0)
	started, completed := false, false
	streamFailure := ""
	answer := ""
	kill := func(code string) { streamFailure = code; _ = cmd.Process.Kill() }
	for reader.Scan() {
		line := reader.Bytes()
		size += int64(len(line)) + 1
		if size > a.MaxOutputBytes {
			kill("output_budget_exceeded")
			break
		}
		var event struct {
			Type string `json:"type"`
			Item struct {
				Type  string `json:"type"`
				Text  string `json:"text"`
				Phase string `json:"phase"`
			} `json:"item"`
		}
		if json.Unmarshal(line, &event) != nil {
			kill("invalid_agent_stream")
			break
		}
		switch event.Type {
		case "thread.started":
		case "turn.started":
			if started || completed {
				kill("invalid_agent_stream")
				break
			}
			started = true
			if progress != nil {
				progress("Codex is analyzing the supplied findings")
			}
		case "item.started", "item.updated", "item.completed":
			switch event.Item.Type {
			case "reasoning": // Deliberately do not expose reasoning text.
			case "agent_message":
				if event.Type == "item.completed" && event.Item.Phase != "commentary" {
					answer = strings.TrimSpace(event.Item.Text)
				}
			case "error":
				// Exec emits nonfatal startup notices as error items (for example model
				// metadata notices). Strict config rejects unknown options before a turn.
				if started {
					kill("agent_reported_error")
				}
			default:
				kill("agent_tool_request_rejected")
			}
		case "turn.completed":
			if !started || completed {
				kill("invalid_agent_stream")
				break
			}
			completed = true
		case "error", "turn.failed":
			kill("agent_reported_error")
		default:
			kill("invalid_agent_stream")
		}
		if streamFailure != "" {
			break
		}
	}
	if reader.Err() != nil {
		kill("output_budget_or_stream_error")
	}
	err = cmd.Wait()
	if ctx.Err() != nil {
		out.Status = "cancelled"
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			out.Status = "timeout"
		}
		return finish(out.Status)
	}
	if streamFailure != "" {
		return finish(streamFailure)
	}
	if err != nil || !completed || answer == "" {
		return finish("codex_process_failed")
	}
	out.Text = answer
	out.Status = "completed"
	return finish("")
}
