package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/chzyer/readline"
	"github.com/google/shlex"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

// shellInput keeps terminal editing separate from selection/session behavior. In
// batch mode read exactly one line so confirmations in child commands share stdin.
type shellInput interface {
	Read(string) (string, error)
	Remember(string)
	Close() error
}

type terminalShellInput struct{ *readline.Instance }

func (in terminalShellInput) Read(prompt string) (string, error) {
	in.SetPrompt(prompt)
	return in.Readline()
}

func (in terminalShellInput) Remember(line string) { _ = in.SaveHistory(line) }

type batchShellInput struct{ r io.Reader }

func (in batchShellInput) Read(_ string) (string, error) {
	// A one-byte reader avoids stealing the next command or confirmation from a
	// child process. Do not replace this with a buffered reader over shared stdin.
	var line strings.Builder
	var b [1]byte
	for {
		n, err := in.r.Read(b[:])
		if n > 0 {
			if b[0] == '\n' {
				return strings.TrimSuffix(line.String(), "\r"), nil
			}
			line.WriteByte(b[0])
		}
		if err != nil {
			if errors.Is(err, io.EOF) && line.Len() > 0 {
				return line.String(), nil
			}
			return "", err
		}
	}
}

func (batchShellInput) Remember(string) {}
func (batchShellInput) Close() error    { return nil }

func newShellCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "shell",
		Short: "Open a session with separate service and tenant metadata targets",
		Long: "Open an interactive operational shell. Without --context, choose a context from cli.yaml.\n" +
			"Start with no tenant metadata attached; use connect to attach a tenant.\n" +
			"For piped input, specify --context explicitly. Type help for commands or exit to leave.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			file, path, err := loadFileFromEnvOrDefault()
			if err != nil {
				return shellConfigError(err, path)
			}
			if len(file.Contexts) == 0 {
				return shellConfigError(fmt.Errorf("no contexts configured"), path)
			}
			interactive := false
			if f, ok := cmd.InOrStdin().(*os.File); ok {
				interactive = readline.IsTerminal(int(f.Fd()))
			}
			if !interactive && !cmd.Flags().Changed("context") {
				return wperrors.NewUsageError("non-interactive shell input requires --context <name>; run wp ctx list to see configured contexts")
			}
			s := &shellSession{file: file, configPath: path, in: cmd.InOrStdin(), out: cmd.OutOrStdout(), errOut: cmd.ErrOrStderr()}
			// Capture explicitly supplied preferences before building any command trees:
			// existing constructors bind a per-process Globals singleton.
			s.preferences = shellPreferences(cmd)
			startup := Globals
			var input shellInput = batchShellInput{s.in}
			if interactive {
				rl, err := readline.NewEx(&readline.Config{
					Stdin: readline.NewCancelableStdin(s.in), Stdout: s.errOut, Stderr: s.errOut,
					DisableAutoSaveHistory: true, HistoryLimit: 500,
					AutoComplete: shellCompleter{s: s}, FuncIsTerminal: func() bool { return true },
				})
				if err != nil {
					return err
				}
				input = terminalShellInput{rl}
			}
			defer input.Close()
			name := startup.Context
			if !cmd.Flags().Changed("context") {
				name, err = selectShellContext(file, path, input, s.errOut)
				if errors.Is(err, io.EOF) || errors.Is(err, readline.ErrInterrupt) {
					return nil
				}
				if err != nil {
					return err
				}
			}
			if name == "" {
				return wperrors.NewUsageError("--context must name a configured service context")
			}
			if err := s.use(name); err != nil {
				return err
			}
			// Only explicit startup overrides affect the first selected service.
			// An environment endpoint must not silently retarget a named shell context.
			if cmd.Flags().Changed("endpoint") {
				s.service.Endpoint = startup.Endpoint
			}
			for _, entry := range startup.NodeAdminURLs {
				key, value, ok := strings.Cut(entry, "=")
				if !ok {
					return wperrors.NewUsageError("--node-admin-url requires KEY=URL")
				}
				s.service.NodeAdminURLs[key] = value
			}
			if err := s.validateService(); err != nil {
				return err
			}
			s.printStatus(false, cmd.Context())
			return s.loop(cmd.Context(), input, interactive)
		},
	}
}

func shellConfigError(err error, path string) error {
	if path == "" {
		path = strings.Join(config.DefaultConfigPaths(), ", ")
	}
	return wperrors.NewConfigError(fmt.Sprintf("%v (config: %s). Configure contexts.<name>.endpoint and node_admin_urls for external access; set WOODPECKER_CLI_CONFIG to select another cli.yaml", err, path))
}

func shellPreferences(cmd *cobra.Command) []string {
	var args []string
	cmd.Flags().Visit(func(f *pflag.Flag) {
		switch f.Name {
		case "context", "endpoint", "node-admin-url":
			return
		}
		args = append(args, "--"+f.Name+"="+f.Value.String())
	})
	return args
}

func selectShellContext(file *config.File, path string, input shellInput, out io.Writer) (string, error) {
	names := make([]string, 0, len(file.Contexts))
	for name := range file.Contexts {
		names = append(names, name)
	}
	sort.Strings(names)
	defaultIndex := 0
	fmt.Fprintf(out, "Contexts from %s:\n", path)
	for i, name := range names {
		marker := ""
		if name == file.CurrentContext {
			defaultIndex, marker = i+1, "  [current]"
		}
		fmt.Fprintf(out, "  %d) %s%s\n", i+1, name, marker)
	}
	prompt := "Select context by number or name"
	if defaultIndex > 0 {
		prompt += fmt.Sprintf(" [%d]", defaultIndex)
	}
	for {
		line, err := input.Read(prompt + ": ")
		if err != nil {
			return "", err
		}
		line = strings.TrimSpace(line)
		if line == "" && defaultIndex > 0 {
			return names[defaultIndex-1], nil
		}
		if _, ok := file.Contexts[line]; ok {
			return line, nil
		}
		if n, err := strconv.Atoi(line); err == nil && n > 0 && n <= len(names) {
			return names[n-1], nil
		}
		fmt.Fprintf(out, "Choose a displayed number or an exact context name: %s\n", strings.Join(names, ", "))
	}
}

func (s *shellSession) loop(ctx context.Context, input shellInput, interactive bool) error {
	var batchError error
	for {
		line, err := input.Read(fmt.Sprintf("wp[context=%s | meta=%s]> ", s.contextName, s.tenantName()))
		if errors.Is(err, io.EOF) {
			return batchError
		}
		if errors.Is(err, readline.ErrInterrupt) {
			input.Remember("")
			continue
		}
		if err != nil {
			return err
		}
		args, err := shlex.Split(line)
		// Reset readline's editable history draft even for rejected/secret lines;
		// omitting SaveHistory alone leaves a draft reachable through history keys.
		if err == nil && shellHistorySafe(line, args) {
			input.Remember(line)
		} else {
			input.Remember("")
		}
		if err == nil && len(args) == 0 {
			continue
		}
		if err == nil {
			var done bool
			done, err = s.execute(ctx, args)
			if done {
				return batchError
			}
		}
		if err != nil {
			fmt.Fprintln(s.errOut, "wp shell:", err)
			if !interactive && batchError == nil {
				batchError = err
			}
		}
	}
}

func shellHistorySafe(line string, args []string) bool {
	if len(args) == 0 || args[0] == "connect" {
		return false
	}
	line = strings.ToLower(line)
	for _, sensitive := range []string{"password", "secret", "token", "credential", "://"} {
		if strings.Contains(line, sensitive) {
			return false
		}
	}
	return true
}
