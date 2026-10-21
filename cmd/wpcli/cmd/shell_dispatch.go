package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/google/shlex"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"

	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

const shellChildEnv = "WOODPECKER_CLI_SHELL_CHILD"

// shellRootDefaults applies context transport defaults only in isolated shell
// children. Regular wp invocations retain their existing defaults and precedence.
// Supplying defaults here also avoids injecting --timeout into commands whose
// local flag shadows the global request timeout (for example node decommission).
func shellRootDefaults() GlobalFlags {
	d := GlobalFlags{AdminPort: 9091, Timeout: 30 * time.Second, Concurrency: 8, Output: "table"}
	if os.Getenv(shellChildEnv) != "1" {
		return d
	}
	f, err := config.Load(os.Getenv("WOODPECKER_CLI_CONFIG"))
	if err != nil {
		return d
	}
	c, err := f.ResolveContext("")
	if err != nil {
		return d
	}
	d.AdminPort, d.Timeout, d.Concurrency, d.Strict = c.AdminPort, c.Timeout, c.Concurrency, c.Strict
	if f.Defaults.Output != "" {
		d.Output = f.Defaults.Output
	}
	d.NoColor = f.Defaults.NoColor
	return d
}

func (s *shellSession) commandRoot() *cobra.Command {
	root := NewRootCommand()
	root.SetIn(s.in)
	root.SetOut(s.out)
	root.SetErr(s.errOut)
	root.SilenceErrors = true
	s.addCommands(root)
	return root
}

func (s *shellSession) execute(parent context.Context, args []string) (bool, error) {
	ctx, stop := signal.NotifyContext(parent, os.Interrupt)
	defer stop()
	root := s.commandRoot()
	leaf, rest, err := root.Find(args)
	if err != nil {
		return false, err
	}
	leaf.InitDefaultHelpFlag()
	if err := leaf.ParseFlags(rest); err != nil {
		return false, err
	}
	help, _ := leaf.Flags().GetBool("help")
	if leaf.Name() == "shell" {
		return false, wperrors.NewUsageError("nested shells are not supported")
	}
	if !help && leaf.Name() != "help" {
		if err := leaf.ValidateArgs(leaf.Flags().Args()); err != nil {
			return false, err
		}
	}
	if leaf.Flags().Changed("context") && Globals.Context != s.contextName {
		return false, wperrors.NewUsageError("use 'use <context>' to switch the session's service context")
	}
	if leaf.Parent() == root {
		switch leaf.Name() {
		case "exit":
			if !help {
				return true, nil
			}
		case "use", "connect", "disconnect", "status":
			// The preflight tree has already parsed the arguments. Execute a fresh
			// tree so repeated array flags cannot be appended twice.
			root = s.commandRoot()
			root.SetArgs(append(append([]string{}, s.preferences...), args...))
			return false, root.ExecuteContext(ctx)
		}
	}
	if !help && leaf.Name() != "help" && leaf.Runnable() {
		if err := s.applyPrerequisites(leaf, &args); err != nil {
			return false, err
		}
	}
	if leaf.Name() == "help" || leaf == root || help && leaf.Name() == "exit" {
		root.SetArgs(args)
		return false, root.ExecuteContext(ctx)
	}
	// The existing CLI runs in a separate process. Every flag, client, context,
	// timer and mutation option starts fresh; Ctrl+C terminates the active CLI
	// without abandoning the shell or its selected configuration.
	return false, s.dispatch(ctx, leaf, args)
}

func (s *shellSession) applyPrerequisites(cmd *cobra.Command, args *[]string) error {
	metadata := cmd.Flags().Lookup("etcd") != nil
	if metadata && s.tenant == nil {
		return wperrors.NewPrerequisiteError("no tenant metadata connection configured. Connect with: connect <tenant> --etcd <host:port> --meta-prefix <prefix>. Physical inspection also needs --bucket <bucket> --root-path <root-path>. Run status to view the session")
	}
	if metadata {
		for _, name := range []string{"etcd", "meta-prefix"} {
			value, _ := cmd.Flags().GetString(name)
			if cmd.Flags().Changed(name) && strings.TrimSpace(value) == "" {
				return wperrors.NewUsageError("--" + name + " must not be empty; shell commands never discover a tenant implicitly")
			}
		}
	}
	rootFlag := "root-path"
	if cmd.Flags().Lookup(rootFlag) == nil {
		rootFlag = "root"
	}
	if cmd.Flags().Lookup("bucket") == nil || cmd.Flags().Lookup(rootFlag) == nil {
		return nil
	}
	bucket, _ := cmd.Flags().GetString("bucket")
	root, _ := cmd.Flags().GetString(rootFlag)
	if s.tenant != nil {
		if !cmd.Flags().Changed("bucket") {
			bucket = s.tenant.bucket
		}
		if !cmd.Flags().Changed(rootFlag) {
			root = s.tenant.rootPath
		}
	}
	if (bucket == "") != (root == "") || metadata && (bucket == "" || root == "") {
		return wperrors.NewPrerequisiteError("physical inspection requires --bucket <bucket> and --" + rootFlag + " <root-path>; provide them on this command or reconnect with --bucket and --root-path")
	}
	// Prefix the defaults so -- remains an argument boundary and explicit flags
	// retain their ordinary CLI meaning.
	var defaults []string
	if bucket != "" && !cmd.Flags().Changed("bucket") {
		defaults = append(defaults, "--bucket="+bucket)
	}
	if root != "" && !cmd.Flags().Changed(rootFlag) {
		defaults = append(defaults, "--"+rootFlag+"="+root)
	}
	*args = append(defaults, (*args)...)
	return nil
}

func (s *shellSession) dispatch(ctx context.Context, leaf *cobra.Command, args []string) error {
	// Store only the current session's selected target in a private transient
	// cli.yaml. Never copy the service context's tenant settings into this file.
	dir, err := os.MkdirTemp("", "wp-shell-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	c := s.service
	if s.tenant != nil {
		c.Etcd = s.tenant.etcd
	}
	f := config.File{CurrentContext: s.contextName, Contexts: map[string]config.Context{s.contextName: c}, Defaults: s.file.Defaults}
	data, err := yaml.Marshal(f)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, "cli.yaml")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return err
	}
	// ctx is the existing persistent configuration interface. Keep its original
	// destination distinct from the session-local use command.
	for c := leaf; c != nil; c = c.Parent() {
		if c.Name() == "ctx" {
			path = s.configPath
			break
		}
	}
	childArgs := append([]string{"--context=" + s.contextName}, s.preferences...)
	childArgs = append(childArgs, args...)
	if s.runChild != nil {
		return s.runChild(ctx, childArgs, path)
	}
	return s.runProcess(ctx, childArgs, path)
}

func shellChildEnvironment(path string) []string {
	var env []string
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.EqualFold(name, "WOODPECKER_ENDPOINT") || strings.EqualFold(name, "WOODPECKER_CLI_CONFIG") || strings.EqualFold(name, shellChildEnv) {
			continue
		}
		env = append(env, entry)
	}
	return append(env, "WOODPECKER_CLI_CONFIG="+path, shellChildEnv+"=1")
}

func (s *shellSession) runProcess(ctx context.Context, args []string, path string) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	// #nosec G204 -- Execute this wp binary with parsed CLI arguments, never an OS shell.
	child := exec.CommandContext(ctx, executable, args...)
	child.Env = shellChildEnvironment(path)
	child.Stdin, child.Stdout, child.Stderr = s.in, s.out, s.errOut
	return runShellProcess(ctx, child)
}

func runShellProcess(ctx context.Context, child *exec.Cmd) error {
	child.WaitDelay = 2 * time.Second
	err := child.Run()
	if ctx.Err() != nil {
		return fmt.Errorf("operation canceled; session retained: %w", ctx.Err())
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return fmt.Errorf("command exited with code %d", exit.ExitCode())
	}
	return err
}

type shellCompleter struct{ s *shellSession }

func (c shellCompleter) Do(line []rune, pos int) ([][]rune, int) {
	if pos < 0 || pos > len(line) {
		return nil, 0
	}
	text := string(line[:pos])
	start := strings.LastIndexFunc(text, unicode.IsSpace) + 1
	prefix := text[start:]
	previous, err := shlex.Split(text[:start])
	if err != nil {
		return nil, 0
	}
	root := c.s.commandRoot()
	leaf, _, err := root.Find(previous)
	if err != nil {
		return nil, 0
	}
	var candidates []string
	if len(previous) == 1 && previous[0] == "use" {
		for name := range c.s.file.Contexts {
			candidates = append(candidates, name)
		}
	} else if strings.HasPrefix(prefix, "-") {
		leaf.Flags().AddFlagSet(leaf.InheritedFlags())
		leaf.Flags().VisitAll(func(f *pflag.Flag) {
			if !f.Hidden {
				candidates = append(candidates, "--"+f.Name)
			}
		})
		candidates = append(candidates, "--help")
	} else {
		for _, child := range leaf.Commands() {
			if !child.Hidden {
				candidates = append(candidates, child.Name())
			}
		}
		if leaf == root {
			candidates = append(candidates, "help")
		}
	}
	sort.Strings(candidates)
	var matches [][]rune
	last := ""
	for _, name := range candidates {
		if name != last && strings.HasPrefix(name, prefix) {
			matches = append(matches, []rune(strings.TrimPrefix(name, prefix)+" "))
		}
		last = name
	}
	return matches, len([]rune(prefix))
}
