// Package k8s provides a thin wrapper around kubectl for the F family commands.
// Default mode prints commands without executing; -x mode runs them.
package k8s

import (
	"fmt"
	"io"
	"os/exec"
	"strings"
)

// Opts configures kubectl execution.
type Opts struct {
	Kubectl     string // path to kubectl binary (default: "kubectl")
	Kubeconfig  string // --kubeconfig passthrough
	KubeContext string // --context passthrough (kubectl's --context)
	Namespace   string // -n passthrough
}

// Executor wraps kubectl command construction and execution.
type Executor struct {
	opts Opts
}

// NewExecutor creates a new Executor. Resolves kubectl path if not provided.
func NewExecutor(opts Opts) *Executor {
	if opts.Kubectl == "" {
		opts.Kubectl = "kubectl"
	}
	return &Executor{opts: opts}
}

// Available returns true if kubectl is found in PATH or at the configured path.
func (e *Executor) Available() bool {
	_, err := exec.LookPath(e.opts.Kubectl)
	return err == nil
}

// globalArgs returns the common kubectl flags.
func (e *Executor) globalArgs() []string {
	var args []string
	if e.opts.Kubeconfig != "" {
		args = append(args, "--kubeconfig", e.opts.Kubeconfig)
	}
	if e.opts.KubeContext != "" {
		args = append(args, "--context", e.opts.KubeContext)
	}
	if e.opts.Namespace != "" {
		args = append(args, "-n", e.opts.Namespace)
	}
	return args
}

// BuildCommand constructs a full kubectl command line string (for printing).
func (e *Executor) BuildCommand(cmdArgs ...string) string {
	all := append([]string{e.opts.Kubectl}, e.globalArgs()...)
	all = append(all, cmdArgs...)
	return strings.Join(all, " ")
}

// Run executes a kubectl command, streaming stdout/stderr to the given writers.
// Returns the exit code.
func (e *Executor) Run(stdout, stderr io.Writer, cmdArgs ...string) (int, error) {
	all := append(e.globalArgs(), cmdArgs...)
	cmd := exec.Command(e.opts.Kubectl, all...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	err := cmd.Run()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode(), nil
		}
		return 1, fmt.Errorf("kubectl: %w", err)
	}
	return 0, nil
}

// PrintCommands writes kubectl commands to w without executing.
func (e *Executor) PrintCommands(w io.Writer, commands [][]string) {
	fmt.Fprintln(w, "# The following kubectl commands would be executed:")
	fmt.Fprintln(w)
	for _, args := range commands {
		fmt.Fprintln(w, e.BuildCommand(args...))
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "# Tip: add -x to execute these commands directly")
}

// RunCommands executes multiple kubectl commands sequentially.
// Returns the first non-zero exit code, or 0 if all succeed.
func (e *Executor) RunCommands(stdout, stderr io.Writer, commands [][]string) (int, error) {
	for _, args := range commands {
		fmt.Fprintf(stdout, "$ %s\n", e.BuildCommand(args...))
		code, err := e.Run(stdout, stderr, args...)
		if err != nil {
			return code, err
		}
		if code != 0 {
			return code, nil
		}
		fmt.Fprintln(stdout)
	}
	return 0, nil
}
