package k8s

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutor_BuildCommand(t *testing.T) {
	e := NewExecutor(Opts{
		Kubectl:     "kubectl",
		Namespace:   "woodpecker",
		KubeContext: "prod",
	})
	cmd := e.BuildCommand("get", "pods")
	assert.Equal(t, "kubectl --context prod -n woodpecker get pods", cmd)
}

func TestExecutor_BuildCommand_WithKubeconfig(t *testing.T) {
	e := NewExecutor(Opts{
		Kubectl:    "/usr/local/bin/kubectl",
		Kubeconfig: "/etc/kube/config",
		Namespace:  "wp",
	})
	cmd := e.BuildCommand("get", "nodes")
	assert.Contains(t, cmd, "--kubeconfig /etc/kube/config")
	assert.Contains(t, cmd, "-n wp")
}

func TestExecutor_BuildCommand_Minimal(t *testing.T) {
	e := NewExecutor(Opts{})
	cmd := e.BuildCommand("version", "--client")
	assert.Equal(t, "kubectl version --client", cmd)
}

func TestExecutor_PrintCommands(t *testing.T) {
	e := NewExecutor(Opts{Namespace: "wp"})
	buf := new(bytes.Buffer)
	e.PrintCommands(buf, [][]string{
		{"get", "pods"},
		{"get", "svc"},
	})
	out := buf.String()
	assert.Contains(t, out, "kubectl -n wp get pods")
	assert.Contains(t, out, "kubectl -n wp get svc")
	assert.Contains(t, out, "add -x")
}

func TestExecutor_Available_True(t *testing.T) {
	// "echo" should exist on all platforms
	e := NewExecutor(Opts{Kubectl: "echo"})
	assert.True(t, e.Available())
}

func TestExecutor_Available_False(t *testing.T) {
	e := NewExecutor(Opts{Kubectl: "/nonexistent/kubectl-xyz"})
	assert.False(t, e.Available())
}

func TestExecutor_Run_Echo(t *testing.T) {
	e := NewExecutor(Opts{Kubectl: "echo"})
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	code, err := e.Run(stdout, stderr, "hello", "world")
	require.NoError(t, err)
	assert.Equal(t, 0, code)
	assert.Contains(t, stdout.String(), "hello world")
}
