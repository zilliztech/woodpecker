package cmd

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runK8sCmd(t *testing.T, args ...string) (string, error) {
	t.Helper()
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs(args)
	err := root.Execute()
	return buf.String(), err
}

func TestK8sStatus_PrintMode(t *testing.T) {
	out, err := runK8sCmd(t, "k8s", "status", "--wp-cluster", "wp-prod", "-n", "woodpecker")
	require.NoError(t, err)
	assert.Contains(t, out, "kubectl")
	assert.Contains(t, out, "woodpeckercluster")
	assert.Contains(t, out, "wp-prod")
	assert.Contains(t, out, "add -x")
}

func TestK8sScale_PrintMode(t *testing.T) {
	out, err := runK8sCmd(t, "k8s", "scale", "--replicas", "5", "--wp-cluster", "wp-prod")
	require.NoError(t, err)
	assert.Contains(t, out, "patch")
	assert.Contains(t, out, `"replicas":5`)
}

func TestK8sScale_MissingReplicas(t *testing.T) {
	_, err := runK8sCmd(t, "k8s", "scale")
	require.Error(t, err)
}

func TestK8sLogs_PrintMode(t *testing.T) {
	out, err := runK8sCmd(t, "k8s", "logs", "0", "--wp-cluster", "wp-prod", "-n", "woodpecker")
	require.NoError(t, err)
	assert.Contains(t, out, "kubectl")
	assert.Contains(t, out, "logs")
	assert.Contains(t, out, "wp-prod-server-0")
}

func TestK8sLogs_WithFollow(t *testing.T) {
	out, err := runK8sCmd(t, "k8s", "logs", "my-pod", "--wp-cluster", "wp", "-f", "--tail", "50", "--since", "1h")
	require.NoError(t, err)
	assert.Contains(t, out, "-f")
	assert.Contains(t, out, "--tail 50")
	assert.Contains(t, out, "--since 1h")
}

func TestK8sDoctor_NotImplemented(t *testing.T) {
	_, err := runK8sCmd(t, "k8s", "doctor")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not implemented")
}

func TestK8sStatus_ExecuteMode_NoKubectl(t *testing.T) {
	// With a nonexistent kubectl path, execute mode should fall back to print
	out, err := runK8sCmd(t, "k8s", "status", "--wp-cluster", "wp-test", "--kubectl", "/nonexistent/kubectl-xyz", "-x")
	require.NoError(t, err)
	assert.Contains(t, out, "kubectl not found")
	assert.Contains(t, out, "woodpeckercluster")
}
