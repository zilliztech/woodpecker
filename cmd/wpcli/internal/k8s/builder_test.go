package k8s

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatusCommands(t *testing.T) {
	cmds := StatusCommands("wp-prod")
	require.Len(t, cmds, 3)
	assert.Equal(t, []string{"get", "woodpeckercluster", "wp-prod", "-o", "wide"}, cmds[0])
	assert.Equal(t, []string{"describe", "woodpeckercluster", "wp-prod"}, cmds[1])
	assert.Contains(t, cmds[2], "app.kubernetes.io/instance=wp-prod")
}

func TestScaleCommand(t *testing.T) {
	cmd := ScaleCommand("wp-prod", 5)
	assert.Equal(t, "patch", cmd[0])
	assert.Contains(t, cmd, "woodpeckercluster")
	assert.Contains(t, cmd[len(cmd)-1], `"replicas":5`)
}

func TestLogsCommand_Basic(t *testing.T) {
	cmd := LogsCommand("wp-prod", "wp-prod-server-0", false, 0, "")
	assert.Equal(t, []string{"logs", "wp-prod-server-0"}, cmd)
}

func TestLogsCommand_WithFlags(t *testing.T) {
	cmd := LogsCommand("wp-prod", "wp-prod-server-0", true, 100, "1h")
	assert.Contains(t, cmd, "-f")
	assert.Contains(t, cmd, "--tail")
	assert.Contains(t, cmd, "--since")
}

func TestResolvePodName_AlreadyPodName(t *testing.T) {
	assert.Equal(t, "wp-prod-server-0", ResolvePodName("wp-prod", "wp-prod-server-0"))
}

func TestResolvePodName_Ordinal(t *testing.T) {
	assert.Equal(t, "wp-prod-server-2", ResolvePodName("wp-prod", "2"))
}

func TestResolvePodName_NodeID(t *testing.T) {
	// Non-numeric, non-prefix — returned as-is
	assert.Equal(t, "my-custom-pod", ResolvePodName("wp-prod", "my-custom-pod"))
}
