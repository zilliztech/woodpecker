package chaos

import (
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/zilliztech/woodpecker/tests/docker/framework"
)

// ChaosCluster manages a Docker Compose-based Woodpecker cluster for chaos testing.
// It embeds the shared framework.DockerCluster and adds chaos-specific operations.
type ChaosCluster struct {
	*framework.DockerCluster
}

// chaosDir returns the absolute path to the chaos suite directory,
// derived from this source file's location so it works regardless of CWD.
func chaosDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Dir(thisFile)
}

// NewChaosCluster creates a ChaosCluster with chaos test configuration.
func NewChaosCluster(t *testing.T) *ChaosCluster {
	t.Helper()
	base := framework.NewDockerCluster(t, framework.ClusterConfig{
		TestDir:      chaosDir(),
		ProjectName:  "woodpecker-chaos",
		OverrideFile: "docker-compose.chaos.yaml",
		NetworkName:  "woodpecker-chaos_woodpecker",
		GossipWait:   10 * time.Second,
	})
	return &ChaosCluster{DockerCluster: base}
}

// KillNode sends SIGKILL to a container (simulates crash). Container stays dead
// because docker-compose.chaos.yaml sets restart: "no".
func (cc *ChaosCluster) KillNode(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Killing container %s...", containerName)
	framework.RunCommandNoFail(t, "docker", "kill", containerName)
	t.Logf("Container %s killed", containerName)
}

// PauseNode freezes all processes in a container (simulates process hang).
func (cc *ChaosCluster) PauseNode(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Pausing container %s...", containerName)
	framework.RunCommandNoFail(t, "docker", "pause", containerName)
	t.Logf("Container %s paused", containerName)
}

// UnpauseNode resumes a paused container.
func (cc *ChaosCluster) UnpauseNode(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Unpausing container %s...", containerName)
	framework.RunCommandNoFail(t, "docker", "unpause", containerName)
	t.Logf("Container %s unpaused", containerName)
}

// DisconnectNetwork disconnects a container from the Docker network,
// simulating a network partition.
func (cc *ChaosCluster) DisconnectNetwork(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Disconnecting %s from network %s...", containerName, cc.NetworkName)
	framework.RunCommandNoFail(t, "docker", "network", "disconnect", "-f", cc.NetworkName, containerName)
	t.Logf("Container %s disconnected from network", containerName)
}

// ConnectNetwork reconnects a container to the Docker network.
func (cc *ChaosCluster) ConnectNetwork(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Connecting %s to network %s...", containerName, cc.NetworkName)
	framework.RunCommandNoFail(t, "docker", "network", "connect", cc.NetworkName, containerName)
	t.Logf("Container %s connected to network", containerName)
}

// TryUnpauseNode resumes a paused container, ignoring errors
// (e.g., if the container is not paused). Useful in t.Cleanup handlers.
func (cc *ChaosCluster) TryUnpauseNode(t *testing.T, containerName string) {
	t.Helper()
	_, _, err := framework.RunCommand(t, "docker", "unpause", containerName)
	if err != nil {
		t.Logf("TryUnpauseNode %s: ignored error (likely not paused): %v", containerName, err)
	} else {
		t.Logf("Container %s unpaused", containerName)
	}
}

// TryConnectNetwork reconnects a container to the Docker network, ignoring errors
// (e.g., if the container is already connected). Useful in t.Cleanup handlers.
func (cc *ChaosCluster) TryConnectNetwork(t *testing.T, containerName string) {
	t.Helper()
	_, _, err := framework.RunCommand(t, "docker", "network", "connect", cc.NetworkName, containerName)
	if err != nil {
		t.Logf("TryConnectNetwork %s: ignored error (likely already connected): %v", containerName, err)
	} else {
		t.Logf("Container %s reconnected to network", containerName)
	}
}
