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

// StopEtcd stops the etcd container gracefully.
func (cc *ChaosCluster) StopEtcd(t *testing.T) {
	t.Helper()
	t.Logf("Stopping etcd...")
	framework.RunCommandNoFail(t, "docker", "stop", "-t", "10", "etcd")
	t.Logf("etcd stopped")
}

// StartEtcd starts the etcd container.
func (cc *ChaosCluster) StartEtcd(t *testing.T) {
	t.Helper()
	t.Logf("Starting etcd...")
	framework.RunCommandNoFail(t, "docker", "start", "etcd")
	t.Logf("etcd started")
}

// KillEtcd sends SIGKILL to the etcd container.
func (cc *ChaosCluster) KillEtcd(t *testing.T) {
	t.Helper()
	t.Logf("Killing etcd...")
	framework.RunCommandNoFail(t, "docker", "kill", "etcd")
	t.Logf("etcd killed")
}

// StopMinIO stops the MinIO container gracefully.
func (cc *ChaosCluster) StopMinIO(t *testing.T) {
	t.Helper()
	t.Logf("Stopping MinIO...")
	framework.RunCommandNoFail(t, "docker", "stop", "-t", "10", "minio")
	t.Logf("MinIO stopped")
}

// StartMinIO starts the MinIO container.
func (cc *ChaosCluster) StartMinIO(t *testing.T) {
	t.Helper()
	t.Logf("Starting MinIO...")
	framework.RunCommandNoFail(t, "docker", "start", "minio")
	t.Logf("MinIO started")
}

// KillMinIO sends SIGKILL to the MinIO container.
func (cc *ChaosCluster) KillMinIO(t *testing.T) {
	t.Helper()
	t.Logf("Killing MinIO...")
	framework.RunCommandNoFail(t, "docker", "kill", "minio")
	t.Logf("MinIO killed")
}

// DisconnectEtcd disconnects etcd from the Docker network.
func (cc *ChaosCluster) DisconnectEtcd(t *testing.T) {
	t.Helper()
	t.Logf("Disconnecting etcd from network %s...", cc.NetworkName)
	framework.RunCommandNoFail(t, "docker", "network", "disconnect", "-f", cc.NetworkName, "etcd")
	t.Logf("etcd disconnected from network")
}

// ConnectEtcd reconnects etcd to the Docker network.
func (cc *ChaosCluster) ConnectEtcd(t *testing.T) {
	t.Helper()
	t.Logf("Connecting etcd to network %s...", cc.NetworkName)
	framework.RunCommandNoFail(t, "docker", "network", "connect", cc.NetworkName, "etcd")
	t.Logf("etcd connected to network")
}

// DisconnectMinIO disconnects MinIO from the Docker network.
func (cc *ChaosCluster) DisconnectMinIO(t *testing.T) {
	t.Helper()
	t.Logf("Disconnecting MinIO from network %s...", cc.NetworkName)
	framework.RunCommandNoFail(t, "docker", "network", "disconnect", "-f", cc.NetworkName, "minio")
	t.Logf("MinIO disconnected from network")
}

// ConnectMinIO reconnects MinIO to the Docker network.
func (cc *ChaosCluster) ConnectMinIO(t *testing.T) {
	t.Helper()
	t.Logf("Connecting MinIO to network %s...", cc.NetworkName)
	framework.RunCommandNoFail(t, "docker", "network", "connect", cc.NetworkName, "minio")
	t.Logf("MinIO connected to network")
}

// TryConnectEtcd reconnects etcd, ignoring errors. Useful in t.Cleanup.
func (cc *ChaosCluster) TryConnectEtcd(t *testing.T) {
	t.Helper()
	_, _, err := framework.RunCommand(t, "docker", "network", "connect", cc.NetworkName, "etcd")
	if err != nil {
		t.Logf("TryConnectEtcd: ignored error (likely already connected): %v", err)
	}
}

// TryConnectMinIO reconnects MinIO, ignoring errors. Useful in t.Cleanup.
func (cc *ChaosCluster) TryConnectMinIO(t *testing.T) {
	t.Helper()
	_, _, err := framework.RunCommand(t, "docker", "network", "connect", cc.NetworkName, "minio")
	if err != nil {
		t.Logf("TryConnectMinIO: ignored error (likely already connected): %v", err)
	}
}

// TryStartEtcd starts etcd, ignoring errors. Useful in t.Cleanup.
func (cc *ChaosCluster) TryStartEtcd(t *testing.T) {
	t.Helper()
	_, _, err := framework.RunCommand(t, "docker", "start", "etcd")
	if err != nil {
		t.Logf("TryStartEtcd: ignored error: %v", err)
	}
}

// TryStartMinIO starts MinIO, ignoring errors. Useful in t.Cleanup.
func (cc *ChaosCluster) TryStartMinIO(t *testing.T) {
	t.Helper()
	_, _, err := framework.RunCommand(t, "docker", "start", "minio")
	if err != nil {
		t.Logf("TryStartMinIO: ignored error: %v", err)
	}
}
