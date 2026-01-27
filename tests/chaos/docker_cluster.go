package chaos

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/woodpecker"
)

const (
	// DefaultProjectName is the docker compose project name for chaos tests.
	DefaultProjectName = "woodpecker-chaos"

	// DefaultComposeFile is the main docker-compose file.
	DefaultComposeFile = "docker-compose.yaml"

	// DefaultOverrideFile is the chaos testing override file.
	DefaultOverrideFile = "docker-compose.chaos.yaml"

	// DefaultNetworkName is the docker network used by the cluster.
	DefaultNetworkName = "woodpecker-chaos_woodpecker"

	// DefaultImageName is the expected docker image name.
	DefaultImageName = "woodpecker:latest"

	// DefaultConfigPath is the relative path from tests/chaos to the config file.
	DefaultConfigPath = "../../config/woodpecker.yaml"
)

// NodeInfo describes a Woodpecker node in the Docker Compose cluster.
type NodeInfo struct {
	ContainerName string
	ServicePort   int
	GossipPort    int
}

// DefaultNodes returns the standard 4-node cluster configuration.
func DefaultNodes() []NodeInfo {
	return []NodeInfo{
		{ContainerName: "woodpecker-node1", ServicePort: 18080, GossipPort: 17946},
		{ContainerName: "woodpecker-node2", ServicePort: 18081, GossipPort: 17947},
		{ContainerName: "woodpecker-node3", ServicePort: 18082, GossipPort: 17948},
		{ContainerName: "woodpecker-node4", ServicePort: 18083, GossipPort: 17949},
	}
}

// DockerCluster manages a Docker Compose-based Woodpecker cluster for chaos testing.
type DockerCluster struct {
	ComposeDir   string     // Absolute path to the deployments/ directory
	ComposeFile  string     // Main compose file name
	OverrideFile string     // Override file (sets restart: no)
	OverridePath string     // Absolute path to override file
	ProjectName  string     // Docker Compose project name
	Nodes        []NodeInfo // Cluster nodes
	NetworkName  string     // Docker network name
}

// NewDockerCluster creates a DockerCluster with default configuration.
// It resolves paths relative to the chaos test directory.
func NewDockerCluster(t *testing.T) *DockerCluster {
	t.Helper()

	// Resolve the chaos test directory (where this code lives)
	chaosDir, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("failed to resolve chaos test directory: %v", err)
	}

	// The deployments/ directory is two levels up from tests/chaos/
	deploymentsDir := filepath.Join(chaosDir, "..", "..", "deployments")
	deploymentsDir, err = filepath.Abs(deploymentsDir)
	if err != nil {
		t.Fatalf("failed to resolve deployments directory: %v", err)
	}

	overridePath := filepath.Join(chaosDir, DefaultOverrideFile)

	return &DockerCluster{
		ComposeDir:   deploymentsDir,
		ComposeFile:  DefaultComposeFile,
		OverrideFile: DefaultOverrideFile,
		OverridePath: overridePath,
		ProjectName:  DefaultProjectName,
		Nodes:        DefaultNodes(),
		NetworkName:  DefaultNetworkName,
	}
}

// composeArgs returns the base docker compose command arguments.
func (dc *DockerCluster) composeArgs() []string {
	return []string{
		"compose",
		"-f", filepath.Join(dc.ComposeDir, dc.ComposeFile),
		"-f", dc.OverridePath,
		"-p", dc.ProjectName,
	}
}

// runCommand executes a command and returns stdout, stderr, and error.
func runCommand(t *testing.T, name string, args ...string) (string, string, error) {
	t.Helper()
	cmd := exec.Command(name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// runCommandNoFail executes a command and fails the test on error.
func runCommandNoFail(t *testing.T, name string, args ...string) string {
	t.Helper()
	stdout, stderr, err := runCommand(t, name, args...)
	if err != nil {
		t.Fatalf("command %s %v failed: %v\nstdout: %s\nstderr: %s", name, args, err, stdout, stderr)
	}
	return stdout
}

// BuildImageIfNeeded builds the Docker image if it does not already exist.
func (dc *DockerCluster) BuildImageIfNeeded(t *testing.T) {
	t.Helper()
	t.Log("Checking if Docker image exists...")

	stdout, _, _ := runCommand(t, "docker", "images", "-q", DefaultImageName)
	if strings.TrimSpace(stdout) != "" {
		t.Logf("Docker image %s already exists, skipping build", DefaultImageName)
		return
	}

	t.Logf("Building Docker image %s...", DefaultImageName)
	projectRoot := filepath.Join(dc.ComposeDir, "..")
	buildScript := filepath.Join(projectRoot, "build", "build_image.sh")
	cmd := exec.Command(buildScript, "ubuntu22.04", "auto", "-t", DefaultImageName)
	cmd.Dir = projectRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to build Docker image: %v", err)
	}
	t.Log("Docker image built successfully")
}

// Up starts the entire cluster using docker compose.
func (dc *DockerCluster) Up(t *testing.T) {
	t.Helper()
	t.Log("Starting chaos test cluster...")

	args := append(dc.composeArgs(), "up", "-d", "--wait")
	runCommandNoFail(t, "docker", args...)

	t.Log("Cluster started")
}

// Down stops the cluster (containers removed, volumes preserved).
func (dc *DockerCluster) Down(t *testing.T) {
	t.Helper()
	t.Log("Stopping chaos test cluster...")

	args := append(dc.composeArgs(), "down")
	_, _, _ = runCommand(t, "docker", args...)

	t.Log("Cluster stopped")
}

// Clean stops the cluster and removes all volumes.
func (dc *DockerCluster) Clean(t *testing.T) {
	t.Helper()
	t.Log("Cleaning chaos test cluster...")

	args := append(dc.composeArgs(), "down", "-v", "--remove-orphans")
	_, _, _ = runCommand(t, "docker", args...)

	t.Log("Cluster cleaned")
}

// KillNode sends SIGKILL to a container (simulates crash). Container stays dead
// because docker-compose.chaos.yaml sets restart: "no".
func (dc *DockerCluster) KillNode(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Killing container %s...", containerName)
	runCommandNoFail(t, "docker", "kill", containerName)
	t.Logf("Container %s killed", containerName)
}

// StopNode gracefully stops a container.
func (dc *DockerCluster) StopNode(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Stopping container %s...", containerName)
	runCommandNoFail(t, "docker", "stop", "-t", "10", containerName)
	t.Logf("Container %s stopped", containerName)
}

// StartNode starts a previously stopped/killed container.
func (dc *DockerCluster) StartNode(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Starting container %s...", containerName)
	runCommandNoFail(t, "docker", "start", containerName)
	t.Logf("Container %s started", containerName)
}

// PauseNode freezes all processes in a container (simulates process hang).
func (dc *DockerCluster) PauseNode(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Pausing container %s...", containerName)
	runCommandNoFail(t, "docker", "pause", containerName)
	t.Logf("Container %s paused", containerName)
}

// UnpauseNode resumes a paused container.
func (dc *DockerCluster) UnpauseNode(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Unpausing container %s...", containerName)
	runCommandNoFail(t, "docker", "unpause", containerName)
	t.Logf("Container %s unpaused", containerName)
}

// DisconnectNetwork disconnects a container from the Docker network,
// simulating a network partition.
func (dc *DockerCluster) DisconnectNetwork(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Disconnecting %s from network %s...", containerName, dc.NetworkName)
	runCommandNoFail(t, "docker", "network", "disconnect", "-f", dc.NetworkName, containerName)
	t.Logf("Container %s disconnected from network", containerName)
}

// ConnectNetwork reconnects a container to the Docker network.
func (dc *DockerCluster) ConnectNetwork(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Connecting %s to network %s...", containerName, dc.NetworkName)
	runCommandNoFail(t, "docker", "network", "connect", dc.NetworkName, containerName)
	t.Logf("Container %s connected to network", containerName)
}

// TryUnpauseNode resumes a paused container, ignoring errors
// (e.g., if the container is not paused). Useful in t.Cleanup handlers.
func (dc *DockerCluster) TryUnpauseNode(t *testing.T, containerName string) {
	t.Helper()
	_, _, err := runCommand(t, "docker", "unpause", containerName)
	if err != nil {
		t.Logf("TryUnpauseNode %s: ignored error (likely not paused): %v", containerName, err)
	} else {
		t.Logf("Container %s unpaused", containerName)
	}
}

// TryConnectNetwork reconnects a container to the Docker network, ignoring errors
// (e.g., if the container is already connected). Useful in t.Cleanup handlers.
func (dc *DockerCluster) TryConnectNetwork(t *testing.T, containerName string) {
	t.Helper()
	_, _, err := runCommand(t, "docker", "network", "connect", dc.NetworkName, containerName)
	if err != nil {
		t.Logf("TryConnectNetwork %s: ignored error (likely already connected): %v", containerName, err)
	} else {
		t.Logf("Container %s reconnected to network", containerName)
	}
}

// IsRunning checks if a container is in "running" state.
func (dc *DockerCluster) IsRunning(containerName string) bool {
	stdout, _, err := runCommandDirect("docker", "inspect", "-f", "{{.State.Running}}", containerName)
	if err != nil {
		return false
	}
	return strings.TrimSpace(stdout) == "true"
}

// runCommandDirect executes a command without requiring a testing.T (for use in non-test contexts).
func runCommandDirect(name string, args ...string) (string, string, error) {
	cmd := exec.Command(name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// WaitForHealthy polls until a container is running, with a timeout.
func (dc *DockerCluster) WaitForHealthy(t *testing.T, containerName string, timeout time.Duration) {
	t.Helper()
	t.Logf("Waiting for %s to be healthy (timeout: %v)...", containerName, timeout)

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if dc.IsRunning(containerName) {
			t.Logf("Container %s is running", containerName)
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("container %s did not become healthy within %v", containerName, timeout)
}

// WaitForClusterReady waits for all nodes, etcd, and minio to be running.
func (dc *DockerCluster) WaitForClusterReady(t *testing.T, timeout time.Duration) {
	t.Helper()
	t.Log("Waiting for cluster to be ready...")

	containers := []string{"etcd", "minio"}
	for _, n := range dc.Nodes {
		containers = append(containers, n.ContainerName)
	}

	deadline := time.Now().Add(timeout)
	for _, c := range containers {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timeout waiting for cluster readiness")
		}
		dc.WaitForHealthy(t, c, remaining)
	}

	// Additional grace period for gossip convergence
	t.Log("Cluster containers are running, waiting for gossip convergence...")
	time.Sleep(10 * time.Second)
	t.Log("Cluster is ready")
}

// StopAllWoodpeckerNodes stops all Woodpecker nodes (not etcd/minio).
func (dc *DockerCluster) StopAllWoodpeckerNodes(t *testing.T) {
	t.Helper()
	t.Log("Stopping all Woodpecker nodes...")
	for _, n := range dc.Nodes {
		dc.StopNode(t, n.ContainerName)
	}
	t.Log("All Woodpecker nodes stopped")
}

// StartAllWoodpeckerNodes starts all Woodpecker nodes.
func (dc *DockerCluster) StartAllWoodpeckerNodes(t *testing.T) {
	t.Helper()
	t.Log("Starting all Woodpecker nodes...")
	for _, n := range dc.Nodes {
		dc.StartNode(t, n.ContainerName)
	}
	t.Log("All Woodpecker nodes started")
}

// GetLogs returns the last N lines of logs from a container.
func (dc *DockerCluster) GetLogs(t *testing.T, containerName string, tail int) string {
	t.Helper()
	stdout, _, _ := runCommand(t, "docker", "logs", "--tail", fmt.Sprintf("%d", tail), containerName)
	return stdout
}

// DumpAllLogs dumps logs from all containers to the test log (useful on failure).
func (dc *DockerCluster) DumpAllLogs(t *testing.T, tail int) {
	t.Helper()
	containers := []string{"etcd", "minio"}
	for _, n := range dc.Nodes {
		containers = append(containers, n.ContainerName)
	}
	for _, c := range containers {
		logs := dc.GetLogs(t, c, tail)
		t.Logf("=== Logs from %s (last %d lines) ===\n%s", c, tail, logs)
	}
}

// NewConfig creates a Woodpecker configuration for connecting to the chaos cluster.
func (dc *DockerCluster) NewConfig(t *testing.T) *config.Configuration {
	t.Helper()
	cfg, err := config.NewConfiguration(DefaultConfigPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	cfg.Log.Level = "debug"
	cfg.Woodpecker.Storage.Type = "service"
	cfg.Woodpecker.Storage.RootPath = filepath.Join(t.TempDir(), t.Name())

	seeds := make([]string, 0, len(dc.Nodes))
	for _, n := range dc.Nodes[:3] { // Use first 3 nodes as seeds
		seeds = append(seeds, fmt.Sprintf("localhost:%d", n.ServicePort))
	}
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds

	return cfg
}

// NewEtcdClient creates an etcd client connected to the chaos cluster.
func (dc *DockerCluster) NewEtcdClient(t *testing.T, cfg *config.Configuration) *clientv3.Client {
	t.Helper()
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	if err != nil {
		t.Fatalf("failed to create etcd client: %v", err)
	}
	return etcdCli
}

// NewClient creates a full Woodpecker client connected to the chaos cluster.
func (dc *DockerCluster) NewClient(t *testing.T, ctx context.Context) (woodpecker.Client, *config.Configuration) {
	t.Helper()

	cfg := dc.NewConfig(t)
	etcdCli := dc.NewEtcdClient(t, cfg)

	client, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	if err != nil {
		etcdCli.Close()
		t.Fatalf("failed to create Woodpecker client: %v", err)
	}

	// Register cleanup to close both client and etcd
	t.Cleanup(func() {
		_ = client.Close(ctx)
		_ = etcdCli.Close()
	})

	return client, cfg
}

// NewClientManual creates a Woodpecker client without registering t.Cleanup.
// The caller is responsible for closing the returned client and etcd client.
// Use this when you need explicit control over client lifecycle (e.g., closing
// before stopping the cluster).
func (dc *DockerCluster) NewClientManual(t *testing.T, ctx context.Context) (woodpecker.Client, *clientv3.Client, *config.Configuration) {
	t.Helper()

	cfg := dc.NewConfig(t)
	etcdCli := dc.NewEtcdClient(t, cfg)

	client, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	if err != nil {
		etcdCli.Close()
		t.Fatalf("failed to create Woodpecker client: %v", err)
	}

	return client, etcdCli, cfg
}

// ServiceSeeds returns the seed addresses for the first N nodes.
func (dc *DockerCluster) ServiceSeeds(n int) []string {
	seeds := make([]string, 0, n)
	for i := 0; i < n && i < len(dc.Nodes); i++ {
		seeds = append(seeds, fmt.Sprintf("localhost:%d", dc.Nodes[i].ServicePort))
	}
	return seeds
}
