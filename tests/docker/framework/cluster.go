// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package framework

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
	// DefaultComposeFile is the main docker-compose file.
	DefaultComposeFile = "docker-compose.yaml"

	// DefaultImageName is the expected docker image name.
	DefaultImageName = "woodpecker:latest"
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

// DockerCluster manages a Docker Compose-based Woodpecker cluster for integration testing.
type DockerCluster struct {
	TestDir         string        // Absolute path to the caller's test directory
	ComposeDir      string        // Absolute path to the deployments/ directory
	ComposeFile     string        // Main compose file name
	OverrideFile    string        // Override file name
	OverridePath    string        // Absolute path to override file
	ProjectName     string        // Docker Compose project name
	Nodes           []NodeInfo    // Cluster nodes
	NetworkName     string        // Docker network name
	ExtraContainers []string      // Extra containers (e.g., "prometheus", "grafana")
	GossipWait      time.Duration // Gossip convergence wait time
}

// ClusterConfig holds configuration for creating a new DockerCluster.
type ClusterConfig struct {
	// TestDir is the absolute path to the suite directory containing the
	// override compose file. If empty, filepath.Abs(".") is used. Callers
	// that may be imported from sub-packages should set this explicitly
	// (e.g., via runtime.Caller) so that paths resolve correctly regardless
	// of the test binary's working directory.
	TestDir         string
	ProjectName     string
	OverrideFile    string
	NetworkName     string
	ExtraContainers []string
	GossipWait      time.Duration
}

// NewDockerCluster creates a DockerCluster with the given configuration.
// It resolves paths relative to TestDir (or CWD if TestDir is empty).
func NewDockerCluster(t *testing.T, cfg ClusterConfig) *DockerCluster {
	t.Helper()

	testDir := cfg.TestDir
	if testDir == "" {
		var err error
		testDir, err = filepath.Abs(".")
		if err != nil {
			t.Fatalf("failed to resolve test directory: %v", err)
		}
	}

	// The deployments/ directory is three levels up from tests/docker/<suite>/
	deploymentsDir, err := filepath.Abs(filepath.Join(testDir, "..", "..", "..", "deployments"))
	if err != nil {
		t.Fatalf("failed to resolve deployments directory: %v", err)
	}

	overridePath := filepath.Join(testDir, cfg.OverrideFile)

	gossipWait := cfg.GossipWait
	if gossipWait == 0 {
		gossipWait = 10 * time.Second
	}

	return &DockerCluster{
		TestDir:         testDir,
		ComposeDir:      deploymentsDir,
		ComposeFile:     DefaultComposeFile,
		OverrideFile:    cfg.OverrideFile,
		OverridePath:    overridePath,
		ProjectName:     cfg.ProjectName,
		Nodes:           DefaultNodes(),
		NetworkName:     cfg.NetworkName,
		ExtraContainers: cfg.ExtraContainers,
		GossipWait:      gossipWait,
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

// AllContainers returns the full list of containers managed by this cluster
// (etcd, minio, extra containers, and woodpecker nodes).
func (dc *DockerCluster) AllContainers() []string {
	containers := []string{"etcd", "minio"}
	containers = append(containers, dc.ExtraContainers...)
	for _, n := range dc.Nodes {
		containers = append(containers, n.ContainerName)
	}
	return containers
}

// RunCommand executes a command and returns stdout, stderr, and error.
func RunCommand(t *testing.T, name string, args ...string) (string, string, error) {
	t.Helper()
	cmd := exec.Command(name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// RunCommandNoFail executes a command and fails the test on error.
func RunCommandNoFail(t *testing.T, name string, args ...string) string {
	t.Helper()
	stdout, stderr, err := RunCommand(t, name, args...)
	if err != nil {
		t.Fatalf("command %s %v failed: %v\nstdout: %s\nstderr: %s", name, args, err, stdout, stderr)
	}
	return stdout
}

// RunCommandDirect executes a command without requiring a testing.T (for use in non-test contexts).
func RunCommandDirect(name string, args ...string) (string, string, error) {
	cmd := exec.Command(name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// BuildImageIfNeeded builds the Docker image if it does not already exist.
func (dc *DockerCluster) BuildImageIfNeeded(t *testing.T) {
	t.Helper()
	t.Log("Checking if Docker image exists...")

	stdout, _, _ := RunCommand(t, "docker", "images", "-q", DefaultImageName)
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
	t.Logf("Starting %s test cluster...", dc.ProjectName)

	args := append(dc.composeArgs(), "up", "-d", "--wait")
	RunCommandNoFail(t, "docker", args...)

	t.Log("Cluster started")
}

// Down stops the cluster (containers removed, volumes preserved).
func (dc *DockerCluster) Down(t *testing.T) {
	t.Helper()
	t.Logf("Stopping %s test cluster...", dc.ProjectName)

	args := append(dc.composeArgs(), "down")
	_, _, _ = RunCommand(t, "docker", args...)

	t.Log("Cluster stopped")
}

// Clean stops the cluster and removes all volumes.
func (dc *DockerCluster) Clean(t *testing.T) {
	t.Helper()
	t.Logf("Cleaning %s test cluster...", dc.ProjectName)

	args := append(dc.composeArgs(), "down", "-v", "--remove-orphans")
	_, _, _ = RunCommand(t, "docker", args...)

	t.Log("Cluster cleaned")
}

// IsRunning checks if a container is in "running" state.
func (dc *DockerCluster) IsRunning(containerName string) bool {
	stdout, _, err := RunCommandDirect("docker", "inspect", "-f", "{{.State.Running}}", containerName)
	if err != nil {
		return false
	}
	return strings.TrimSpace(stdout) == "true"
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

// WaitForClusterReady waits for all containers to be running.
func (dc *DockerCluster) WaitForClusterReady(t *testing.T, timeout time.Duration) {
	t.Helper()
	t.Log("Waiting for cluster to be ready...")

	containers := dc.AllContainers()

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
	time.Sleep(dc.GossipWait)
	t.Log("Cluster is ready")
}

// StopNode gracefully stops a container.
func (dc *DockerCluster) StopNode(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Stopping container %s...", containerName)
	RunCommandNoFail(t, "docker", "stop", "-t", "10", containerName)
	t.Logf("Container %s stopped", containerName)
}

// StartNode starts a previously stopped/killed container.
func (dc *DockerCluster) StartNode(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Starting container %s...", containerName)
	RunCommandNoFail(t, "docker", "start", containerName)
	t.Logf("Container %s started", containerName)
}

// GetLogs returns the last N lines of logs from a container.
func (dc *DockerCluster) GetLogs(t *testing.T, containerName string, tail int) string {
	t.Helper()
	stdout, _, _ := RunCommand(t, "docker", "logs", "--tail", fmt.Sprintf("%d", tail), containerName)
	return stdout
}

// DumpAllLogs dumps logs from all containers to the test log (useful on failure).
func (dc *DockerCluster) DumpAllLogs(t *testing.T, tail int) {
	t.Helper()
	for _, c := range dc.AllContainers() {
		logs := dc.GetLogs(t, c, tail)
		t.Logf("=== Logs from %s (last %d lines) ===\n%s", c, tail, logs)
	}
}

// NewConfig creates a Woodpecker configuration for connecting to the cluster.
func (dc *DockerCluster) NewConfig(t *testing.T) *config.Configuration {
	t.Helper()
	configPath := filepath.Join(dc.TestDir, "..", "..", "..", "config", "woodpecker.yaml")
	cfg, err := config.NewConfiguration(configPath)
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

// NewEtcdClient creates an etcd client connected to the cluster.
func (dc *DockerCluster) NewEtcdClient(t *testing.T, cfg *config.Configuration) *clientv3.Client {
	t.Helper()
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	if err != nil {
		t.Fatalf("failed to create etcd client: %v", err)
	}
	return etcdCli
}

// NewClient creates a full Woodpecker client connected to the cluster.
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

// StopAllWoodpeckerNodes stops all Woodpecker nodes (not etcd/minio/extras).
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

// ServiceSeeds returns the seed addresses for the first N nodes.
func (dc *DockerCluster) ServiceSeeds(n int) []string {
	seeds := make([]string, 0, n)
	for i := 0; i < n && i < len(dc.Nodes); i++ {
		seeds = append(seeds, fmt.Sprintf("localhost:%d", dc.Nodes[i].ServicePort))
	}
	return seeds
}
