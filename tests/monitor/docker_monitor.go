package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	// DefaultProjectName is the docker compose project name for monitor tests.
	DefaultProjectName = "woodpecker-monitor"

	// DefaultComposeFile is the main docker-compose file.
	DefaultComposeFile = "docker-compose.yaml"

	// DefaultOverrideFile is the monitor testing override file.
	DefaultOverrideFile = "docker-compose.monitor.yaml"

	// DefaultNetworkName is the docker network used by the cluster.
	DefaultNetworkName = "woodpecker-monitor_woodpecker"

	// DefaultImageName is the expected docker image name.
	DefaultImageName = "woodpecker:latest"

	// DefaultConfigPath is the relative path from tests/monitor to the config file.
	DefaultConfigPath = "../../config/woodpecker.yaml"

	// PrometheusURL is the base URL for the Prometheus HTTP API.
	PrometheusURL = "http://localhost:9090"
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

// MonitorCluster manages a Docker Compose-based Woodpecker cluster for monitor testing.
type MonitorCluster struct {
	ComposeDir   string     // Absolute path to the deployments/ directory
	ComposeFile  string     // Main compose file name
	OverrideFile string     // Override file name
	OverridePath string     // Absolute path to override file
	ProjectName  string     // Docker Compose project name
	Nodes        []NodeInfo // Cluster nodes
	NetworkName  string     // Docker network name
}

// NewMonitorCluster creates a MonitorCluster with default configuration.
// It resolves paths relative to the monitor test directory.
func NewMonitorCluster(t *testing.T) *MonitorCluster {
	t.Helper()

	// Resolve the monitor test directory (where this code lives)
	monitorDir, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("failed to resolve monitor test directory: %v", err)
	}

	// The deployments/ directory is two levels up from tests/monitor/
	deploymentsDir := filepath.Join(monitorDir, "..", "..", "deployments")
	deploymentsDir, err = filepath.Abs(deploymentsDir)
	if err != nil {
		t.Fatalf("failed to resolve deployments directory: %v", err)
	}

	overridePath := filepath.Join(monitorDir, DefaultOverrideFile)

	return &MonitorCluster{
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
func (mc *MonitorCluster) composeArgs() []string {
	return []string{
		"compose",
		"-f", filepath.Join(mc.ComposeDir, mc.ComposeFile),
		"-f", mc.OverridePath,
		"-p", mc.ProjectName,
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

// runCommandDirect executes a command without requiring a testing.T.
func runCommandDirect(name string, args ...string) (string, string, error) {
	cmd := exec.Command(name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// BuildImageIfNeeded builds the Docker image if it does not already exist.
func (mc *MonitorCluster) BuildImageIfNeeded(t *testing.T) {
	t.Helper()
	t.Log("Checking if Docker image exists...")

	stdout, _, _ := runCommand(t, "docker", "images", "-q", DefaultImageName)
	if strings.TrimSpace(stdout) != "" {
		t.Logf("Docker image %s already exists, skipping build", DefaultImageName)
		return
	}

	t.Logf("Building Docker image %s...", DefaultImageName)
	projectRoot := filepath.Join(mc.ComposeDir, "..")
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
func (mc *MonitorCluster) Up(t *testing.T) {
	t.Helper()
	t.Log("Starting monitor test cluster...")

	args := append(mc.composeArgs(), "up", "-d", "--wait")
	runCommandNoFail(t, "docker", args...)

	t.Log("Cluster started")
}

// Down stops the cluster (containers removed, volumes preserved).
func (mc *MonitorCluster) Down(t *testing.T) {
	t.Helper()
	t.Log("Stopping monitor test cluster...")

	args := append(mc.composeArgs(), "down")
	_, _, _ = runCommand(t, "docker", args...)

	t.Log("Cluster stopped")
}

// Clean stops the cluster and removes all volumes.
func (mc *MonitorCluster) Clean(t *testing.T) {
	t.Helper()
	t.Log("Cleaning monitor test cluster...")

	args := append(mc.composeArgs(), "down", "-v", "--remove-orphans")
	_, _, _ = runCommand(t, "docker", args...)

	t.Log("Cluster cleaned")
}

// IsRunning checks if a container is in "running" state.
func (mc *MonitorCluster) IsRunning(containerName string) bool {
	stdout, _, err := runCommandDirect("docker", "inspect", "-f", "{{.State.Running}}", containerName)
	if err != nil {
		return false
	}
	return strings.TrimSpace(stdout) == "true"
}

// WaitForHealthy polls until a container is running, with a timeout.
func (mc *MonitorCluster) WaitForHealthy(t *testing.T, containerName string, timeout time.Duration) {
	t.Helper()
	t.Logf("Waiting for %s to be healthy (timeout: %v)...", containerName, timeout)

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if mc.IsRunning(containerName) {
			t.Logf("Container %s is running", containerName)
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("container %s did not become healthy within %v", containerName, timeout)
}

// WaitForClusterReady waits for all nodes, etcd, minio, prometheus, and grafana to be running.
func (mc *MonitorCluster) WaitForClusterReady(t *testing.T, timeout time.Duration) {
	t.Helper()
	t.Log("Waiting for cluster to be ready...")

	containers := []string{"etcd", "minio", "prometheus", "grafana"}
	for _, n := range mc.Nodes {
		containers = append(containers, n.ContainerName)
	}

	deadline := time.Now().Add(timeout)
	for _, c := range containers {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timeout waiting for cluster readiness")
		}
		mc.WaitForHealthy(t, c, remaining)
	}

	// Additional grace period for gossip convergence
	t.Log("Cluster containers are running, waiting for gossip convergence...")
	time.Sleep(15 * time.Second)
	t.Log("Cluster is ready")
}

// StopNode gracefully stops a container.
func (mc *MonitorCluster) StopNode(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Stopping container %s...", containerName)
	runCommandNoFail(t, "docker", "stop", "-t", "10", containerName)
	t.Logf("Container %s stopped", containerName)
}

// StartNode starts a previously stopped/killed container.
func (mc *MonitorCluster) StartNode(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Starting container %s...", containerName)
	runCommandNoFail(t, "docker", "start", containerName)
	t.Logf("Container %s started", containerName)
}

// GetLogs returns the last N lines of logs from a container.
func (mc *MonitorCluster) GetLogs(t *testing.T, containerName string, tail int) string {
	t.Helper()
	stdout, _, _ := runCommand(t, "docker", "logs", "--tail", fmt.Sprintf("%d", tail), containerName)
	return stdout
}

// DumpAllLogs dumps logs from all containers to the test log (useful on failure).
func (mc *MonitorCluster) DumpAllLogs(t *testing.T, tail int) {
	t.Helper()
	containers := []string{"etcd", "minio", "prometheus", "grafana"}
	for _, n := range mc.Nodes {
		containers = append(containers, n.ContainerName)
	}
	for _, c := range containers {
		logs := mc.GetLogs(t, c, tail)
		t.Logf("=== Logs from %s (last %d lines) ===\n%s", c, tail, logs)
	}
}

// NewConfig creates a Woodpecker configuration for connecting to the monitor cluster.
func (mc *MonitorCluster) NewConfig(t *testing.T) *config.Configuration {
	t.Helper()
	cfg, err := config.NewConfiguration(DefaultConfigPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	cfg.Log.Level = "debug"
	cfg.Woodpecker.Storage.Type = "service"
	cfg.Woodpecker.Storage.RootPath = filepath.Join(t.TempDir(), t.Name())

	seeds := make([]string, 0, len(mc.Nodes))
	for _, n := range mc.Nodes[:3] { // Use first 3 nodes as seeds
		seeds = append(seeds, fmt.Sprintf("localhost:%d", n.ServicePort))
	}
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds

	return cfg
}

// NewEtcdClient creates an etcd client connected to the monitor cluster.
func (mc *MonitorCluster) NewEtcdClient(t *testing.T, cfg *config.Configuration) *clientv3.Client {
	t.Helper()
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	if err != nil {
		t.Fatalf("failed to create etcd client: %v", err)
	}
	return etcdCli
}

// NewClient creates a full Woodpecker client connected to the monitor cluster.
func (mc *MonitorCluster) NewClient(t *testing.T, ctx context.Context) (woodpecker.Client, *config.Configuration) {
	t.Helper()

	cfg := mc.NewConfig(t)
	etcdCli := mc.NewEtcdClient(t, cfg)

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

// --- Prometheus Query Methods ---

// PrometheusQueryResult represents the parsed response from Prometheus /api/v1/query.
type PrometheusQueryResult struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// WaitForPrometheusReady waits until the Prometheus server is ready to accept queries.
func (mc *MonitorCluster) WaitForPrometheusReady(t *testing.T, timeout time.Duration) {
	t.Helper()
	t.Logf("Waiting for Prometheus to be ready (timeout: %v)...", timeout)

	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 5 * time.Second}

	for time.Now().Before(deadline) {
		resp, err := client.Get(PrometheusURL + "/-/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				t.Log("Prometheus is ready")
				return
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("Prometheus did not become ready within %v", timeout)
}

// QueryMetric queries Prometheus for a metric by name and returns whether it exists
// (has at least one result) and the full query result.
func (mc *MonitorCluster) QueryMetric(t *testing.T, metricName string) (bool, *PrometheusQueryResult) {
	t.Helper()

	url := fmt.Sprintf("%s/api/v1/query?query=%s", PrometheusURL, metricName)
	client := &http.Client{Timeout: 10 * time.Second}

	resp, err := client.Get(url)
	if err != nil {
		t.Logf("Failed to query Prometheus for %s: %v", metricName, err)
		return false, nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Logf("Failed to read Prometheus response for %s: %v", metricName, err)
		return false, nil
	}

	var result PrometheusQueryResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Logf("Failed to parse Prometheus response for %s: %v", metricName, err)
		return false, nil
	}

	if result.Status != "success" {
		t.Logf("Prometheus query for %s returned status: %s", metricName, result.Status)
		return false, &result
	}

	exists := len(result.Data.Result) > 0
	return exists, &result
}

// QueryMetricHasValue queries Prometheus for a metric and returns true if it exists
// with at least one non-zero value.
func (mc *MonitorCluster) QueryMetricHasValue(t *testing.T, metricName string) bool {
	t.Helper()

	exists, result := mc.QueryMetric(t, metricName)
	if !exists || result == nil {
		return false
	}

	for _, r := range result.Data.Result {
		if len(r.Value) >= 2 {
			valStr, ok := r.Value[1].(string)
			if ok && valStr != "0" && valStr != "0.0" {
				return true
			}
		}
	}
	return false
}

// QueryPromQL queries Prometheus with an arbitrary PromQL expression
// and returns the first result value as a string.
func (mc *MonitorCluster) QueryPromQL(t *testing.T, query string) (string, bool) {
	t.Helper()

	reqURL := fmt.Sprintf("%s/api/v1/query?query=%s", PrometheusURL, url.QueryEscape(query))
	client := &http.Client{Timeout: 10 * time.Second}

	resp, err := client.Get(reqURL)
	if err != nil {
		t.Logf("Failed to query Prometheus: %v", err)
		return "", false
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Logf("Failed to read Prometheus response: %v", err)
		return "", false
	}

	var result PrometheusQueryResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Logf("Failed to parse Prometheus response: %v", err)
		return "", false
	}

	if result.Status != "success" || len(result.Data.Result) == 0 {
		return "", false
	}

	if len(result.Data.Result[0].Value) >= 2 {
		if val, ok := result.Data.Result[0].Value[1].(string); ok {
			return val, true
		}
	}

	return "", false
}
