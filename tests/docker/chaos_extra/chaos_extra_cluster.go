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

package chaos_extra

import (
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/zilliztech/woodpecker/tests/docker/framework"
)

// ChaosExtraCluster manages a Docker Compose-based Woodpecker cluster for extended chaos testing.
// It embeds the shared framework.DockerCluster and adds chaos-specific operations.
type ChaosExtraCluster struct {
	*framework.DockerCluster
}

// chaosExtraDir returns the absolute path to the chaos_extra suite directory,
// derived from this source file's location so it works regardless of CWD.
func chaosExtraDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Dir(thisFile)
}

// NewChaosExtraCluster creates a ChaosExtraCluster with chaos test configuration.
func NewChaosExtraCluster(t *testing.T) *ChaosExtraCluster {
	t.Helper()
	base := framework.NewDockerCluster(t, framework.ClusterConfig{
		TestDir:      chaosExtraDir(),
		ProjectName:  "woodpecker-chaos-extra",
		OverrideFile: "docker-compose.chaos-extra.yaml",
		NetworkName:  "woodpecker-chaos-extra_woodpecker",
		GossipWait:   10 * time.Second,
	})
	return &ChaosExtraCluster{DockerCluster: base}
}

// KillNode sends SIGKILL to a container (simulates crash). Container stays dead
// because docker-compose.chaos-extra.yaml sets restart: "no".
func (cc *ChaosExtraCluster) KillNode(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Killing container %s...", containerName)
	framework.RunCommandNoFail(t, "docker", "kill", containerName)
	t.Logf("Container %s killed", containerName)
}

// PauseNode freezes all processes in a container (simulates process hang / GC pause).
func (cc *ChaosExtraCluster) PauseNode(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Pausing container %s...", containerName)
	framework.RunCommandNoFail(t, "docker", "pause", containerName)
	t.Logf("Container %s paused", containerName)
}

// UnpauseNode resumes a paused container.
func (cc *ChaosExtraCluster) UnpauseNode(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Unpausing container %s...", containerName)
	framework.RunCommandNoFail(t, "docker", "unpause", containerName)
	t.Logf("Container %s unpaused", containerName)
}

// DisconnectNetwork disconnects a container from the Docker network,
// simulating a network partition.
func (cc *ChaosExtraCluster) DisconnectNetwork(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Disconnecting %s from network %s...", containerName, cc.NetworkName)
	framework.RunCommandNoFail(t, "docker", "network", "disconnect", "-f", cc.NetworkName, containerName)
	t.Logf("Container %s disconnected from network", containerName)
}

// ConnectNetwork reconnects a container to the Docker network.
func (cc *ChaosExtraCluster) ConnectNetwork(t *testing.T, containerName string) {
	t.Helper()
	t.Logf("Connecting %s to network %s...", containerName, cc.NetworkName)
	framework.RunCommandNoFail(t, "docker", "network", "connect", cc.NetworkName, containerName)
	t.Logf("Container %s connected to network", containerName)
}

// TryUnpauseNode resumes a paused container, ignoring errors
// (e.g., if the container is not paused). Useful in t.Cleanup handlers.
func (cc *ChaosExtraCluster) TryUnpauseNode(t *testing.T, containerName string) {
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
func (cc *ChaosExtraCluster) TryConnectNetwork(t *testing.T, containerName string) {
	t.Helper()
	_, _, err := framework.RunCommand(t, "docker", "network", "connect", cc.NetworkName, containerName)
	if err != nil {
		t.Logf("TryConnectNetwork %s: ignored error (likely already connected): %v", containerName, err)
	} else {
		t.Logf("Container %s reconnected to network", containerName)
	}
}

// StopEtcd stops the etcd container gracefully.
func (cc *ChaosExtraCluster) StopEtcd(t *testing.T) {
	t.Helper()
	t.Logf("Stopping etcd...")
	framework.RunCommandNoFail(t, "docker", "stop", "-t", "10", "etcd")
	t.Logf("etcd stopped")
}

// StartEtcd starts the etcd container.
func (cc *ChaosExtraCluster) StartEtcd(t *testing.T) {
	t.Helper()
	t.Logf("Starting etcd...")
	framework.RunCommandNoFail(t, "docker", "start", "etcd")
	t.Logf("etcd started")
}

// KillEtcd sends SIGKILL to the etcd container.
func (cc *ChaosExtraCluster) KillEtcd(t *testing.T) {
	t.Helper()
	t.Logf("Killing etcd...")
	framework.RunCommandNoFail(t, "docker", "kill", "etcd")
	t.Logf("etcd killed")
}

// StopMinIO stops the MinIO container gracefully.
func (cc *ChaosExtraCluster) StopMinIO(t *testing.T) {
	t.Helper()
	t.Logf("Stopping MinIO...")
	framework.RunCommandNoFail(t, "docker", "stop", "-t", "10", "minio")
	t.Logf("MinIO stopped")
}

// StartMinIO starts the MinIO container.
func (cc *ChaosExtraCluster) StartMinIO(t *testing.T) {
	t.Helper()
	t.Logf("Starting MinIO...")
	framework.RunCommandNoFail(t, "docker", "start", "minio")
	t.Logf("MinIO started")
}

// KillMinIO sends SIGKILL to the MinIO container.
func (cc *ChaosExtraCluster) KillMinIO(t *testing.T) {
	t.Helper()
	t.Logf("Killing MinIO...")
	framework.RunCommandNoFail(t, "docker", "kill", "minio")
	t.Logf("MinIO killed")
}

// DisconnectEtcd disconnects etcd from the Docker network.
func (cc *ChaosExtraCluster) DisconnectEtcd(t *testing.T) {
	t.Helper()
	t.Logf("Disconnecting etcd from network %s...", cc.NetworkName)
	framework.RunCommandNoFail(t, "docker", "network", "disconnect", "-f", cc.NetworkName, "etcd")
	t.Logf("etcd disconnected from network")
}

// ConnectEtcd reconnects etcd to the Docker network.
func (cc *ChaosExtraCluster) ConnectEtcd(t *testing.T) {
	t.Helper()
	t.Logf("Connecting etcd to network %s...", cc.NetworkName)
	framework.RunCommandNoFail(t, "docker", "network", "connect", cc.NetworkName, "etcd")
	t.Logf("etcd connected to network")
}

// DisconnectMinIO disconnects MinIO from the Docker network.
func (cc *ChaosExtraCluster) DisconnectMinIO(t *testing.T) {
	t.Helper()
	t.Logf("Disconnecting MinIO from network %s...", cc.NetworkName)
	framework.RunCommandNoFail(t, "docker", "network", "disconnect", "-f", cc.NetworkName, "minio")
	t.Logf("MinIO disconnected from network")
}

// ConnectMinIO reconnects MinIO to the Docker network.
func (cc *ChaosExtraCluster) ConnectMinIO(t *testing.T) {
	t.Helper()
	t.Logf("Connecting MinIO to network %s...", cc.NetworkName)
	framework.RunCommandNoFail(t, "docker", "network", "connect", cc.NetworkName, "minio")
	t.Logf("MinIO connected to network")
}

// TryConnectEtcd reconnects etcd, ignoring errors. Useful in t.Cleanup.
func (cc *ChaosExtraCluster) TryConnectEtcd(t *testing.T) {
	t.Helper()
	_, _, err := framework.RunCommand(t, "docker", "network", "connect", cc.NetworkName, "etcd")
	if err != nil {
		t.Logf("TryConnectEtcd: ignored error (likely already connected): %v", err)
	}
}

// TryConnectMinIO reconnects MinIO, ignoring errors. Useful in t.Cleanup.
func (cc *ChaosExtraCluster) TryConnectMinIO(t *testing.T) {
	t.Helper()
	_, _, err := framework.RunCommand(t, "docker", "network", "connect", cc.NetworkName, "minio")
	if err != nil {
		t.Logf("TryConnectMinIO: ignored error (likely already connected): %v", err)
	}
}

// TryStartEtcd starts etcd, ignoring errors. Useful in t.Cleanup.
func (cc *ChaosExtraCluster) TryStartEtcd(t *testing.T) {
	t.Helper()
	_, _, err := framework.RunCommand(t, "docker", "start", "etcd")
	if err != nil {
		t.Logf("TryStartEtcd: ignored error: %v", err)
	}
}

// TryStartMinIO starts MinIO, ignoring errors. Useful in t.Cleanup.
func (cc *ChaosExtraCluster) TryStartMinIO(t *testing.T) {
	t.Helper()
	_, _, err := framework.RunCommand(t, "docker", "start", "minio")
	if err != nil {
		t.Logf("TryStartMinIO: ignored error: %v", err)
	}
}
