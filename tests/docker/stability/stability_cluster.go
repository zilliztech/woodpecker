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

package stability

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/zilliztech/woodpecker/tests/docker/framework"
)

// memberAlive is memberlist's StateAlive.
const memberAlive = 0

// StabilityCluster is the Docker Compose cluster the Docker tier runs against:
// 4 nodes, each a container a test can kill, stop, pause and start again.
type StabilityCluster struct {
	*framework.DockerCluster
}

func stabilityDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Dir(thisFile)
}

// NewStabilityCluster describes the cluster started by run_service_stability.sh
// or the workflow. It does not start it.
func NewStabilityCluster(t *testing.T) *StabilityCluster {
	t.Helper()
	return &StabilityCluster{DockerCluster: framework.NewDockerCluster(t, framework.ClusterConfig{
		TestDir:      stabilityDir(),
		ProjectName:  "woodpecker-stability",
		OverrideFile: "docker-compose.stability.yaml",
		NetworkName:  "woodpecker-stability_woodpecker",
	})}
}

// ServiceAddrs are the nodes' advertised service addresses, which is what a
// segment's quorum lists.
func (c *StabilityCluster) ServiceAddrs() []string {
	out := make([]string, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		out = append(out, fmt.Sprintf("localhost:%d", n.ServicePort))
	}
	return out
}

// NodeByServiceAddr returns the container behind a quorum address.
func (c *StabilityCluster) NodeByServiceAddr(t *testing.T, addr string) framework.NodeInfo {
	t.Helper()
	for _, n := range c.Nodes {
		if addr == fmt.Sprintf("localhost:%d", n.ServicePort) {
			return n
		}
	}
	t.Fatalf("quorum node %q is not a node of the cluster", addr)
	return framework.NodeInfo{}
}

// adminPort is the node's HTTP admin/metrics port on the host: 9091 for the
// first node, 9092 for the second, and so on (deployments/docker-compose.yaml).
func (c *StabilityCluster) adminPort(n framework.NodeInfo) int {
	for i, m := range c.Nodes {
		if m.ContainerName == n.ContainerName {
			return 9091 + i
		}
	}
	return 0
}

// Kill sends SIGKILL: no gossip leave, no drain, like an OOM kill or a crash.
func (c *StabilityCluster) Kill(t *testing.T, n framework.NodeInfo) {
	t.Helper()
	t.Logf("kill %s", n.ContainerName)
	framework.RunCommandNoFail(t, "docker", "kill", n.ContainerName)
}

// Pause freezes the node's processes. Its sockets stay open and the kernel
// still accepts connections, but nothing answers: what a client sees of a pod
// whose node died or was partitioned, without the resets a kill produces.
func (c *StabilityCluster) Pause(t *testing.T, n framework.NodeInfo) {
	t.Helper()
	t.Logf("pause %s", n.ContainerName)
	framework.RunCommandNoFail(t, "docker", "pause", n.ContainerName)
}

// Unpause resumes a paused node.
func (c *StabilityCluster) Unpause(t *testing.T, n framework.NodeInfo) {
	t.Helper()
	t.Logf("unpause %s", n.ContainerName)
	framework.RunCommandNoFail(t, "docker", "unpause", n.ContainerName)
}

// Restart stops the node gracefully (SIGTERM, 10s grace: a gossip leave and a
// drain, as a pod deletion does) and starts it again.
func (c *StabilityCluster) Restart(t *testing.T, n framework.NodeInfo) {
	t.Helper()
	c.StopNode(t, n.ContainerName)
	c.StartNode(t, n.ContainerName)
}

func (c *StabilityCluster) isPaused(name string) bool {
	out, _, err := framework.RunCommandDirect("docker", "inspect", "-f", "{{.State.Paused}}", name)
	return err == nil && strings.TrimSpace(out) == "true"
}

// RestoreAll brings every node back, whatever a test left it in, and waits
// for the cluster to reconverge, so the next test starts from 4 live nodes.
func (c *StabilityCluster) RestoreAll(t *testing.T) {
	t.Helper()
	for _, n := range c.Nodes {
		if c.isPaused(n.ContainerName) {
			c.Unpause(t, n)
		}
		if !c.IsRunning(n.ContainerName) {
			c.StartNode(t, n.ContainerName)
		}
	}
	c.WaitConverged(t, 60*time.Second)
}

// WaitConverged waits until every node's memberlist lists every node as alive.
func (c *StabilityCluster) WaitConverged(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		last = ""
		for _, n := range c.Nodes {
			alive, err := c.aliveMembers(n)
			if err != nil {
				last = fmt.Sprintf("%s: %v", n.ContainerName, err)
				break
			}
			if alive != len(c.Nodes) {
				last = fmt.Sprintf("%s sees %d/%d members alive", n.ContainerName, alive, len(c.Nodes))
				break
			}
		}
		if last == "" {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("cluster did not reconverge within %v: %s", timeout, last)
}

func (c *StabilityCluster) aliveMembers(n framework.NodeInfo) (int, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:%d/admin/memberlist", c.adminPort(n)), nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")
	httpClient := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil}}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	var list struct {
		Members []struct {
			State int `json:"state"`
		} `json:"members"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return 0, fmt.Errorf("memberlist: %w (%q)", err, body)
	}
	alive := 0
	for _, m := range list.Members {
		if m.State == memberAlive {
			alive++
		}
	}
	return alive, nil
}
