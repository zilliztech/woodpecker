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

package utils

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/membership"
	"github.com/zilliztech/woodpecker/server"
	"github.com/zilliztech/woodpecker/tests/utils/faultproxy"
)

// PodCluster is a MiniCluster whose nodes are reached the way pods are: through
// a stable address (a faultproxy standing in for the pod's DNS name) that is
// advertised in gossip and used as a service seed, while the server behind it
// binds a fresh port every time it is restarted, the way a replacement pod gets
// a fresh IP.
//
// That indirection is what lets a test reproduce a pod replacement's network
// phases (see faultproxy) instead of the instant "connection refused" a plain
// localhost restart produces.
type PodCluster struct {
	*MiniCluster
	Proxies     map[int]*faultproxy.Proxy
	GossipSeeds []string
}

// PodPlan describes one pod going away and, unless NoRestart, coming back.
type PodPlan struct {
	// Kill ends the process abruptly (SIGKILL / crash): no gossip leave, no
	// drain, open connections reset. Otherwise the exit is graceful (SIGTERM).
	Kill bool
	// Vanish makes the pod disappear from the network before its process ends
	// (node loss, partition): open connections go silent instead of resetting,
	// and there is no refusal phase. Implies Kill.
	Vanish bool
	// RefuseFor is how long the old IP still answers dials with RST after the
	// process is gone.
	RefuseFor time.Duration
	// RefuseLatency delays each of those RSTs, standing in for the network
	// round trip a remote refusal costs. On localhost a refusal beats every
	// ack; across a real network the healthy replicas' acks can arrive first,
	// and then the client treats the dead replica's failure as a straggler
	// instead of rolling away from it.
	RefuseLatency time.Duration
	// BlackholeFor is how long dials get no answer at all: the old IP has been
	// reclaimed and the replacement pod is not serving yet.
	BlackholeFor time.Duration
	// NoRestart leaves the address blackholed for good: the pod never returns.
	NoRestart bool
}

// RollingRestartPlan is a graceful k8s pod replacement with the phase lengths
// observed in production: about half a second of refusals while the old
// sandbox still exists, then about ten seconds of silence until the
// replacement pod serves.
func RollingRestartPlan() PodPlan {
	return PodPlan{RefuseFor: 500 * time.Millisecond, BlackholeFor: 10 * time.Second}
}

// StartPodCluster starts nodeCount nodes, each behind its own proxy, and waits
// until they all see each other. Clients must use cluster.ServiceSeeds().
func StartPodCluster(t *testing.T, nodeCount int, baseDir string) (*PodCluster, *config.Configuration) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	return StartPodClusterWithCfg(t, nodeCount, baseDir, cfg)
}

// StartPodClusterWithCfg is StartPodCluster with a caller-provided configuration.
func StartPodClusterWithCfg(t *testing.T, nodeCount int, baseDir string, cfg *config.Configuration) (*PodCluster, *config.Configuration) {
	cfg.Woodpecker.Storage.Type = "service"
	cfg.Woodpecker.Storage.RootPath = baseDir
	cfg.Minio.RootPath = strings.TrimPrefix(baseDir, "/")
	DisableDiskWatermark(cfg)

	c := &PodCluster{
		MiniCluster: &MiniCluster{
			Servers:        make(map[int]*server.Server),
			Config:         cfg,
			BaseDir:        baseDir,
			UsedPorts:      make(map[int]int),
			UsedAddresses:  make(map[int]string),
			NodeConfigs:    make(map[int]NodeConfig),
			MaxNodeIndex:   -1,
			allocatedPorts: make(map[int]bool),
		},
		Proxies: make(map[int]*faultproxy.Proxy),
	}

	// Gossip seeds must be known before any node starts; the service ports
	// behind the proxies are allocated per start.
	gossipPorts := make([]int, nodeCount)
	for i := 0; i < nodeCount; i++ {
		port, err := c.allocateUniquePort()
		require.NoError(t, err)
		gossipPorts[i] = port
		c.GossipSeeds = append(c.GossipSeeds, fmt.Sprintf("127.0.0.1:%d", port))

		// The proxy starts with no backend; startNode points it at the server.
		p, err := faultproxy.New("127.0.0.1:0", "")
		require.NoError(t, err)
		c.Proxies[i] = p
		c.NodeConfigs[i] = NodeConfig{Index: i, ResourceGroup: "default", AZ: "default"}
	}
	for i := 0; i < nodeCount; i++ {
		require.NoError(t, c.startNode(t, i, gossipPorts[i], c.GossipSeeds))
	}
	waitClusterReady(t, c.MiniCluster, nodeCount)
	return c, cfg
}

// ServiceSeeds returns the stable per-pod addresses clients should use.
func (c *PodCluster) ServiceSeeds() []string {
	seeds := make([]string, 0, len(c.Proxies))
	for i := 0; i <= c.MaxNodeIndex; i++ {
		if p, ok := c.Proxies[i]; ok {
			seeds = append(seeds, p.Addr())
		}
	}
	return seeds
}

// NodeIndexByServiceAddr maps an address from a segment's quorum back to a node.
func (c *PodCluster) NodeIndexByServiceAddr(addr string) (int, bool) {
	for i, p := range c.Proxies {
		if p.Addr() == addr {
			return i, true
		}
	}
	return -1, false
}

// startNode starts node idx on a fresh service port, advertising its proxy's
// address, and points the proxy at it once it serves.
func (c *PodCluster) startNode(t *testing.T, idx int, gossipPort int, gossipSeeds []string) error {
	servicePort, err := c.allocateUniquePort()
	if err != nil {
		return err
	}
	proxy := c.Proxies[idx]
	_, proxyPortStr, _ := strings.Cut(proxy.Addr(), ":")
	var proxyPort int
	if _, err := fmt.Sscanf(proxyPortStr, "%d", &proxyPort); err != nil {
		return fmt.Errorf("parse proxy port %q: %w", proxy.Addr(), err)
	}

	nodeCfg := *c.Config
	nodeCfg.Woodpecker.Storage.RootPath = filepath.Join(c.BaseDir, fmt.Sprintf("node%d", idx))
	meta := c.NodeConfigs[idx]
	srv, err := server.NewServerWithConfig(context.Background(), &nodeCfg, &membership.ServerConfig{
		NodeID:               fmt.Sprintf("node%d", idx),
		BindPort:             gossipPort,
		AdvertisePort:        gossipPort,
		AdvertiseAddr:        "127.0.0.1",
		ServicePort:          servicePort,
		AdvertiseServicePort: proxyPort,
		AdvertiseServiceAddr: "127.0.0.1",
		ResourceGroup:        meta.ResourceGroup,
		AZ:                   meta.AZ,
		Tags:                 map[string]string{"role": "test"},
	}, gossipSeeds)
	if err != nil {
		return fmt.Errorf("create node %d: %w", idx, err)
	}
	if err := srv.Prepare(); err != nil {
		return fmt.Errorf("prepare node %d: %w", idx, err)
	}
	go func() {
		if runErr := srv.Run(); runErr != nil {
			fmt.Printf("pod node %d run error: %v\n", idx, runErr)
		}
	}()

	c.Servers[idx] = srv
	c.UsedPorts[idx] = servicePort
	c.UsedAddresses[idx] = fmt.Sprintf("127.0.0.1:%d", gossipPort)
	if idx > c.MaxNodeIndex {
		c.MaxNodeIndex = idx
	}
	// Prepare has opened the listener, so dials through the proxy succeed now.
	if err := proxy.SetForward(fmt.Sprintf("127.0.0.1:%d", servicePort)); err != nil {
		return err
	}
	t.Logf("pod node%d serving on 127.0.0.1:%d behind %s (gossip %d)", idx, servicePort, proxy.Addr(), gossipPort)
	return nil
}

// PodRestart takes pod idx through plan and returns once the replacement is
// serving and has rejoined the cluster (or, with NoRestart, once the pod is
// gone). It blocks; run workloads in other goroutines.
func (c *PodCluster) PodRestart(t *testing.T, idx int, plan PodPlan) {
	t.Helper()
	srv := c.Servers[idx]
	require.NotNil(t, srv, "pod node%d is not running", idx)
	proxy := c.Proxies[idx]
	start := time.Now()
	phase := func(name string) {
		t.Logf("pod node%d +%v: %s", idx, time.Since(start).Round(time.Millisecond), name)
	}

	switch {
	case plan.Vanish:
		phase("vanish: network silent, process killed")
		require.NoError(t, proxy.Blackhole())
		_ = srv.Kill()
	case plan.Kill:
		phase("kill: process killed, connections reset, IP still answers with RST")
		c.refuse(t, proxy, plan)
		_ = srv.Kill()
		proxy.ResetAll()
	default:
		phase("terminate: graceful stop, IP still answers with RST")
		c.refuse(t, proxy, plan)
		_ = srv.Stop()
	}
	c.Servers[idx] = nil
	phase("process gone")

	if !plan.Vanish && plan.RefuseFor > 0 {
		time.Sleep(plan.RefuseFor)
	}
	if plan.NoRestart {
		require.NoError(t, proxy.Blackhole())
		phase("IP reclaimed, pod never returns")
		return
	}
	if plan.BlackholeFor > 0 {
		require.NoError(t, proxy.Blackhole())
		phase("IP reclaimed: dials get no answer")
		time.Sleep(plan.BlackholeFor)
	}

	gossipPort, err := c.allocateUniquePort()
	require.NoError(t, err)
	require.NoError(t, c.startNode(t, idx, gossipPort, c.liveGossipSeeds()))
	phase("replacement pod serving")
	waitClusterReady(t, c.MiniCluster, c.GetActiveNodes())
	phase("replacement pod rejoined")
}

func (c *PodCluster) refuse(t *testing.T, proxy *faultproxy.Proxy, plan PodPlan) {
	if plan.RefuseLatency > 0 {
		require.NoError(t, proxy.RefuseAfter(plan.RefuseLatency))
		return
	}
	proxy.Refuse()
}

// liveGossipSeeds returns the gossip addresses of running nodes.
func (c *PodCluster) liveGossipSeeds() []string {
	seeds := make([]string, 0, len(c.Servers))
	for i, srv := range c.Servers {
		if srv != nil {
			seeds = append(seeds, c.UsedAddresses[i])
		}
	}
	return seeds
}

// Stop stops every node and proxy.
func (c *PodCluster) Stop(t *testing.T) {
	c.StopMultiNodeCluster(t)
	for _, p := range c.Proxies {
		p.Close()
	}
}
