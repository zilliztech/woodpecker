package membership

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

// Description: Similar to manual_sim_test.go in the repository root, but demonstrates carrying ServerMeta.

func startServerNodeForTest(t *testing.T, name, rg, az string, bindPort int, servicePort int, seed string) (*ServerNode, string) {
	t.Helper()
	cfg := &ServerConfig{
		NodeID:        name,
		ResourceGroup: rg,
		AZ:            az,
		BindAddr:      "127.0.0.1",
		BindPort:      bindPort,
		ServicePort:   servicePort,
		Tags:          map[string]string{"role": "demo"},
	}
	n, err := NewServerNode(cfg)
	if err != nil {
		t.Fatalf("create server failed: %v", err)
	}

	// Return advertise address (ip:port)
	adv := fmt.Sprintf("%s:%d", n.memberlist.LocalNode().Addr.String(), int(n.memberlist.LocalNode().Port))
	fmt.Printf("NODE_READY name=%s advertise=%s\n", name, adv)

	if seed != "" {
		if err := n.Join([]string{seed}); err != nil {
			t.Fatalf("join failed: %v", err)
		}
	}
	return n, adv
}

func startServerNodeForTest2(t *testing.T, name, rg, az string, bindPort int, servicePort int, seed, seed2 string) (*ServerNode, string) {
	t.Helper()
	cfg := &ServerConfig{
		NodeID:        name,
		ResourceGroup: rg,
		AZ:            az,
		BindAddr:      "127.0.0.1",
		BindPort:      bindPort,
		ServicePort:   servicePort,
		Tags:          map[string]string{"role": "demo"},
	}
	n, err := NewServerNode(cfg)
	if err != nil {
		t.Fatalf("create server failed: %v", err)
	}

	// Return advertise address (ip:port)
	adv := fmt.Sprintf("%s:%d", n.memberlist.LocalNode().Addr.String(), int(n.memberlist.LocalNode().Port))
	fmt.Printf("NODE_READY name=%s advertise=%s\n", name, adv)

	if seed != "" {
		if err := n.Join([]string{seed, seed2}); err != nil {
			t.Fatalf("join failed: %v", err)
		}
	}
	return n, adv
}

func TestDemoJoinA(t *testing.T) {
	n, adv := startServerNodeForTest(t, "demo-a", "rg-001", "az-a", 28080, 18080, os.Getenv("DEMO_SEED"))
	_ = os.Setenv("DEMO_SEED", adv)
	defer n.Shutdown()
	select {} // Block forever, exit with Ctrl+C
}

func TestDemoJoinB(t *testing.T) {
	seed := "127.0.0.1:28080" // first node addr
	n, _ := startServerNodeForTest(t, "demo-b", "rg-001", "az-b", 28081, 18081, seed)
	defer n.Shutdown()
	select {}
}

func TestDemoJoinC(t *testing.T) {
	seed := "127.0.0.1:28080" // first node addr
	n, _ := startServerNodeForTest2(t, "demo-c", "rg-001", "az-c", 28082, 18082, "127.0.0.1:28081", seed)
	defer n.Shutdown()
	select {}
}

// Temporary List node: join, print members and their metadata, then exit
func TestDemoListMembers(t *testing.T) {
	seed := "127.0.0.1:28080" // first node addr
	// Use client role to avoid excessive gossip overhead
	c, err := NewClientNode(&ClientConfig{NodeID: fmt.Sprintf("lister-%d", time.Now().UnixNano()), BindAddr: "127.0.0.1", BindPort: 0})
	if err != nil {
		t.Fatalf("create client failed: %v", err)
	}
	defer c.Shutdown()
	if err := c.Join([]string{seed}); err != nil {
		t.Fatalf("join cluster failed: %v", err)
	}

	// Wait for event convergence
	time.Sleep(500 * time.Millisecond)

	fmt.Println("MEMBERS_BEGIN")
	for _, m := range c.memberlist.Members() {
		fmt.Printf("%s %s:%d\n", m.Name, m.Addr.String(), m.Port)
		if len(m.Meta) > 0 {
			var meta ServerMeta
			if err := json.Unmarshal(m.Meta, &meta); err == nil {
				b, _ := json.MarshalIndent(meta, "  ", "  ")
				fmt.Printf("  META: %s\n", string(b))
			}
		}
	}
	fmt.Println("MEMBERS_END")
}
