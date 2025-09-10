// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package membership

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// Test only

func waitForExit() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Shutting down...")
}

func main() {
	var (
		role   = flag.String("role", "server", "Node role: server or client")
		nodeID = flag.String("node", "", "Node ID")

		bindAddr = flag.String("bind", "127.0.0.1", "Bind address")
		bindPort = flag.Int("port", 7946, "Bind port")
		joinAddr = flag.String("join", "", "Join addresses (comma separated)")

		resourceGroup = flag.String("rg", "rg-001", "Resource group (server only)")
		az            = flag.String("az", "us-west-2a", "Availability zone (server only)")
		servicePort   = flag.Int("service-port", 8080, "Service port (server only)")
	)
	flag.Parse()

	if *nodeID == "" {
		hostname, _ := os.Hostname()
		*nodeID = fmt.Sprintf("%s-%s-%d", hostname, *role, *bindPort)
	}

	if *role == "server" {
		config := &ServerConfig{
			NodeID:        *nodeID,
			ResourceGroup: *resourceGroup,
			AZ:            *az,
			BindAddr:      *bindAddr,
			BindPort:      *bindPort,
			ServicePort:   *servicePort,
			Tags:          map[string]string{"env": "production"},
		}
		node, err := NewServerNode(config)
		if err != nil {
			log.Fatalf("Failed to create server: %v", err)
		}
		defer node.Shutdown()
		if *joinAddr != "" {
			existing := strings.Split(*joinAddr, ",")
			if err := node.Join(existing); err != nil {
				log.Fatalf("Failed to join cluster: %v", err)
			}
		}
		log.Printf("   Server %s started", *nodeID)
		log.Printf("   Resource Group: %s", *resourceGroup)
		log.Printf("   AZ: %s", *az)
		log.Printf("   Service Endpoint: %s:%d", *bindAddr, *servicePort)
		go func() { // This is just for testing active updates, real updates should be handled by business server node's own tick loop
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			updateCount := 0
			for range ticker.C {
				updateCount++
				node.UpdateMeta(map[string]interface{}{"tags": map[string]string{"env": "production", "update": fmt.Sprintf("%d", updateCount)}})
				log.Printf("[SERVER] Metadata updated (version: %d)", updateCount)
			}
		}()
		go func() { // This is just for testing active print debug, real business operations for selecting server nodes can refer to these print operations
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			time.Sleep(2 * time.Second)
			for range ticker.C {
				node.PrintStatus()
			}
		}()
		waitForExit() // Go process stop is usually kill, here we listen for kill terminate
		node.Leave()  // Broadcast a leave message
	} else {
		config := &ClientConfig{NodeID: *nodeID, BindAddr: *bindAddr, BindPort: *bindPort}
		node, err := NewClientNode(config)
		if err != nil {
			log.Fatalf("Failed to create client: %v", err)
		}
		defer node.Shutdown()
		if *joinAddr != "" {
			existing := strings.Split(*joinAddr, ",")
			if err := node.Join(existing); err != nil {
				log.Fatalf("Failed to connect to cluster: %v", err)
			}
		} else {
			log.Fatal("Client must specify -join to connect to cluster")
		}
		log.Printf("   Client %s started", *nodeID)
		log.Printf("   Watching cluster for changes...")
		go func() { // Simulate the server list view seen by client
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			time.Sleep(3 * time.Second)
			for range ticker.C {
				node.PrintStatus()
				groups := node.GetDiscovery().GetResourceGroups()
				for _, rg := range groups {
					fmt.Printf("\n🔄 Attempting to select replicas for %s...\n", rg)
					if _, err := node.SelectReplicas(rg); err != nil {
						fmt.Printf("   Failed: %v\n", err)
					}
				}
			}
		}()
		waitForExit() // Wait for client to exit
		node.Leave()  // Broadcast a leave message
	}
}
