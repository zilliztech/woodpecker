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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/zilliztech/woodpecker/cmd/external"
	"github.com/zilliztech/woodpecker/common/config"
	commonhttp "github.com/zilliztech/woodpecker/common/http"
	"github.com/zilliztech/woodpecker/common/membership"
	"github.com/zilliztech/woodpecker/server"
)

// parseAdvertiseAddr parses address:port format and returns address and port
func parseAdvertiseAddr(addrPort string) (string, int, error) {
	if addrPort == "" {
		return "", 0, nil
	}

	host, portStr, err := net.SplitHostPort(addrPort)
	if err != nil {
		return "", 0, fmt.Errorf("invalid address:port format '%s': %v", addrPort, err)
	}

	port, err := net.LookupPort("tcp", portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port '%s': %v", portStr, err)
	}

	return host, port, nil
}

// resolveAdvertiseAddr resolves hostname to IP address if needed
func resolveAdvertiseAddr(addr string) string {
	if addr == "" {
		return ""
	}

	// Check if it's already an IP address
	if ip := net.ParseIP(addr); ip != nil {
		return addr
	}

	// Try to resolve hostname to IP
	ips, err := net.LookupIP(addr)
	if err != nil {
		log.Printf("Warning: Failed to resolve hostname '%s' to IP: %v. Using as-is.", addr, err)
		return addr
	}

	// Prefer IPv4 address
	for _, ip := range ips {
		if ipv4 := ip.To4(); ipv4 != nil {
			log.Printf("Resolved hostname '%s' to IPv4: %s", addr, ipv4.String())
			return ipv4.String()
		}
	}

	// Fallback to first IP (could be IPv6)
	if len(ips) > 0 {
		log.Printf("Resolved hostname '%s' to IP: %s", addr, ips[0].String())
		return ips[0].String()
	}

	log.Printf("Warning: No IP found for hostname '%s'. Using as-is.", addr)
	return addr
}

func main() {
	var (
		servicePort          = flag.Int("service-port", 18080, "service port")
		gossipPort           = flag.Int("gossip-port", 17946, "Gossip communication port")
		nodeName             = flag.String("node-name", "", "Node name (defaults to hostname)")
		dataDir              = flag.String("data-dir", "/woodpecker/data", "Data directory")
		configFile           = flag.String("config", "/woodpecker/configs/woodpecker.yaml", "Configuration file path")
		seeds                = flag.String("seeds", "", "Comma-separated list of seed nodes for gossip (host:port)")
		advertiseGossipAddr  = flag.String("advertise-gossip-addr", "", "Advertise address:port for gossip (for Docker bridge networking)")
		advertiseServiceAddr = flag.String("advertise-service-addr", "", "Advertise address:port for service (for client connections)")
		resourceGroup        = flag.String("resource-group", "default", "Resource group for node placement")
		availabilityZone     = flag.String("availability-zone", "default", "Availability zone for node placement")
		externalConfigFile   = flag.String("external-user-config", "/woodpecker/configs/user.yaml", "external user Configuration file path")
	)

	// First argument should be command
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <command> [flags]", os.Args[0])
	}

	command := os.Args[1]
	if command != "server" {
		log.Fatalf("Unknown command: %s. Only 'server' is supported.", command)
	}

	// Parse flags starting from the second argument
	flag.CommandLine.Parse(os.Args[2:])

	// Set default node name if not provided
	if *nodeName == "" {
		hostname, _ := os.Hostname()
		*nodeName = hostname
	}

	// Load configuration
	cfg, err := config.NewConfiguration(*configFile)
	if err != nil {
		log.Fatalf("Failed to load configuration from %s: %v", *configFile, err)
	}

	externalCfg, err := external.LoadUserConfig(*externalConfigFile)
	if err != nil {
		log.Fatalf("Failed to load external user configuration from %s: %v", *externalConfigFile, err)
	}

	if err := externalCfg.ApplyToConfig(cfg); err != nil {
		log.Fatalf("Failed to apply external user configuration: %v", err)
	}

	// Override data directory in config if specified
	if *dataDir != "" {
		cfg.Woodpecker.Storage.RootPath = *dataDir
	}

	// Parse seed nodes
	var seedNodes []string
	if *seeds != "" {
		seedNodes = strings.Split(*seeds, ",")
		for i, seed := range seedNodes {
			seedNodes[i] = strings.TrimSpace(seed)
		}
	}

	// Parse advertise addresses
	var advertiseAddrStr string
	var advertisePort int
	var advertiseServiceAddrStr string
	var advertiseServicePort int

	if *advertiseGossipAddr != "" {
		addr, port, err := parseAdvertiseAddr(*advertiseGossipAddr)
		if err != nil {
			log.Fatalf("Failed to parse advertise-gossip-addr: %v", err)
		}
		advertiseAddrStr = addr
		advertisePort = port
	} else {
		advertisePort = *gossipPort
	}
	resourceAdvertiseGossipAddrStr := resolveAdvertiseAddr(advertiseAddrStr)

	if *advertiseServiceAddr != "" {
		addr, port, err := parseAdvertiseAddr(*advertiseServiceAddr)
		if err != nil {
			log.Fatalf("Failed to parse advertise-service-addr: %v", err)
		}
		advertiseServiceAddrStr = addr
		advertiseServicePort = port
	} else {
		advertiseServicePort = *servicePort
	}

	// Create server config with advertise options and node metadata
	serverConfig := &membership.ServerConfig{
		NodeID:               *nodeName,
		BindPort:             *gossipPort,
		ServicePort:          *servicePort,
		AdvertiseAddr:        resourceAdvertiseGossipAddrStr, // Gossip advertise address (IP only)
		AdvertisePort:        advertisePort,                  // Gossip advertise port
		AdvertiseServiceAddr: advertiseServiceAddrStr,        // Service advertise address (hostname only)
		AdvertiseServicePort: advertiseServicePort,           // Service advertise port
		ResourceGroup:        *resourceGroup,
		AZ:                   *availabilityZone,
		Tags:                 map[string]string{"role": "logstore"},
	}

	// Create server
	ctx := context.Background()
	srv := server.NewServerWithConfig(ctx, cfg, serverConfig, seedNodes)

	// Prepare server (sets up listener and gossip)
	if err := srv.Prepare(); err != nil {
		log.Fatalf("Failed to prepare server: %v", err)
	}

	// Start HTTP server for metrics, health check, and pprof
	if err := commonhttp.Start(cfg); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
	log.Printf("HTTP server started on port %s (metrics, health, pprof)", commonhttp.DefaultListenPort)

	log.Printf("Starting Woodpecker Server:")
	log.Printf("  Node Name: %s", *nodeName)
	log.Printf("  Service Port: %d", *servicePort)
	log.Printf("  Gossip Port: %d", *gossipPort)
	log.Printf("  Resource Group: %s", *resourceGroup)
	log.Printf("  Availability Zone: %s", *availabilityZone)

	// Log gossip advertise configuration
	if *advertiseGossipAddr != "" {
		log.Printf("  Gossip Advertise Addr: %s Port: %d", advertiseAddrStr, advertisePort)
	}

	// Log service advertise configuration
	if *advertiseServiceAddr != "" {
		log.Printf("  Service Advertise Addr: %s Port: %d", advertiseServiceAddrStr, advertiseServicePort)
	}
	log.Printf("  Seeds: %v", seedNodes)
	log.Printf("  Data Directory: %s", cfg.Woodpecker.Storage.RootPath)
	log.Printf("  Config File: %s", *configFile)

	// Start server in a goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := srv.Run(); err != nil {
			errChan <- fmt.Errorf("server run failed: %w", err)
		}
	}()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for either an error or shutdown signal
	select {
	case err := <-errChan:
		log.Printf("Server error: %v", err)
		os.Exit(1)
	case sig := <-sigChan:
		log.Printf("Received signal %s, shutting down...", sig)
	}

	// Graceful shutdown
	log.Println("Stopping server...")

	// Stop main server
	if err := srv.Stop(); err != nil {
		log.Printf("Error during server shutdown: %v", err)
		os.Exit(1)
	}

	// Stop HTTP server
	if err := commonhttp.Stop(); err != nil {
		log.Printf("Error during HTTP server shutdown: %v", err)
		os.Exit(1)
	}

	log.Println("Server stopped successfully")
}
