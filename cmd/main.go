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

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/server"
)

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
		grpcPort            = flag.Int("grpc-port", 18080, "gRPC service port")
		gossipPort          = flag.Int("gossip-port", 17946, "Gossip communication port")
		nodeName            = flag.String("node-name", "", "Node name (defaults to hostname)")
		dataDir             = flag.String("data-dir", "/woodpecker/data", "Data directory")
		configFile          = flag.String("config", "/woodpecker/configs/woodpecker.yaml", "Configuration file path")
		seeds               = flag.String("seeds", "", "Comma-separated list of seed nodes for gossip (host:port)")
		advertiseAddr       = flag.String("advertise-addr", "", "Advertise address for gossip (for Docker bridge networking)")
		advertiseGrpcPort   = flag.Int("advertise-grpc-port", 0, "Advertise gRPC port (defaults to grpc-port)")
		advertiseGossipPort = flag.Int("advertise-gossip-port", 0, "Advertise gossip port (defaults to gossip-port)")
		resourceGroup       = flag.String("resource-group", "default", "Resource group for node placement")
		availabilityZone    = flag.String("availability-zone", "default", "Availability zone for node placement")
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

	// Set default advertise ports if not specified
	if *advertiseGrpcPort == 0 {
		*advertiseGrpcPort = *grpcPort
	}
	if *advertiseGossipPort == 0 {
		*advertiseGossipPort = *gossipPort
	}

	// Resolve advertise address to IP if it's a hostname
	resolvedAdvertiseAddr := resolveAdvertiseAddr(*advertiseAddr)

	// Create server config with advertise options and node metadata
	serverConfig := &server.Config{
		BindPort:            *gossipPort,
		ServicePort:         *grpcPort,
		AdvertiseAddr:       resolvedAdvertiseAddr,
		AdvertiseGrpcPort:   *advertiseGrpcPort,
		AdvertiseGossipPort: *advertiseGossipPort,
		SeedNodes:           seedNodes,
		ResourceGroup:       *resourceGroup,
		AZ:                  *availabilityZone,
	}

	// Create server
	ctx := context.Background()
	srv := server.NewServerWithConfig(ctx, cfg, serverConfig)

	// Prepare server (sets up listener and gossip)
	if err := srv.Prepare(); err != nil {
		log.Fatalf("Failed to prepare server: %v", err)
	}

	log.Printf("Starting Woodpecker Server:")
	log.Printf("  Node Name: %s", *nodeName)
	log.Printf("  gRPC Port: %d", *grpcPort)
	log.Printf("  Gossip Port: %d", *gossipPort)
	log.Printf("  Resource Group: %s", *resourceGroup)
	log.Printf("  Availability Zone: %s", *availabilityZone)
	if *advertiseAddr != "" {
		if resolvedAdvertiseAddr != *advertiseAddr {
			log.Printf("  Advertise Address: %s (resolved from %s)", resolvedAdvertiseAddr, *advertiseAddr)
		} else {
			log.Printf("  Advertise Address: %s", *advertiseAddr)
		}
		log.Printf("  Advertise gRPC Port: %d", *advertiseGrpcPort)
		log.Printf("  Advertise Gossip Port: %d", *advertiseGossipPort)
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
	if err := srv.Stop(); err != nil {
		log.Printf("Error during shutdown: %v", err)
		os.Exit(1)
	}
	log.Println("Server stopped successfully")
}
