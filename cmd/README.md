# Command Package

## Overview

The `cmd` package provides the main entry point for running Woodpecker as a distributed service in multi-replica mode. It is responsible for bootstrapping and managing single Woodpecker server node that form a cluster.

## Purpose

In service mode, multiple Woodpecker nodes run concurrently to form a **Woodpecker node pool**. These nodes coordinate through a gossip-based protocol and provide high availability for distributed log storage. Clients can select quorum nodes from this pool to perform multi-replica reads and writes, ensuring data consistency and fault tolerance.

## Key Features

### 1. Command-Line Interface
The main entry point (`main.go`) provides a comprehensive CLI for configuring and launching Woodpecker server nodes:

- **Service Port**: gRPC service port for client connections
- **Gossip Port**: Port for inter-node gossip communication
- **Node Identity**: Configurable node name and metadata (resource group, availability zone)
- **Data Directory**: Persistent storage location for log data
- **Seed Nodes**: Bootstrap nodes for cluster discovery
- **Advertise Addresses**: Support for Docker bridge networking and complex network topologies

### 2. Configuration Management
The package implements a two-layer configuration system:

- **Default Configuration**: Loaded from `woodpecker.yaml`
- **User Configuration**: External overrides loaded from `user.yaml`

The external configuration system (in `external/` subdirectory) allows users to override specific settings without modifying the default configuration file, supporting:
- Etcd configuration (endpoints, authentication, SSL)
- Storage backends (MinIO, local filesystem)
- Woodpecker-specific settings (segment policies, quorum strategies)
- Logging and tracing options

### 3. Service Discovery
The server integrates with the membership package to enable:
- Gossip-based service discovery
- Dynamic node registration and health monitoring
- Resource group and availability zone awareness
- Automatic cluster formation from seed nodes

### 4. Graceful Lifecycle Management
- **Preparation Phase**: Sets up network listeners and gossip membership
- **Startup**: Launches the gRPC service and registers with the cluster
- **Signal Handling**: Responds to SIGINT/SIGTERM for graceful shutdown
- **Cleanup**: Properly closes connections and flushes pending data

## Usage

```bash
./woodpecker server \
  --node-name=node1 \
  --service-port=18080 \
  --gossip-port=17946 \
  --data-dir=/data/woodpecker \
  --config=/etc/woodpecker/woodpecker.yaml \
  --external-user-config=/etc/woodpecker/user.yaml \
  --seeds=node2:17946,node3:17946 \
  --resource-group=default \
  --availability-zone=us-east-1a
```

## Directory Structure

- `main.go`: Main entry point and CLI implementation
- `external/`: User configuration management
  - `user_config.go`: Configuration merging logic
  - `user_config_test.go`: Configuration tests
  - `user_example.yaml`: Example user configuration file

## Architecture

The command package acts as the orchestration layer that:
1. Parses user input and configuration files
2. Initializes the Woodpecker server with proper settings
3. Registers the node with the distributed cluster
4. Manages the server lifecycle from startup to shutdown

This design allows Woodpecker to be deployed as a scalable, distributed system where each node is independently managed but collectively forms a cohesive cluster.