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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/proto"
)

// Test user carefully designed O(1) ServiceDiscovery

func createFinalTestNode(nodeID, rg, az string, tags map[string]string) *proto.NodeMeta {
	return &proto.NodeMeta{
		NodeId:        nodeID,
		ResourceGroup: rg,
		Az:            az,
		Endpoint:      nodeID + ".example.com:8080",
		Tags:          tags,
		Version:       1,
		LastUpdate:    time.Now().UnixMilli(),
	}
}

func TestFinalServiceDiscovery(t *testing.T) {
	sd := NewServiceDiscovery()

	// Create test data
	// prod-us: us-east-1a, us-east-1b
	// prod-eu: eu-west-1a
	// staging-us: us-east-1a
	nodes := []*proto.NodeMeta{
		createFinalTestNode("node1", "prod-us", "us-east-1a", map[string]string{"env": "prod", "role": "writer"}),
		createFinalTestNode("node2", "prod-us", "us-east-1b", map[string]string{"env": "prod", "role": "reader"}),
		createFinalTestNode("node3", "prod-eu", "eu-west-1a", map[string]string{"env": "prod", "role": "writer"}),
		createFinalTestNode("node4", "staging-us", "us-east-1a", map[string]string{"env": "staging", "role": "writer"}),
	}

	for _, node := range nodes {
		sd.UpdateServer(node.NodeId, node)
	}

	t.Run("Basic structure validation", func(t *testing.T) {
		// Verify main storage
		assert.Equal(t, 4, len(sd.Nodes))
		assert.Equal(t, nodes[0], sd.Nodes["node1"])

		// Verify azList and rgList
		assert.Equal(t, 3, len(sd.azList)) // us-east-1a, us-east-1b, eu-west-1a
		assert.Equal(t, 3, len(sd.rgList)) // prod-us, prod-eu, staging-us
		assert.Contains(t, sd.azList, "us-east-1a")
		assert.Contains(t, sd.rgList, "prod-us")

		// Verify azRgIndex
		assert.Equal(t, 2, len(sd.azRgIndex["us-east-1a"])) // prod-us, staging-us
		assert.Equal(t, 1, len(sd.azRgIndex["us-east-1b"])) // prod-us
		assert.Equal(t, 1, len(sd.azRgIndex["eu-west-1a"])) // prod-eu

		// Verify rgAzIndex
		assert.Equal(t, 2, len(sd.rgAzIndex["prod-us"]))    // us-east-1a, us-east-1b
		assert.Equal(t, 1, len(sd.rgAzIndex["prod-eu"]))    // eu-west-1a
		assert.Equal(t, 1, len(sd.rgAzIndex["staging-us"])) // us-east-1a

		// Verify Keys index
		assert.Equal(t, 2, len(sd.azRgIndexKeys["us-east-1a"])) // prod-us, staging-us
		assert.Equal(t, 2, len(sd.rgAzIndexKeys["prod-us"]))    // us-east-1a, us-east-1b
	})

	t.Run("SelectSingleAzSingleRg O(1) strategy", func(t *testing.T) {
		// No filter: should randomly select an AZ and an RG
		// Due to randomness, may return 0 nodes (SOFT mode)
		filter := &proto.NodeFilter{Limit: 1}
		nodes, err := sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		// SOFT mode may return 0 nodes, which is normal
		assert.LessOrEqual(t, len(nodes), 1)

		// Specified RG filter - should always have results
		filter = &proto.NodeFilter{ResourceGroup: "prod-us", Limit: 1}
		nodes, err = sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(nodes), 1)
		if len(nodes) > 0 {
			assert.Equal(t, "prod-us", nodes[0].ResourceGroup)
		}

		// Specified AZ filter - should always have results
		filter = &proto.NodeFilter{Az: "us-east-1a", Limit: 1}
		nodes, err = sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(nodes), 1)
		if len(nodes) > 0 {
			assert.Equal(t, "us-east-1a", nodes[0].Az)
		}

		// Both RG and AZ specified - exact match, should always have results
		filter = &proto.NodeFilter{ResourceGroup: "prod-us", Az: "us-east-1a", Limit: 1}
		nodes, err = sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(nodes))
		assert.Equal(t, "node1", nodes[0].NodeId)
	})

	t.Run("SelectSingleAzMultiRg O(1) strategy", func(t *testing.T) {
		// Select multiple RGs in us-east-1a
		filter := &proto.NodeFilter{Az: "us-east-1a", Limit: 2}
		nodes, err := sd.SelectSingleAzMultiRg(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(nodes))

		// All nodes should be in the same AZ
		for _, node := range nodes {
			assert.Equal(t, "us-east-1a", node.Az)
		}

		// Should come from different RGs
		rgs := make(map[string]bool)
		for _, node := range nodes {
			rgs[node.ResourceGroup] = true
		}
		assert.Equal(t, 2, len(rgs)) // prod-us, staging-us
	})

	t.Run("SelectMultiAzSingleRg O(1) strategy", func(t *testing.T) {
		// Select multiple AZs in prod-us
		filter := &proto.NodeFilter{ResourceGroup: "prod-us", Limit: 2}
		nodes, err := sd.SelectMultiAzSingleRg(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(nodes))

		// All nodes should be in the same RG
		for _, node := range nodes {
			assert.Equal(t, "prod-us", node.ResourceGroup)
		}

		// Should come from different AZs
		azs := make(map[string]bool)
		for _, node := range nodes {
			azs[node.Az] = true
		}
		assert.Equal(t, 2, len(azs)) // us-east-1a, us-east-1b
	})

	t.Run("SelectMultiAzMultiRg O(1) strategy", func(t *testing.T) {
		// Select multiple AZs and multiple RGs
		filter := &proto.NodeFilter{Limit: 3}
		nodes, err := sd.SelectMultiAzMultiRg(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.LessOrEqual(t, len(nodes), 3)

		// Verify diversity
		azs := make(map[string]bool)
		rgs := make(map[string]bool)
		for _, node := range nodes {
			azs[node.Az] = true
			rgs[node.ResourceGroup] = true
		}
		assert.Greater(t, len(azs), 1) // Multiple AZs
		assert.Greater(t, len(rgs), 1) // Multiple RGs
	})

	t.Run("SelectRandom with filters", func(t *testing.T) {
		// No filter
		filter := &proto.NodeFilter{Limit: 0}
		nodes, err := sd.SelectRandom(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 4, len(nodes))

		// RG filter
		filter = &proto.NodeFilter{ResourceGroup: "prod-us", Limit: 0}
		nodes, err = sd.SelectRandom(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(nodes))
		for _, node := range nodes {
			assert.Equal(t, "prod-us", node.ResourceGroup)
		}

		// Tag filter
		filter = &proto.NodeFilter{Tags: map[string]string{"role": "writer"}, Limit: 0}
		nodes, err = sd.SelectRandom(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(nodes)) // node1, node3, node4
		for _, node := range nodes {
			assert.Equal(t, "writer", node.Tags["role"])
		}
	})

	t.Run("Regex pattern support", func(t *testing.T) {
		// RG regex pattern
		filter := &proto.NodeFilter{ResourceGroup: "prod-.*", Limit: 0}
		nodes, err := sd.SelectRandom(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(nodes)) // node1, node2, node3 (all prod-*)

		// AZ regex pattern
		filter = &proto.NodeFilter{Az: "us-.*", Limit: 0}
		nodes, err = sd.SelectRandom(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(nodes)) // node1, node2, node4 (all us-*)
	})

	t.Run("Remove server updates indexes", func(t *testing.T) {
		// Remove node2
		sd.RemoveServer("node2")

		// Verify main storage
		assert.Equal(t, 3, len(sd.Nodes))
		assert.NotContains(t, sd.Nodes, "node2")

		// Verify index updates
		assert.Equal(t, 1, len(sd.rgAzIndex["prod-us"]))    // Only us-east-1a left
		assert.Equal(t, 0, len(sd.azRgIndex["us-east-1b"])) // us-east-1b should be cleaned up

		// Verify Keys index updates
		assert.Equal(t, 1, len(sd.rgAzIndexKeys["prod-us"]))  // Only us-east-1a left
		assert.NotContains(t, sd.azRgIndexKeys, "us-east-1b") // Should be cleaned up

		// Verify queries still work
		filter := &proto.NodeFilter{ResourceGroup: "prod-us", Limit: 0}
		nodes, err := sd.SelectRandom(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(nodes))
		assert.Equal(t, "node1", nodes[0].NodeId)
	})
}

// === Node update and removal tests ===

func TestNodeUpdateAndRemove(t *testing.T) {
	sd := NewServiceDiscovery()

	// Create initial test data
	initialNodes := []*proto.NodeMeta{
		createFinalTestNode("node1", "rg1", "az1", map[string]string{"env": "prod", "version": "1.0"}),
		createFinalTestNode("node2", "rg1", "az2", map[string]string{"env": "prod", "version": "1.0"}),
		createFinalTestNode("node3", "rg2", "az1", map[string]string{"env": "staging", "version": "1.0"}),
		createFinalTestNode("node4", "rg2", "az2", map[string]string{"env": "staging", "version": "1.0"}),
	}

	for _, node := range initialNodes {
		sd.UpdateServer(node.NodeId, node)
	}

	t.Run("Node Update maintains indexes correctly", func(t *testing.T) {
		// Verify initial state
		assert.Equal(t, 4, len(sd.Nodes))
		assert.Equal(t, 2, len(sd.azList)) // az1, az2
		assert.Equal(t, 2, len(sd.rgList)) // rg1, rg2

		// Update node1's tags and version
		updatedNode1 := createFinalTestNode("node1", "rg1", "az1", map[string]string{"env": "prod", "version": "2.0", "role": "master"})
		sd.UpdateServer("node1", updatedNode1)

		// Verify node was properly updated
		assert.Equal(t, 4, len(sd.Nodes))
		actualNode := sd.Nodes["node1"]
		assert.Equal(t, "2.0", actualNode.Tags["version"])
		assert.Equal(t, "master", actualNode.Tags["role"])

		// Verify indexes unchanged (since RG and AZ didn't change)
		assert.Equal(t, 2, len(sd.azList))
		assert.Equal(t, 2, len(sd.rgList))
		assert.Equal(t, 2, len(sd.azRgIndex["az1"])) // rg1, rg2
		assert.Equal(t, 2, len(sd.rgAzIndex["rg1"])) // az1, az2

		// Verify queries still work
		filter := &proto.NodeFilter{Tags: map[string]string{"version": "2.0"}, Limit: 1}
		nodes, err := sd.SelectRandom(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(nodes))
		assert.Equal(t, "node1", nodes[0].NodeId)
	})

	t.Run("Node Update changes RG and AZ correctly", func(t *testing.T) {
		// Update node2 to new RG and AZ
		updatedNode2 := createFinalTestNode("node2", "rg3", "az3", map[string]string{"env": "test", "version": "1.5"})
		sd.UpdateServer("node2", updatedNode2)

		// Verify node info updated
		actualNode := sd.Nodes["node2"]
		assert.Equal(t, "rg3", actualNode.ResourceGroup)
		assert.Equal(t, "az3", actualNode.Az)
		assert.Equal(t, "test", actualNode.Tags["env"])

		// Verify index updates
		assert.Equal(t, 3, len(sd.azList)) // az1, az2, az3
		assert.Equal(t, 3, len(sd.rgList)) // rg1, rg2, rg3

		// Verify old indexes cleaned up (if necessary)
		assert.Equal(t, 1, len(sd.rgAzIndex["rg1"])) // Only az1 left (node1)

		// Verify new indexes created
		assert.Contains(t, sd.azRgIndex, "az3")
		assert.Contains(t, sd.rgAzIndex, "rg3")
		assert.Equal(t, 1, len(sd.azRgIndex["az3"])) // rg3
		assert.Equal(t, 1, len(sd.rgAzIndex["rg3"])) // az3

		// Verify query functionality
		filter := &proto.NodeFilter{ResourceGroup: "rg3", Limit: 1}
		nodes, err := sd.SelectRandom(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(nodes))
		assert.Equal(t, "node2", nodes[0].NodeId)
	})

	t.Run("Multiple node removals update indexes correctly", func(t *testing.T) {
		// Remove node3 and node4 (all nodes in rg2)
		sd.RemoveServer("node3")
		sd.RemoveServer("node4")

		// Verify nodes removed
		assert.Equal(t, 2, len(sd.Nodes)) // Only node1 and node2 left
		assert.NotContains(t, sd.Nodes, "node3")
		assert.NotContains(t, sd.Nodes, "node4")

		// Verify indexes properly cleaned up
		assert.Equal(t, 2, len(sd.azList)) // az1, az3 (az2 should be cleaned up)
		assert.Equal(t, 2, len(sd.rgList)) // rg1, rg3 (rg2 should be cleaned up)
		assert.NotContains(t, sd.azList, "az2")
		assert.NotContains(t, sd.rgList, "rg2")

		// Verify related indexes cleaned up
		assert.NotContains(t, sd.azRgIndex, "az2")
		assert.NotContains(t, sd.rgAzIndex, "rg2")

		// Verify query for rg2 should return empty
		filter := &proto.NodeFilter{ResourceGroup: "rg2", Limit: 1}
		nodes, err := sd.SelectRandom(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(nodes))
	})

	t.Run("Remove non-existent node is safe", func(t *testing.T) {
		initialCount := len(sd.Nodes)

		// Removing non-existent node should be safe operation
		sd.RemoveServer("non-existent-node")

		// Verify no impact
		assert.Equal(t, initialCount, len(sd.Nodes))
	})
}

// === Concurrent operations tests ===

func TestConcurrentOperations(t *testing.T) {
	t.Run("Concurrent updates and queries", func(t *testing.T) {
		sd := NewServiceDiscovery()

		// Create initial data
		for i := 0; i < 50; i++ {
			nodeID := fmt.Sprintf("initial-node-%d", i)
			rg := fmt.Sprintf("rg-%d", i%5)
			az := fmt.Sprintf("az-%d", i%3)
			tags := map[string]string{"env": "prod", "initial": "true"}
			node := createFinalTestNode(nodeID, rg, az, tags)
			sd.UpdateServer(nodeID, node)
		}

		// Channels for concurrent operations
		done := make(chan bool)
		errorsChan := make(chan error, 100)

		// Start concurrent update goroutine
		go func() {
			defer close(done)
			for i := 0; i < 100; i++ {
				nodeID := fmt.Sprintf("update-node-%d", i)
				rg := fmt.Sprintf("rg-%d", i%7)
				az := fmt.Sprintf("az-%d", i%4)
				tags := map[string]string{
					"env":     "staging",
					"batch":   "concurrent",
					"counter": fmt.Sprintf("%d", i),
				}
				node := createFinalTestNode(nodeID, rg, az, tags)
				sd.UpdateServer(nodeID, node)

				// Occasionally remove some nodes
				if i%10 == 0 && i > 0 {
					sd.RemoveServer(fmt.Sprintf("update-node-%d", i-5))
				}
			}
		}()

		// Start concurrent query goroutines
		for worker := 0; worker < 5; worker++ {
			go func(workerID int) {
				for i := 0; i < 50; i++ {
					// Test different query strategies
					strategies := []func(*proto.NodeFilter, proto.AffinityMode) ([]*proto.NodeMeta, error){
						sd.SelectSingleAzSingleRg,
						sd.SelectSingleAzMultiRg,
						sd.SelectMultiAzSingleRg,
						sd.SelectMultiAzMultiRg,
						sd.SelectRandom,
					}

					strategy := strategies[i%len(strategies)]
					filter := &proto.NodeFilter{
						Limit: 3,
					}

					// Occasionally add filter conditions
					if i%3 == 0 {
						filter.ResourceGroup = fmt.Sprintf("rg-%d", i%7)
					}
					if i%5 == 0 {
						filter.Az = fmt.Sprintf("az-%d", i%4)
					}

					_, err := strategy(filter, proto.AffinityMode_SOFT)
					if err != nil {
						select {
						case errorsChan <- fmt.Errorf("worker %d iteration %d: %w", workerID, i, err):
						default:
						}
					}
				}
			}(worker)
		}

		// Wait for updates to complete
		<-done

		// Check for errors
		close(errorsChan)
		var errors []error
		for err := range errorsChan {
			errors = append(errors, err)
		}

		// Concurrent operations should not produce errors
		if len(errors) > 0 {
			t.Fatalf("Concurrent operations produced %d errors, first error: %v", len(errors), errors[0])
		}

		// Verify final state consistency
		assert.Greater(t, len(sd.Nodes), 50, "Should have initial nodes plus new nodes")
		assert.LessOrEqual(t, len(sd.Nodes), 150, "Should not exceed expected maximum node count")

		// Verify index consistency
		for az, rgMap := range sd.azRgIndex {
			assert.Contains(t, sd.azList, az, "AZ in azRgIndex should be in azList")
			for rg := range rgMap {
				assert.Contains(t, sd.rgList, rg, "RG in azRgIndex should be in rgList")
			}
		}

		for rg, azMap := range sd.rgAzIndex {
			assert.Contains(t, sd.rgList, rg, "RG in rgAzIndex should be in rgList")
			for az := range azMap {
				assert.Contains(t, sd.azList, az, "AZ in rgAzIndex should be in azList")
			}
		}
	})

	t.Run("Concurrent updates same node", func(t *testing.T) {
		sd := NewServiceDiscovery()

		numWorkers := 10
		updatesPerWorker := 20
		nodeID := "contested-node"

		// Concurrent updates to the same node
		var wg sync.WaitGroup
		for worker := 0; worker < numWorkers; worker++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for i := 0; i < updatesPerWorker; i++ {
					rg := fmt.Sprintf("rg-%d", workerID)
					az := fmt.Sprintf("az-%d", i%3)
					tags := map[string]string{
						"worker": fmt.Sprintf("%d", workerID),
						"update": fmt.Sprintf("%d", i),
						"env":    "concurrent-test",
					}
					node := createFinalTestNode(nodeID, rg, az, tags)
					sd.UpdateServer(nodeID, node)
				}
			}(worker)
		}

		wg.Wait()

		// Verify final state
		assert.Equal(t, 1, len(sd.Nodes), "Should only have one node")
		finalNode := sd.Nodes[nodeID]
		assert.NotNil(t, finalNode, "Node should exist")
		assert.Equal(t, "concurrent-test", finalNode.Tags["env"], "Node should have correct tag")

		// Verify index consistency
		foundInIndex := false
		for az, rgMap := range sd.azRgIndex {
			if len(rgMap[finalNode.ResourceGroup]) > 0 {
				assert.Equal(t, finalNode.Az, az, "Node should be in correct AZ index")
				foundInIndex = true
				break
			}
		}
		assert.True(t, foundInIndex, "Node should be found in index")
	})

	t.Run("Concurrent add and remove operations", func(t *testing.T) {
		sd := NewServiceDiscovery()

		numWorkers := 5
		opsPerWorker := 50

		var wg sync.WaitGroup

		// Concurrent add and remove operations
		for worker := 0; worker < numWorkers; worker++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for i := 0; i < opsPerWorker; i++ {
					nodeID := fmt.Sprintf("worker%d-node%d", workerID, i)

					// Add node
					rg := fmt.Sprintf("rg-%d", workerID)
					az := fmt.Sprintf("az-%d", i%3)
					tags := map[string]string{"worker": fmt.Sprintf("%d", workerID)}
					node := createFinalTestNode(nodeID, rg, az, tags)
					sd.UpdateServer(nodeID, node)

					// Occasionally remove previous nodes
					if i > 5 && i%7 == 0 {
						oldNodeID := fmt.Sprintf("worker%d-node%d", workerID, i-5)
						sd.RemoveServer(oldNodeID)
					}
				}
			}(worker)
		}

		wg.Wait()

		// Verify final state consistency
		// Calculate expected node count (each worker should have last few nodes remaining)
		expectedMinNodes := numWorkers * 5 // Each worker should have at least last 5 nodes
		assert.GreaterOrEqual(t, len(sd.Nodes), expectedMinNodes, "Should have reasonable number of nodes")

		// Verify all nodes are in correct indexes - check index consistency, not specific node references
		for nodeID, node := range sd.Nodes {
			// Verify node in AZ-RG index
			azRgMap, exists := sd.azRgIndex[node.Az]
			assert.True(t, exists, "Node's AZ should be in azRgIndex: %s", nodeID)

			rgNodes, exists := azRgMap[node.ResourceGroup]
			assert.True(t, exists, "Node's RG should be in azRgIndex[AZ]: %s", nodeID)

			// Verify node ID in slice (not pointer reference)
			found := false
			for _, n := range rgNodes {
				if n.NodeId == nodeID {
					found = true
					break
				}
			}
			assert.True(t, found, "Node should be in azRgIndex[AZ][RG]: %s", nodeID)

			// Verify node in RG-AZ index
			rgAzMap, exists := sd.rgAzIndex[node.ResourceGroup]
			assert.True(t, exists, "Node's RG should be in rgAzIndex: %s", nodeID)

			azNodes, exists := rgAzMap[node.Az]
			assert.True(t, exists, "Node's AZ should be in rgAzIndex[RG]: %s", nodeID)

			// Verify node ID in slice (not pointer reference)
			found = false
			for _, n := range azNodes {
				if n.NodeId == nodeID {
					found = true
					break
				}
			}
			assert.True(t, found, "Node should be in rgAzIndex[RG][AZ]: %s", nodeID)
		}
	})
}

func TestPerformanceValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	sd := NewServiceDiscovery()

	// Create large amount of test data
	for rg := 0; rg < 10; rg++ {
		for az := 0; az < 10; az++ {
			for node := 0; node < 10; node++ {
				nodeID := fmt.Sprintf("node-rg%d-az%d-%d", rg, az, node)
				rgName := fmt.Sprintf("rg-%d", rg)
				azName := fmt.Sprintf("az-%d", az)
				tags := map[string]string{
					"env":  []string{"prod", "staging", "test"}[node%3],
					"role": []string{"writer", "reader"}[node%2],
				}

				nodeMeta := createFinalTestNode(nodeID, rgName, azName, tags)
				sd.UpdateServer(nodeID, nodeMeta)
			}
		}
	}

	t.Run("Verify index sizes", func(t *testing.T) {
		assert.Equal(t, 1000, len(sd.Nodes))   // 10*10*10 = 1000 nodes
		assert.Equal(t, 10, len(sd.azList))    // 10 AZs
		assert.Equal(t, 10, len(sd.rgList))    // 10 RGs
		assert.Equal(t, 10, len(sd.azRgIndex)) // 10 AZs
		assert.Equal(t, 10, len(sd.rgAzIndex)) // 10 RGs

		// Verify structure of each index
		for _, azRgMap := range sd.azRgIndex {
			assert.Equal(t, 10, len(azRgMap)) // Each AZ has 10 RGs
		}
		for _, rgAzMap := range sd.rgAzIndex {
			assert.Equal(t, 10, len(rgAzMap)) // Each RG has 10 AZs
		}
	})

	t.Run("Performance test all strategies", func(t *testing.T) {
		// These queries should all be O(1) time complexity
		filter := &proto.NodeFilter{Limit: 5}

		// SingleAzSingleRg: O(1) - select one AZ one RG, randomly select limit nodes
		nodes, err := sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.LessOrEqual(t, len(nodes), 5) // May be less than 5, depends on nodes in that AZ-RG combination

		// SingleAzMultiRg: O(1)
		nodes, err = sd.SelectSingleAzMultiRg(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(nodes))

		// MultiAzSingleRg: O(1)
		nodes, err = sd.SelectMultiAzSingleRg(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(nodes))

		// MultiAzMultiRg: O(1)
		nodes, err = sd.SelectMultiAzMultiRg(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(nodes))

		// SelectRandom: O(number of AZ * RG combinations)
		nodes, err = sd.SelectRandom(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(nodes))
	})
}

// === Fallback mechanism tests ===

func TestFallbackMechanism(t *testing.T) {
	t.Run("Fallback mechanism ensures 100% correctness in SOFT mode", func(t *testing.T) {
		sd := NewServiceDiscovery()

		// Create a "needle" distribution: only a specific AZ-RG combination has qualifying nodes
		nodes := []*proto.NodeMeta{
			// az1: all env=staging
			createFinalTestNode("node1", "rg1", "az1", map[string]string{"env": "staging"}),
			createFinalTestNode("node2", "rg2", "az1", map[string]string{"env": "staging"}),
			createFinalTestNode("node3", "rg3", "az1", map[string]string{"env": "staging"}),

			// az2: all env=test
			createFinalTestNode("node4", "rg1", "az2", map[string]string{"env": "test"}),
			createFinalTestNode("node5", "rg2", "az2", map[string]string{"env": "test"}),
			createFinalTestNode("node6", "rg3", "az2", map[string]string{"env": "test"}),

			// az3: all env=staging
			createFinalTestNode("node7", "rg1", "az3", map[string]string{"env": "staging"}),
			createFinalTestNode("node8", "rg2", "az3", map[string]string{"env": "staging"}),
			createFinalTestNode("node9", "rg3", "az3", map[string]string{"env": "staging"}),

			// Only az4-rg1 has one env=prod node (needle distribution)
			createFinalTestNode("node10", "rg1", "az4", map[string]string{"env": "prod"}),
			createFinalTestNode("node11", "rg2", "az4", map[string]string{"env": "staging"}),
			createFinalTestNode("node12", "rg3", "az4", map[string]string{"env": "test"}),
		}

		for _, node := range nodes {
			sd.UpdateServer(node.NodeId, node)
		}

		// Search for env=prod nodes (only node10 qualifies)
		filter := &proto.NodeFilter{
			Tags:  map[string]string{"env": "prod"},
			Limit: 1,
		}

		// Test multiple times, should always find node10
		successCount := 0
		totalAttempts := 50

		for i := 0; i < totalAttempts; i++ {
			nodes, err := sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_SOFT)
			assert.NoError(t, err)

			if len(nodes) > 0 {
				successCount++
				assert.Equal(t, "node10", nodes[0].NodeId, "Should always find the unique qualifying node")
				assert.Equal(t, "prod", nodes[0].Tags["env"])
			}
		}

		// Fallback mechanism should ensure 100% success rate
		t.Logf("Success rate: %d/%d (%.1f%%)", successCount, totalAttempts, float64(successCount)/float64(totalAttempts)*100)
		assert.Equal(t, totalAttempts, successCount, "Fallback mechanism should ensure 100% success rate")
	})

	t.Run("Performance impact analysis of fallback mechanism", func(t *testing.T) {
		sd := NewServiceDiscovery()

		// Create a medium-scale dataset
		for az := 1; az <= 5; az++ {
			for rg := 1; rg <= 4; rg++ {
				for node := 1; node <= 3; node++ {
					azName := fmt.Sprintf("az%d", az)
					rgName := fmt.Sprintf("rg%d", rg)
					nodeID := fmt.Sprintf("%s-%s-node%d", azName, rgName, node)

					node := createFinalTestNode(nodeID, rgName, azName, map[string]string{"env": "prod"})
					sd.UpdateServer(node.NodeId, node)
				}
			}
		}

		filter := &proto.NodeFilter{
			Tags:  map[string]string{"env": "prod"},
			Limit: 1,
		}

		// Benchmark: measure performance with fallback mechanism
		start := time.Now()
		iterations := 1000

		for i := 0; i < iterations; i++ {
			nodes, err := sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_SOFT)
			assert.NoError(t, err)
			assert.Greater(t, len(nodes), 0)
		}

		duration := time.Since(start)
		avgTime := duration.Nanoseconds() / int64(iterations)

		t.Logf("Average query time: %d ns/op", avgTime)

		// Even with fallback mechanism, performance should be within reasonable range (< 10μs)
		assert.Less(t, avgTime, int64(10000), "Fallback mechanism should not significantly impact performance")
	})
}

func TestHardModeFallbackNecessity(t *testing.T) {
	t.Run("HARD mode requires fallback to ensure correct judgment", func(t *testing.T) {
		sd := NewServiceDiscovery()

		// Create an extremely sparse scenario: 5 AZs, each AZ has 3 RGs, but only one specific combination has target nodes
		for az := 1; az <= 5; az++ {
			for rg := 1; rg <= 3; rg++ {
				azName := fmt.Sprintf("az%d", az)
				rgName := fmt.Sprintf("rg%d", rg)
				nodeID := fmt.Sprintf("%s-%s-node", azName, rgName)

				// Only az3-rg2 has env=critical node
				var env string
				if az == 3 && rg == 2 {
					env = "critical"
				} else {
					env = "normal"
				}

				node := createFinalTestNode(nodeID, rgName, azName, map[string]string{"env": env})
				sd.UpdateServer(node.NodeId, node)
			}
		}

		// Test search in HARD mode
		filter := &proto.NodeFilter{
			Tags:  map[string]string{"env": "critical"},
			Limit: 1,
		}

		t.Run("HARD mode should find existing nodes", func(t *testing.T) {
			// Test multiple times to verify effectiveness of fallback mechanism
			for i := 0; i < 20; i++ {
				nodes, err := sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_HARD)
				assert.NoError(t, err, "HARD mode should find existing nodes")
				assert.Equal(t, 1, len(nodes), "Should return exactly 1 node")
				assert.Equal(t, "critical", nodes[0].Tags["env"], "Returned node should match condition")
				assert.Equal(t, "az3-rg2-node", nodes[0].NodeId, "Should return correct node")
			}
		})

		t.Run("HARD mode should correctly report non-existent nodes", func(t *testing.T) {
			// Search for non-existent tag
			invalidFilter := &proto.NodeFilter{
				Tags:  map[string]string{"env": "nonexistent"},
				Limit: 1,
			}

			nodes, err := sd.SelectSingleAzSingleRg(invalidFilter, proto.AffinityMode_HARD)
			assert.Error(t, err, "HARD mode should return error when nodes not found")
			assert.Nil(t, nodes, "Should return nil")
			assert.Contains(t, err.Error(), "no matching nodes found", "Error message should be clear")
		})
	})

	t.Run("Verify consistency between HARD and SOFT modes in same scenario", func(t *testing.T) {
		sd := NewServiceDiscovery()

		// Create test data
		nodes := []*proto.NodeMeta{
			createFinalTestNode("node1", "rg1", "az1", map[string]string{"type": "worker"}),
			createFinalTestNode("node2", "rg1", "az2", map[string]string{"type": "worker"}),
			createFinalTestNode("node3", "rg2", "az1", map[string]string{"type": "master"}),
		}

		for _, node := range nodes {
			sd.UpdateServer(node.NodeId, node)
		}

		testCases := []struct {
			name       string
			filter     *proto.NodeFilter
			shouldFind bool
		}{
			{
				name: "Existing node type",
				filter: &proto.NodeFilter{
					Tags:  map[string]string{"type": "worker"},
					Limit: 1,
				},
				shouldFind: true,
			},
			{
				name: "Non-existent node type",
				filter: &proto.NodeFilter{
					Tags:  map[string]string{"type": "nonexistent"},
					Limit: 1,
				},
				shouldFind: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// HARD mode test
				hardNodes, hardErr := sd.SelectSingleAzSingleRg(tc.filter, proto.AffinityMode_HARD)

				// SOFT mode test
				softNodes, softErr := sd.SelectSingleAzSingleRg(tc.filter, proto.AffinityMode_SOFT)

				if tc.shouldFind {
					// Should find nodes
					assert.NoError(t, hardErr, "HARD mode should find nodes")
					assert.NoError(t, softErr, "SOFT mode should find nodes")
					assert.Greater(t, len(hardNodes), 0, "HARD mode should return nodes")
					assert.Greater(t, len(softNodes), 0, "SOFT mode should return nodes")

					// Verify returned nodes actually meet conditions
					for _, node := range hardNodes {
						assert.Equal(t, tc.filter.Tags["type"], node.Tags["type"])
					}
					for _, node := range softNodes {
						assert.Equal(t, tc.filter.Tags["type"], node.Tags["type"])
					}
				} else {
					// Should not find nodes
					assert.Error(t, hardErr, "HARD mode should return error")
					assert.NoError(t, softErr, "SOFT mode should not return error")
					assert.Nil(t, hardNodes, "HARD mode should return nil")
					assert.Equal(t, 0, len(softNodes), "SOFT mode should return empty array")
				}
			})
		}
	})
}

func TestAllStrategiesFallbackMechanism(t *testing.T) {
	sd := NewServiceDiscovery()

	// Create a complex test scenario: multiple AZs and RGs, but only specific combinations have target nodes
	testData := []struct {
		nodeID string
		rg     string
		az     string
		tags   map[string]string
	}{
		// Most nodes are "normal" environment
		{"node1", "rg1", "az1", map[string]string{"env": "normal", "role": "worker"}},
		{"node2", "rg1", "az2", map[string]string{"env": "normal", "role": "worker"}},
		{"node3", "rg1", "az3", map[string]string{"env": "normal", "role": "worker"}},
		{"node4", "rg2", "az1", map[string]string{"env": "normal", "role": "worker"}},
		{"node5", "rg2", "az2", map[string]string{"env": "normal", "role": "worker"}},
		{"node6", "rg2", "az3", map[string]string{"env": "normal", "role": "worker"}},
		{"node7", "rg3", "az1", map[string]string{"env": "normal", "role": "worker"}},
		{"node8", "rg3", "az2", map[string]string{"env": "normal", "role": "worker"}},

		// Only a few nodes are "critical" environment
		{"critical1", "rg1", "az3", map[string]string{"env": "critical", "role": "master"}},
		{"critical2", "rg3", "az2", map[string]string{"env": "critical", "role": "master"}},
	}

	for _, data := range testData {
		node := createFinalTestNode(data.nodeID, data.rg, data.az, data.tags)
		sd.UpdateServer(node.NodeId, node)
	}

	// Define strategy methods for testing
	strategies := []struct {
		name string
		fn   func(*proto.NodeFilter, proto.AffinityMode) ([]*proto.NodeMeta, error)
	}{
		{"SelectSingleAzSingleRg", sd.SelectSingleAzSingleRg},
		{"SelectSingleAzMultiRg", sd.SelectSingleAzMultiRg},
		{"SelectMultiAzSingleRg", sd.SelectMultiAzSingleRg},
		{"SelectMultiAzMultiRg", sd.SelectMultiAzMultiRg},
		{"SelectRandom", sd.SelectRandom},
		{"SelectCustom", sd.SelectCustom},
	}

	t.Run("All strategies can find sparse critical nodes", func(t *testing.T) {
		filter := &proto.NodeFilter{
			Tags:  map[string]string{"env": "critical"},
			Limit: 1,
		}

		for _, strategy := range strategies {
			t.Run(strategy.name, func(t *testing.T) {
				// Test HARD mode
				successCount := 0
				totalAttempts := 10

				for i := 0; i < totalAttempts; i++ {
					nodes, err := strategy.fn(filter, proto.AffinityMode_HARD)
					assert.NoError(t, err, "HARD mode should find critical nodes")

					if len(nodes) > 0 {
						successCount++
						// Verify returned node is indeed critical environment
						assert.Equal(t, "critical", nodes[0].Tags["env"], "Returned node should match condition")
						assert.Contains(t, []string{"critical1", "critical2"}, nodes[0].NodeId, "Should return correct critical node")
					}
				}

				t.Logf("%s HARD mode success rate: %d/%d (%.1f%%)",
					strategy.name, successCount, totalAttempts, float64(successCount)/float64(totalAttempts)*100)

				// All strategies should have high success rate (guaranteed by fallback mechanism)
				assert.GreaterOrEqual(t, float64(successCount)/float64(totalAttempts), 0.8,
					"Fallback mechanism should ensure 80%+ success rate")

				// Test SOFT mode
				softNodes, softErr := strategy.fn(filter, proto.AffinityMode_SOFT)
				assert.NoError(t, softErr, "SOFT mode should not return error")
				if len(softNodes) > 0 {
					assert.Equal(t, "critical", softNodes[0].Tags["env"], "SOFT mode returned node should match condition")
				}
			})
		}
	})

	t.Run("All strategies correctly handle non-existent nodes", func(t *testing.T) {
		filter := &proto.NodeFilter{
			Tags:  map[string]string{"env": "nonexistent"},
			Limit: 1,
		}

		for _, strategy := range strategies {
			t.Run(strategy.name, func(t *testing.T) {
				// HARD mode should return error
				hardNodes, hardErr := strategy.fn(filter, proto.AffinityMode_HARD)
				assert.Error(t, hardErr, "HARD mode should return error when nodes not found")
				assert.Nil(t, hardNodes, "HARD mode should return nil")

				// SOFT mode should return empty array
				softNodes, softErr := strategy.fn(filter, proto.AffinityMode_SOFT)
				assert.NoError(t, softErr, "SOFT mode should not return error")
				assert.Equal(t, 0, len(softNodes), "SOFT mode should return empty array")
			})
		}
	})

	t.Run("Verify performance impact of fallback mechanism", func(t *testing.T) {
		filter := &proto.NodeFilter{
			Tags:  map[string]string{"env": "critical"},
			Limit: 1,
		}

		// Measure average execution time for each strategy
		for _, strategy := range strategies {
			t.Run(strategy.name, func(t *testing.T) {
				start := time.Now()
				iterations := 100

				for i := 0; i < iterations; i++ {
					nodes, err := strategy.fn(filter, proto.AffinityMode_SOFT)
					assert.NoError(t, err)
					_ = nodes // Use return value to avoid optimization
				}

				duration := time.Since(start)
				avgTime := duration.Nanoseconds() / int64(iterations)

				t.Logf("%s average execution time: %d ns/op", strategy.name, avgTime)

				// Even with fallback mechanism, performance should be within reasonable range (< 50μs)
				assert.Less(t, avgTime, int64(50000), "Fallback mechanism should not significantly impact performance")
			})
		}
	})
}

// === Extreme cases tests ===

func TestExtremeCasesReturnEmpty(t *testing.T) {
	t.Run("Extreme case 1: All nodes don't match tag filter", func(t *testing.T) {
		sd := NewServiceDiscovery()

		// Create some nodes, but all are env=prod
		nodes := []*proto.NodeMeta{
			createFinalTestNode("node1", "rg1", "az1", map[string]string{"env": "prod"}),
			createFinalTestNode("node2", "rg1", "az2", map[string]string{"env": "prod"}),
			createFinalTestNode("node3", "rg2", "az1", map[string]string{"env": "prod"}),
		}

		for _, node := range nodes {
			sd.UpdateServer(node.NodeId, node)
		}

		// Search for env=staging nodes (don't exist)
		filter := &proto.NodeFilter{
			Tags:  map[string]string{"env": "staging"},
			Limit: 1,
		}

		nodes, err := sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(nodes), "SOFT mode should return empty array instead of error")

		// HARD mode should return error
		nodes, err = sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_HARD)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no matching nodes found")
	})

	t.Run("Extreme case 2: AZ filter doesn't match any nodes", func(t *testing.T) {
		sd := NewServiceDiscovery()

		// Create nodes in az1, az2
		nodes := []*proto.NodeMeta{
			createFinalTestNode("node1", "rg1", "az1", map[string]string{"env": "prod"}),
			createFinalTestNode("node2", "rg1", "az2", map[string]string{"env": "prod"}),
		}

		for _, node := range nodes {
			sd.UpdateServer(node.NodeId, node)
		}

		// Search for az3 (doesn't exist)
		filter := &proto.NodeFilter{
			Az:    "az3",
			Limit: 1,
		}

		nodes, err := sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(nodes), "SOFT mode should return empty array")

		// HARD mode should return error
		nodes, err = sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_HARD)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no matching AZs found")
	})

	t.Run("Extreme case 3: RG filter doesn't match any nodes", func(t *testing.T) {
		sd := NewServiceDiscovery()

		// Create nodes in rg1, rg2
		nodes := []*proto.NodeMeta{
			createFinalTestNode("node1", "rg1", "az1", map[string]string{"env": "prod"}),
			createFinalTestNode("node2", "rg2", "az1", map[string]string{"env": "prod"}),
		}

		for _, node := range nodes {
			sd.UpdateServer(node.NodeId, node)
		}

		// Search for rg3 (doesn't exist)
		filter := &proto.NodeFilter{
			ResourceGroup: "rg3",
			Limit:         1,
		}

		nodes, err := sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(nodes), "SOFT mode should return empty array")
	})

	t.Run("Extreme case 4: Regex doesn't match", func(t *testing.T) {
		sd := NewServiceDiscovery()

		// Create nodes
		nodes := []*proto.NodeMeta{
			createFinalTestNode("node1", "prod-us", "us-east-1a", map[string]string{"env": "prod"}),
			createFinalTestNode("node2", "prod-eu", "eu-west-1a", map[string]string{"env": "prod"}),
		}

		for _, node := range nodes {
			sd.UpdateServer(node.NodeId, node)
		}

		// Search for non-matching regex
		filter := &proto.NodeFilter{
			ResourceGroup: "staging-.*", // Doesn't match any RG
			Limit:         1,
		}

		nodes, err := sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(nodes), "Should return empty array when regex doesn't match")
	})

	t.Run("Extreme case 5: Strict composite filter matching", func(t *testing.T) {
		sd := NewServiceDiscovery()

		// Create sparse data distribution
		nodes := []*proto.NodeMeta{
			createFinalTestNode("node1", "rg1", "az1", map[string]string{"env": "prod", "role": "writer"}),
			createFinalTestNode("node2", "rg1", "az2", map[string]string{"env": "prod", "role": "reader"}),
			createFinalTestNode("node3", "rg2", "az1", map[string]string{"env": "staging", "role": "writer"}),
			createFinalTestNode("node4", "rg2", "az2", map[string]string{"env": "staging", "role": "reader"}),
		}

		for _, node := range nodes {
			sd.UpdateServer(node.NodeId, node)
		}

		// Search for very strict combination: specific AZ + specific RG + specific tags
		filter := &proto.NodeFilter{
			Az:            "az1",
			ResourceGroup: "rg2",
			Tags:          map[string]string{"env": "prod", "role": "reader"}, // This combination doesn't exist
			Limit:         1,
		}

		// Run multiple tests since it's random selection
		emptyCount := 0
		totalAttempts := 20

		for i := 0; i < totalAttempts; i++ {
			nodes, err := sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_SOFT)
			assert.NoError(t, err)
			if len(nodes) == 0 {
				emptyCount++
			}
		}

		// Should return empty most of the time (since no nodes meet criteria)
		assert.Greater(t, emptyCount, totalAttempts/2, "Strict filter conditions should often return empty results")
	})
}

func TestRetryMechanismEffectiveness(t *testing.T) {
	t.Run("Retry mechanism improves success rate", func(t *testing.T) {
		sd := NewServiceDiscovery()

		// Create a "might succeed but unstable" scenario
		nodes := []*proto.NodeMeta{
			// az1 only has one non-matching node
			createFinalTestNode("node1", "rg1", "az1", map[string]string{"env": "staging"}),
			// az2 has one matching node
			createFinalTestNode("node2", "rg1", "az2", map[string]string{"env": "prod"}),
			// az3 only has one non-matching node
			createFinalTestNode("node3", "rg1", "az3", map[string]string{"env": "test"}),
		}

		for _, node := range nodes {
			sd.UpdateServer(node.NodeId, node)
		}

		// Search for env=prod, only az2's node qualifies
		filter := &proto.NodeFilter{
			Tags:  map[string]string{"env": "prod"},
			Limit: 1,
		}

		// Multiple attempts, count success rate
		successCount := 0
		totalAttempts := 100

		for i := 0; i < totalAttempts; i++ {
			nodes, err := sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_SOFT)
			assert.NoError(t, err)
			if len(nodes) > 0 {
				successCount++
				assert.Equal(t, "prod", nodes[0].Tags["env"])
			}
		}

		// Retry mechanism should provide quite high success rate (theoretically 1/3 * retry times)
		expectedMinSuccessRate := 0.50 // At least 50% success rate
		actualSuccessRate := float64(successCount) / float64(totalAttempts)

		t.Logf("Actual success rate: %.2f%%, Expected minimum success rate: %.2f%%",
			actualSuccessRate*100, expectedMinSuccessRate*100)

		assert.GreaterOrEqual(t, actualSuccessRate, expectedMinSuccessRate,
			"Retry mechanism should significantly improve success rate")
	})
}

// === SelectRandomGroup tests ===

func TestSelectRandomGroup_BasicGrouping(t *testing.T) {
	sd := NewServiceDiscovery()

	// Create 9 nodes across different AZs/RGs
	for i := 1; i <= 9; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		rg := fmt.Sprintf("rg%d", (i-1)%3+1)
		az := fmt.Sprintf("az%d", (i-1)/3+1)
		node := createFinalTestNode(nodeID, rg, az, map[string]string{"env": "prod"})
		sd.UpdateServer(nodeID, node)
	}

	// Select groups of 3 from 9 nodes → should form 3 non-overlapping groups
	filter := &proto.NodeFilter{Limit: 3}
	nodes, err := sd.SelectRandomGroup(filter, proto.AffinityMode_SOFT)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(nodes), "Should return exactly limit nodes")

	// Verify all returned nodes are from the same group (consecutive in sorted order)
	// Since nodes are sorted by NodeId, the groups should be deterministic
	nodeIDs := make([]string, len(nodes))
	for i, n := range nodes {
		nodeIDs[i] = n.NodeId
	}

	// Verify nodes are sorted within the group (they come from a sorted slice)
	for i := 1; i < len(nodeIDs); i++ {
		assert.True(t, nodeIDs[i-1] < nodeIDs[i], "Nodes within a group should be in sorted order")
	}
}

func TestSelectRandomGroup_NoOverlapBetweenGroups(t *testing.T) {
	sd := NewServiceDiscovery()

	// Create 6 nodes
	for i := 1; i <= 6; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		node := createFinalTestNode(nodeID, "rg1", "az1", map[string]string{"env": "prod"})
		sd.UpdateServer(nodeID, node)
	}

	// With limit=3, should form 2 groups: [node1,node2,node3] and [node4,node5,node6]
	filter := &proto.NodeFilter{Limit: 3}

	// Track which groups we see over multiple runs
	groupSeen := make(map[string]int) // group key → count
	iterations := 100

	for i := 0; i < iterations; i++ {
		nodes, err := sd.SelectRandomGroup(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(nodes))

		// Build a group key from sorted node IDs
		ids := make([]string, len(nodes))
		for j, n := range nodes {
			ids[j] = n.NodeId
		}
		key := fmt.Sprintf("%v", ids)
		groupSeen[key]++
	}

	// Should see exactly 2 distinct groups
	assert.Equal(t, 2, len(groupSeen), "Should see exactly 2 non-overlapping groups")

	// Both groups should be selected roughly equally (with some randomness tolerance)
	for key, count := range groupSeen {
		t.Logf("Group %s: selected %d/%d times", key, count, iterations)
		assert.Greater(t, count, 10, "Each group should be selected a reasonable number of times")
	}
}

func TestSelectRandomGroup_PartialGroup(t *testing.T) {
	sd := NewServiceDiscovery()

	// Create 7 nodes → with limit=3, groups are [0-2], [3-5], partial [6]
	for i := 1; i <= 7; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		node := createFinalTestNode(nodeID, "rg1", "az1", map[string]string{"env": "prod"})
		sd.UpdateServer(nodeID, node)
	}

	filter := &proto.NodeFilter{Limit: 3}

	// Run multiple times - should always return exactly 3 nodes
	for i := 0; i < 50; i++ {
		nodes, err := sd.SelectRandomGroup(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(nodes), "Should always return exactly limit nodes even with partial last group")
	}
}

func TestSelectRandomGroup_FilteredNodes(t *testing.T) {
	sd := NewServiceDiscovery()

	// Create nodes across different AZs and RGs with different tags
	sd.UpdateServer("node1", createFinalTestNode("node1", "rg1", "az1", map[string]string{"env": "prod"}))
	sd.UpdateServer("node2", createFinalTestNode("node2", "rg1", "az1", map[string]string{"env": "staging"}))
	sd.UpdateServer("node3", createFinalTestNode("node3", "rg1", "az2", map[string]string{"env": "prod"}))
	sd.UpdateServer("node4", createFinalTestNode("node4", "rg2", "az1", map[string]string{"env": "prod"}))
	sd.UpdateServer("node5", createFinalTestNode("node5", "rg2", "az2", map[string]string{"env": "prod"}))

	// Filter by AZ
	t.Run("AZ filter", func(t *testing.T) {
		filter := &proto.NodeFilter{Az: "az1", Limit: 2}
		nodes, err := sd.SelectRandomGroup(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(nodes))
		for _, n := range nodes {
			assert.Equal(t, "az1", n.Az)
		}
	})

	// Filter by tags
	t.Run("Tag filter", func(t *testing.T) {
		filter := &proto.NodeFilter{Tags: map[string]string{"env": "prod"}, Limit: 2}
		nodes, err := sd.SelectRandomGroup(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(nodes))
		for _, n := range nodes {
			assert.Equal(t, "prod", n.Tags["env"])
		}
	})

	// Filter by RG
	t.Run("RG filter", func(t *testing.T) {
		filter := &proto.NodeFilter{ResourceGroup: "rg1", Limit: 2}
		nodes, err := sd.SelectRandomGroup(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.LessOrEqual(t, len(nodes), 2)
	})
}

func TestSelectRandomGroup_InsufficientNodes(t *testing.T) {
	sd := NewServiceDiscovery()

	// Only 2 nodes but limit=3
	sd.UpdateServer("node1", createFinalTestNode("node1", "rg1", "az1", map[string]string{"env": "prod"}))
	sd.UpdateServer("node2", createFinalTestNode("node2", "rg1", "az1", map[string]string{"env": "prod"}))

	t.Run("Limit exceeds candidates returns all", func(t *testing.T) {
		filter := &proto.NodeFilter{Limit: 3}
		nodes, err := sd.SelectRandomGroup(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(nodes), "Should return all available nodes when limit exceeds count")
	})

	t.Run("No matching nodes HARD mode", func(t *testing.T) {
		filter := &proto.NodeFilter{Tags: map[string]string{"env": "nonexistent"}, Limit: 1}
		nodes, err := sd.SelectRandomGroup(filter, proto.AffinityMode_HARD)
		assert.Error(t, err)
		assert.Nil(t, nodes)
		assert.Contains(t, err.Error(), "no matching nodes found")
	})

	t.Run("No matching nodes SOFT mode", func(t *testing.T) {
		filter := &proto.NodeFilter{Tags: map[string]string{"env": "nonexistent"}, Limit: 1}
		nodes, err := sd.SelectRandomGroup(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(nodes))
	})

	t.Run("Limit zero returns all", func(t *testing.T) {
		filter := &proto.NodeFilter{Limit: 0}
		nodes, err := sd.SelectRandomGroup(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(nodes))
	})

	t.Run("Empty service discovery", func(t *testing.T) {
		emptySd := NewServiceDiscovery()
		filter := &proto.NodeFilter{Limit: 3}
		nodes, err := emptySd.SelectRandomGroup(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(nodes))

		nodes, err = emptySd.SelectRandomGroup(filter, proto.AffinityMode_HARD)
		assert.Error(t, err)
		assert.Nil(t, nodes)
	})
}

func TestRandomSelectNodes_FisherYates(t *testing.T) {
	sd := NewServiceDiscovery()

	// Create 10 nodes
	nodes := make([]*proto.NodeMeta, 10)
	for i := 0; i < 10; i++ {
		nodes[i] = &proto.NodeMeta{NodeId: fmt.Sprintf("node-%d", i)}
	}

	t.Run("Returns exactly limit nodes", func(t *testing.T) {
		result := sd.randomSelectNodes(nodes, 3)
		assert.Equal(t, 3, len(result))
	})

	t.Run("No duplicates in result", func(t *testing.T) {
		for trial := 0; trial < 100; trial++ {
			result := sd.randomSelectNodes(nodes, 5)
			seen := make(map[string]bool)
			for _, n := range result {
				assert.False(t, seen[n.NodeId], "duplicate node: %s", n.NodeId)
				seen[n.NodeId] = true
			}
		}
	})

	t.Run("All results are from input", func(t *testing.T) {
		inputSet := make(map[string]bool)
		for _, n := range nodes {
			inputSet[n.NodeId] = true
		}
		for trial := 0; trial < 100; trial++ {
			result := sd.randomSelectNodes(nodes, 4)
			for _, n := range result {
				assert.True(t, inputSet[n.NodeId], "unexpected node: %s", n.NodeId)
			}
		}
	})

	t.Run("Does not modify original slice", func(t *testing.T) {
		original := make([]string, len(nodes))
		for i, n := range nodes {
			original[i] = n.NodeId
		}
		for trial := 0; trial < 50; trial++ {
			sd.randomSelectNodes(nodes, 3)
		}
		for i, n := range nodes {
			assert.Equal(t, original[i], n.NodeId, "original slice was modified at index %d", i)
		}
	})

	t.Run("Uniform distribution across nodes", func(t *testing.T) {
		counts := make(map[string]int)
		iterations := 10000
		limit := 3
		for i := 0; i < iterations; i++ {
			result := sd.randomSelectNodes(nodes, limit)
			for _, n := range result {
				counts[n.NodeId]++
			}
		}
		// Expected count per node: iterations * limit / len(nodes) = 10000 * 3 / 10 = 3000
		expected := float64(iterations) * float64(limit) / float64(len(nodes))
		for _, node := range nodes {
			count := counts[node.NodeId]
			// Allow 20% deviation from expected
			assert.InDelta(t, expected, float64(count), expected*0.2,
				"node %s count %d deviates too much from expected %.0f", node.NodeId, count, expected)
		}
	})

	t.Run("Edge cases", func(t *testing.T) {
		// Empty input
		result := sd.randomSelectNodes([]*proto.NodeMeta{}, 3)
		assert.Equal(t, 0, len(result))

		// Limit 0 returns all
		result = sd.randomSelectNodes(nodes, 0)
		assert.Equal(t, len(nodes), len(result))

		// Limit >= len returns all
		result = sd.randomSelectNodes(nodes, 15)
		assert.Equal(t, len(nodes), len(result))

		// Limit == len returns all
		result = sd.randomSelectNodes(nodes, 10)
		assert.Equal(t, len(nodes), len(result))

		// Single node, limit 1
		single := []*proto.NodeMeta{{NodeId: "only"}}
		result = sd.randomSelectNodes(single, 1)
		assert.Equal(t, 1, len(result))
		assert.Equal(t, "only", result[0].NodeId)
	})
}

func TestRandomSelectStrings_FisherYates(t *testing.T) {
	sd := NewServiceDiscovery()
	strs := []string{"a", "b", "c", "d", "e", "f", "g", "h"}

	t.Run("Returns exactly limit strings", func(t *testing.T) {
		result := sd.randomSelectStrings(strs, 3)
		assert.Equal(t, 3, len(result))
	})

	t.Run("No duplicates in result", func(t *testing.T) {
		for trial := 0; trial < 100; trial++ {
			result := sd.randomSelectStrings(strs, 4)
			seen := make(map[string]bool)
			for _, s := range result {
				assert.False(t, seen[s], "duplicate string: %s", s)
				seen[s] = true
			}
		}
	})

	t.Run("Does not modify original slice", func(t *testing.T) {
		original := make([]string, len(strs))
		copy(original, strs)
		for trial := 0; trial < 50; trial++ {
			sd.randomSelectStrings(strs, 3)
		}
		assert.Equal(t, original, strs, "original slice was modified")
	})

	t.Run("Edge cases", func(t *testing.T) {
		// Empty input
		result := sd.randomSelectStrings([]string{}, 3)
		assert.Equal(t, 0, len(result))

		// Limit >= len returns all
		result = sd.randomSelectStrings(strs, 10)
		assert.Equal(t, len(strs), len(result))
	})
}

func TestIsRegexLike(t *testing.T) {
	sd := NewServiceDiscovery()

	t.Run("Plain names are not regex", func(t *testing.T) {
		// Simple names
		assert.False(t, sd.isRegexLike("us-east-1"))
		assert.False(t, sd.isRegexLike("az1"))
		assert.False(t, sd.isRegexLike("rg-prod"))
		assert.False(t, sd.isRegexLike(""))
	})

	t.Run("Dotted hostnames are not regex", func(t *testing.T) {
		// This was the bug: dots in hostnames caused false positives
		assert.False(t, sd.isRegexLike("us-east-1.aws"))
		assert.False(t, sd.isRegexLike("az.1"))
		assert.False(t, sd.isRegexLike("node1.cluster.local"))
		assert.False(t, sd.isRegexLike("192.168.1.1"))
	})

	t.Run("Actual regex patterns are detected", func(t *testing.T) {
		assert.True(t, sd.isRegexLike("us-east-.*"))
		assert.True(t, sd.isRegexLike("az[12]"))
		assert.True(t, sd.isRegexLike("^prod"))
		assert.True(t, sd.isRegexLike("rg-prod|rg-staging"))
		assert.True(t, sd.isRegexLike("node.+"))
		assert.True(t, sd.isRegexLike("az(1|2)"))
		assert.True(t, sd.isRegexLike("rg?"))
		assert.True(t, sd.isRegexLike("prod$"))
		assert.True(t, sd.isRegexLike("\\d+"))
		assert.True(t, sd.isRegexLike("node{1,3}"))
	})
}

func TestIsRegexLike_AZFilterIntegration(t *testing.T) {
	// Verify that dotted AZ names work correctly with exact match (not regex)
	sd := NewServiceDiscovery()

	node1 := createFinalTestNode("n1", "rg1", "us-east-1.aws", nil)
	node2 := createFinalTestNode("n2", "rg1", "us-west-2.aws", nil)
	sd.UpdateServer("n1", node1)
	sd.UpdateServer("n2", node2)

	t.Run("Exact match with dotted AZ", func(t *testing.T) {
		filter := &proto.NodeFilter{
			Az:    "us-east-1.aws",
			Limit: 10,
		}
		nodes, err := sd.SelectRandom(filter, proto.AffinityMode_HARD)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(nodes))
		assert.Equal(t, "n1", nodes[0].NodeId)
	})

	t.Run("Regex still works for AZ", func(t *testing.T) {
		filter := &proto.NodeFilter{
			Az:    "us-.*",
			Limit: 10,
		}
		nodes, err := sd.SelectRandom(filter, proto.AffinityMode_HARD)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(nodes))
	})
}

func TestRegexAnchoring(t *testing.T) {
	// Bug fix: MatchString does substring matching, so "rg[12]" would match
	// "rg1-extra". After anchoring with ^(?:...)$, only full-string matches succeed.
	sd := NewServiceDiscovery()

	// Setup nodes with names that could cause substring false positives
	sd.UpdateServer("n1", createFinalTestNode("n1", "rg1", "az1", nil))
	sd.UpdateServer("n2", createFinalTestNode("n2", "rg1-extra", "az1-extended", nil))
	sd.UpdateServer("n3", createFinalTestNode("n3", "rg2", "az2", nil))
	sd.UpdateServer("n4", createFinalTestNode("n4", "xrg1", "xaz1", nil))

	t.Run("RG regex does not substring match", func(t *testing.T) {
		// "rg[12]" should match "rg1" and "rg2" ONLY, not "rg1-extra" or "xrg1"
		filter := &proto.NodeFilter{ResourceGroup: "rg[12]", Limit: 10}
		nodes, err := sd.SelectRandom(filter, proto.AffinityMode_HARD)
		assert.NoError(t, err)

		nodeIDs := make(map[string]bool)
		for _, n := range nodes {
			nodeIDs[n.NodeId] = true
		}
		assert.True(t, nodeIDs["n1"], "rg1 should match rg[12]")
		assert.True(t, nodeIDs["n3"], "rg2 should match rg[12]")
		assert.False(t, nodeIDs["n2"], "rg1-extra should NOT match rg[12]")
		assert.False(t, nodeIDs["n4"], "xrg1 should NOT match rg[12]")
		assert.Equal(t, 2, len(nodes))
	})

	t.Run("AZ regex does not substring match", func(t *testing.T) {
		// "az[12]" should match "az1" and "az2" ONLY
		filter := &proto.NodeFilter{Az: "az[12]", Limit: 10}
		nodes, err := sd.SelectRandom(filter, proto.AffinityMode_HARD)
		assert.NoError(t, err)

		nodeIDs := make(map[string]bool)
		for _, n := range nodes {
			nodeIDs[n.NodeId] = true
		}
		assert.True(t, nodeIDs["n1"], "az1 should match az[12]")
		assert.True(t, nodeIDs["n3"], "az2 should match az[12]")
		assert.False(t, nodeIDs["n2"], "az1-extended should NOT match az[12]")
		assert.False(t, nodeIDs["n4"], "xaz1 should NOT match az[12]")
		assert.Equal(t, 2, len(nodes))
	})

	t.Run("Wildcard regex still works correctly", func(t *testing.T) {
		// "rg.*" should match rg1, rg1-extra, rg2 but NOT xrg1
		filter := &proto.NodeFilter{ResourceGroup: "rg.*", Limit: 10}
		nodes, err := sd.SelectRandom(filter, proto.AffinityMode_HARD)
		assert.NoError(t, err)

		nodeIDs := make(map[string]bool)
		for _, n := range nodes {
			nodeIDs[n.NodeId] = true
		}
		assert.True(t, nodeIDs["n1"], "rg1 should match rg.*")
		assert.True(t, nodeIDs["n2"], "rg1-extra should match rg.*")
		assert.True(t, nodeIDs["n3"], "rg2 should match rg.*")
		assert.False(t, nodeIDs["n4"], "xrg1 should NOT match rg.*")
		assert.Equal(t, 3, len(nodes))
	})

	t.Run("Alternation regex anchored correctly", func(t *testing.T) {
		// "rg1|rg2" should match exactly rg1 and rg2
		filter := &proto.NodeFilter{ResourceGroup: "rg1|rg2", Limit: 10}
		nodes, err := sd.SelectRandom(filter, proto.AffinityMode_HARD)
		assert.NoError(t, err)

		nodeIDs := make(map[string]bool)
		for _, n := range nodes {
			nodeIDs[n.NodeId] = true
		}
		assert.True(t, nodeIDs["n1"], "rg1 should match rg1|rg2")
		assert.True(t, nodeIDs["n3"], "rg2 should match rg1|rg2")
		assert.False(t, nodeIDs["n2"], "rg1-extra should NOT match rg1|rg2")
		assert.Equal(t, 2, len(nodes))
	})
}

func TestCompareHardVsSoftMode(t *testing.T) {
	t.Run("HARD vs SOFT mode comparison", func(t *testing.T) {
		sd := NewServiceDiscovery()

		// Create a scenario where only some nodes meet criteria
		nodes := []*proto.NodeMeta{
			createFinalTestNode("node1", "rg1", "az1", map[string]string{"env": "prod"}),
			createFinalTestNode("node2", "rg1", "az2", map[string]string{"env": "staging"}),
		}

		for _, node := range nodes {
			sd.UpdateServer(node.NodeId, node)
		}

		// Search for non-existent tag
		filter := &proto.NodeFilter{
			Tags:  map[string]string{"env": "test"},
			Limit: 1,
		}

		// SOFT mode: should return empty array, no error
		nodes, err := sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_SOFT)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(nodes))

		// HARD mode: should return error
		nodes, err = sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_HARD)
		assert.Error(t, err)
		assert.Nil(t, nodes)
		assert.Contains(t, err.Error(), "no matching nodes found")
	})
}
