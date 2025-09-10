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
	"testing"
	"time"

	"github.com/zilliztech/woodpecker/proto"
)

// Benchmark tests to verify O(1) query performance

func createBenchNode(nodeID, rg, az string, tags map[string]string) *proto.NodeMeta {
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

func setupBenchmarkData(sd *ServiceDiscovery, numRGs, numAZs, nodesPerCell int) {
	for rg := 0; rg < numRGs; rg++ {
		for az := 0; az < numAZs; az++ {
			for node := 0; node < nodesPerCell; node++ {
				nodeID := fmt.Sprintf("node-rg%d-az%d-%d", rg, az, node)
				rgName := fmt.Sprintf("rg-%d", rg)
				azName := fmt.Sprintf("az-%d", az)
				tags := map[string]string{
					"env":  []string{"prod", "staging", "test"}[node%3],
					"role": []string{"writer", "reader"}[node%2],
				}

				nodeMeta := createBenchNode(nodeID, rgName, azName, tags)
				sd.UpdateServer(nodeID, nodeMeta)
			}
		}
	}
}

// BenchmarkSelectStrategiesO1 verifies that all strategies have O(1) performance
func BenchmarkSelectStrategiesO1(b *testing.B) {
	// Test different data scales
	testSizes := []struct {
		name    string
		rgs     int
		azs     int
		perCell int
	}{
		{"Small-100nodes", 5, 4, 5},      // 100 nodes
		{"Medium-1000nodes", 10, 10, 10}, // 1000 nodes
		{"Large-10000nodes", 20, 25, 20}, // 10000 nodes
	}

	for _, size := range testSizes {
		b.Run(size.name, func(b *testing.B) {
			sd := NewServiceDiscovery()
			setupBenchmarkData(sd, size.rgs, size.azs, size.perCell)

			filter := &proto.NodeFilter{Limit: 5}

			b.Run("SelectSingleAzSingleRg", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_SOFT)
				}
			})

			b.Run("SelectSingleAzMultiRg", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					sd.SelectSingleAzMultiRg(filter, proto.AffinityMode_SOFT)
				}
			})

			b.Run("SelectMultiAzSingleRg", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					sd.SelectMultiAzSingleRg(filter, proto.AffinityMode_SOFT)
				}
			})

			b.Run("SelectMultiAzMultiRg", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					sd.SelectMultiAzMultiRg(filter, proto.AffinityMode_SOFT)
				}
			})

			b.Run("SelectRandom", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					sd.SelectRandom(filter, proto.AffinityMode_SOFT)
				}
			})
		})
	}
}

// BenchmarkIndexMaintenance tests the performance of index maintenance
func BenchmarkIndexMaintenance(b *testing.B) {
	b.Run("UpdateServer", func(b *testing.B) {
		sd := NewServiceDiscovery()

		// Pre-create some data
		setupBenchmarkData(sd, 10, 10, 5) // 500 nodes

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			nodeID := fmt.Sprintf("bench-node-%d", i)
			rg := fmt.Sprintf("rg-%d", i%10)
			az := fmt.Sprintf("az-%d", i%10)
			tags := map[string]string{"env": "test"}

			node := createBenchNode(nodeID, rg, az, tags)
			sd.UpdateServer(nodeID, node)
		}
	})

	b.Run("RemoveServer", func(b *testing.B) {
		sd := NewServiceDiscovery()

		// Pre-create data for removal
		numNodes := 1000
		for i := 0; i < numNodes; i++ {
			nodeID := fmt.Sprintf("bench-node-%d", i)
			rg := fmt.Sprintf("rg-%d", i%10)
			az := fmt.Sprintf("az-%d", i%10)
			tags := map[string]string{"env": "test"}

			node := createBenchNode(nodeID, rg, az, tags)
			sd.UpdateServer(nodeID, node)
		}

		b.ResetTimer()
		for i := 0; i < b.N && i < numNodes; i++ {
			nodeID := fmt.Sprintf("bench-node-%d", i)
			sd.RemoveServer(nodeID)
		}
	})
}

// BenchmarkFilterPerformance tests filter performance
func BenchmarkFilterPerformance(b *testing.B) {
	sd := NewServiceDiscovery()
	setupBenchmarkData(sd, 10, 10, 10) // 1000 nodes

	b.Run("NoFilter", func(b *testing.B) {
		filter := &proto.NodeFilter{Limit: 10}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sd.SelectRandom(filter, proto.AffinityMode_SOFT)
		}
	})

	b.Run("ExactRGFilter", func(b *testing.B) {
		filter := &proto.NodeFilter{ResourceGroup: "rg-5", Limit: 10}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sd.SelectRandom(filter, proto.AffinityMode_SOFT)
		}
	})

	b.Run("ExactAZFilter", func(b *testing.B) {
		filter := &proto.NodeFilter{Az: "az-5", Limit: 10}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sd.SelectRandom(filter, proto.AffinityMode_SOFT)
		}
	})

	b.Run("RegexRGFilter", func(b *testing.B) {
		filter := &proto.NodeFilter{ResourceGroup: "rg-[5-7]", Limit: 10}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sd.SelectRandom(filter, proto.AffinityMode_SOFT)
		}
	})

	b.Run("TagFilter", func(b *testing.B) {
		filter := &proto.NodeFilter{Tags: map[string]string{"env": "prod"}, Limit: 10}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sd.SelectRandom(filter, proto.AffinityMode_SOFT)
		}
	})

	b.Run("CombinedFilter", func(b *testing.B) {
		filter := &proto.NodeFilter{
			ResourceGroup: "rg-5",
			Az:            "az-5",
			Tags:          map[string]string{"env": "prod"},
			Limit:         10,
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sd.SelectRandom(filter, proto.AffinityMode_SOFT)
		}
	})
}

// BenchmarkScalability tests performance stability under different data scales
func BenchmarkScalability(b *testing.B) {
	dataSizes := []struct {
		name  string
		nodes int
		rgs   int
		azs   int
	}{
		{"1K-nodes", 1000, 10, 10},
		{"10K-nodes", 10000, 20, 25},
		{"100K-nodes", 100000, 50, 40},
	}

	for _, size := range dataSizes {
		b.Run(size.name, func(b *testing.B) {
			sd := NewServiceDiscovery()

			// Calculate nodes per cell
			nodesPerCell := size.nodes / (size.rgs * size.azs)
			if nodesPerCell == 0 {
				nodesPerCell = 1
			}

			b.StopTimer()
			setupBenchmarkData(sd, size.rgs, size.azs, nodesPerCell)
			b.StartTimer()

			filter := &proto.NodeFilter{Limit: 10}

			// Test core O(1) strategies
			for i := 0; i < b.N; i++ {
				// Rotate through different strategies to simulate real usage scenarios
				switch i % 4 {
				case 0:
					sd.SelectSingleAzSingleRg(filter, proto.AffinityMode_SOFT)
				case 1:
					sd.SelectSingleAzMultiRg(filter, proto.AffinityMode_SOFT)
				case 2:
					sd.SelectMultiAzSingleRg(filter, proto.AffinityMode_SOFT)
				case 3:
					sd.SelectMultiAzMultiRg(filter, proto.AffinityMode_SOFT)
				}
			}
		})
	}
}

// BenchmarkUpdatePerformance tests node update performance under different scales
func BenchmarkUpdatePerformance(b *testing.B) {
	// Test different scale datasets
	testSizes := []struct {
		name        string
		totalNodes  int
		rgs         int
		azs         int
		updateRatio float64 // Percentage of nodes affected by each update operation
	}{
		{"Small-100nodes", 100, 5, 4, 0.1},         // 100 nodes, update 10% each time
		{"Medium-1000nodes", 1000, 10, 10, 0.05},   // 1000 nodes, update 5% each time
		{"Large-10000nodes", 10000, 20, 25, 0.02},  // 10000 nodes, update 2% each time
		{"XLarge-50000nodes", 50000, 50, 40, 0.01}, // 50000 nodes, update 1% each time
	}

	for _, size := range testSizes {
		b.Run(size.name, func(b *testing.B) {
			sd := NewServiceDiscovery()

			// Calculate nodes per cell
			nodesPerCell := size.totalNodes / (size.rgs * size.azs)
			if nodesPerCell == 0 {
				nodesPerCell = 1
			}

			// Pre-populate data
			b.StopTimer()
			setupBenchmarkData(sd, size.rgs, size.azs, nodesPerCell)

			// Prepare list of node IDs to update
			updateCount := int(float64(size.totalNodes) * size.updateRatio)
			if updateCount == 0 {
				updateCount = 1
			}

			b.StartTimer()

			b.Run("UpdateExistingNode", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// Select an existing node to update
					nodeIndex := i % updateCount
					nodeID := fmt.Sprintf("node-rg%d-az%d-%d",
						nodeIndex%size.rgs,
						(nodeIndex/size.rgs)%size.azs,
						nodeIndex/(size.rgs*size.azs))

					// Update node tags, keeping RG and AZ unchanged
					rg := fmt.Sprintf("rg-%d", nodeIndex%size.rgs)
					az := fmt.Sprintf("az-%d", (nodeIndex/size.rgs)%size.azs)
					tags := map[string]string{
						"env":     []string{"prod", "staging", "test"}[i%3],
						"role":    []string{"writer", "reader", "coordinator"}[i%3],
						"version": fmt.Sprintf("v%d", i%10),
						"updated": fmt.Sprintf("%d", i),
					}

					nodeMeta := createBenchNode(nodeID, rg, az, tags)
					sd.UpdateServer(nodeID, nodeMeta)
				}
			})

			b.Run("UpdateNodeWithRGChange", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// Select an existing node and change its RG
					nodeIndex := i % updateCount
					nodeID := fmt.Sprintf("node-rg%d-az%d-%d",
						nodeIndex%size.rgs,
						(nodeIndex/size.rgs)%size.azs,
						nodeIndex/(size.rgs*size.azs))

					// Update to new RG (this will trigger index rebuild)
					newRG := fmt.Sprintf("rg-%d", (nodeIndex+1)%size.rgs)
					az := fmt.Sprintf("az-%d", (nodeIndex/size.rgs)%size.azs)
					tags := map[string]string{
						"env":     "prod",
						"role":    "migrated",
						"version": fmt.Sprintf("v%d", i),
					}

					nodeMeta := createBenchNode(nodeID, newRG, az, tags)
					sd.UpdateServer(nodeID, nodeMeta)
				}
			})

			b.Run("UpdateNodeWithAZChange", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// Select an existing node and change its AZ
					nodeIndex := i % updateCount
					nodeID := fmt.Sprintf("node-rg%d-az%d-%d",
						nodeIndex%size.rgs,
						(nodeIndex/size.rgs)%size.azs,
						nodeIndex/(size.rgs*size.azs))

					// Update to new AZ (this will trigger index rebuild)
					rg := fmt.Sprintf("rg-%d", nodeIndex%size.rgs)
					newAZ := fmt.Sprintf("az-%d", ((nodeIndex/size.rgs)+1)%size.azs)
					tags := map[string]string{
						"env":     "prod",
						"role":    "migrated",
						"version": fmt.Sprintf("v%d", i),
					}

					nodeMeta := createBenchNode(nodeID, rg, newAZ, tags)
					sd.UpdateServer(nodeID, nodeMeta)
				}
			})

			b.Run("UpdateNodeWithBothRGAndAZChange", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// Select an existing node and change both its RG and AZ
					nodeIndex := i % updateCount
					nodeID := fmt.Sprintf("node-rg%d-az%d-%d",
						nodeIndex%size.rgs,
						(nodeIndex/size.rgs)%size.azs,
						nodeIndex/(size.rgs*size.azs))

					// Update to new RG and AZ (this will trigger full index rebuild)
					newRG := fmt.Sprintf("rg-%d", (nodeIndex+1)%size.rgs)
					newAZ := fmt.Sprintf("az-%d", ((nodeIndex/size.rgs)+1)%size.azs)
					tags := map[string]string{
						"env":     "prod",
						"role":    "fully-migrated",
						"version": fmt.Sprintf("v%d", i),
					}

					nodeMeta := createBenchNode(nodeID, newRG, newAZ, tags)
					sd.UpdateServer(nodeID, nodeMeta)
				}
			})

			b.Run("AddNewNode", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// Add completely new node
					nodeID := fmt.Sprintf("new-node-%d", i)
					rg := fmt.Sprintf("rg-%d", i%size.rgs)
					az := fmt.Sprintf("az-%d", i%size.azs)
					tags := map[string]string{
						"env":     "prod",
						"role":    "new",
						"version": "v1.0",
						"added":   fmt.Sprintf("%d", i),
					}

					nodeMeta := createBenchNode(nodeID, rg, az, tags)
					sd.UpdateServer(nodeID, nodeMeta)
				}
			})
		})
	}
}

// BenchmarkBatchUpdatePerformance tests the performance of batch update operations
func BenchmarkBatchUpdatePerformance(b *testing.B) {
	batchSizes := []int{10, 50, 100, 500}
	dataSize := 10000 // Fixed dataset size

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize-%d", batchSize), func(b *testing.B) {
			sd := NewServiceDiscovery()

			// Pre-populate data
			b.StopTimer()
			setupBenchmarkData(sd, 20, 25, 20) // 10000 nodes
			b.StartTimer()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Batch update operations
				for j := 0; j < batchSize; j++ {
					nodeIndex := (i*batchSize + j) % dataSize
					nodeID := fmt.Sprintf("node-rg%d-az%d-%d",
						nodeIndex%20,
						(nodeIndex/20)%25,
						nodeIndex/(20*25))

					// Random update type
					updateType := j % 4
					var rg, az string
					var tags map[string]string

					switch updateType {
					case 0: // Only update tags
						rg = fmt.Sprintf("rg-%d", nodeIndex%20)
						az = fmt.Sprintf("az-%d", (nodeIndex/20)%25)
						tags = map[string]string{
							"env":     "updated",
							"version": fmt.Sprintf("v%d", i),
						}
					case 1: // Update RG
						rg = fmt.Sprintf("rg-%d", (nodeIndex+1)%20)
						az = fmt.Sprintf("az-%d", (nodeIndex/20)%25)
						tags = map[string]string{"env": "rg-changed"}
					case 2: // Update AZ
						rg = fmt.Sprintf("rg-%d", nodeIndex%20)
						az = fmt.Sprintf("az-%d", ((nodeIndex/20)+1)%25)
						tags = map[string]string{"env": "az-changed"}
					case 3: // Update RG and AZ
						rg = fmt.Sprintf("rg-%d", (nodeIndex+1)%20)
						az = fmt.Sprintf("az-%d", ((nodeIndex/20)+1)%25)
						tags = map[string]string{"env": "both-changed"}
					}

					nodeMeta := createBenchNode(nodeID, rg, az, tags)
					sd.UpdateServer(nodeID, nodeMeta)
				}
			}
		})
	}
}

// BenchmarkConcurrentUpdatePerformance tests concurrent update performance
func BenchmarkConcurrentUpdatePerformance(b *testing.B) {
	concurrencyLevels := []int{1, 2, 4, 8, 16}

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency-%d", concurrency), func(b *testing.B) {
			sd := NewServiceDiscovery()

			// Pre-populate data
			b.StopTimer()
			setupBenchmarkData(sd, 10, 10, 10) // 1000 nodes
			b.StartTimer()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				counter := 0
				for pb.Next() {
					// Each goroutine updates different nodes
					nodeIndex := counter % 1000
					nodeID := fmt.Sprintf("node-rg%d-az%d-%d",
						nodeIndex%10,
						(nodeIndex/10)%10,
						nodeIndex/100)

					// Update node tags
					rg := fmt.Sprintf("rg-%d", nodeIndex%10)
					az := fmt.Sprintf("az-%d", (nodeIndex/10)%10)
					tags := map[string]string{
						"env":       "concurrent",
						"worker":    fmt.Sprintf("w%d", counter%concurrency),
						"iteration": fmt.Sprintf("%d", counter),
					}

					nodeMeta := createBenchNode(nodeID, rg, az, tags)
					sd.UpdateServer(nodeID, nodeMeta)
					counter++
				}
			})
		})
	}
}
