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

	"github.com/zilliztech/woodpecker/proto"
)

// loadSelectSizes is the node-count sweep for the load-aware selection benchmarks.
var loadSelectSizes = []int{3, 6, 20, 50, 100, 500, 1000, 5000, 10000}

// quorumK is a typical write-quorum / ensemble size requested per selection.
const quorumK = 3

// makeLoadedNodes builds n candidates each carrying a fresh (within loadTTL),
// distinct load in [0,1), so selectLowestLoadNodes exercises the weighted path
// (not the all-unknown random fallback).
func makeLoadedNodes(n int) []*proto.NodeMeta {
	nodes := make([]*proto.NodeMeta, n)
	for i := 0; i < n; i++ {
		load := float64(i%100) / 100.0 // spread across [0.00, 0.99]
		nodes[i] = nodeWithLoad(fmt.Sprintf("n%d", i), load, 1000)
	}
	return nodes
}

// BenchmarkSelectLoadAware measures the core load-aware weighted selection
// (weight = 1 - load, sampling K without replacement) as the candidate count
// grows. This is the algorithm itself, with candidate gathering excluded.
func BenchmarkSelectLoadAware(b *testing.B) {
	for _, n := range loadSelectSizes {
		nodes := makeLoadedNodes(n)
		sd := newTestDiscoveryFixedNow()
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = sd.selectLowestLoadNodes(nodes, quorumK)
			}
		})
	}
}

// BenchmarkSelectRandomFallback measures the no-load baseline (uniform random
// pick of K), i.e. the path taken when no node has a known load. Comparing it
// against BenchmarkSelectLoadAware shows the overhead the load weighting adds.
func BenchmarkSelectRandomFallback(b *testing.B) {
	for _, n := range loadSelectSizes {
		nodes := makeLoadedNodes(n)
		sd := newTestDiscoveryFixedNow()
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = sd.randomSelectNodes(nodes, quorumK)
			}
		})
	}
}

// BenchmarkSelectRandomEndToEnd measures the full SelectRandom entry point
// (candidate gathering from the az/rg index + tag/decommission filtering +
// load-aware selection) as the cluster size grows. All nodes share the default
// AZ/RG, so gathering walks the full candidate set.
func BenchmarkSelectRandomEndToEnd(b *testing.B) {
	filter := &proto.NodeFilter{Limit: quorumK}
	for _, n := range loadSelectSizes {
		sd := newTestDiscoveryFixedNow()
		for _, meta := range makeLoadedNodes(n) {
			sd.UpdateServer(meta.GetNodeId(), meta)
		}
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = sd.SelectRandom(filter, proto.AffinityMode_SOFT)
			}
		})
	}
}
