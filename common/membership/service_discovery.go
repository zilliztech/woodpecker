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
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/proto"
)

// NodeInfo is discovery's complete view of one node: who it is, and how it is
// doing. The two halves have different lifetimes and are stored accordingly.
//
// Meta is identity and topology (node_id, az, resource_group, endpoint). It
// changes rarely, and when it does the node is re-indexed, so it is immutable
// for the lifetime of a NodeInfo — a meta change builds a new one.
//
// runtime is the latest measurement snapshot, replaced on every report. It is
// held in an atomic pointer so a new reading costs one store and no index work,
// and so readers that took a *NodeInfo out of the indexes can read it after
// releasing sd.mu. nil until the node's first announce arrives.
type NodeInfo struct {
	Meta    *proto.NodeMeta
	runtime atomic.Pointer[proto.NodeRuntimeInfo]
}

// Runtime returns the node's latest runtime snapshot, or nil if none has
// arrived. The returned value is immutable — a newer snapshot replaces it
// rather than mutating it.
func (n *NodeInfo) Runtime() *proto.NodeRuntimeInfo {
	if n == nil {
		return nil
	}
	return n.runtime.Load()
}

// metasOf projects a node list down to the identity halves. Selection results
// cross the package boundary as []*proto.NodeMeta: callers pick nodes in order
// to dial them, and runtime signals are an input to that choice, never an
// output of it.
func metasOf(nodes []*NodeInfo) []*proto.NodeMeta {
	out := make([]*proto.NodeMeta, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.Meta)
	}
	return out
}

// ServiceDiscovery - High-performance service discovery implementation designed for O(1) query optimization
// Query frequency >> Update frequency, maintaining indexes for O(1) query priority
type ServiceDiscovery struct {
	mu    sync.RWMutex
	Nodes map[string]*NodeInfo // nodeId -> NodeInfo

	azList []string // All available AZs
	rgList []string // All available RGs

	// Core indexes: bidirectional indexes supporting all query strategies
	azRgIndex map[string]map[string][]*NodeInfo // az -> rg -> []*NodeInfo
	rgAzIndex map[string]map[string][]*NodeInfo // rg -> az -> []*NodeInfo

	// Auxiliary slices for O(1) random key selection
	azRgIndexKeys map[string][]string // az -> []rg (all RGs in this AZ)
	rgAzIndexKeys map[string][]string // rg -> []az (all AZs in this RG)

	// Regular expression cache (used only when needed)
	regexCache *lru.Cache[string, *regexp.Regexp]

	// Load-aware selection (issue #114).
	loadAware   bool          // when false, selection ignores load and picks uniformly at random
	loadTTL     time.Duration // load older than this is treated as unknown; default 30s
	unknownLoad float64       // assumed load for nodes with no fresh report; default 0.5
	randFloat   func() float64
	nowFn       func() time.Time
}

// DiscoveryOption configures a ServiceDiscovery at construction.
type DiscoveryOption func(*ServiceDiscovery)

// WithLoadAware sets whether selection is load-aware and the load freshness TTL.
// When enabled is false, selection ignores load and picks uniformly at random.
// A non-positive loadTTL keeps the default.
func WithLoadAware(enabled bool, loadTTL time.Duration) DiscoveryOption {
	return func(sd *ServiceDiscovery) {
		sd.loadAware = enabled
		if loadTTL > 0 {
			sd.loadTTL = loadTTL
		}
	}
}

func NewServiceDiscovery(opts ...DiscoveryOption) *ServiceDiscovery {
	cache, err := lru.New[string, *regexp.Regexp](100) // TODO should be configurable
	if err != nil {
		panic(fmt.Sprintf("Failed to create regex cache: %v", err))
	}

	sd := &ServiceDiscovery{
		Nodes:         make(map[string]*NodeInfo),
		azList:        make([]string, 0),
		rgList:        make([]string, 0),
		azRgIndex:     make(map[string]map[string][]*NodeInfo),
		rgAzIndex:     make(map[string]map[string][]*NodeInfo),
		azRgIndexKeys: make(map[string][]string),
		rgAzIndexKeys: make(map[string][]string),
		regexCache:    cache,
	}
	// Defaults (load-aware on, matching config default loadAwareEnabled=true).
	sd.loadAware = true
	sd.loadTTL = 30 * time.Second
	sd.unknownLoad = 0.5
	sd.randFloat = rand.Float64
	sd.nowFn = time.Now
	for _, opt := range opts {
		opt(sd)
	}
	return sd
}

// UpdateServer updates a node's identity metadata and maintains all indexes.
//
// Only the identity half is replaced. Whatever runtime snapshot the node
// already had carries over: a meta change (new tags, a re-registration) says
// nothing about how busy the node is, and discarding the reading would send it
// back to the neutral unknownLoad placeholder for no reason.
func (sd *ServiceDiscovery) UpdateServer(nodeID string, meta *proto.NodeMeta) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	node := &NodeInfo{Meta: meta}

	// If node already exists, remove it from all indexes first and carry its
	// runtime snapshot onto the replacement.
	if old, exists := sd.Nodes[nodeID]; exists {
		if rt := old.Runtime(); rt != nil {
			node.runtime.Store(rt)
		}
		sd.removeFromIndexes(nodeID, old)
	}

	// Add to main storage
	sd.Nodes[nodeID] = node

	// Add to all indexes
	sd.addToIndexes(nodeID, node)

	// Compatibility: a peer running a build that predates the runtime gossip
	// path publishes its load inside NodeMeta, and push/pull is the only way it
	// reaches us. Feed that through the same ordering rules as a real snapshot
	// so it can fill in but never roll back a fresher one.
	if meta.GetLoadUpdatedAt() > 0 {
		applyRuntimeSnapshot(node, &proto.NodeRuntimeInfo{
			NodeId:     nodeID,
			UpdatedAt:  meta.GetLoadUpdatedAt(),
			LoadFactor: meta.GetLoadFactor(),
		})
	}
}

// ApplyRuntimeInfo merges a runtime snapshot announced by a peer, leaving the
// node's identity fields alone. Reports whether it was applied.
//
// This is the consumer end of the runtime gossip path: the transport hands over
// whole NodeRuntimeInfo snapshots without interpreting them, and this decides
// which signals discovery actually keeps. Adding a signal to the proto is a
// change here and nowhere else.
//
// A snapshot is dropped when the node is unknown — its join has not landed yet,
// and the next report carries the snapshot again — or when it is not newer than
// what is already held, which is what makes the gossip path safe against
// retransmits and reordering, and what the relay dedupes on. Comparison is
// always between two snapshots from the same node, so it depends only on that
// node's clock moving forward, never on clocks agreeing across the cluster.
func (sd *ServiceDiscovery) ApplyRuntimeInfo(info *proto.NodeRuntimeInfo) bool {
	if info == nil {
		return false
	}

	sd.mu.RLock()
	node, exists := sd.Nodes[info.GetNodeId()]
	sd.mu.RUnlock()
	if !exists {
		return false
	}
	return applyRuntimeSnapshot(node, info)
}

// applyRuntimeSnapshot installs a snapshot on a node if it is newer than the one
// held, reporting whether it did. Storing it is a single atomic swap: identity
// is untouched, so no index moves and no meta is copied.
//
// The compare-and-swap loop keeps two arriving snapshots from losing one another
// — the gossip path and the legacy meta path can both reach a node, and neither
// holds sd.mu for the write.
func applyRuntimeSnapshot(node *NodeInfo, info *proto.NodeRuntimeInfo) bool {
	for {
		cur := node.runtime.Load()
		if cur != nil && info.GetUpdatedAt() <= cur.GetUpdatedAt() {
			return false
		}
		if node.runtime.CompareAndSwap(cur, info) {
			return true
		}
	}
}

// RemoveServer removes server from all indexes
func (sd *ServiceDiscovery) RemoveServer(nodeID string) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	if meta, exists := sd.Nodes[nodeID]; exists {
		delete(sd.Nodes, nodeID)
		sd.removeFromIndexes(nodeID, meta)
	}
}

// RemoveServerIfMatch removes a server only if its current metadata matches the leaving node's metadata.
// This prevents stale leave events from removing a node that has already rejoined with a new incarnation.
// Returns true if the server was removed (or was already absent), false if a newer incarnation exists.
func (sd *ServiceDiscovery) RemoveServerIfMatch(nodeID string, leavingMeta *proto.NodeMeta) bool {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	currentMeta, exists := sd.Nodes[nodeID]
	if !exists {
		return true // Already removed
	}

	// If the current node has a different endpoint or a newer timestamp,
	// a new incarnation has joined — do not remove it.
	if currentMeta.Meta.Endpoint != leavingMeta.Endpoint || currentMeta.Meta.LastUpdate > leavingMeta.LastUpdate {
		return false
	}

	delete(sd.Nodes, nodeID)
	sd.removeFromIndexes(nodeID, currentMeta)
	return true
}

// addToIndexes adds node to all indexes
func (sd *ServiceDiscovery) addToIndexes(nodeID string, node *NodeInfo) {
	az := node.Meta.Az
	rg := node.Meta.ResourceGroup

	// 1. Ensure AZ is in azList
	sd.ensureAZInList(az)

	// 2. Ensure RG is in rgList
	sd.ensureRGInList(rg)

	// 3. Add to azRgIndex
	if sd.azRgIndex[az] == nil {
		sd.azRgIndex[az] = make(map[string][]*NodeInfo)
	}
	sd.azRgIndex[az][rg] = append(sd.azRgIndex[az][rg], node)

	// 4. Add to rgAzIndex
	if sd.rgAzIndex[rg] == nil {
		sd.rgAzIndex[rg] = make(map[string][]*NodeInfo)
	}
	sd.rgAzIndex[rg][az] = append(sd.rgAzIndex[rg][az], node)

	// 5. Update azRgIndexKeys
	sd.ensureRGInAZKeys(az, rg)

	// 6. Update rgAzIndexKeys
	sd.ensureAZInRGKeys(rg, az)
}

// removeFromIndexes removes node from all indexes
func (sd *ServiceDiscovery) removeFromIndexes(nodeID string, node *NodeInfo) {
	az := node.Meta.Az
	rg := node.Meta.ResourceGroup

	// 1. Remove from azRgIndex
	if azMap, exists := sd.azRgIndex[az]; exists {
		if nodeList, exists := azMap[rg]; exists {
			azMap[rg] = sd.removeNodeFromSlice(nodeList, nodeID)
			if len(azMap[rg]) == 0 {
				delete(azMap, rg)
				// If this RG in this AZ has no nodes, remove from azRgIndexKeys
				sd.removeRGFromAZKeys(az, rg)
			}
		}
		if len(azMap) == 0 {
			delete(sd.azRgIndex, az)
			// If this AZ has no RGs, remove from azList
			sd.removeAZFromList(az)
		}
	}

	// 2. Remove from rgAzIndex
	if rgMap, exists := sd.rgAzIndex[rg]; exists {
		if nodeList, exists := rgMap[az]; exists {
			rgMap[az] = sd.removeNodeFromSlice(nodeList, nodeID)
			if len(rgMap[az]) == 0 {
				delete(rgMap, az)
				// If this AZ in this RG has no nodes, remove from rgAzIndexKeys
				sd.removeAZFromRGKeys(rg, az)
			}
		}
		if len(rgMap) == 0 {
			delete(sd.rgAzIndex, rg)
			// If this RG has no AZs, remove from rgList
			sd.removeRGFromList(rg)
		}
	}
}

// === Core query methods: implementing O(1) complexity ===

// SelectSingleAzSingleRg: azList → slice random az → azRgIndexKeys[az] → slice random rg → azRgIndex[az][rg] random node
func (sd *ServiceDiscovery) SelectSingleAzSingleRg(filter *proto.NodeFilter, affinityMode proto.AffinityMode) ([]*proto.NodeMeta, error) {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	// 1. Get candidate AZ list
	candidateAZs := sd.getCandidateAZs(filter)
	if len(candidateAZs) == 0 {
		return sd.handleNoResults("no matching AZs found", affinityMode)
	}

	// Phase 1: Random attempt phase - fast and efficient
	maxRandomAttempts := min(5, len(candidateAZs)*2) // Both modes perform random attempts

	for attempt := 0; attempt < maxRandomAttempts; attempt++ {
		// 2. Randomly select an AZ
		selectedAZ := candidateAZs[rand.Intn(len(candidateAZs))]

		// 3. Get candidate RG list under this AZ
		candidateRGs := sd.getCandidateRGsInAZ(selectedAZ, filter)
		if len(candidateRGs) == 0 {
			continue // Try other AZs
		}

		// 4. Randomly select an RG
		selectedRG := candidateRGs[rand.Intn(len(candidateRGs))]

		// 5. Randomly select nodes from azRgIndex[az][rg]
		nodes := sd.azRgIndex[selectedAZ][selectedRG]
		filteredNodes := sd.filterByTags(nodes, filter.Tags)
		filteredNodes = sd.excludeDecommissioning(filteredNodes)

		if len(filteredNodes) > 0 {
			return metasOf(sd.selectLowestLoadNodes(filteredNodes, int(filter.Limit))), nil
		}
	}

	// Phase 2: Fallback to exhaustive search - ensures correctness (both HARD and SOFT need this)
	return sd.exhaustiveSearchSingleAzSingleRg(candidateAZs, filter, affinityMode)
}

// === Common helper methods ===

// handleNoResults uniformly handles no-result scenarios
func (sd *ServiceDiscovery) handleNoResults(message string, affinityMode proto.AffinityMode) ([]*proto.NodeMeta, error) {
	if affinityMode == proto.AffinityMode_HARD {
		return nil, fmt.Errorf("%s", message)
	}
	return []*proto.NodeMeta{}, nil
}

// exhaustiveSearchSingleAzSingleRg exhaustively searches all candidate AZs to ensure correctness
func (sd *ServiceDiscovery) exhaustiveSearchSingleAzSingleRg(candidateAZs []string, filter *proto.NodeFilter, affinityMode proto.AffinityMode) ([]*proto.NodeMeta, error) {
	// Collect all valid AZ-RG combinations
	var validCombinations []struct {
		az    string
		rg    string
		nodes []*NodeInfo
	}

	for _, az := range candidateAZs {
		candidateRGs := sd.getCandidateRGsInAZ(az, filter)
		for _, rg := range candidateRGs {
			nodes := sd.azRgIndex[az][rg]
			filteredNodes := sd.filterByTags(nodes, filter.Tags)
			filteredNodes = sd.excludeDecommissioning(filteredNodes)
			if len(filteredNodes) > 0 {
				validCombinations = append(validCombinations, struct {
					az    string
					rg    string
					nodes []*NodeInfo
				}{az, rg, filteredNodes})
			}
		}
	}

	// If no valid combinations, handle according to affinity mode
	if len(validCombinations) == 0 {
		return sd.handleNoResults("no matching nodes found", affinityMode)
	}

	// Randomly select from valid combinations
	selectedCombination := validCombinations[rand.Intn(len(validCombinations))]
	return metasOf(sd.selectLowestLoadNodes(selectedCombination.nodes, int(filter.Limit))), nil
}

// SelectSingleAzMultiRg: azList → slice random az → azRgIndexKeys[az] → randomly select multiple rgs → randomly select one node from each rg
func (sd *ServiceDiscovery) SelectSingleAzMultiRg(filter *proto.NodeFilter, affinityMode proto.AffinityMode) ([]*proto.NodeMeta, error) {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	// 1. Get candidate AZ list
	candidateAZs := sd.getCandidateAZs(filter)
	if len(candidateAZs) == 0 {
		return sd.handleNoResults("no matching AZs found", affinityMode)
	}

	// Phase 1: Random attempt phase
	maxRandomAttempts := min(5, len(candidateAZs)*2)

	for attempt := 0; attempt < maxRandomAttempts; attempt++ {
		// 2. Randomly select an AZ
		selectedAZ := candidateAZs[rand.Intn(len(candidateAZs))]

		// 3. Get candidate RG list under this AZ
		candidateRGs := sd.getCandidateRGsInAZ(selectedAZ, filter)
		if len(candidateRGs) == 0 {
			continue // Try other AZs
		}

		// 4. Try to collect qualifying nodes
		var selectedNodes []*NodeInfo
		limit := int(filter.Limit)
		if limit == 0 || limit > len(candidateRGs) {
			limit = len(candidateRGs)
		}

		selectedRGs := sd.randomSelectStrings(candidateRGs, limit)

		// 5. Randomly select one node from each RG
		for _, rg := range selectedRGs {
			nodes := sd.azRgIndex[selectedAZ][rg]
			filteredNodes := sd.filterByTags(nodes, filter.Tags)
			filteredNodes = sd.excludeDecommissioning(filteredNodes)
			if len(filteredNodes) > 0 {
				selectedNode := sd.selectLowestLoadNode(filteredNodes)
				selectedNodes = append(selectedNodes, selectedNode)
			}
		}

		if len(selectedNodes) > 0 {
			return metasOf(selectedNodes), nil
		}
	}

	// Phase 2: Fallback to exhaustive search
	return sd.exhaustiveSearchSingleAzMultiRg(candidateAZs, filter, affinityMode)
}

// exhaustiveSearchSingleAzMultiRg exhaustive search for SingleAzMultiRg strategy
func (sd *ServiceDiscovery) exhaustiveSearchSingleAzMultiRg(candidateAZs []string, filter *proto.NodeFilter, affinityMode proto.AffinityMode) ([]*proto.NodeMeta, error) {
	var allValidNodes []*NodeInfo

	// Collect all qualifying nodes
	for _, az := range candidateAZs {
		candidateRGs := sd.getCandidateRGsInAZ(az, filter)
		for _, rg := range candidateRGs {
			nodes := sd.azRgIndex[az][rg]
			filteredNodes := sd.filterByTags(nodes, filter.Tags)
			filteredNodes = sd.excludeDecommissioning(filteredNodes)
			if len(filteredNodes) > 0 {
				// Randomly select one node from each RG
				selectedNode := sd.selectLowestLoadNode(filteredNodes)
				allValidNodes = append(allValidNodes, selectedNode)
			}
		}
	}

	if len(allValidNodes) == 0 {
		return sd.handleNoResults("no matching nodes found", affinityMode)
	}

	// Apply limit constraint
	limit := int(filter.Limit)
	if limit == 0 || limit >= len(allValidNodes) {
		return metasOf(allValidNodes), nil
	}

	return metasOf(sd.selectLowestLoadNodes(allValidNodes, limit)), nil
}

// SelectMultiAzSingleRg: rgList → slice random rg → rgAzIndexKeys[rg] → randomly select multiple azs → randomly select one node from each az
func (sd *ServiceDiscovery) SelectMultiAzSingleRg(filter *proto.NodeFilter, affinityMode proto.AffinityMode) ([]*proto.NodeMeta, error) {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	// 1. Get candidate RG list
	candidateRGs := sd.getCandidateRGs(filter)
	if len(candidateRGs) == 0 {
		return sd.handleNoResults("no matching RGs found", affinityMode)
	}

	// Phase 1: Random attempt phase
	maxRandomAttempts := min(5, len(candidateRGs)*2)

	for attempt := 0; attempt < maxRandomAttempts; attempt++ {
		// 2. Randomly select an RG
		selectedRG := candidateRGs[rand.Intn(len(candidateRGs))]

		// 3. Get candidate AZ list under this RG
		candidateAZs := sd.getCandidateAZsInRG(selectedRG, filter)
		if len(candidateAZs) == 0 {
			continue // Try other RGs
		}

		// 4. Try to collect qualifying nodes
		var selectedNodes []*NodeInfo
		limit := int(filter.Limit)
		if limit == 0 || limit > len(candidateAZs) {
			limit = len(candidateAZs)
		}

		selectedAZs := sd.randomSelectStrings(candidateAZs, limit)

		// 5. Randomly select one node from each AZ
		for _, az := range selectedAZs {
			nodes := sd.rgAzIndex[selectedRG][az]
			filteredNodes := sd.filterByTags(nodes, filter.Tags)
			filteredNodes = sd.excludeDecommissioning(filteredNodes)
			if len(filteredNodes) > 0 {
				selectedNode := sd.selectLowestLoadNode(filteredNodes)
				selectedNodes = append(selectedNodes, selectedNode)
			}
		}

		if len(selectedNodes) > 0 {
			return metasOf(selectedNodes), nil
		}
	}

	// Phase 2: Fallback to exhaustive search
	return sd.exhaustiveSearchMultiAzSingleRg(candidateRGs, filter, affinityMode)
}

// exhaustiveSearchMultiAzSingleRg exhaustive search for MultiAzSingleRg strategy
func (sd *ServiceDiscovery) exhaustiveSearchMultiAzSingleRg(candidateRGs []string, filter *proto.NodeFilter, affinityMode proto.AffinityMode) ([]*proto.NodeMeta, error) {
	var allValidNodes []*NodeInfo

	// Collect all qualifying nodes
	for _, rg := range candidateRGs {
		candidateAZs := sd.getCandidateAZsInRG(rg, filter)
		for _, az := range candidateAZs {
			nodes := sd.rgAzIndex[rg][az]
			filteredNodes := sd.filterByTags(nodes, filter.Tags)
			filteredNodes = sd.excludeDecommissioning(filteredNodes)
			if len(filteredNodes) > 0 {
				// Randomly select one node from each AZ
				selectedNode := sd.selectLowestLoadNode(filteredNodes)
				allValidNodes = append(allValidNodes, selectedNode)
			}
		}
	}

	if len(allValidNodes) == 0 {
		return sd.handleNoResults("no matching nodes found", affinityMode)
	}

	// Apply limit constraint
	limit := int(filter.Limit)
	if limit == 0 || limit >= len(allValidNodes) {
		return metasOf(allValidNodes), nil
	}

	return metasOf(sd.selectLowestLoadNodes(allValidNodes, limit)), nil
}

// SelectMultiAzMultiRg: azList → slice randomly select multiple azs → randomly select rg from each az → azRgIndex[az][rg] random node
func (sd *ServiceDiscovery) SelectMultiAzMultiRg(filter *proto.NodeFilter, affinityMode proto.AffinityMode) ([]*proto.NodeMeta, error) {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	// 1. Get candidate AZ list
	candidateAZs := sd.getCandidateAZs(filter)
	if len(candidateAZs) == 0 {
		return sd.handleNoResults("no matching AZs found", affinityMode)
	}

	// Phase 1: Random attempt phase
	maxRandomAttempts := min(5, len(candidateAZs)*2)

	for attempt := 0; attempt < maxRandomAttempts; attempt++ {
		// 2. Randomly select multiple AZs
		limit := int(filter.Limit)
		var numAZs int
		if limit == 0 {
			numAZs = len(candidateAZs) // All AZs
		} else {
			numAZs = min(limit, len(candidateAZs))
		}

		var selectedNodes []*NodeInfo
		selectedAZs := sd.randomSelectStrings(candidateAZs, numAZs)

		// 3. Randomly select RG from each AZ, then randomly select node
		for _, az := range selectedAZs {
			candidateRGs := sd.getCandidateRGsInAZ(az, filter)
			if len(candidateRGs) > 0 {
				selectedRG := candidateRGs[rand.Intn(len(candidateRGs))]
				nodes := sd.azRgIndex[az][selectedRG]
				filteredNodes := sd.filterByTags(nodes, filter.Tags)
				filteredNodes = sd.excludeDecommissioning(filteredNodes)
				if len(filteredNodes) > 0 {
					selectedNode := sd.selectLowestLoadNode(filteredNodes)
					selectedNodes = append(selectedNodes, selectedNode)
				}
			}
		}

		// 4. If limit is specified, randomly select again
		if limit > 0 && len(selectedNodes) > limit {
			selectedNodes = sd.selectLowestLoadNodes(selectedNodes, limit)
		}

		if len(selectedNodes) > 0 {
			return metasOf(selectedNodes), nil
		}
	}

	// Phase 2: Fallback to exhaustive search
	return sd.exhaustiveSearchMultiAzMultiRg(candidateAZs, filter, affinityMode)
}

// exhaustiveSearchMultiAzMultiRg exhaustive search for MultiAzMultiRg strategy
func (sd *ServiceDiscovery) exhaustiveSearchMultiAzMultiRg(candidateAZs []string, filter *proto.NodeFilter, affinityMode proto.AffinityMode) ([]*proto.NodeMeta, error) {
	var allValidNodes []*NodeInfo

	// Collect all qualifying nodes (select one node from each AZ-RG combination)
	for _, az := range candidateAZs {
		candidateRGs := sd.getCandidateRGsInAZ(az, filter)
		for _, rg := range candidateRGs {
			nodes := sd.azRgIndex[az][rg]
			filteredNodes := sd.filterByTags(nodes, filter.Tags)
			filteredNodes = sd.excludeDecommissioning(filteredNodes)
			if len(filteredNodes) > 0 {
				// Randomly select one node from each AZ-RG combination
				selectedNode := sd.selectLowestLoadNode(filteredNodes)
				allValidNodes = append(allValidNodes, selectedNode)
			}
		}
	}

	if len(allValidNodes) == 0 {
		return sd.handleNoResults("no matching nodes found", affinityMode)
	}

	// Apply limit constraint
	limit := int(filter.Limit)
	if limit == 0 || limit >= len(allValidNodes) {
		return metasOf(allValidNodes), nil
	}

	return metasOf(sd.selectLowestLoadNodes(allValidNodes, limit)), nil
}

// SelectRandom: random selection (equivalent to SelectMultiAzMultiRg but without AZ/RG quantity constraints)
func (sd *ServiceDiscovery) SelectRandom(filter *proto.NodeFilter, affinityMode proto.AffinityMode) ([]*proto.NodeMeta, error) {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	// Collect all qualifying nodes
	var allCandidates []*NodeInfo

	candidateAZs := sd.getCandidateAZs(filter)
	for _, az := range candidateAZs {
		candidateRGs := sd.getCandidateRGsInAZ(az, filter)
		for _, rg := range candidateRGs {
			nodes := sd.azRgIndex[az][rg]
			filteredNodes := sd.filterByTags(nodes, filter.Tags)
			filteredNodes = sd.excludeDecommissioning(filteredNodes)
			allCandidates = append(allCandidates, filteredNodes...)
		}
	}

	if len(allCandidates) == 0 && affinityMode == proto.AffinityMode_HARD {
		return nil, fmt.Errorf("no matching nodes found")
	}

	return metasOf(sd.selectLowestLoadNodes(allCandidates, int(filter.Limit))), nil
}

// SelectCustom filter matches az/rg → corresponding slice randomly selects nodes
func (sd *ServiceDiscovery) SelectCustom(filter *proto.NodeFilter, affinityMode proto.AffinityMode) ([]*proto.NodeMeta, error) {
	// Custom strategy is equivalent to SelectRandom, but can be extended with special logic
	return sd.SelectRandom(filter, affinityMode)
}

// SelectRandomGroup pre-partitions candidates into non-overlapping groups of `limit` size,
// then randomly picks one group. This reduces overlap across selections compared to pure random.
func (sd *ServiceDiscovery) SelectRandomGroup(filter *proto.NodeFilter, affinityMode proto.AffinityMode) ([]*proto.NodeMeta, error) {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	// 1. Collect all candidates (same as SelectRandom)
	var allCandidates []*NodeInfo
	candidateAZs := sd.getCandidateAZs(filter)
	for _, az := range candidateAZs {
		candidateRGs := sd.getCandidateRGsInAZ(az, filter)
		for _, rg := range candidateRGs {
			nodes := sd.azRgIndex[az][rg]
			filteredNodes := sd.filterByTags(nodes, filter.Tags)
			filteredNodes = sd.excludeDecommissioning(filteredNodes)
			allCandidates = append(allCandidates, filteredNodes...)
		}
	}

	if len(allCandidates) == 0 {
		if affinityMode == proto.AffinityMode_HARD {
			return nil, fmt.Errorf("no matching nodes found")
		}
		return []*proto.NodeMeta{}, nil
	}

	limit := int(filter.Limit)
	if limit == 0 || limit >= len(allCandidates) {
		return metasOf(allCandidates), nil
	}

	// 2. Sort by NodeId (deterministic, stable grouping)
	sort.Slice(allCandidates, func(i, j int) bool {
		return allCandidates[i].Meta.NodeId < allCandidates[j].Meta.NodeId
	})

	// 3. Partition into groups of `limit` size
	numGroups := len(allCandidates) / limit
	if numGroups == 0 {
		numGroups = 1
	}

	// 4. Randomly pick a group
	groupIdx := rand.Intn(numGroups)
	start := groupIdx * limit
	end := start + limit

	if end <= len(allCandidates) {
		// Full group — return exactly `limit` nodes
		return metasOf(allCandidates[start:end]), nil
	}

	// 5. Partial last group — take what's available, fill from remaining
	selected := make([]*NodeInfo, 0, limit)
	selected = append(selected, allCandidates[start:]...)

	// Fill from other nodes (those not in this group)
	remaining := make([]*NodeInfo, 0, start)
	remaining = append(remaining, allCandidates[:start]...)

	needed := limit - len(selected)
	if needed > len(remaining) {
		needed = len(remaining)
	}
	// Random fill from remaining
	filled := sd.selectLowestLoadNodes(remaining, needed)
	selected = append(selected, filled...)

	return metasOf(selected), nil
}

// === Helper method implementations ===

// Get candidate AZ list (supports regex matching)
func (sd *ServiceDiscovery) getCandidateAZs(filter *proto.NodeFilter) []string {
	if filter.Az == "" {
		return sd.azList
	}

	// Check if it's a regular expression
	if sd.isRegexLike(filter.Az) {
		regex, err := sd.getCompiledRegex(filter.Az)
		if err != nil {
			return []string{}
		}
		var candidates []string
		for _, az := range sd.azList {
			if regex.MatchString(az) {
				candidates = append(candidates, az)
			}
		}
		return candidates
	} else {
		// Exact match
		for _, az := range sd.azList {
			if az == filter.Az {
				return []string{az}
			}
		}
		return []string{}
	}
}

// Get candidate RG list (supports regex matching)
func (sd *ServiceDiscovery) getCandidateRGs(filter *proto.NodeFilter) []string {
	if filter.ResourceGroup == "" {
		return sd.rgList
	}

	// Check if it's a regular expression
	if sd.isRegexLike(filter.ResourceGroup) {
		regex, err := sd.getCompiledRegex(filter.ResourceGroup)
		if err != nil {
			return []string{}
		}
		var candidates []string
		for _, rg := range sd.rgList {
			if regex.MatchString(rg) {
				candidates = append(candidates, rg)
			}
		}
		return candidates
	} else {
		// Exact match
		for _, rg := range sd.rgList {
			if rg == filter.ResourceGroup {
				return []string{rg}
			}
		}
		return []string{}
	}
}

// Get candidate RG list under specified AZ
func (sd *ServiceDiscovery) getCandidateRGsInAZ(az string, filter *proto.NodeFilter) []string {
	allRGs, exists := sd.azRgIndexKeys[az]
	if !exists {
		return []string{}
	}

	if filter.ResourceGroup == "" {
		return allRGs
	}

	// Filter RGs
	if sd.isRegexLike(filter.ResourceGroup) {
		regex, err := sd.getCompiledRegex(filter.ResourceGroup)
		if err != nil {
			return []string{}
		}
		var candidates []string
		for _, rg := range allRGs {
			if regex.MatchString(rg) {
				candidates = append(candidates, rg)
			}
		}
		return candidates
	} else {
		// Exact match
		for _, rg := range allRGs {
			if rg == filter.ResourceGroup {
				return []string{rg}
			}
		}
		return []string{}
	}
}

// Get candidate AZ list under specified RG
func (sd *ServiceDiscovery) getCandidateAZsInRG(rg string, filter *proto.NodeFilter) []string {
	allAZs, exists := sd.rgAzIndexKeys[rg]
	if !exists {
		return []string{}
	}

	if filter.Az == "" {
		return allAZs
	}

	// Filter AZs
	if sd.isRegexLike(filter.Az) {
		regex, err := sd.getCompiledRegex(filter.Az)
		if err != nil {
			return []string{}
		}
		var candidates []string
		for _, az := range allAZs {
			if regex.MatchString(az) {
				candidates = append(candidates, az)
			}
		}
		return candidates
	} else {
		// Exact match
		for _, az := range allAZs {
			if az == filter.Az {
				return []string{az}
			}
		}
		return []string{}
	}
}

// === Compatibility methods ===

func (sd *ServiceDiscovery) GetAllServers() map[string]*proto.NodeMeta {
	sd.mu.RLock()
	defer sd.mu.RUnlock()
	result := make(map[string]*proto.NodeMeta, len(sd.Nodes))
	for k, v := range sd.Nodes {
		result[k] = v.Meta
	}
	return result
}

// RuntimeOf returns a node's latest runtime snapshot, or nil when the node is
// unknown or has not announced one yet. The snapshot is immutable: a newer
// reading replaces it rather than mutating it, so the caller may read it after
// this returns.
func (sd *ServiceDiscovery) RuntimeOf(nodeID string) *proto.NodeRuntimeInfo {
	sd.mu.RLock()
	node, exists := sd.Nodes[nodeID]
	sd.mu.RUnlock()
	if !exists {
		return nil
	}
	return node.Runtime()
}

func (sd *ServiceDiscovery) GetResourceGroups() []string {
	sd.mu.RLock()
	defer sd.mu.RUnlock()
	return append([]string{}, sd.rgList...)
}

func (sd *ServiceDiscovery) GetServersByResourceGroup(resourceGroup string) []*proto.NodeMeta {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	var result []*NodeInfo
	if azMap, exists := sd.rgAzIndex[resourceGroup]; exists {
		for _, nodes := range azMap {
			result = append(result, nodes...)
		}
	}
	return metasOf(result)
}

func (sd *ServiceDiscovery) GetAZDistribution(resourceGroup string) map[string]int {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	distribution := make(map[string]int)
	if azMap, exists := sd.rgAzIndex[resourceGroup]; exists {
		for az, nodes := range azMap {
			distribution[az] = len(nodes)
		}
	}
	return distribution
}

func (sd *ServiceDiscovery) SelectServersAcrossAZ(resourceGroup string, count int) ([]*proto.NodeMeta, []string, error) {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	availableAZs, exists := sd.rgAzIndexKeys[resourceGroup]
	if !exists {
		return nil, nil, fmt.Errorf("resource group %s not found", resourceGroup)
	}

	if len(availableAZs) < count {
		return nil, nil, fmt.Errorf("insufficient AZs: need %d, have %d", count, len(availableAZs))
	}

	selected := make([]*NodeInfo, 0, count)
	selectedAZs := make([]string, 0, count)

	// Randomly select AZs
	chosenAZs := sd.randomSelectStrings(availableAZs, count)

	for _, az := range chosenAZs {
		nodes := sd.rgAzIndex[resourceGroup][az]
		if len(nodes) > 0 {
			randomNode := sd.selectLowestLoadNode(nodes)
			selected = append(selected, randomNode)
			selectedAZs = append(selectedAZs, az)
		}
	}

	if len(selected) < count {
		return nil, nil, fmt.Errorf("cannot select %d servers across different AZs", count)
	}

	return metasOf(selected), selectedAZs, nil
}

// === Index maintenance helper methods ===

func (sd *ServiceDiscovery) ensureAZInList(az string) {
	for _, existingAZ := range sd.azList {
		if existingAZ == az {
			return
		}
	}
	sd.azList = append(sd.azList, az)
}

func (sd *ServiceDiscovery) ensureRGInList(rg string) {
	for _, existingRG := range sd.rgList {
		if existingRG == rg {
			return
		}
	}
	sd.rgList = append(sd.rgList, rg)
}

func (sd *ServiceDiscovery) ensureRGInAZKeys(az, rg string) {
	rgList, exists := sd.azRgIndexKeys[az]
	if !exists {
		sd.azRgIndexKeys[az] = []string{rg}
		return
	}

	for _, existingRG := range rgList {
		if existingRG == rg {
			return
		}
	}
	sd.azRgIndexKeys[az] = append(rgList, rg)
}

func (sd *ServiceDiscovery) ensureAZInRGKeys(rg, az string) {
	azList, exists := sd.rgAzIndexKeys[rg]
	if !exists {
		sd.rgAzIndexKeys[rg] = []string{az}
		return
	}

	for _, existingAZ := range azList {
		if existingAZ == az {
			return
		}
	}
	sd.rgAzIndexKeys[rg] = append(azList, az)
}

func (sd *ServiceDiscovery) removeAZFromList(az string) {
	for i, existingAZ := range sd.azList {
		if existingAZ == az {
			sd.azList = append(sd.azList[:i], sd.azList[i+1:]...)
			return
		}
	}
}

func (sd *ServiceDiscovery) removeRGFromList(rg string) {
	for i, existingRG := range sd.rgList {
		if existingRG == rg {
			sd.rgList = append(sd.rgList[:i], sd.rgList[i+1:]...)
			return
		}
	}
}

func (sd *ServiceDiscovery) removeRGFromAZKeys(az, rg string) {
	rgList, exists := sd.azRgIndexKeys[az]
	if !exists {
		return
	}

	for i, existingRG := range rgList {
		if existingRG == rg {
			sd.azRgIndexKeys[az] = append(rgList[:i], rgList[i+1:]...)
			if len(sd.azRgIndexKeys[az]) == 0 {
				delete(sd.azRgIndexKeys, az)
			}
			return
		}
	}
}

func (sd *ServiceDiscovery) removeAZFromRGKeys(rg, az string) {
	azList, exists := sd.rgAzIndexKeys[rg]
	if !exists {
		return
	}

	for i, existingAZ := range azList {
		if existingAZ == az {
			sd.rgAzIndexKeys[rg] = append(azList[:i], azList[i+1:]...)
			if len(sd.rgAzIndexKeys[rg]) == 0 {
				delete(sd.rgAzIndexKeys, rg)
			}
			return
		}
	}
}

func (sd *ServiceDiscovery) removeNodeFromSlice(nodes []*NodeInfo, nodeID string) []*NodeInfo {
	for i, node := range nodes {
		if node.Meta.NodeId == nodeID {
			return append(nodes[:i], nodes[i+1:]...)
		}
	}
	return nodes
}

// === Selection and filtering helper methods ===

// loadOf returns the node's effective load and whether it is known (reported
// and fresh within loadTTL). It reads the runtime snapshot, which is where load
// lives — NodeMeta's copy exists only to carry the value to peers running an
// older build (issue #273).
func (sd *ServiceDiscovery) loadOf(node *NodeInfo) (float64, bool) {
	rt := node.Runtime()
	if rt == nil {
		return 0, false
	}
	updated := rt.GetUpdatedAt()
	if updated <= 0 {
		return 0, false
	}
	age := sd.nowFn().UnixMilli() - updated
	if age < 0 || time.Duration(age)*time.Millisecond > sd.loadTTL {
		return 0, false
	}
	load := rt.GetLoadFactor()
	if load < 0 {
		load = 0
	}
	if load > 1 {
		load = 1
	}
	return load, true
}

func (sd *ServiceDiscovery) selectLowestLoadNode(nodes []*NodeInfo) *NodeInfo {
	selected := sd.selectLowestLoadNodes(nodes, 1)
	if len(selected) == 0 {
		return nil
	}
	return selected[0]
}

// selectLowestLoadNodes ranks candidates by load and returns up to `limit` of
// them, preferring lower-load nodes. Load only influences a node's selection
// weight (weight = 1 - load, so lower load => higher weight => more likely
// picked first); it never makes a node unselectable. The result therefore always
// contains min(limit, len(nodes)) distinct nodes, so quorum formation is never
// starved even when every candidate is busy. Nodes with unknown/stale load get a
// neutral weight; if no node has a known load, selection falls back to uniform
// random. When load-aware selection is disabled, load is ignored entirely and
// selection is uniform random.
func (sd *ServiceDiscovery) selectLowestLoadNodes(nodes []*NodeInfo, limit int) []*NodeInfo {
	if len(nodes) == 0 {
		return []*NodeInfo{}
	}

	if !sd.loadAware {
		return sd.randomSelectNodes(nodes, limit)
	}

	weights := make([]float64, len(nodes))
	const minWeight = 0.01 // keep even a near-fully-loaded node selectable
	anyKnown := false
	for i, n := range nodes {
		load, known := sd.loadOf(n)
		if known {
			anyKnown = true
		} else {
			load = sd.unknownLoad
		}
		w := 1 - load
		if w < minWeight {
			w = minWeight
		}
		weights[i] = w
	}

	if !anyKnown {
		metrics.WpQuorumSelectionSkew.WithLabelValues("random_no_load").Inc()
		return sd.randomSelectNodes(nodes, limit)
	}
	metrics.WpQuorumSelectionSkew.WithLabelValues("weighted").Inc()

	picked := weightedSampleIndices(weights, limit, sd.randFloat)
	out := make([]*NodeInfo, 0, len(picked))
	for _, i := range picked {
		out = append(out, nodes[i])
	}
	return out
}

func (sd *ServiceDiscovery) randomSelectNodes(nodes []*NodeInfo, limit int) []*NodeInfo {
	if len(nodes) == 0 {
		return []*NodeInfo{}
	}

	if limit == 0 || limit >= len(nodes) {
		return nodes
	}

	// Fisher-Yates partial shuffle: O(limit) guaranteed, no retries
	shuffled := make([]*NodeInfo, len(nodes))
	copy(shuffled, nodes)
	for i := 0; i < limit; i++ {
		j := i + rand.Intn(len(shuffled)-i)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}
	return shuffled[:limit]
}

func (sd *ServiceDiscovery) randomSelectStrings(strs []string, limit int) []string {
	if len(strs) == 0 {
		return []string{}
	}

	if limit >= len(strs) {
		return strs
	}

	// Fisher-Yates partial shuffle: O(limit) guaranteed, no retries
	shuffled := make([]string, len(strs))
	copy(shuffled, strs)
	for i := 0; i < limit; i++ {
		j := i + rand.Intn(len(shuffled)-i)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}
	return shuffled[:limit]
}

func (sd *ServiceDiscovery) filterByTags(nodes []*NodeInfo, tags map[string]string) []*NodeInfo {
	if len(tags) == 0 {
		return nodes
	}

	var filtered []*NodeInfo
	for _, node := range nodes {
		if node.Meta.Tags != nil {
			matches := true
			for key, value := range tags {
				if nodeValue, exists := node.Meta.Tags[key]; !exists || nodeValue != value {
					matches = false
					break
				}
			}
			if matches {
				filtered = append(filtered, node)
			}
		}
	}
	return filtered
}

// excludeDecommissioning filters out nodes that have the "status" tag set to "decommissioning" or "decommissioned".
func (sd *ServiceDiscovery) excludeDecommissioning(nodes []*NodeInfo) []*NodeInfo {
	var active []*NodeInfo
	for _, node := range nodes {
		if node.Meta.Tags != nil {
			if status, exists := node.Meta.Tags["status"]; exists && (status == "decommissioning" || status == "decommissioned") {
				continue
			}
		}
		active = append(active, node)
	}
	return active
}

func (sd *ServiceDiscovery) isRegexLike(pattern string) bool {
	// Simple regular expression detection
	regexChars := []string{"*", "+", "?", "[", "]", "(", ")", "{", "}", "^", "$", "|", "\\"}
	for _, char := range regexChars {
		if len(pattern) > 0 && strings.Contains(pattern, char) {
			return true
		}
	}
	return false
}

func (sd *ServiceDiscovery) getCompiledRegex(pattern string) (*regexp.Regexp, error) {
	if pattern == "" {
		return nil, nil
	}

	if regex, exists := sd.regexCache.Get(pattern); exists {
		return regex, nil
	}

	// Anchor for full-string matching: MatchString does substring match by default,
	// which would cause "rg[12]" to match "rg1-extra". Wrapping with ^(?:...)$
	// ensures AZ/RG names are matched as complete strings.
	anchored := "^(?:" + pattern + ")$"
	regex, err := regexp.Compile(anchored)
	if err != nil {
		return nil, err
	}

	sd.regexCache.Add(pattern, regex)
	return regex, nil
}
