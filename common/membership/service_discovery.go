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
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/zilliztech/woodpecker/proto"
)

// ServiceDiscovery - High-performance service discovery implementation designed for O(1) query optimization
// Query frequency >> Update frequency, maintaining indexes for O(1) query priority
type ServiceDiscovery struct {
	mu    sync.RWMutex
	Nodes map[string]*proto.NodeMeta // nodeId -> NodeMeta

	azList []string // All available AZs
	rgList []string // All available RGs

	// Core indexes: bidirectional indexes supporting all query strategies
	azRgIndex map[string]map[string][]*proto.NodeMeta // az -> rg -> []*NodeMeta
	rgAzIndex map[string]map[string][]*proto.NodeMeta // rg -> az -> []*NodeMeta

	// Auxiliary slices for O(1) random key selection
	azRgIndexKeys map[string][]string // az -> []rg (all RGs in this AZ)
	rgAzIndexKeys map[string][]string // rg -> []az (all AZs in this RG)

	// Regular expression cache (used only when needed)
	regexCache *lru.Cache[string, *regexp.Regexp]
}

func NewServiceDiscovery() *ServiceDiscovery {
	cache, err := lru.New[string, *regexp.Regexp](100) // TODO should be configurable
	if err != nil {
		panic(fmt.Sprintf("Failed to create regex cache: %v", err))
	}

	return &ServiceDiscovery{
		Nodes:         make(map[string]*proto.NodeMeta),
		azList:        make([]string, 0),
		rgList:        make([]string, 0),
		azRgIndex:     make(map[string]map[string][]*proto.NodeMeta),
		rgAzIndex:     make(map[string]map[string][]*proto.NodeMeta),
		azRgIndexKeys: make(map[string][]string),
		rgAzIndexKeys: make(map[string][]string),
		regexCache:    cache,
	}
}

// UpdateServer updates server information and maintains all indexes
func (sd *ServiceDiscovery) UpdateServer(nodeID string, meta *proto.NodeMeta) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	// If node already exists, remove it from all indexes first
	if oldMeta, exists := sd.Nodes[nodeID]; exists {
		sd.removeFromIndexes(nodeID, oldMeta)
	}

	// Add to main storage
	sd.Nodes[nodeID] = meta

	// Add to all indexes
	sd.addToIndexes(nodeID, meta)
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

// addToIndexes adds node to all indexes
func (sd *ServiceDiscovery) addToIndexes(nodeID string, meta *proto.NodeMeta) {
	az := meta.Az
	rg := meta.ResourceGroup

	// 1. Ensure AZ is in azList
	sd.ensureAZInList(az)

	// 2. Ensure RG is in rgList
	sd.ensureRGInList(rg)

	// 3. Add to azRgIndex
	if sd.azRgIndex[az] == nil {
		sd.azRgIndex[az] = make(map[string][]*proto.NodeMeta)
	}
	sd.azRgIndex[az][rg] = append(sd.azRgIndex[az][rg], meta)

	// 4. Add to rgAzIndex
	if sd.rgAzIndex[rg] == nil {
		sd.rgAzIndex[rg] = make(map[string][]*proto.NodeMeta)
	}
	sd.rgAzIndex[rg][az] = append(sd.rgAzIndex[rg][az], meta)

	// 5. Update azRgIndexKeys
	sd.ensureRGInAZKeys(az, rg)

	// 6. Update rgAzIndexKeys
	sd.ensureAZInRGKeys(rg, az)
}

// removeFromIndexes removes node from all indexes
func (sd *ServiceDiscovery) removeFromIndexes(nodeID string, meta *proto.NodeMeta) {
	az := meta.Az
	rg := meta.ResourceGroup

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

		if len(filteredNodes) > 0 {
			return sd.randomSelectNodes(filteredNodes, int(filter.Limit)), nil
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
		nodes []*proto.NodeMeta
	}

	for _, az := range candidateAZs {
		candidateRGs := sd.getCandidateRGsInAZ(az, filter)
		for _, rg := range candidateRGs {
			nodes := sd.azRgIndex[az][rg]
			filteredNodes := sd.filterByTags(nodes, filter.Tags)
			if len(filteredNodes) > 0 {
				validCombinations = append(validCombinations, struct {
					az    string
					rg    string
					nodes []*proto.NodeMeta
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
	return sd.randomSelectNodes(selectedCombination.nodes, int(filter.Limit)), nil
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
		var selectedNodes []*proto.NodeMeta
		limit := int(filter.Limit)
		if limit == 0 || limit > len(candidateRGs) {
			limit = len(candidateRGs)
		}

		selectedRGs := sd.randomSelectStrings(candidateRGs, limit)

		// 5. Randomly select one node from each RG
		for _, rg := range selectedRGs {
			nodes := sd.azRgIndex[selectedAZ][rg]
			filteredNodes := sd.filterByTags(nodes, filter.Tags)
			if len(filteredNodes) > 0 {
				selectedNode := filteredNodes[rand.Intn(len(filteredNodes))]
				selectedNodes = append(selectedNodes, selectedNode)
			}
		}

		if len(selectedNodes) > 0 {
			return selectedNodes, nil
		}
	}

	// Phase 2: Fallback to exhaustive search
	return sd.exhaustiveSearchSingleAzMultiRg(candidateAZs, filter, affinityMode)
}

// exhaustiveSearchSingleAzMultiRg exhaustive search for SingleAzMultiRg strategy
func (sd *ServiceDiscovery) exhaustiveSearchSingleAzMultiRg(candidateAZs []string, filter *proto.NodeFilter, affinityMode proto.AffinityMode) ([]*proto.NodeMeta, error) {
	var allValidNodes []*proto.NodeMeta

	// Collect all qualifying nodes
	for _, az := range candidateAZs {
		candidateRGs := sd.getCandidateRGsInAZ(az, filter)
		for _, rg := range candidateRGs {
			nodes := sd.azRgIndex[az][rg]
			filteredNodes := sd.filterByTags(nodes, filter.Tags)
			if len(filteredNodes) > 0 {
				// Randomly select one node from each RG
				selectedNode := filteredNodes[rand.Intn(len(filteredNodes))]
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
		return allValidNodes, nil
	}

	return sd.randomSelectNodes(allValidNodes, limit), nil
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
		var selectedNodes []*proto.NodeMeta
		limit := int(filter.Limit)
		if limit == 0 || limit > len(candidateAZs) {
			limit = len(candidateAZs)
		}

		selectedAZs := sd.randomSelectStrings(candidateAZs, limit)

		// 5. Randomly select one node from each AZ
		for _, az := range selectedAZs {
			nodes := sd.rgAzIndex[selectedRG][az]
			filteredNodes := sd.filterByTags(nodes, filter.Tags)
			if len(filteredNodes) > 0 {
				selectedNode := filteredNodes[rand.Intn(len(filteredNodes))]
				selectedNodes = append(selectedNodes, selectedNode)
			}
		}

		if len(selectedNodes) > 0 {
			return selectedNodes, nil
		}
	}

	// Phase 2: Fallback to exhaustive search
	return sd.exhaustiveSearchMultiAzSingleRg(candidateRGs, filter, affinityMode)
}

// exhaustiveSearchMultiAzSingleRg exhaustive search for MultiAzSingleRg strategy
func (sd *ServiceDiscovery) exhaustiveSearchMultiAzSingleRg(candidateRGs []string, filter *proto.NodeFilter, affinityMode proto.AffinityMode) ([]*proto.NodeMeta, error) {
	var allValidNodes []*proto.NodeMeta

	// Collect all qualifying nodes
	for _, rg := range candidateRGs {
		candidateAZs := sd.getCandidateAZsInRG(rg, filter)
		for _, az := range candidateAZs {
			nodes := sd.rgAzIndex[rg][az]
			filteredNodes := sd.filterByTags(nodes, filter.Tags)
			if len(filteredNodes) > 0 {
				// Randomly select one node from each AZ
				selectedNode := filteredNodes[rand.Intn(len(filteredNodes))]
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
		return allValidNodes, nil
	}

	return sd.randomSelectNodes(allValidNodes, limit), nil
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
		numAZs := len(candidateAZs)
		if limit == 0 {
			numAZs = len(candidateAZs) // All AZs
		} else {
			numAZs = min(limit, len(candidateAZs))
		}

		var selectedNodes []*proto.NodeMeta
		selectedAZs := sd.randomSelectStrings(candidateAZs, numAZs)

		// 3. Randomly select RG from each AZ, then randomly select node
		for _, az := range selectedAZs {
			candidateRGs := sd.getCandidateRGsInAZ(az, filter)
			if len(candidateRGs) > 0 {
				selectedRG := candidateRGs[rand.Intn(len(candidateRGs))]
				nodes := sd.azRgIndex[az][selectedRG]
				filteredNodes := sd.filterByTags(nodes, filter.Tags)
				if len(filteredNodes) > 0 {
					selectedNode := filteredNodes[rand.Intn(len(filteredNodes))]
					selectedNodes = append(selectedNodes, selectedNode)
				}
			}
		}

		// 4. If limit is specified, randomly select again
		if limit > 0 && len(selectedNodes) > limit {
			selectedNodes = sd.randomSelectNodes(selectedNodes, limit)
		}

		if len(selectedNodes) > 0 {
			return selectedNodes, nil
		}
	}

	// Phase 2: Fallback to exhaustive search
	return sd.exhaustiveSearchMultiAzMultiRg(candidateAZs, filter, affinityMode)
}

// exhaustiveSearchMultiAzMultiRg exhaustive search for MultiAzMultiRg strategy
func (sd *ServiceDiscovery) exhaustiveSearchMultiAzMultiRg(candidateAZs []string, filter *proto.NodeFilter, affinityMode proto.AffinityMode) ([]*proto.NodeMeta, error) {
	var allValidNodes []*proto.NodeMeta

	// Collect all qualifying nodes (select one node from each AZ-RG combination)
	for _, az := range candidateAZs {
		candidateRGs := sd.getCandidateRGsInAZ(az, filter)
		for _, rg := range candidateRGs {
			nodes := sd.azRgIndex[az][rg]
			filteredNodes := sd.filterByTags(nodes, filter.Tags)
			if len(filteredNodes) > 0 {
				// Randomly select one node from each AZ-RG combination
				selectedNode := filteredNodes[rand.Intn(len(filteredNodes))]
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
		return allValidNodes, nil
	}

	return sd.randomSelectNodes(allValidNodes, limit), nil
}

// SelectRandom: random selection (equivalent to SelectMultiAzMultiRg but without AZ/RG quantity constraints)
func (sd *ServiceDiscovery) SelectRandom(filter *proto.NodeFilter, affinityMode proto.AffinityMode) ([]*proto.NodeMeta, error) {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	// Collect all qualifying nodes
	var allCandidates []*proto.NodeMeta

	candidateAZs := sd.getCandidateAZs(filter)
	for _, az := range candidateAZs {
		candidateRGs := sd.getCandidateRGsInAZ(az, filter)
		for _, rg := range candidateRGs {
			nodes := sd.azRgIndex[az][rg]
			filteredNodes := sd.filterByTags(nodes, filter.Tags)
			allCandidates = append(allCandidates, filteredNodes...)
		}
	}

	if len(allCandidates) == 0 && affinityMode == proto.AffinityMode_HARD {
		return nil, fmt.Errorf("no matching nodes found")
	}

	return sd.randomSelectNodes(allCandidates, int(filter.Limit)), nil
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
	var allCandidates []*proto.NodeMeta
	candidateAZs := sd.getCandidateAZs(filter)
	for _, az := range candidateAZs {
		candidateRGs := sd.getCandidateRGsInAZ(az, filter)
		for _, rg := range candidateRGs {
			nodes := sd.azRgIndex[az][rg]
			filteredNodes := sd.filterByTags(nodes, filter.Tags)
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
		return allCandidates, nil
	}

	// 2. Sort by NodeId (deterministic, stable grouping)
	sort.Slice(allCandidates, func(i, j int) bool {
		return allCandidates[i].NodeId < allCandidates[j].NodeId
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
		return allCandidates[start:end], nil
	}

	// 5. Partial last group — take what's available, fill from remaining
	selected := make([]*proto.NodeMeta, 0, limit)
	selected = append(selected, allCandidates[start:]...)

	// Fill from other nodes (those not in this group)
	remaining := make([]*proto.NodeMeta, 0, start)
	remaining = append(remaining, allCandidates[:start]...)

	needed := limit - len(selected)
	if needed > len(remaining) {
		needed = len(remaining)
	}
	// Random fill from remaining
	filled := sd.randomSelectNodes(remaining, needed)
	selected = append(selected, filled...)

	return selected, nil
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
	result := make(map[string]*proto.NodeMeta)
	for k, v := range sd.Nodes {
		result[k] = v
	}
	return result
}

func (sd *ServiceDiscovery) GetResourceGroups() []string {
	sd.mu.RLock()
	defer sd.mu.RUnlock()
	return append([]string{}, sd.rgList...)
}

func (sd *ServiceDiscovery) GetServersByResourceGroup(resourceGroup string) []*proto.NodeMeta {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	var result []*proto.NodeMeta
	if azMap, exists := sd.rgAzIndex[resourceGroup]; exists {
		for _, nodes := range azMap {
			result = append(result, nodes...)
		}
	}
	return result
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

	selected := make([]*proto.NodeMeta, 0, count)
	selectedAZs := make([]string, 0, count)

	// Randomly select AZs
	chosenAZs := sd.randomSelectStrings(availableAZs, count)

	for _, az := range chosenAZs {
		nodes := sd.rgAzIndex[resourceGroup][az]
		if len(nodes) > 0 {
			randomNode := nodes[rand.Intn(len(nodes))]
			selected = append(selected, randomNode)
			selectedAZs = append(selectedAZs, az)
		}
	}

	if len(selected) < count {
		return nil, nil, fmt.Errorf("cannot select %d servers across different AZs", count)
	}

	return selected, selectedAZs, nil
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

func (sd *ServiceDiscovery) removeNodeFromSlice(nodes []*proto.NodeMeta, nodeID string) []*proto.NodeMeta {
	for i, node := range nodes {
		if node.NodeId == nodeID {
			return append(nodes[:i], nodes[i+1:]...)
		}
	}
	return nodes
}

// === Selection and filtering helper methods ===

func (sd *ServiceDiscovery) randomSelectNodes(nodes []*proto.NodeMeta, limit int) []*proto.NodeMeta {
	if len(nodes) == 0 {
		return []*proto.NodeMeta{}
	}

	if limit == 0 || limit >= len(nodes) {
		return nodes
	}

	// Fisher-Yates partial shuffle: O(limit) guaranteed, no retries
	shuffled := make([]*proto.NodeMeta, len(nodes))
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

func (sd *ServiceDiscovery) filterByTags(nodes []*proto.NodeMeta, tags map[string]string) []*proto.NodeMeta {
	if len(tags) == 0 {
		return nodes
	}

	var filtered []*proto.NodeMeta
	for _, node := range nodes {
		if node.Tags != nil {
			matches := true
			for key, value := range tags {
				if nodeValue, exists := node.Tags[key]; !exists || nodeValue != value {
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

func (sd *ServiceDiscovery) isRegexLike(pattern string) bool {
	// Simple regular expression detection
	regexChars := []string{".", "*", "+", "?", "[", "]", "(", ")", "{", "}", "^", "$", "|", "\\"}
	for _, char := range regexChars {
		if len(pattern) > 0 && contains(pattern, char) {
			return true
		}
	}
	return false
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
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

	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	sd.regexCache.Add(pattern, regex)
	return regex, nil
}

// === Utility methods ===

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
