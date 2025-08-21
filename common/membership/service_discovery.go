package membership

import (
	"fmt"
	"sync"
)

// ServiceDiscovery service discovery (shared by Client and Server)
type ServiceDiscovery struct {
	mu             sync.RWMutex
	servers        map[string]*ServerMeta              // nodeID -> ServerMeta
	resourceGroups map[string]map[string]*ServerMeta   // resourceGroup -> nodeID -> ServerMeta
	azNodes        map[string]map[string][]*ServerMeta // resourceGroup -> AZ -> servers
}

func NewServiceDiscovery() *ServiceDiscovery {
	return &ServiceDiscovery{
		servers:        make(map[string]*ServerMeta),
		resourceGroups: make(map[string]map[string]*ServerMeta),
		azNodes:        make(map[string]map[string][]*ServerMeta),
	}
}

// UpdateServer updates server information
func (sd *ServiceDiscovery) UpdateServer(nodeID string, meta *ServerMeta) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	if oldMeta, exists := sd.servers[nodeID]; exists {
		if oldMeta.ResourceGroup != meta.ResourceGroup {
			sd.removeFromResourceGroup(nodeID, oldMeta.ResourceGroup)
		}
	}

	sd.servers[nodeID] = meta

	if _, exists := sd.resourceGroups[meta.ResourceGroup]; !exists {
		sd.resourceGroups[meta.ResourceGroup] = make(map[string]*ServerMeta)
	}
	sd.resourceGroups[meta.ResourceGroup][nodeID] = meta

	sd.rebuildAZIndex(meta.ResourceGroup)
}

// RemoveServer removes server
func (sd *ServiceDiscovery) RemoveServer(nodeID string) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	if meta, exists := sd.servers[nodeID]; exists {
		delete(sd.servers, nodeID)
		sd.removeFromResourceGroup(nodeID, meta.ResourceGroup)
		sd.rebuildAZIndex(meta.ResourceGroup)
	}
}

func (sd *ServiceDiscovery) removeFromResourceGroup(nodeID, resourceGroup string) {
	if group, exists := sd.resourceGroups[resourceGroup]; exists {
		delete(group, nodeID)
		if len(group) == 0 {
			delete(sd.resourceGroups, resourceGroup)
			delete(sd.azNodes, resourceGroup)
		}
	}
}

func (sd *ServiceDiscovery) rebuildAZIndex(resourceGroup string) {
	sd.azNodes[resourceGroup] = make(map[string][]*ServerMeta)
	if group, exists := sd.resourceGroups[resourceGroup]; exists {
		for _, server := range group {
			sd.azNodes[resourceGroup][server.AZ] = append(sd.azNodes[resourceGroup][server.AZ], server)
		}
	}
}

func (sd *ServiceDiscovery) GetServersByResourceGroup(resourceGroup string) []*ServerMeta {
	sd.mu.RLock()
	defer sd.mu.RUnlock()
	servers := make([]*ServerMeta, 0)
	if group, exists := sd.resourceGroups[resourceGroup]; exists {
		for _, server := range group {
			servers = append(servers, server)
		}
	}
	return servers
}

func (sd *ServiceDiscovery) GetAZDistribution(resourceGroup string) map[string]int {
	sd.mu.RLock()
	defer sd.mu.RUnlock()
	distribution := make(map[string]int)
	if azMap, exists := sd.azNodes[resourceGroup]; exists {
		for az, servers := range azMap {
			distribution[az] = len(servers)
		}
	}
	return distribution
}

// SelectServersAcrossAZ
// TODO need more sophisticated, random, and balanced selection strategy
func (sd *ServiceDiscovery) SelectServersAcrossAZ(resourceGroup string, count int) ([]*ServerMeta, []string, error) {
	sd.mu.RLock()
	defer sd.mu.RUnlock()
	azMap, exists := sd.azNodes[resourceGroup]
	if !exists {
		return nil, nil, fmtError("resource group %s not found", resourceGroup)
	}
	if len(azMap) < count {
		return nil, nil, fmtError("insufficient AZs: need %d, have %d", count, len(azMap))
	}
	selected := make([]*ServerMeta, 0, count)
	selectedAZs := make([]string, 0, count)
	for az, servers := range azMap {
		if len(selected) >= count {
			break
		}
		if len(servers) > 0 {
			selected = append(selected, servers[0])
			selectedAZs = append(selectedAZs, az)
		}
	}
	if len(selected) < count {
		return nil, nil, fmtError("cannot select %d servers across different AZs", count)
	}
	return selected, selectedAZs, nil
}

func (sd *ServiceDiscovery) GetAllServers() map[string]*ServerMeta {
	sd.mu.RLock()
	defer sd.mu.RUnlock()
	result := make(map[string]*ServerMeta)
	for k, v := range sd.servers {
		result[k] = v
	}
	return result
}

func (sd *ServiceDiscovery) GetResourceGroups() []string {
	sd.mu.RLock()
	defer sd.mu.RUnlock()
	groups := make([]string, 0, len(sd.resourceGroups))
	for group := range sd.resourceGroups {
		groups = append(groups, group)
	}
	return groups
}

// lightweight fmt.Errorf to avoid importing fmt everywhere
func fmtError(format string, a ...interface{}) error { return fmt.Errorf(format, a...) }
