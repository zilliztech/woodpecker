package membership

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

var _ memberlist.Delegate = (*ServerDelegate)(nil)

// ServerDelegate memberlist delegate for server nodes
type ServerDelegate struct {
	mu          sync.RWMutex
	meta        *ServerMeta
	metaVersion int64 // metadata version, corresponds to version in ServerMeta for compatibility between nodes of different versions
}

func NewServerDelegate(meta *ServerMeta) *ServerDelegate {
	return &ServerDelegate{meta: meta, metaVersion: 1}
}

// NodeMeta returns node metadata for gossip propagation
func (d *ServerDelegate) NodeMeta(limit int) []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()

	d.meta.Version = d.metaVersion
	d.meta.LastUpdate = time.Now()

	data, _ := json.Marshal(d.meta)
	if len(data) > limit {
		log.Printf("Warning: meta data size %d exceeds limit %d", len(data), limit)
		return data[:limit]
	}
	return data
}

// NotifyMsg handles received messages
func (d *ServerDelegate) NotifyMsg(buf []byte) {}

// GetBroadcasts returns messages to be broadcast
func (d *ServerDelegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }

// LocalState returns local state
func (d *ServerDelegate) LocalState(join bool) []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()
	data, _ := json.Marshal(d.meta)
	return data
}

// MergeRemoteState merges remote state
func (d *ServerDelegate) MergeRemoteState(buf []byte, join bool) {}

// UpdateMeta updates metadata
func (d *ServerDelegate) UpdateMeta(updates map[string]interface{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if rg, ok := updates["resource_group"].(string); ok {
		d.meta.ResourceGroup = rg
	}
	if az, ok := updates["az"].(string); ok {
		d.meta.AZ = az
	}
	if tags, ok := updates["tags"].(map[string]string); ok {
		d.meta.Tags = tags
	}
	d.metaVersion++
	d.meta.Version = d.metaVersion
	d.meta.LastUpdate = time.Now()
}
