package membership

import "time"

// ServerMeta Server node metadata
type ServerMeta struct {
	NodeID        string            `json:"node_id"`        // globally unique node name
	ResourceGroup string            `json:"resource_group"` // resource group the node belongs to
	AZ            string            `json:"az"`             // availability zone the node belongs to
	Endpoint      string            `json:"endpoint"`       // node service address, usually business-related
	Tags          map[string]string `json:"tags"`           // additional tags for the node
	Version       int64             `json:"version"`        // node metadata definition version
	LastUpdate    time.Time         `json:"last_update"`    // last update time
}
