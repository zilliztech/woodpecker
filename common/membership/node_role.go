package membership

// NodeRole represents the role of a node
type NodeRole string

const (
	RoleServer NodeRole = "server" // participates in gossip, has AZ and ResourceGroup
	RoleClient NodeRole = "client" // observes only, does not participate in gossip
)
