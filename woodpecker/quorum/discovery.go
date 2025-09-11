package quorum

import (
	"context"

	"github.com/zilliztech/woodpecker/proto"
)

// QuorumDiscovery is the interface for quorum nodes discovery.
type QuorumDiscovery interface {
	SelectQuorumNodes(ctx context.Context) (*proto.QuorumInfo, error)
	Close(ctx context.Context) error
}
