package quorum

import (
	"context"
	"fmt"
	"time"

	netutil "github.com/zilliztech/woodpecker/common/net"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/membership"
	"github.com/zilliztech/woodpecker/common/retry"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

var _ QuorumDiscovery = (*passiveQuorumDiscovery)(nil)

type passiveQuorumDiscovery struct {
	cfg         *config.QuorumConfig
	clientNodes []*membership.ClientNode
}

func NewPassiveQuorumDiscovery(ctx context.Context, cfg *config.QuorumConfig) (QuorumDiscovery, error) {
	logger.Ctx(ctx).Info("Initializing passive quorum discovery",
		zap.Int("bufferPoolsCount", len(cfg.BufferPools)))

	localIP := netutil.GetLocalIP()

	// Initialize client nodes for each buffer pool (region)
	clientNodes := make([]*membership.ClientNode, 0, len(cfg.BufferPools))

	for i, pool := range cfg.BufferPools {
		clientConfig := &membership.ClientConfig{
			NodeID:   fmt.Sprintf("C%d-%s", time.Now().Nanosecond(), pool.Name),
			BindAddr: localIP,
			BindPort: 0, // auto-assign port for each client node
		}

		clientNode, newNodeErr := membership.NewClientNode(clientConfig)
		if newNodeErr != nil {
			// Clean up any previously created client nodes before returning error
			for _, existingNode := range clientNodes {
				if existingNode != nil {
					_ = existingNode.Leave() // Best effort cleanup
				}
			}
			return nil, fmt.Errorf("failed to create client node for pool %s: %w", pool.Name, newNodeErr)
		}

		// Join the seeds for this pool/region
		if len(pool.Seeds) > 0 {
			joinErr := clientNode.Join(pool.Seeds)
			if joinErr != nil {
				logger.Ctx(ctx).Warn("Failed to join seeds for pool",
					zap.String("poolName", pool.Name),
					zap.Strings("seeds", pool.Seeds),
					zap.Error(joinErr))
				// Continue with other pools even if one fails
			} else {
				logger.Ctx(ctx).Info("Successfully joined pool",
					zap.String("poolName", pool.Name),
					zap.Strings("seeds", pool.Seeds))
			}
		} else {
			logger.Ctx(ctx).Warn("No seeds provided for pool, using legacy ServiceSeedNodes",
				zap.String("poolName", pool.Name))
			continue
		}

		clientNodes = append(clientNodes, clientNode)

		// Add small delay between joins to avoid overwhelming the network
		if i < len(cfg.BufferPools)-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	if len(clientNodes) == 0 {
		return nil, fmt.Errorf("no client nodes were successfully created")
	}

	logger.Ctx(ctx).Info("Passive quorum discovery initialization completed",
		zap.Int("createdClientNodes", len(clientNodes)))

	return &passiveQuorumDiscovery{cfg: cfg, clientNodes: clientNodes}, nil
}

func (d *passiveQuorumDiscovery) SelectQuorumNodes(ctx context.Context) (*proto.QuorumInfo, error) {
	requiredNodes := d.cfg.GetEnsembleSize()
	writeQuorumSize := d.cfg.GetWriteQuorumSize()
	ackNodes := d.cfg.GetAckQuorumSize()

	logger.Ctx(ctx).Info("Passive discovery: Starting quorum node selection",
		zap.String("strategy", d.cfg.SelectStrategy.Strategy),
		zap.String("affinityMode", d.cfg.SelectStrategy.AffinityMode),
		zap.Int("ensembleSize", requiredNodes),
		zap.Int("writeQuorumSize", writeQuorumSize),
		zap.Int("ackQuorumSize", ackNodes),
		zap.Int("availableClientNodes", len(d.clientNodes)))

	var result *proto.QuorumInfo
	var affinityMode proto.AffinityMode
	if d.cfg.SelectStrategy.AffinityMode == "hard" {
		affinityMode = proto.AffinityMode_HARD
	} else {
		affinityMode = proto.AffinityMode_SOFT
	}

	// Pre-validate configuration before retry to avoid unnecessary retries
	if d.cfg.SelectStrategy.Strategy == "custom" {
		if len(d.cfg.SelectStrategy.CustomPlacement) == 0 {
			return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg("custom placement strategy requires CustomPlacement configuration")
		}
		if len(d.cfg.SelectStrategy.CustomPlacement) != requiredNodes {
			return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg(fmt.Sprintf("custom placement rules count (%d) must equal required nodes count (%d)", len(d.cfg.SelectStrategy.CustomPlacement), requiredNodes))
		}
	}

	// Pre-validate client nodes availability
	if len(d.clientNodes) == 0 {
		return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg("no client nodes available")
	}

	// Pre-validate cross-region requirements
	if d.cfg.SelectStrategy.Strategy == "cross-region" && len(d.clientNodes) < 2 {
		if affinityMode == proto.AffinityMode_HARD {
			return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg("cross-region strategy requires at least 2 client nodes")
		}
	}

	err := retry.Do(ctx, func() error {
		switch d.cfg.SelectStrategy.Strategy {
		case "cross-region":
			return d.selectCrossRegionNodes(ctx, requiredNodes, writeQuorumSize, ackNodes, affinityMode, &result)
		case "custom":
			return d.selectCustomPlacementNodes(ctx, requiredNodes, writeQuorumSize, ackNodes, affinityMode, &result)
		default:
			// For single region strategies, use first available client node
			return d.selectSingleRegionNodes(ctx, requiredNodes, writeQuorumSize, ackNodes, affinityMode, &result)
		}
	}, retry.AttemptAlways(), retry.Sleep(200*time.Millisecond), retry.MaxSleepTime(2*time.Second))

	if err != nil {
		return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErr(err)
	}

	return result, nil
}

func (d *passiveQuorumDiscovery) selectSingleRegionNodes(ctx context.Context, requiredNodes, writeQuorumSize, ackNodes int, affinityMode proto.AffinityMode, result **proto.QuorumInfo) error {
	// Validation done in SelectQuorumNodes method

	// Use the first available client node
	clientNode := d.clientNodes[0]
	discovery := clientNode.GetDiscovery()
	if discovery == nil {
		return werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg("service discovery not available")
	}

	return d.selectNodesFromDiscovery(ctx, discovery, requiredNodes, writeQuorumSize, ackNodes, affinityMode, "", "", result)
}

func (d *passiveQuorumDiscovery) selectCrossRegionNodes(ctx context.Context, requiredNodes, writeQuorumSize, ackNodes int, affinityMode proto.AffinityMode, result **proto.QuorumInfo) error {
	// Validation done in SelectQuorumNodes method
	// In soft mode, may fall back to single region if insufficient client nodes
	if len(d.clientNodes) < 2 && affinityMode == proto.AffinityMode_SOFT {
		return d.selectSingleRegionNodes(ctx, requiredNodes, writeQuorumSize, ackNodes, affinityMode, result)
	}

	// Calculate nodes per region (try to distribute evenly)
	nodesPerRegion := requiredNodes / len(d.clientNodes)
	remainingNodes := requiredNodes % len(d.clientNodes)

	var allSelectedNodes []string

	for i, clientNode := range d.clientNodes {
		discovery := clientNode.GetDiscovery()
		if discovery == nil {
			logger.Ctx(ctx).Warn("Service discovery not available for client node", zap.Int("nodeIndex", i))
			continue
		}

		// Distribute remaining nodes to first regions
		regionNodes := nodesPerRegion
		if i < remainingNodes {
			regionNodes++
		}

		if regionNodes == 0 {
			continue
		}

		// Select nodes from this region's discovery
		var regionResult *proto.QuorumInfo
		err := d.selectNodesFromDiscovery(ctx, discovery, regionNodes, regionNodes, regionNodes, affinityMode, "", "", &regionResult)
		if err != nil {
			if affinityMode == proto.AffinityMode_HARD {
				return err
			}
			logger.Ctx(ctx).Warn("Failed to get nodes from region, continuing with soft affinity",
				zap.Int("clientNodeIndex", i),
				zap.Error(err))
			continue
		}

		if regionResult != nil {
			allSelectedNodes = append(allSelectedNodes, regionResult.Nodes...)
		}
	}

	if len(allSelectedNodes) < requiredNodes {
		if affinityMode == proto.AffinityMode_HARD {
			return werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg(fmt.Sprintf("insufficient nodes across regions: got %d, required %d", len(allSelectedNodes), requiredNodes))
		}
		// In soft mode, try to fill remaining nodes from any available client node
		return d.fillRemainingNodes(ctx, allSelectedNodes, requiredNodes, writeQuorumSize, ackNodes, result)
	}

	// Trim to exact required number if we got more
	if len(allSelectedNodes) > requiredNodes {
		allSelectedNodes = allSelectedNodes[:requiredNodes]
	}

	*result = &proto.QuorumInfo{
		Id:    1,
		Nodes: allSelectedNodes,
		Aq:    int32(ackNodes),
		Wq:    int32(writeQuorumSize),
		Es:    int32(requiredNodes),
	}

	return nil
}

func (d *passiveQuorumDiscovery) selectCustomPlacementNodes(ctx context.Context, requiredNodes, writeQuorumSize, ackNodes int, affinityMode proto.AffinityMode, result **proto.QuorumInfo) error {
	// Validation is now done before retry in SelectQuorumNodes method

	logger.Ctx(ctx).Info("Custom placement node selection started",
		zap.Int("placementRulesCount", len(d.cfg.SelectStrategy.CustomPlacement)),
		zap.Int("requiredNodes", requiredNodes))

	var allSelectedNodes []string

	for i, placement := range d.cfg.SelectStrategy.CustomPlacement {
		logger.Ctx(ctx).Info("Processing custom placement rule",
			zap.Int("ruleIndex", i),
			zap.String("region", placement.Region),
			zap.String("az", placement.Az),
			zap.String("resourceGroup", placement.ResourceGroup))

		// Find the client node for this placement region
		var targetClientNode *membership.ClientNode
		for j, pool := range d.cfg.BufferPools {
			if pool.Name == placement.Region && j < len(d.clientNodes) {
				targetClientNode = d.clientNodes[j]
				break
			}
		}

		if targetClientNode == nil {
			return werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg(fmt.Sprintf("client node not found for custom placement rule %d (region: %s)", i, placement.Region))
		}

		discovery := targetClientNode.GetDiscovery()
		if discovery == nil {
			return werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg(fmt.Sprintf("service discovery not available for custom placement rule %d (region: %s)", i, placement.Region))
		}

		// Request exactly one node with specific placement constraints
		// Each CustomPlacement rule must select exactly one node
		var regionResult *proto.QuorumInfo
		err := d.selectNodesFromDiscovery(ctx, discovery, 1, 1, 1, affinityMode, placement.Az, placement.ResourceGroup, &regionResult)
		if err != nil {
			return fmt.Errorf("failed to select node for custom placement rule %d (region: %s, az: %s, rg: %s): %w", i, placement.Region, placement.Az, placement.ResourceGroup, err)
		}

		if regionResult == nil || len(regionResult.Nodes) == 0 {
			return werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg(fmt.Sprintf("no node found for custom placement rule %d (region: %s, az: %s, rg: %s)", i, placement.Region, placement.Az, placement.ResourceGroup))
		}

		// Each rule must produce exactly one node
		selectedNode := regionResult.Nodes[0]
		allSelectedNodes = append(allSelectedNodes, selectedNode)

		logger.Ctx(ctx).Info("Successfully selected node for custom placement rule",
			zap.Int("ruleIndex", i),
			zap.String("region", placement.Region),
			zap.String("az", placement.Az),
			zap.String("resourceGroup", placement.ResourceGroup),
			zap.String("selectedNode", selectedNode))
	}

	// At this point, we must have exactly requiredNodes nodes
	if len(allSelectedNodes) != requiredNodes {
		return werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg(fmt.Sprintf("custom placement validation failed: expected %d nodes, got %d nodes", requiredNodes, len(allSelectedNodes)))
	}

	logger.Ctx(ctx).Info("Custom placement node selection completed successfully",
		zap.Int("selectedNodesCount", len(allSelectedNodes)),
		zap.Strings("selectedNodes", allSelectedNodes))

	*result = &proto.QuorumInfo{
		Id:    1,
		Nodes: allSelectedNodes,
		Aq:    int32(ackNodes),
		Wq:    int32(writeQuorumSize),
		Es:    int32(len(allSelectedNodes)),
	}

	return nil
}

func (d *passiveQuorumDiscovery) fillRemainingNodes(ctx context.Context, currentNodes []string, requiredNodes, writeQuorumSize, ackNodes int, result **proto.QuorumInfo) error {
	remainingNeeded := requiredNodes - len(currentNodes)
	if remainingNeeded <= 0 {
		*result = &proto.QuorumInfo{
			Id:    1,
			Nodes: currentNodes,
			Aq:    int32(ackNodes),
			Wq:    int32(writeQuorumSize),
			Es:    int32(requiredNodes),
		}
		return nil
	}

	// Try to get remaining nodes from any available client node
	for i, clientNode := range d.clientNodes {
		discovery := clientNode.GetDiscovery()
		if discovery == nil {
			continue
		}

		var fillResult *proto.QuorumInfo
		err := d.selectNodesFromDiscovery(ctx, discovery, remainingNeeded, remainingNeeded, remainingNeeded, proto.AffinityMode_SOFT, "", "", &fillResult)
		if err == nil && fillResult != nil {
			currentNodes = append(currentNodes, fillResult.Nodes...)
			logger.Ctx(ctx).Info("Filled remaining nodes from client node",
				zap.Int("clientNodeIndex", i),
				zap.Int("addedNodes", len(fillResult.Nodes)))
			break
		}
	}

	*result = &proto.QuorumInfo{
		Id:    1,
		Nodes: currentNodes,
		Aq:    int32(ackNodes),
		Wq:    int32(writeQuorumSize),
		Es:    int32(len(currentNodes)), // Use actual count
	}

	return nil
}

func (d *passiveQuorumDiscovery) selectNodesFromDiscovery(ctx context.Context, discovery *membership.ServiceDiscovery, nodeCount, writeQuorumSize, ackNodes int, affinityMode proto.AffinityMode, az, resourceGroup string, result **proto.QuorumInfo) error {
	// Create node filter
	filter := &proto.NodeFilter{
		Limit: int32(nodeCount),
	}

	if az != "" {
		filter.Az = az
	}
	if resourceGroup != "" {
		filter.ResourceGroup = resourceGroup
	}

	// Select nodes using the appropriate strategy
	var selectedNodes []*proto.NodeMeta
	var err error

	switch d.cfg.SelectStrategy.Strategy {
	case "single-az-single-rg":
		selectedNodes, err = discovery.SelectSingleAzSingleRg(filter, affinityMode)
	case "single-az-multi-rg":
		selectedNodes, err = discovery.SelectSingleAzMultiRg(filter, affinityMode)
	case "multi-az-single-rg":
		selectedNodes, err = discovery.SelectMultiAzSingleRg(filter, affinityMode)
	case "multi-az-multi-rg":
		selectedNodes, err = discovery.SelectMultiAzMultiRg(filter, affinityMode)
	default:
		// Default to random selection
		selectedNodes, err = discovery.SelectRandom(filter, affinityMode)
	}

	if err != nil {
		return werr.ErrWoodpeckerClientConnectionFailed.WithCauseErr(err)
	}

	if len(selectedNodes) == 0 {
		return werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg("no nodes returned from service discovery")
	}

	// Convert to endpoint addresses
	nodeEndpoints := make([]string, len(selectedNodes))
	for i, node := range selectedNodes {
		nodeEndpoints[i] = node.Endpoint
	}

	*result = &proto.QuorumInfo{
		Id:    1,
		Nodes: nodeEndpoints,
		Aq:    int32(ackNodes),
		Wq:    int32(writeQuorumSize),
		Es:    int32(len(selectedNodes)),
	}

	logger.Ctx(ctx).Info("Passive discovery: Successfully selected nodes from service discovery",
		zap.String("strategy", d.cfg.SelectStrategy.Strategy),
		zap.String("filter_az", az),
		zap.String("filter_resourceGroup", resourceGroup),
		zap.Int("requestedNodes", nodeCount),
		zap.Int("returnedNodes", len(selectedNodes)),
		zap.Strings("endpoints", nodeEndpoints))

	return nil
}

func (d *passiveQuorumDiscovery) Close(ctx context.Context) error {
	logger.Ctx(ctx).Info("Closing passive quorum discovery",
		zap.Int("clientNodesCount", len(d.clientNodes)))

	var closeErrors []error
	for i, clientNode := range d.clientNodes {
		if clientNode != nil {
			err := clientNode.Leave()
			if err != nil {
				logger.Ctx(ctx).Warn("Failed to close client node",
					zap.Int("nodeIndex", i),
					zap.Error(err))
				closeErrors = append(closeErrors, err)
			} else {
				logger.Ctx(ctx).Info("Successfully closed client node",
					zap.Int("nodeIndex", i))
			}
		}
	}

	if len(closeErrors) > 0 {
		logger.Ctx(ctx).Warn("Some client nodes failed to close",
			zap.Int("failedCount", len(closeErrors)),
			zap.Int("totalCount", len(d.clientNodes)))
		// Return the first error, but log all
		return closeErrors[0]
	}

	logger.Ctx(ctx).Info("All client nodes closed successfully")
	return nil
}
