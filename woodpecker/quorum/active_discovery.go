package quorum

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/retry"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/client"
)

// Ensure activeQuorumDiscovery implements QuorumDiscovery interface
var _ QuorumDiscovery = (*activeQuorumDiscovery)(nil)

type activeQuorumDiscovery struct {
	cfg        *config.QuorumConfig
	clientPool client.LogStoreClientPool
}

func NewActiveQuorumDiscovery(ctx context.Context, cfg *config.QuorumConfig, clientPool client.LogStoreClientPool) QuorumDiscovery {
	logger.Ctx(ctx).Info("Initializing active quorum discovery")
	return &activeQuorumDiscovery{cfg: cfg, clientPool: clientPool}
}

func (d *activeQuorumDiscovery) SelectQuorumNodes(ctx context.Context) (*proto.QuorumInfo, error) {
	requiredNodes := d.cfg.GetEnsembleSize()
	writeQuorumSize := d.cfg.GetWriteQuorumSize()
	ackNodes := d.cfg.GetAckQuorumSize()

	logger.Ctx(ctx).Info("Active discovery: Starting quorum node selection",
		zap.String("strategy", d.cfg.SelectStrategy.Strategy),
		zap.String("affinityMode", d.cfg.SelectStrategy.AffinityMode),
		zap.Int("ensembleSize", requiredNodes),
		zap.Int("writeQuorumSize", writeQuorumSize),
		zap.Int("ackQuorumSize", ackNodes))

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

	// Pre-validate buffer pools configuration
	if len(d.cfg.BufferPools) == 0 {
		return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg("no buffer pools configured")
	}

	// Pre-validate cross-region requirements
	if d.cfg.SelectStrategy.Strategy == "cross-region" && len(d.cfg.BufferPools) < 2 {
		return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg("cross-region strategy requires at least 2 buffer pools")
	}

	err := retry.Do(ctx, func() error {
		switch d.cfg.SelectStrategy.Strategy {
		case "cross-region":
			return d.selectCrossRegionNodes(ctx, requiredNodes, writeQuorumSize, ackNodes, affinityMode, &result)
		case "custom":
			return d.selectCustomPlacementNodes(ctx, requiredNodes, writeQuorumSize, ackNodes, affinityMode, &result)
		default:
			// For single region strategies, use first available pool
			return d.selectSingleRegionNodes(ctx, requiredNodes, writeQuorumSize, ackNodes, affinityMode, &result)
		}
	}, retry.AttemptAlways(), retry.Sleep(200*time.Millisecond), retry.MaxSleepTime(2*time.Second))

	if err != nil {
		return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErr(err)
	}

	return result, nil
}

func (d *activeQuorumDiscovery) selectSingleRegionNodes(ctx context.Context, requiredNodes, writeQuorumSize, ackNodes int, affinityMode proto.AffinityMode, result **proto.QuorumInfo) error {
	// Use the first available buffer pool (validation done in SelectQuorumNodes)

	pool := d.cfg.BufferPools[0]
	if len(pool.Seeds) == 0 {
		return werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg(fmt.Sprintf("no seeds configured for pool %s", pool.Name))
	}

	// Select a random seed to query
	selectedSeed := pool.Seeds[rand.Intn(len(pool.Seeds))]

	return d.requestNodesFromSeed(ctx, selectedSeed, requiredNodes, writeQuorumSize, ackNodes, affinityMode, result)
}

func (d *activeQuorumDiscovery) selectCrossRegionNodes(ctx context.Context, requiredNodes, writeQuorumSize, ackNodes int, affinityMode proto.AffinityMode, result **proto.QuorumInfo) error {
	// Validation done in SelectQuorumNodes method

	// Calculate nodes per region (try to distribute evenly)
	nodesPerRegion := requiredNodes / len(d.cfg.BufferPools)
	remainingNodes := requiredNodes % len(d.cfg.BufferPools)

	var allSelectedNodes []string

	for i, pool := range d.cfg.BufferPools {
		if len(pool.Seeds) == 0 {
			logger.Ctx(ctx).Warn("Pool has no seeds, skipping", zap.String("poolName", pool.Name))
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

		// Select a random seed from this region
		selectedSeed := pool.Seeds[rand.Intn(len(pool.Seeds))]

		// Request nodes from this region
		var regionResult *proto.QuorumInfo
		err := d.requestNodesFromSeed(ctx, selectedSeed, regionNodes, regionNodes, regionNodes, affinityMode, &regionResult)
		if err != nil {
			if affinityMode == proto.AffinityMode_HARD {
				return err
			}
			logger.Ctx(ctx).Warn("Failed to get nodes from region, continuing with soft affinity",
				zap.String("poolName", pool.Name),
				zap.String("seed", selectedSeed),
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
		// In soft mode, try to fill remaining nodes from any available region
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

func (d *activeQuorumDiscovery) selectCustomPlacementNodes(ctx context.Context, requiredNodes, writeQuorumSize, ackNodes int, affinityMode proto.AffinityMode, result **proto.QuorumInfo) error {
	// Validation is now done before retry in SelectQuorumNodes method

	logger.Ctx(ctx).Info("Active custom placement node selection started",
		zap.Int("placementRulesCount", len(d.cfg.SelectStrategy.CustomPlacement)),
		zap.Int("requiredNodes", requiredNodes))

	var allSelectedNodes []string

	for i, placement := range d.cfg.SelectStrategy.CustomPlacement {
		logger.Ctx(ctx).Info("Processing active custom placement rule",
			zap.Int("ruleIndex", i),
			zap.String("region", placement.Region),
			zap.String("az", placement.Az),
			zap.String("resourceGroup", placement.ResourceGroup))
		// Find the buffer pool for this placement
		var targetPool *config.QuorumBufferPool
		for _, pool := range d.cfg.BufferPools {
			if pool.Name == placement.Region {
				targetPool = &pool
				break
			}
		}

		if targetPool == nil {
			return werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg(fmt.Sprintf("buffer pool not found for custom placement rule %d (region: %s)", i, placement.Region))
		}

		if len(targetPool.Seeds) == 0 {
			return werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg(fmt.Sprintf("no seeds configured for custom placement rule %d (region: %s)", i, placement.Region))
		}

		// Select a random seed from this region
		selectedSeed := targetPool.Seeds[rand.Intn(len(targetPool.Seeds))]

		// Request exactly one node with specific placement constraints
		// Each CustomPlacement rule must select exactly one node
		var regionResult *proto.QuorumInfo
		err := d.requestNodesFromSeedWithFilter(ctx, selectedSeed, 1, placement.Az, placement.ResourceGroup, affinityMode, &regionResult)
		if err != nil {
			return fmt.Errorf("failed to select node for custom placement rule %d (region: %s, az: %s, rg: %s): %w", i, placement.Region, placement.Az, placement.ResourceGroup, err)
		}

		if regionResult == nil || len(regionResult.Nodes) == 0 {
			return werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg(fmt.Sprintf("no node found for custom placement rule %d (region: %s, az: %s, rg: %s)", i, placement.Region, placement.Az, placement.ResourceGroup))
		}

		// Each rule must produce exactly one node
		selectedNode := regionResult.Nodes[0]
		allSelectedNodes = append(allSelectedNodes, selectedNode)

		logger.Ctx(ctx).Info("Successfully selected node for active custom placement rule",
			zap.Int("ruleIndex", i),
			zap.String("region", placement.Region),
			zap.String("az", placement.Az),
			zap.String("resourceGroup", placement.ResourceGroup),
			zap.String("selectedSeed", selectedSeed),
			zap.String("selectedNode", selectedNode))
	}

	// At this point, we must have exactly requiredNodes nodes
	if len(allSelectedNodes) != requiredNodes {
		return werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg(fmt.Sprintf("active custom placement validation failed: expected %d nodes, got %d nodes", requiredNodes, len(allSelectedNodes)))
	}

	logger.Ctx(ctx).Info("Active custom placement node selection completed successfully",
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

func (d *activeQuorumDiscovery) fillRemainingNodes(ctx context.Context, currentNodes []string, requiredNodes, writeQuorumSize, ackNodes int, result **proto.QuorumInfo) error {
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

	// Try to get remaining nodes from any available pool
	for _, pool := range d.cfg.BufferPools {
		if len(pool.Seeds) == 0 {
			continue
		}

		selectedSeed := pool.Seeds[rand.Intn(len(pool.Seeds))]
		var fillResult *proto.QuorumInfo
		err := d.requestNodesFromSeed(ctx, selectedSeed, remainingNeeded, remainingNeeded, remainingNeeded, proto.AffinityMode_SOFT, &fillResult)
		if err == nil && fillResult != nil {
			currentNodes = append(currentNodes, fillResult.Nodes...)
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

func (d *activeQuorumDiscovery) requestNodesFromSeed(ctx context.Context, seed string, nodeCount, writeQuorumSize, ackNodes int, affinityMode proto.AffinityMode, result **proto.QuorumInfo) error {
	return d.requestNodesFromSeedWithFilter(ctx, seed, nodeCount, "", "", affinityMode, result)
}

func (d *activeQuorumDiscovery) requestNodesFromSeedWithFilter(ctx context.Context, seed string, nodeCount int, az, resourceGroup string, affinityMode proto.AffinityMode, result **proto.QuorumInfo) error {
	logger.Ctx(ctx).Info("Requesting nodes from seed via gRPC",
		zap.String("seed", seed),
		zap.Int("nodeCount", nodeCount),
		zap.String("az", az),
		zap.String("resourceGroup", resourceGroup),
		zap.String("affinityMode", affinityMode.String()))

	// Get gRPC client from client pool
	grpcClient, err := d.clientPool.GetLogStoreClient(ctx, seed)
	if err != nil {
		return fmt.Errorf("failed to get gRPC client for seed %s: %w", seed, err)
	}

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

	// Determine strategy type based on configuration
	var strategyType proto.StrategyType
	switch d.cfg.SelectStrategy.Strategy {
	case "single-az-single-rg":
		strategyType = proto.StrategyType_SINGLE_AZ_SINGLE_RG
	case "single-az-multi-rg":
		strategyType = proto.StrategyType_SINGLE_AZ_MULTI_RG
	case "multi-az-single-rg":
		strategyType = proto.StrategyType_MULTI_AZ_SINGLE_RG
	case "multi-az-multi-rg":
		strategyType = proto.StrategyType_MULTI_AZ_MULTI_RG
	case "cross-region":
		strategyType = proto.StrategyType_CROSS_REGION
	case "custom":
		strategyType = proto.StrategyType_CUSTOM
	default:
		strategyType = proto.StrategyType_RANDOM
	}

	// Make gRPC call to SelectNodes
	selectedNodes, err := grpcClient.SelectNodes(ctx, strategyType, affinityMode, []*proto.NodeFilter{filter})
	if err != nil {
		return fmt.Errorf("gRPC SelectNodes call failed for seed %s: %w", seed, err)
	}

	if len(selectedNodes) == 0 {
		return werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg(fmt.Sprintf("no nodes returned from seed: %s", seed))
	}

	// Convert to endpoint addresses
	nodeEndpoints := make([]string, len(selectedNodes))
	for i, node := range selectedNodes {
		nodeEndpoints[i] = node.Endpoint
	}

	// Use configuration values for ack and write quorum
	ackNodes := d.cfg.GetAckQuorumSize()
	writeQuorumSize := d.cfg.GetWriteQuorumSize()

	*result = &proto.QuorumInfo{
		Id:    1,
		Nodes: nodeEndpoints,
		Aq:    int32(ackNodes),
		Wq:    int32(writeQuorumSize),
		Es:    int32(len(selectedNodes)),
	}

	logger.Ctx(ctx).Info("Active discovery: Successfully selected nodes from seed via gRPC",
		zap.String("seed", seed),
		zap.String("strategy", d.cfg.SelectStrategy.Strategy),
		zap.String("strategyType", strategyType.String()),
		zap.Int("requestedNodes", nodeCount),
		zap.Int("returnedNodes", len(selectedNodes)),
		zap.Strings("endpoints", nodeEndpoints))

	return nil
}

func (d *activeQuorumDiscovery) Close(ctx context.Context) error {
	// No resources to close for active discovery
	logger.Ctx(ctx).Info("Closing active quorum discovery")
	return nil
}
