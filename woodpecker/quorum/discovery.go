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

// QuorumDiscovery is the interface for quorum nodes discovery.
type QuorumDiscovery interface {
	SelectQuorum(ctx context.Context) (*proto.QuorumInfo, error)
	Close(ctx context.Context) error
}

// Ensure quorumDiscoveryImpl implements QuorumDiscovery interface
var _ QuorumDiscovery = (*quorumDiscovery)(nil)

type quorumDiscovery struct {
	cfg        *config.QuorumConfig
	clientPool client.LogStoreClientPool

	// initialized config values
	es           int32               // Ensemble Size
	wq           int32               // Write Quorum
	aq           int32               // Ack Quorum
	affinityMode proto.AffinityMode  // Parsed affinity mode
	strategyType proto.StrategyType  // Parsed strategy type
	filters      []*proto.NodeFilter // Pre-built filters for node selection
}

func NewQuorumDiscovery(ctx context.Context, cfg *config.QuorumConfig, clientPool client.LogStoreClientPool) (QuorumDiscovery, error) {
	logger.Ctx(ctx).Info("Initializing active quorum discovery")

	discovery := &quorumDiscovery{
		cfg:        cfg,
		clientPool: clientPool,
		es:         int32(cfg.GetEnsembleSize()),
		wq:         int32(cfg.GetWriteQuorumSize()),
		aq:         int32(cfg.GetAckQuorumSize()),
	}

	// Parse and validate affinity mode
	if err := discovery.parseAffinityMode(); err != nil {
		logger.Ctx(ctx).Warn("Invalid affinity mode", zap.Error(err))
		return nil, err
	}

	// Parse and validate strategy type
	if err := discovery.parseStrategyType(); err != nil {
		logger.Ctx(ctx).Warn("Invalid strategy type", zap.Error(err))
		return nil, err
	}

	// Pre-build filters based on strategy
	if err := discovery.buildFilters(ctx); err != nil {
		logger.Ctx(ctx).Warn("Failed to build filters", zap.Error(err))
		return nil, err
	}

	logger.Ctx(ctx).Info("Successfully initialized quorum discovery",
		zap.Int32("ensembleSize", discovery.es),
		zap.Int32("writeQuorum", discovery.wq),
		zap.Int32("ackQuorum", discovery.aq),
		zap.String("strategy", cfg.SelectStrategy.Strategy),
		zap.String("affinityMode", cfg.SelectStrategy.AffinityMode),
		zap.Int("filtersCount", len(discovery.filters)))

	return discovery, nil
}

// parseAffinityMode converts string affinity mode to proto enum
func (d *quorumDiscovery) parseAffinityMode() error {
	switch d.cfg.SelectStrategy.AffinityMode {
	case "hard":
		d.affinityMode = proto.AffinityMode_HARD
	case "soft", "": // Default to soft for empty values
		d.affinityMode = proto.AffinityMode_SOFT
	default:
		return fmt.Errorf("invalid affinity mode: %s, must be 'hard' or 'soft'", d.cfg.SelectStrategy.AffinityMode)
	}
	return nil
}

// parseStrategyType converts string strategy to proto enum
func (d *quorumDiscovery) parseStrategyType() error {
	switch d.cfg.SelectStrategy.Strategy {
	case "single-az-single-rg":
		d.strategyType = proto.StrategyType_SINGLE_AZ_SINGLE_RG
	case "single-az-multi-rg":
		d.strategyType = proto.StrategyType_SINGLE_AZ_MULTI_RG
	case "multi-az-single-rg":
		d.strategyType = proto.StrategyType_MULTI_AZ_SINGLE_RG
	case "multi-az-multi-rg":
		d.strategyType = proto.StrategyType_MULTI_AZ_MULTI_RG
	case "cross-region":
		d.strategyType = proto.StrategyType_CROSS_REGION
	case "custom":
		d.strategyType = proto.StrategyType_CUSTOM
	case "random-group":
		d.strategyType = proto.StrategyType_RANDOM_GROUP
	case "random", "": // Default to random for empty values
		d.strategyType = proto.StrategyType_RANDOM
	default:
		// Default to RANDOM for unknown strategies for backward compatibility
		d.strategyType = proto.StrategyType_RANDOM
	}
	return nil
}

// buildFilters creates the appropriate filters based on strategy
func (d *quorumDiscovery) buildFilters(ctx context.Context) error {
	strategy := d.cfg.SelectStrategy.Strategy

	switch strategy {
	case "custom":
		return d.buildCustomFilters(ctx)
	case "cross-region":
		return d.buildCrossRegionFilters(ctx)
	default:
		// For single-* and multi-* strategies, and empty strategy, use a single filter with limit
		return d.buildSingleFilter()
	}
}

// buildCustomFilters creates one filter per custom placement rule
func (d *quorumDiscovery) buildCustomFilters(ctx context.Context) error {
	requiredNodes := int(d.es)

	if len(d.cfg.SelectStrategy.CustomPlacement) == 0 {
		return fmt.Errorf("custom strategy requires CustomPlacement configuration")
	}

	if len(d.cfg.SelectStrategy.CustomPlacement) != requiredNodes {
		return fmt.Errorf("custom placement rules count (%d) must equal required nodes count (%d)",
			len(d.cfg.SelectStrategy.CustomPlacement), requiredNodes)
	}

	d.filters = make([]*proto.NodeFilter, len(d.cfg.SelectStrategy.CustomPlacement))

	for i, placement := range d.cfg.SelectStrategy.CustomPlacement {
		d.filters[i] = &proto.NodeFilter{
			Limit:         1, // Each custom placement selects exactly one node
			Az:            placement.Az,
			ResourceGroup: placement.ResourceGroup,
		}

		logger.Ctx(ctx).Debug("Built custom filter",
			zap.Int("index", i),
			zap.String("region", placement.Region),
			zap.String("az", placement.Az),
			zap.String("resourceGroup", placement.ResourceGroup))
	}

	return nil
}

// buildCrossRegionFilters creates filters for cross-region strategy
func (d *quorumDiscovery) buildCrossRegionFilters(ctx context.Context) error {
	if len(d.cfg.BufferPools) < 2 {
		return fmt.Errorf("cross-region strategy requires at least two buffer pools")
	}

	// For cross-region, we'll use single-az-single-rg strategy per region
	// Create one filter that will be used for each region
	d.filters = []*proto.NodeFilter{
		{
			Limit: int32(d.es), // Will be adjusted per region during selection
		},
	}

	logger.Ctx(ctx).Debug("Built cross-region filter", zap.Int("poolsCount", len(d.cfg.BufferPools)))
	return nil
}

// buildSingleFilter creates a single filter for non-custom strategies
func (d *quorumDiscovery) buildSingleFilter() error {
	d.filters = []*proto.NodeFilter{
		{
			Limit: d.es, // Select all required nodes with one filter
		},
	}
	return nil
}

func (d *quorumDiscovery) SelectQuorum(ctx context.Context) (*proto.QuorumInfo, error) {
	logger.Ctx(ctx).Info("Active discovery: Starting quorum node selection",
		zap.String("strategy", d.cfg.SelectStrategy.Strategy),
		zap.String("affinityMode", d.cfg.SelectStrategy.AffinityMode),
		zap.Int32("ensembleSize", d.es),
		zap.Int("filtersCount", len(d.filters)))

	var result *proto.QuorumInfo

	// Additional runtime validation for buffer pools
	if len(d.cfg.BufferPools) == 0 {
		return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg("no buffer pools configured")
	}

	// Always retry until success
	err := retry.Do(ctx, func() error {
		var err error
		switch d.cfg.SelectStrategy.Strategy {
		case "cross-region":
			result, err = d.selectCrossRegionQuorum(ctx)
		case "custom":
			result, err = d.selectCustomPlacementQuorum(ctx)
		default:
			result, err = d.selectSingleRegionQuorum(ctx)
		}
		return err
	}, retry.AttemptAlways(), retry.Sleep(200*time.Millisecond), retry.MaxSleepTime(2*time.Second))

	if err != nil {
		return nil, werr.ErrServiceSelectQuorumFailed.WithCauseErr(err)
	}

	return result, nil
}

func (d *quorumDiscovery) selectSingleRegionQuorum(ctx context.Context) (*proto.QuorumInfo, error) {
	// Randomly select a buffer pool for load distribution
	pool := d.cfg.BufferPools[rand.Intn(len(d.cfg.BufferPools))]
	if len(pool.Seeds) == 0 {
		return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg(fmt.Sprintf("no seeds configured for pool %s", pool.Name))
	}

	// Select a random seed to query
	selectedSeed := pool.Seeds[rand.Intn(len(pool.Seeds))]

	// Use pre-built filter (should have exactly one filter for single-region strategies)
	if len(d.filters) != 1 {
		return nil, fmt.Errorf("expected 1 filter for single-region strategy, got %d", len(d.filters))
	}

	return d.requestNodesFromSeed(ctx, selectedSeed, d.filters[0], int(d.wq))
}

func (d *quorumDiscovery) selectCrossRegionQuorum(ctx context.Context) (*proto.QuorumInfo, error) {
	requiredNodes := int(d.es)

	// Calculate nodes per region (try to distribute evenly)
	nodesPerRegion := requiredNodes / len(d.cfg.BufferPools)
	remainingNodes := requiredNodes % len(d.cfg.BufferPools)

	var allSelectedNodes []string
	selectedSet := make(map[string]bool) // Track selected endpoints to avoid duplicates

	// Use pre-built filter as template, but adjust limit per region
	if len(d.filters) != 1 {
		return nil, fmt.Errorf("expected 1 filter for cross-region strategy, got %d", len(d.filters))
	}

	baseFilter := d.filters[0]

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

		// Create region-specific filter by adjusting the limit
		regionFilter := &proto.NodeFilter{
			Limit:         int32(regionNodes),
			Az:            baseFilter.Az,
			ResourceGroup: baseFilter.ResourceGroup,
		}

		// Request nodes from this region
		regionResult, err := d.requestNodesFromSeed(ctx, selectedSeed, regionFilter, 0)
		if err != nil {
			if d.affinityMode == proto.AffinityMode_HARD {
				return nil, err
			}
			logger.Ctx(ctx).Warn("Failed to get nodes from region, continuing with soft affinity",
				zap.String("poolName", pool.Name),
				zap.String("seed", selectedSeed),
				zap.Error(err))
			continue
		}

		if regionResult != nil {
			for _, node := range regionResult.Nodes {
				if !selectedSet[node] {
					selectedSet[node] = true
					allSelectedNodes = append(allSelectedNodes, node)
				}
			}
		}
	}

	if len(allSelectedNodes) < requiredNodes {
		if d.affinityMode == proto.AffinityMode_HARD {
			return nil, werr.ErrServiceInsufficientQuorum.WithCauseErrMsg(fmt.Sprintf("insufficient nodes across regions: got %d, required %d", len(allSelectedNodes), requiredNodes))
		}
		// In soft mode, try to fill remaining nodes from any available region
		return d.fillRemainingNodes(ctx, allSelectedNodes, selectedSet)
	}

	// Trim to exact required number if we got more
	if len(allSelectedNodes) > requiredNodes {
		allSelectedNodes = allSelectedNodes[:requiredNodes]
	}

	result := &proto.QuorumInfo{
		Id:    1,
		Nodes: allSelectedNodes,
		Aq:    d.aq,
		Wq:    d.wq,
		Es:    d.es,
	}

	return result, nil
}

func (d *quorumDiscovery) selectCustomPlacementQuorum(ctx context.Context) (*proto.QuorumInfo, error) {
	requiredNodes := int(d.es)

	logger.Ctx(ctx).Info("Active custom placement node selection started",
		zap.Int("placementRulesCount", len(d.cfg.SelectStrategy.CustomPlacement)),
		zap.Int("requiredNodes", requiredNodes),
		zap.Int("filtersCount", len(d.filters)))

	// Validate we have the expected number of filters
	if len(d.filters) != len(d.cfg.SelectStrategy.CustomPlacement) {
		return nil, fmt.Errorf("filters count (%d) does not match custom placement rules count (%d)",
			len(d.filters), len(d.cfg.SelectStrategy.CustomPlacement))
	}

	var allSelectedNodes []string

	for i, placement := range d.cfg.SelectStrategy.CustomPlacement {
		logger.Ctx(ctx).Debug("Processing active custom placement rule",
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
			return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg(fmt.Sprintf("buffer pool not found for custom placement rule %d (region: %s)", i, placement.Region))
		}

		if len(targetPool.Seeds) == 0 {
			return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg(fmt.Sprintf("no seeds configured for custom placement rule %d (region: %s)", i, placement.Region))
		}

		// Select a random seed from this region
		selectedSeed := targetPool.Seeds[rand.Intn(len(targetPool.Seeds))]

		// Use pre-built filter for this placement
		regionResult, err := d.requestNodesFromSeed(ctx, selectedSeed, d.filters[i], int(d.filters[i].Limit))
		if err != nil {
			return nil, fmt.Errorf("failed to select node for custom placement rule %d (region: %s, az: %s, rg: %s): %w",
				i, placement.Region, placement.Az, placement.ResourceGroup, err)
		}

		if regionResult == nil || len(regionResult.Nodes) == 0 {
			return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg(fmt.Sprintf("no node found for custom placement rule %d (region: %s, az: %s, rg: %s)",
				i, placement.Region, placement.Az, placement.ResourceGroup))
		}

		// Each rule must produce exactly one node
		selectedNode := regionResult.Nodes[0]
		allSelectedNodes = append(allSelectedNodes, selectedNode)

		logger.Ctx(ctx).Debug("Successfully selected node for active custom placement rule",
			zap.Int("ruleIndex", i),
			zap.String("region", placement.Region),
			zap.String("az", placement.Az),
			zap.String("resourceGroup", placement.ResourceGroup),
			zap.String("selectedSeed", selectedSeed),
			zap.String("selectedNode", selectedNode))
	}

	// At this point, we must have exactly requiredNodes nodes
	if len(allSelectedNodes) != requiredNodes {
		return nil, werr.ErrServiceInsufficientQuorum.WithCauseErrMsg(fmt.Sprintf("active custom placement validation failed: expected %d nodes, got %d nodes", requiredNodes, len(allSelectedNodes)))
	}

	logger.Ctx(ctx).Info("Active custom placement node selection completed successfully",
		zap.Int("selectedNodesCount", len(allSelectedNodes)),
		zap.Strings("selectedNodes", allSelectedNodes))

	result := &proto.QuorumInfo{
		Id:    1,
		Nodes: allSelectedNodes,
		Aq:    d.aq,
		Wq:    d.wq,
		Es:    d.es,
	}

	return result, nil
}

func (d *quorumDiscovery) fillRemainingNodes(ctx context.Context, currentNodes []string, selectedSet map[string]bool) (*proto.QuorumInfo, error) {
	requiredNodes := int(d.es)
	remainingNeeded := requiredNodes - len(currentNodes)
	if remainingNeeded <= 0 {
		result := &proto.QuorumInfo{
			Id:    1,
			Nodes: currentNodes,
			Aq:    d.aq,
			Wq:    d.wq,
			Es:    d.es,
		}
		return result, nil
	}

	// Initialize selectedSet if nil (defensive)
	if selectedSet == nil {
		selectedSet = make(map[string]bool)
		for _, node := range currentNodes {
			selectedSet[node] = true
		}
	}

	// Create a temporary filter for remaining nodes
	fillFilter := &proto.NodeFilter{
		Limit: int32(remainingNeeded),
	}

	// Try to get remaining nodes from any available pool
	for _, pool := range d.cfg.BufferPools {
		if len(pool.Seeds) == 0 {
			continue
		}

		selectedSeed := pool.Seeds[rand.Intn(len(pool.Seeds))]
		fillFilter.Limit = int32(requiredNodes - len(currentNodes))
		fillResult, err := d.requestNodesFromSeed(ctx, selectedSeed, fillFilter, 0)
		if err != nil {
			logger.Ctx(ctx).Warn("Failed to get remaining nodes from pool, continuing with soft affinity",
				zap.String("poolName", pool.Name),
				zap.Error(err))
			continue
		}
		if fillResult != nil {
			for _, node := range fillResult.Nodes {
				if !selectedSet[node] {
					selectedSet[node] = true
					currentNodes = append(currentNodes, node)
				}
			}
			if len(currentNodes) >= requiredNodes {
				break
			}
		}
	}

	// Still not enough nodes, return error
	if len(currentNodes) < requiredNodes {
		return nil, werr.ErrServiceInsufficientQuorum.WithCauseErrMsg(fmt.Sprintf("insufficient quorum: expected %d nodes, got %d nodes", requiredNodes, len(currentNodes)))
	}

	result := &proto.QuorumInfo{
		Id:    1,
		Nodes: currentNodes,
		Aq:    d.aq,
		Wq:    d.wq,
		Es:    d.es,
	}

	return result, nil
}

func (d *quorumDiscovery) requestNodesFromSeed(ctx context.Context, seed string, filter *proto.NodeFilter, expectedAtLeast int) (*proto.QuorumInfo, error) {
	logger.Ctx(ctx).Debug("Requesting nodes from seed via gRPC",
		zap.String("seed", seed),
		zap.Int32("nodeCount", filter.Limit),
		zap.String("az", filter.Az),
		zap.String("resourceGroup", filter.ResourceGroup),
		zap.String("affinityMode", d.affinityMode.String()),
		zap.String("strategyType", d.strategyType.String()))

	// Get gRPC client from client pool
	grpcClient, err := d.clientPool.GetLogStoreClient(ctx, seed)
	if err != nil {
		return nil, fmt.Errorf("failed to get gRPC client for seed %s: %w", seed, err)
	}

	// Make gRPC call to SelectNodes using pre-computed values
	selectedNodes, err := grpcClient.SelectNodes(ctx, d.strategyType, d.affinityMode, []*proto.NodeFilter{filter})
	if err != nil {
		return nil, fmt.Errorf("gRPC SelectNodes call failed for seed %s: %w", seed, err)
	}

	if len(selectedNodes) < expectedAtLeast {
		return nil, werr.ErrServiceInsufficientQuorum.WithCauseErrMsg(fmt.Sprintf("insufficient nodes (%d/%d) satisfy the current selection strategy, returned by seed: %s", len(selectedNodes), expectedAtLeast, seed))
	}

	// Convert to endpoint addresses
	nodeEndpoints := make([]string, len(selectedNodes))
	for i, node := range selectedNodes {
		nodeEndpoints[i] = node.Endpoint
	}

	result := &proto.QuorumInfo{ // Intermediate result
		Id:    1,
		Nodes: nodeEndpoints,
		Aq:    d.aq,
		Wq:    d.wq,
		Es:    d.es,
	}

	logger.Ctx(ctx).Debug("Active discovery: Successfully selected nodes from seed via gRPC",
		zap.String("seed", seed),
		zap.String("strategy", d.cfg.SelectStrategy.Strategy),
		zap.String("strategyType", d.strategyType.String()),
		zap.Int32("requestedNodes", filter.Limit),
		zap.Int("returnedNodes", len(selectedNodes)),
		zap.Strings("endpoints", nodeEndpoints))

	return result, nil
}

func (d *quorumDiscovery) Close(ctx context.Context) error {
	// No resources to close for active discovery
	logger.Ctx(ctx).Info("Closing active quorum discovery")
	return nil
}
