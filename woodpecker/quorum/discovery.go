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
}

func NewQuorumDiscovery(ctx context.Context, cfg *config.QuorumConfig, clientPool client.LogStoreClientPool) (QuorumDiscovery, error) {
	logger.Ctx(ctx).Info("Initializing active quorum discovery")

	d := &quorumDiscovery{cfg: cfg, clientPool: clientPool}

	// Fail-fast validation of the STATIC config (preserves current behavior and
	// the construction-error tests). The per-call read path (SelectQuorum) is
	// tolerant so a bad dynamic override cannot wedge writes.
	if _, err := parseAffinity(cfg.SelectStrategy.AffinityMode.Get()); err != nil {
		logger.Ctx(ctx).Warn("Invalid affinity mode", zap.Error(err))
		return nil, err
	}
	if _, err := buildFilters(ctx, cfg.SelectStrategy.Strategy.Get(), int32(cfg.GetEnsembleSize()), cfg.SelectStrategy.CustomPlacement, cfg.BufferPools); err != nil {
		logger.Ctx(ctx).Warn("Failed to build filters", zap.Error(err))
		return nil, err
	}

	logger.Ctx(ctx).Info("Successfully initialized quorum discovery",
		zap.Int32("ensembleSize", int32(cfg.GetEnsembleSize())),
		zap.String("strategy", cfg.SelectStrategy.Strategy.Get()),
		zap.String("affinityMode", cfg.SelectStrategy.AffinityMode.Get()))
	return d, nil
}

// --- derived values, recomputed fresh from cfg on each read ---

func (d *quorumDiscovery) es() int32 { return int32(d.cfg.GetEnsembleSize()) }
func (d *quorumDiscovery) wq() int32 { return int32(d.cfg.GetWriteQuorumSize()) }
func (d *quorumDiscovery) aq() int32 { return int32(d.cfg.GetAckQuorumSize()) }

func (d *quorumDiscovery) strategyType() proto.StrategyType {
	return parseStrategy(d.cfg.SelectStrategy.Strategy.Get())
}

// affinityMode is tolerant at runtime: an unknown value degrades to SOFT with a
// warning instead of erroring (the static value is validated at construction).
func (d *quorumDiscovery) affinityMode() proto.AffinityMode {
	mode, err := parseAffinity(d.cfg.SelectStrategy.AffinityMode.Get())
	if err != nil {
		logger.Ctx(context.Background()).Warn("Unknown affinity mode at runtime, defaulting to SOFT", zap.Error(err))
		return proto.AffinityMode_SOFT
	}
	return mode
}

// parseAffinity converts a string affinity mode to the proto enum. Empty or
// "soft" => SOFT, "hard" => HARD, anything else => error.
func parseAffinity(mode string) (proto.AffinityMode, error) {
	switch mode {
	case "hard":
		return proto.AffinityMode_HARD, nil
	case "soft", "":
		return proto.AffinityMode_SOFT, nil
	default:
		return proto.AffinityMode_SOFT, fmt.Errorf("invalid affinity mode: %s, must be 'hard' or 'soft'", mode)
	}
}

// parseStrategy converts a string strategy to the proto enum. Unknown/empty
// values default to RANDOM for backward compatibility.
func parseStrategy(strategy string) proto.StrategyType {
	switch strategy {
	case "single-az-single-rg":
		return proto.StrategyType_SINGLE_AZ_SINGLE_RG
	case "single-az-multi-rg":
		return proto.StrategyType_SINGLE_AZ_MULTI_RG
	case "multi-az-single-rg":
		return proto.StrategyType_MULTI_AZ_SINGLE_RG
	case "multi-az-multi-rg":
		return proto.StrategyType_MULTI_AZ_MULTI_RG
	case "cross-region":
		return proto.StrategyType_CROSS_REGION
	case "custom":
		return proto.StrategyType_CUSTOM
	case "random-group":
		return proto.StrategyType_RANDOM_GROUP
	default: // "random", "", and any unknown value
		return proto.StrategyType_RANDOM
	}
}

// buildFilters creates node selection filters for the given strategy.
func buildFilters(ctx context.Context, strategy string, es int32, custom []config.CustomPlacement, pools []config.QuorumBufferPool) ([]*proto.NodeFilter, error) {
	switch strategy {
	case "custom":
		return buildCustomFilters(ctx, es, custom)
	case "cross-region":
		return buildCrossRegionFilters(ctx, es, pools)
	default:
		return buildSingleFilter(es), nil
	}
}

func buildCustomFilters(ctx context.Context, es int32, custom []config.CustomPlacement) ([]*proto.NodeFilter, error) {
	requiredNodes := int(es)
	if len(custom) == 0 {
		return nil, fmt.Errorf("custom strategy requires CustomPlacement configuration")
	}
	if len(custom) != requiredNodes {
		return nil, fmt.Errorf("custom placement rules count (%d) must equal required nodes count (%d)", len(custom), requiredNodes)
	}
	filters := make([]*proto.NodeFilter, len(custom))
	for i, placement := range custom {
		filters[i] = &proto.NodeFilter{
			Limit:         1,
			Az:            placement.Az,
			ResourceGroup: placement.ResourceGroup,
		}
		logger.Ctx(ctx).Debug("Built custom filter",
			zap.Int("index", i),
			zap.String("region", placement.Region),
			zap.String("az", placement.Az),
			zap.String("resourceGroup", placement.ResourceGroup))
	}
	return filters, nil
}

func buildCrossRegionFilters(ctx context.Context, es int32, pools []config.QuorumBufferPool) ([]*proto.NodeFilter, error) {
	if len(pools) < 2 {
		return nil, fmt.Errorf("cross-region strategy requires at least two buffer pools")
	}
	logger.Ctx(ctx).Debug("Built cross-region filter", zap.Int("poolsCount", len(pools)))
	return []*proto.NodeFilter{{Limit: es}}, nil
}

func buildSingleFilter(es int32) []*proto.NodeFilter {
	return []*proto.NodeFilter{{Limit: es}}
}

func (d *quorumDiscovery) SelectQuorum(ctx context.Context) (*proto.QuorumInfo, error) {
	logger.Ctx(ctx).Info("Active discovery: Starting quorum node selection",
		zap.String("strategy", d.cfg.SelectStrategy.Strategy.Get()),
		zap.String("affinityMode", d.cfg.SelectStrategy.AffinityMode.Get()),
		zap.Int32("ensembleSize", d.es()))

	if len(d.cfg.BufferPools) == 0 {
		return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg("no buffer pools configured")
	}

	// Build filters once (before the retry loop) so a genuine misconfiguration
	// fails fast instead of being retried forever.
	filters, err := buildFilters(ctx, d.cfg.SelectStrategy.Strategy.Get(), d.es(), d.cfg.SelectStrategy.CustomPlacement, d.cfg.BufferPools)
	if err != nil {
		return nil, werr.ErrServiceSelectQuorumFailed.WithCauseErr(err)
	}

	var result *proto.QuorumInfo
	err = retry.Do(ctx, func() error {
		var selErr error
		switch d.cfg.SelectStrategy.Strategy.Get() {
		case "cross-region":
			result, selErr = d.selectCrossRegionQuorum(ctx, filters)
		case "custom":
			result, selErr = d.selectCustomPlacementQuorum(ctx, filters)
		default:
			result, selErr = d.selectSingleRegionQuorum(ctx, filters)
		}
		return selErr
	}, retry.AttemptAlways(), retry.Sleep(200*time.Millisecond), retry.MaxSleepTime(2*time.Second))
	if err != nil {
		return nil, werr.ErrServiceSelectQuorumFailed.WithCauseErr(err)
	}
	return result, nil
}

func (d *quorumDiscovery) selectSingleRegionQuorum(ctx context.Context, filters []*proto.NodeFilter) (*proto.QuorumInfo, error) {
	// Randomly select a buffer pool for load distribution
	pool := d.cfg.BufferPools[rand.Intn(len(d.cfg.BufferPools))]
	if len(pool.Seeds) == 0 {
		return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg(fmt.Sprintf("no seeds configured for pool %s", pool.Name))
	}

	// Use pre-built filter (should have exactly one filter for single-region strategies)
	if len(filters) != 1 {
		return nil, fmt.Errorf("expected 1 filter for single-region strategy, got %d", len(filters))
	}

	return d.requestNodesFromPool(ctx, pool, filters[0], int(d.wq()))
}

func (d *quorumDiscovery) newQuorumInfo(nodes []string, replicas []*proto.QuorumNode) *proto.QuorumInfo {
	return &proto.QuorumInfo{
		Id:       1,
		Nodes:    nodes,
		Replicas: replicas,
		Aq:       d.aq(),
		Wq:       d.wq(),
		Es:       d.es(),
	}
}

func quorumNodeFromMeta(node *proto.NodeMeta) *proto.QuorumNode {
	if node == nil {
		return nil
	}
	tags := make(map[string]string, len(node.Tags))
	for k, v := range node.Tags {
		tags[k] = v
	}
	return &proto.QuorumNode{
		Endpoint:      node.Endpoint,
		NodeId:        node.NodeId,
		ClusterName:   node.ClusterName,
		Region:        node.Region,
		Az:            node.Az,
		ResourceGroup: node.ResourceGroup,
		Tags:          tags,
	}
}

func replicaForEndpoint(info *proto.QuorumInfo, endpoint string) *proto.QuorumNode {
	if info == nil {
		return nil
	}
	for _, replica := range info.Replicas {
		if replica != nil && replica.Endpoint == endpoint {
			return replica
		}
	}
	return nil
}

func (d *quorumDiscovery) selectCrossRegionQuorum(ctx context.Context, filters []*proto.NodeFilter) (*proto.QuorumInfo, error) {
	requiredNodes := int(d.es())

	// Calculate nodes per region (try to distribute evenly)
	nodesPerRegion := requiredNodes / len(d.cfg.BufferPools)
	remainingNodes := requiredNodes % len(d.cfg.BufferPools)

	var allSelectedNodes []string
	var allSelectedReplicas []*proto.QuorumNode
	selectedSet := make(map[string]bool) // Track selected endpoints to avoid duplicates

	// Use pre-built filter as template, but adjust limit per region
	if len(filters) != 1 {
		return nil, fmt.Errorf("expected 1 filter for cross-region strategy, got %d", len(filters))
	}

	baseFilter := filters[0]

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

		// Create region-specific filter by adjusting the limit
		regionFilter := &proto.NodeFilter{
			Limit:         int32(regionNodes),
			Az:            baseFilter.Az,
			ResourceGroup: baseFilter.ResourceGroup,
		}

		// Request nodes from this region (tries all seeds in the pool)
		regionResult, err := d.requestNodesFromPool(ctx, pool, regionFilter, 0)
		if err != nil {
			if d.affinityMode() == proto.AffinityMode_HARD {
				return nil, err
			}
			logger.Ctx(ctx).Warn("Failed to get nodes from region, continuing with soft affinity",
				zap.String("poolName", pool.Name),
				zap.Error(err))
			continue
		}

		if regionResult != nil {
			for _, node := range regionResult.Nodes {
				if !selectedSet[node] {
					selectedSet[node] = true
					allSelectedNodes = append(allSelectedNodes, node)
					allSelectedReplicas = append(allSelectedReplicas, replicaForEndpoint(regionResult, node))
				}
			}
		}
	}

	if len(allSelectedNodes) < requiredNodes {
		if d.affinityMode() == proto.AffinityMode_HARD {
			return nil, werr.ErrServiceInsufficientQuorum.WithCauseErrMsg(fmt.Sprintf("insufficient nodes across regions: got %d, required %d", len(allSelectedNodes), requiredNodes))
		}
		// In soft mode, try to fill remaining nodes from any available region
		return d.fillRemainingNodesWithReplicas(ctx, allSelectedNodes, allSelectedReplicas, selectedSet)
	}

	// Trim to exact required number if we got more (random to avoid bias toward first pools)
	if len(allSelectedNodes) > requiredNodes {
		rand.Shuffle(len(allSelectedNodes), func(i, j int) {
			allSelectedNodes[i], allSelectedNodes[j] = allSelectedNodes[j], allSelectedNodes[i]
			allSelectedReplicas[i], allSelectedReplicas[j] = allSelectedReplicas[j], allSelectedReplicas[i]
		})
		allSelectedNodes = allSelectedNodes[:requiredNodes]
		allSelectedReplicas = allSelectedReplicas[:requiredNodes]
	}

	return d.newQuorumInfo(allSelectedNodes, allSelectedReplicas), nil
}

func (d *quorumDiscovery) selectCustomPlacementQuorum(ctx context.Context, filters []*proto.NodeFilter) (*proto.QuorumInfo, error) {
	requiredNodes := int(d.es())

	logger.Ctx(ctx).Info("Active custom placement node selection started",
		zap.Int("placementRulesCount", len(d.cfg.SelectStrategy.CustomPlacement)),
		zap.Int("requiredNodes", requiredNodes),
		zap.Int("filtersCount", len(filters)))

	// Validate we have the expected number of filters
	if len(filters) != len(d.cfg.SelectStrategy.CustomPlacement) {
		return nil, fmt.Errorf("filters count (%d) does not match custom placement rules count (%d)",
			len(filters), len(d.cfg.SelectStrategy.CustomPlacement))
	}

	var allSelectedNodes []string
	var allSelectedReplicas []*proto.QuorumNode
	selectedSet := make(map[string]bool) // Track selected endpoints to avoid duplicates

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

		// Request extra candidates to allow deduplication across placement rules
		placementFilter := &proto.NodeFilter{
			Limit:         d.es(), // Request more than 1 to have alternatives if first is a duplicate
			Az:            filters[i].Az,
			ResourceGroup: filters[i].ResourceGroup,
		}

		// Use pre-built filter for this placement (tries all seeds in the pool)
		regionResult, err := d.requestNodesFromPool(ctx, *targetPool, placementFilter, 1)
		if err != nil {
			return nil, fmt.Errorf("failed to select node for custom placement rule %d (region: %s, az: %s, rg: %s): %w",
				i, placement.Region, placement.Az, placement.ResourceGroup, err)
		}

		if regionResult == nil || len(regionResult.Nodes) == 0 {
			return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg(fmt.Sprintf("no node found for custom placement rule %d (region: %s, az: %s, rg: %s)",
				i, placement.Region, placement.Az, placement.ResourceGroup))
		}

		// Pick the first non-duplicate node from candidates
		var selectedNode string
		for _, node := range regionResult.Nodes {
			if !selectedSet[node] {
				selectedNode = node
				break
			}
		}
		if selectedNode == "" {
			return nil, werr.ErrServiceInsufficientQuorum.WithCauseErrMsg(fmt.Sprintf(
				"no unique node available for custom placement rule %d (region: %s, az: %s, rg: %s): all %d candidates already selected by prior rules",
				i, placement.Region, placement.Az, placement.ResourceGroup, len(regionResult.Nodes)))
		}

		selectedSet[selectedNode] = true
		allSelectedNodes = append(allSelectedNodes, selectedNode)
		allSelectedReplicas = append(allSelectedReplicas, replicaForEndpoint(regionResult, selectedNode))

		logger.Ctx(ctx).Debug("Successfully selected node for active custom placement rule",
			zap.Int("ruleIndex", i),
			zap.String("region", placement.Region),
			zap.String("az", placement.Az),
			zap.String("resourceGroup", placement.ResourceGroup),
			zap.String("selectedNode", selectedNode))
	}

	// At this point, we must have exactly requiredNodes nodes
	if len(allSelectedNodes) != requiredNodes {
		return nil, werr.ErrServiceInsufficientQuorum.WithCauseErrMsg(fmt.Sprintf("active custom placement validation failed: expected %d nodes, got %d nodes", requiredNodes, len(allSelectedNodes)))
	}

	logger.Ctx(ctx).Info("Active custom placement node selection completed successfully",
		zap.Int("selectedNodesCount", len(allSelectedNodes)),
		zap.Strings("selectedNodes", allSelectedNodes))

	return d.newQuorumInfo(allSelectedNodes, allSelectedReplicas), nil
}

func (d *quorumDiscovery) fillRemainingNodes(ctx context.Context, currentNodes []string, selectedSet map[string]bool) (*proto.QuorumInfo, error) {
	return d.fillRemainingNodesWithReplicas(ctx, currentNodes, nil, selectedSet)
}

func (d *quorumDiscovery) fillRemainingNodesWithReplicas(ctx context.Context, currentNodes []string, currentReplicas []*proto.QuorumNode, selectedSet map[string]bool) (*proto.QuorumInfo, error) {
	requiredNodes := int(d.es())
	remainingNeeded := requiredNodes - len(currentNodes)
	if remainingNeeded <= 0 {
		return d.newQuorumInfo(currentNodes, currentReplicas), nil
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

		fillFilter.Limit = int32(requiredNodes - len(currentNodes))
		fillResult, err := d.requestNodesFromPool(ctx, pool, fillFilter, 0)
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
					currentReplicas = append(currentReplicas, replicaForEndpoint(fillResult, node))
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

	return d.newQuorumInfo(currentNodes, currentReplicas), nil
}

// requestNodesFromPool tries all seeds in a pool in shuffled order, returning on the first success.
// This ensures that a single dead seed doesn't block the request when other seeds are healthy.
func (d *quorumDiscovery) requestNodesFromPool(ctx context.Context, pool config.QuorumBufferPool, filter *proto.NodeFilter, expectedAtLeast int) (*proto.QuorumInfo, error) {
	// Shuffle seeds to avoid always hitting the same one
	seeds := make([]string, len(pool.Seeds))
	copy(seeds, pool.Seeds)
	rand.Shuffle(len(seeds), func(i, j int) { seeds[i], seeds[j] = seeds[j], seeds[i] })

	var lastErr error
	for _, seed := range seeds {
		result, err := d.requestNodesFromSeed(ctx, seed, filter, expectedAtLeast)
		if err == nil {
			return result, nil
		}
		lastErr = err
		logger.Ctx(ctx).Debug("Seed failed, trying next",
			zap.String("failedSeed", seed),
			zap.String("poolName", pool.Name),
			zap.Error(err))
	}
	return nil, fmt.Errorf("all seeds in pool %s failed, last error: %w", pool.Name, lastErr)
}

func (d *quorumDiscovery) requestNodesFromSeed(ctx context.Context, seed string, filter *proto.NodeFilter, expectedAtLeast int) (*proto.QuorumInfo, error) {
	logger.Ctx(ctx).Debug("Requesting nodes from seed via gRPC",
		zap.String("seed", seed),
		zap.Int32("nodeCount", filter.Limit),
		zap.String("az", filter.Az),
		zap.String("resourceGroup", filter.ResourceGroup),
		zap.String("affinityMode", d.affinityMode().String()),
		zap.String("strategyType", d.strategyType().String()))

	// Get gRPC client from client pool
	grpcClient, err := d.clientPool.GetLogStoreClient(ctx, seed)
	if err != nil {
		return nil, fmt.Errorf("failed to get gRPC client for seed %s: %w", seed, err)
	}

	// Make gRPC call to SelectNodes using per-call computed values
	selectedNodes, err := grpcClient.SelectNodes(ctx, d.strategyType(), d.affinityMode(), []*proto.NodeFilter{filter})
	if err != nil {
		return nil, fmt.Errorf("gRPC SelectNodes call failed for seed %s: %w", seed, err)
	}

	if len(selectedNodes) < expectedAtLeast {
		return nil, werr.ErrServiceInsufficientQuorum.WithCauseErrMsg(fmt.Sprintf("insufficient nodes (%d/%d) satisfy the current selection strategy, returned by seed: %s", len(selectedNodes), expectedAtLeast, seed))
	}

	// Convert to endpoint addresses and keep topology metadata for read locality.
	nodeEndpoints := make([]string, len(selectedNodes))
	replicas := make([]*proto.QuorumNode, len(selectedNodes))
	for i, node := range selectedNodes {
		nodeEndpoints[i] = node.Endpoint
		replicas[i] = quorumNodeFromMeta(node)
	}

	result := d.newQuorumInfo(nodeEndpoints, replicas)

	logger.Ctx(ctx).Debug("Active discovery: Successfully selected nodes from seed via gRPC",
		zap.String("seed", seed),
		zap.String("strategy", d.cfg.SelectStrategy.Strategy.Get()),
		zap.String("strategyType", d.strategyType().String()),
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
