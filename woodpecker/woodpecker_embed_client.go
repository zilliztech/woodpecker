// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package woodpecker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/common/logger"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/retry"
	"github.com/zilliztech/woodpecker/common/tracer"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server"
	"github.com/zilliztech/woodpecker/woodpecker/client"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// embedLogStore is the singleton of embedded logStore server
var (
	embedLogStoreMu   sync.Mutex
	embedLogStore     server.LogStore
	isLogStoreRunning bool
)

func startEmbedLogStore(cfg *config.Configuration, storageClient storageclient.ObjectStorage) (bool, error) {
	embedLogStoreMu.Lock()
	defer embedLogStoreMu.Unlock()

	if isLogStoreRunning && embedLogStore != nil {
		return false, nil
	}

	var takeControlOfClients bool = false
	embedLogStore = server.NewLogStore(context.Background(), cfg, storageClient)
	embedLogStore.SetAddress("127.0.0.1:18888") // TODO only placeholder now

	initError := embedLogStore.Start()
	if initError != nil {
		return false, initError
	}

	isLogStoreRunning = true
	takeControlOfClients = true
	return takeControlOfClients, nil
}

func StopEmbedLogStore() error {
	embedLogStoreMu.Lock()
	defer embedLogStoreMu.Unlock()

	if !isLogStoreRunning || embedLogStore == nil {
		return nil
	}

	stopErr := embedLogStore.Stop()
	if stopErr == nil {
		isLogStoreRunning = false
		embedLogStore = nil
	}

	return stopErr
}

var _ Client = (*woodpeckerEmbedClient)(nil)

// Implementation of the client interface for embed mode.
type woodpeckerEmbedClient struct {
	mu  sync.RWMutex
	cfg *config.Configuration

	Metadata   meta.MetadataProvider
	clientPool client.LogStoreClientPool

	managedCli    bool
	etcdCli       *clientv3.Client
	storageClient storageclient.ObjectStorage

	// close state
	closeState atomic.Bool
}

// detectAndStoreConditionWriteCapability detects condition write capability and stores result in etcd.
//
// Detection Strategy:
//   - "disable": Force disable condition write, skip detection entirely
//   - "enable": Force enable condition write, detection must succeed within 10 retries or panic
//   - "auto": Pessimistic disable, optimistic enable with cluster consensus
//   - Attempts detection up to 30 times with exponential backoff
//   - Any successful detection enables condition write for the entire cluster
//   - Falls back to disable mode if all attempts fail
//
// Cluster Consensus:
//   - All nodes share a single global result stored in etcd
//   - First successful detection wins (CAS operation ensures atomic agreement)
//   - All nodes follow the agreed cluster result regardless of their local detection
//   - Once set, the result is used by all subsequent nodes without re-detection
func detectAndStoreConditionWriteCapability(ctx context.Context, cfg *config.Configuration, metadata meta.MetadataProvider, storageClient storageclient.ObjectStorage) (bool, error) {
	log := logger.Ctx(ctx)
	fencePolicy := cfg.Woodpecker.Logstore.FencePolicy

	// If explicitly disabled, skip detection
	if fencePolicy.IsConditionWriteDisabled() {
		log.Info("Condition write is explicitly disabled, skipping detection")
		return false, nil
	}

	// Check if result already exists in etcd
	existingResult, err := metadata.GetConditionWriteResult(ctx)
	if err == nil {
		log.Info("Found existing condition write detection result from etcd", zap.Bool("enabled", existingResult))
		return existingResult, nil
	}
	if !errors.Is(err, werr.ErrMetadataKeyNotExists) {
		// Unexpected error, log and continue with detection
		log.Warn("Failed to get condition write result from etcd, will proceed with detection", zap.Error(err))
	}

	// Determine retry strategy based on configuration
	var maxRetries uint
	var panicOnFailure bool
	if fencePolicy.IsConditionWriteEnabled() {
		maxRetries = 10
		panicOnFailure = true
		log.Info("Condition write is explicitly enabled, must detect successfully within 10 retries")
	} else { // auto mode
		maxRetries = 30
		panicOnFailure = false
		log.Info("Condition write is in auto mode, will attempt detection up to 30 times")
	}

	var localDetectionResult *bool // nil means not detected successfully, non-nil means detection succeeded
	var fromEtcd bool              // true if result came from etcd (no need to store)
	var lastDetectErr error
	attemptCount := uint(0)

	// Phase 1: Retry detection up to maxRetries times (check etcd before each attempt)
	retryErr := retry.Do(ctx, func() error {
		attemptCount++

		// Check etcd before each retry in case another node has already set the result
		if attemptCount > 1 {
			result, checkErr := metadata.GetConditionWriteResult(ctx)
			if checkErr == nil {
				log.Info("Another node has already set condition write result",
					zap.Bool("result", result),
					zap.Uint("currentAttempt", attemptCount))
				localDetectionResult = &result
				fromEtcd = true
				return nil // Success, use existing result from etcd
			}
			if !errors.Is(checkErr, werr.ErrMetadataKeyNotExists) {
				log.Warn("Failed to check etcd for existing result, continuing with detection",
					zap.Error(checkErr),
					zap.Uint("attempt", attemptCount))
			}
		}

		// Perform detection
		supported, detectErr := storageclient.CheckIfConditionWriteSupport(ctx, storageClient, cfg.Minio.BucketName, cfg.Minio.RootPath)
		if detectErr != nil {
			lastDetectErr = detectErr
			remainingRetries := maxRetries - attemptCount
			log.Warn("Condition write detection failed",
				zap.Error(detectErr),
				zap.Uint("attempt", attemptCount),
				zap.Uint("remainingRetries", remainingRetries),
				zap.String("fallbackStrategy", "will use distributed lock if all retries fail"))
			return detectErr
		}

		// Detection succeeded
		localDetectionResult = &supported
		log.Info("Condition write detection succeeded",
			zap.Bool("supported", supported),
			zap.Uint("attempt", attemptCount))
		return nil // Success, detection completed
	}, retry.Attempts(maxRetries), retry.Sleep(100*time.Millisecond), retry.MaxSleepTime(2*time.Second))

	// Phase 2: Handle detection result and store to etcd
	if retryErr == nil && localDetectionResult != nil {
		// If result came from etcd, no need to store again
		if fromEtcd {
			return *localDetectionResult, nil
		}

		// Detection succeeded, now store to etcd (with separate retry logic)
		detected := *localDetectionResult

		var agreedResult bool
		var storeErr error

		// Retry etcd store operation independently (not counted in detection retries)
		storeRetryErr := retry.Do(ctx, func() error {
			agreedResult, storeErr = metadata.StoreOrGetConditionWriteResult(ctx, detected)
			if storeErr != nil {
				log.Warn("Failed to store condition write result in etcd, will retry",
					zap.Bool("detected", detected),
					zap.Error(storeErr))
				return storeErr
			}

			// Store succeeded
			if agreedResult != detected {
				log.Info("Using agreed condition write result from cluster",
					zap.Bool("ourDetection", detected),
					zap.Bool("agreedResult", agreedResult))
			}
			return nil
		}, retry.Attempts(10), retry.Sleep(100*time.Millisecond), retry.MaxSleepTime(2*time.Second))

		if storeRetryErr != nil {
			// Etcd store failed after all retries
			return false, fmt.Errorf("failed to store condition write result to etcd after retries: %w", storeErr)
		}

		// Return the agreed cluster result
		return agreedResult, nil
	}

	// Detection failed after all retries
	if fencePolicy.IsConditionWriteEnabled() && panicOnFailure {
		panic(fmt.Sprintf("Condition write is required but detection failed after %d retries: %v", maxRetries, lastDetectErr))
	}

	// Auto mode: fallback to disable
	log.Warn("Condition write detection failed after all retries, falling back to distributed lock mode",
		zap.Uint("totalAttempts", attemptCount),
		zap.Error(lastDetectErr))

	// Store false result in etcd for auto mode
	var agreedResult bool
	var storeErr error

	storeRetryErr := retry.Do(ctx, func() error {
		agreedResult, storeErr = metadata.StoreOrGetConditionWriteResult(ctx, false)
		if storeErr != nil {
			log.Warn("Failed to store false result to etcd, will retry",
				zap.Error(storeErr))
			return storeErr
		}
		return nil
	}, retry.Attempts(10), retry.Sleep(100*time.Millisecond), retry.MaxSleepTime(2*time.Second))

	if storeRetryErr != nil {
		// Etcd store failed after all retries
		return false, fmt.Errorf("failed to store false result to etcd after retries: %w", storeErr)
	}

	return agreedResult, nil
}

func NewEmbedClientFromConfig(ctx context.Context, config *config.Configuration) (Client, error) {
	logger.InitLogger(config)
	initTraceErr := tracer.InitTracer(config, "woodpecker", 1001)
	if initTraceErr != nil {
		logger.Ctx(ctx).Warn("init tracer failed", zap.Error(initTraceErr))
	}
	if config.Woodpecker.Storage.IsStorageService() {
		return nil, werr.ErrOperationNotSupported.WithCauseErrMsg("embed client not support service storage mode")
	}
	etcdCli, err := etcd.GetRemoteEtcdClient(config.Etcd.GetEndpoints())
	if err != nil {
		return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErr(err)
	}
	var storageCli storageclient.ObjectStorage
	if config.Woodpecker.Storage.IsStorageMinio() {
		storageCli, err = storageclient.NewObjectStorage(ctx, config)
		if err != nil {
			return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErr(err)
		}
	}
	return NewEmbedClient(ctx, config, etcdCli, storageCli, true)
}

func NewEmbedClient(ctx context.Context, cfg *config.Configuration, etcdCli *clientv3.Client, storageClient storageclient.ObjectStorage, managed bool) (Client, error) {
	// init logger
	logger.InitLogger(cfg)
	// Initialize metadata provider
	metadataProvider := meta.NewMetadataProvider(ctx, etcdCli, cfg.Etcd.RequestTimeout.Milliseconds())
	// Detect and store condition write capability if storage client is available
	// Note: only perform condition write capability detection for embed mode with minio storage backend
	if storageClient != nil && cfg.Woodpecker.Storage.IsStorageMinio() {
		conditionWriteEnable, err := detectAndStoreConditionWriteCapability(ctx, cfg, metadataProvider, storageClient)
		if err != nil {
			return nil, err
		}
		cfg.Woodpecker.Logstore.FencePolicy.SetConditionWriteEnableOrNot(conditionWriteEnable)
	}
	// start embedded logStore
	managedByLogStore, err := startEmbedLogStore(cfg, storageClient)
	if err != nil {
		return nil, err
	}
	if managedByLogStore && managed {
		managed = false
	}
	clientPool := client.NewLogStoreClientPoolLocal(embedLogStore)
	c := woodpeckerEmbedClient{
		cfg:           cfg,
		Metadata:      metadataProvider,
		clientPool:    clientPool,
		managedCli:    managed,
		etcdCli:       etcdCli,
		storageClient: storageClient,
	}
	c.closeState.Store(false)
	initErr := c.initClient(ctx)
	if initErr != nil {
		return nil, werr.ErrWoodpeckerClientInitFailed.WithCauseErr(initErr)
	}
	return &c, nil
}

func (c *woodpeckerEmbedClient) initClient(ctx context.Context) error {
	initErr := c.Metadata.InitIfNecessary(ctx)
	if initErr != nil {
		return werr.ErrWoodpeckerClientInitFailed.WithCauseErr(initErr)
	}
	return nil
}

func (c *woodpeckerEmbedClient) GetMetadataProvider() meta.MetadataProvider {
	return c.Metadata
}

// CreateLog creates a new log with the specified name.
// It stores segment metadata for the new log.
func (c *woodpeckerEmbedClient) CreateLog(ctx context.Context, logName string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closeState.Load() {
		return werr.ErrWoodpeckerClientClosed
	}
	return createLogUnsafe(ctx, c.Metadata, logName)
}

// OpenLog opens an existing log with the specified name and returns a log handle.
// It retrieves the log metadata and segments metadata, then creates a new log handle.
func (c *woodpeckerEmbedClient) OpenLog(ctx context.Context, logName string) (log.LogHandle, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeState.Load() {
		return nil, werr.ErrWoodpeckerClientClosed
	}
	return openLogUnsafe(ctx, c.Metadata, logName, c.clientPool, c.cfg, c.SelectQuorumNodes)
}

func (c *woodpeckerEmbedClient) SelectQuorumNodes(ctx context.Context) (*proto.QuorumInfo, error) {
	return &proto.QuorumInfo{
		Id:    -1,
		Wq:    1,
		Aq:    1,
		Es:    1,
		Nodes: []string{"127.0.0.1:59456"},
	}, nil
}

// DeleteLog deletes the log with the specified name.
// It should remove the log and its associated metadata.
func (c *woodpeckerEmbedClient) DeleteLog(ctx context.Context, logName string) error {
	// This is just a stub - you'll need to implement this
	return werr.ErrOperationNotSupported.WithCauseErrMsg("delete log is not supported currently")
}

// LogExists checks if a log with the specified name exists.
// It returns true if the log exists, otherwise false.
func (c *woodpeckerEmbedClient) LogExists(ctx context.Context, logName string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closeState.Load() {
		return false, werr.ErrWoodpeckerClientClosed
	}
	return logExistsUnsafe(ctx, c.Metadata, logName)
}

// GetAllLogs retrieves all log names.
// It returns a slice containing all log names.
func (c *woodpeckerEmbedClient) GetAllLogs(ctx context.Context) ([]string, error) {
	if c.closeState.Load() {
		return nil, werr.ErrWoodpeckerClientClosed
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return getAllLogsUnsafe(ctx, c.Metadata)
}

// GetLogsWithPrefix retrieves log names that start with the specified prefix.
// It returns a slice containing log names that match the prefix.
func (c *woodpeckerEmbedClient) GetLogsWithPrefix(ctx context.Context, logNamePrefix string) ([]string, error) {
	if c.closeState.Load() {
		return nil, werr.ErrWoodpeckerClientClosed
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return getLogsWithPrefix(ctx, c.Metadata, logNamePrefix)
}

func (c *woodpeckerEmbedClient) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closeState.CompareAndSwap(false, true) {
		logger.Ctx(ctx).Info("client already closed, skip")
		return werr.ErrWoodpeckerClientClosed
	}

	closeErr := c.Metadata.Close()
	if closeErr != nil {
		logger.Ctx(ctx).Info("close metadata failed", zap.Error(closeErr))
	}
	closePoolErr := c.clientPool.Close(ctx)
	if closePoolErr != nil {
		logger.Ctx(ctx).Info("close client pool failed", zap.Error(closePoolErr))
	}
	if c.managedCli {
		closeEtcdCliErr := c.etcdCli.Close()
		if closeEtcdCliErr != nil {
			logger.Ctx(ctx).Info("close client etcd client failed", zap.Error(closeEtcdCliErr))
			return werr.Combine(closeErr, closePoolErr, closeEtcdCliErr)
		}
	}
	return werr.Combine(closeErr, closePoolErr)
}
