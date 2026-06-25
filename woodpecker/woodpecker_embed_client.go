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

	embedLogStore = server.NewLogStore(context.Background(), cfg, storageClient)
	embedLogStore.SetAddress("127.0.0.1:18888") // TODO only placeholder now

	initError := embedLogStore.Start()
	if initError != nil {
		return false, initError
	}

	isLogStoreRunning = true
	return true, nil
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

// detectAndStoreConditionWriteCapability verifies condition write capability for embed mode.
//
// Runtime modes:
//   - disable: return false directly, without reading etcd, probing object storage, or writing metadata.
//   - auto: if etcd already stores true/false, follow that stored legacy cluster decision;
//     if no metadata exists, verify capability with strict enable semantics, write true on
//     success, and return an error on failure. Never fall back and never persist false.
//   - enable: if etcd already stores true, use it as a fast path; if etcd stores false or
//     no metadata exists, ignore false and verify capability. On verification success,
//     overwrite metadata to true with StoreConditionWriteEnabled. On failure, return an
//     error. Never fall back and never persist false.
func detectAndStoreConditionWriteCapability(ctx context.Context, cfg *config.Configuration, metadata meta.MetadataProvider, storageClient storageclient.ObjectStorage) (bool, error) {
	log := logger.Ctx(ctx)
	fencePolicy := cfg.Woodpecker.Logstore.FencePolicy

	// If explicitly disabled, skip detection
	if fencePolicy.IsConditionWriteDisabled() {
		log.Info("Condition write is explicitly disabled, skipping detection")
		return false, nil
	}

	enableMode := fencePolicy.IsConditionWriteEnabled()
	autoMode := !enableMode

	// Check if result already exists in etcd.
	existingResult, err := metadata.GetConditionWriteResult(ctx)
	if err == nil {
		if autoMode {
			log.Info("Found existing condition write result from etcd in auto mode",
				zap.Bool("enabled", existingResult))
			return existingResult, nil
		}
		if existingResult {
			log.Info("Found existing enabled condition write result from etcd in enable mode")
			return true, nil
		}
		log.Warn("Ignoring persisted disabled condition write result because condition write is explicitly enabled")
	} else if errors.Is(err, werr.ErrMetadataKeyNotExists) {
		log.Info("No existing condition write result found, will verify capability",
			zap.String("conditionWrite", fencePolicy.ConditionWrite))
	} else {
		return false, fmt.Errorf("failed to read condition write result from etcd in %s mode: %w", fencePolicy.ConditionWrite, err)
	}

	const maxRetries uint = 10
	log.Info("Condition write verification is required",
		zap.String("conditionWrite", fencePolicy.ConditionWrite),
		zap.Uint("maxRetries", maxRetries))

	var verified bool
	var fromMetadata bool
	var lastDetectErr error
	attemptCount := uint(0)

	retryErr := retry.Do(ctx, func() error {
		attemptCount++

		// Check etcd before each retry in case another node has already set the result.
		if attemptCount > 1 {
			result, checkErr := metadata.GetConditionWriteResult(ctx)
			if checkErr == nil {
				if autoMode {
					log.Info("Another node has set condition write result in auto mode",
						zap.Bool("result", result),
						zap.Uint("currentAttempt", attemptCount))
					verified = result
					fromMetadata = true
					return nil
				}
				if result {
					log.Info("Another node has enabled condition write",
						zap.Uint("currentAttempt", attemptCount))
					verified = true
					fromMetadata = true
					return nil
				}
				log.Warn("Ignoring persisted disabled condition write result during enable-mode verification",
					zap.Uint("attempt", attemptCount))
			} else if !errors.Is(checkErr, werr.ErrMetadataKeyNotExists) {
				lastDetectErr = checkErr
				return retry.Unrecoverable(checkErr)
			}
		}

		supported, detectErr := storageclient.CheckIfConditionWriteSupport(ctx, storageClient, cfg.Minio.BucketName, cfg.Minio.RootPath)
		if detectErr != nil {
			lastDetectErr = detectErr
			remainingRetries := maxRetries - attemptCount
			log.Warn("Condition write verification failed",
				zap.Error(detectErr),
				zap.Uint("attempt", attemptCount),
				zap.Uint("remainingRetries", remainingRetries),
				zap.String("failureAction", "startup will fail if all retries fail"))
			return detectErr
		}
		if !supported {
			lastDetectErr = fmt.Errorf("condition write verification reported unsupported without an error")
			log.Warn("Condition write verification reported unsupported",
				zap.Uint("attempt", attemptCount),
				zap.String("failureAction", "startup will fail if all retries fail"))
			return lastDetectErr
		}

		verified = true
		log.Info("Condition write verification succeeded", zap.Uint("attempt", attemptCount))
		return nil
	}, retry.Attempts(maxRetries), retry.Sleep(100*time.Millisecond), retry.MaxSleepTime(2*time.Second))

	if retryErr != nil {
		if lastDetectErr == nil {
			lastDetectErr = retryErr
		}
		return false, fmt.Errorf("condition write verification failed after %d attempts: %w", attemptCount, lastDetectErr)
	}

	if fromMetadata {
		return verified, nil
	}

	if !verified {
		return false, fmt.Errorf("condition write verification completed without a verified result")
	}

	var storeErr error
	storeRetryErr := retry.Do(ctx, func() error {
		// This path is reached only after strict enable-style verification succeeds.
		// It is safe to overwrite false with true: explicit-enable nodes all verify support before
		// writing, and auto mode only reaches this path when no stored cluster decision existed.
		storeErr = metadata.StoreConditionWriteEnabled(ctx)
		if storeErr != nil {
			log.Warn("Failed to store condition write enabled result in etcd, will retry",
				zap.Error(storeErr))
			return storeErr
		}
		return nil
	}, retry.Attempts(10), retry.Sleep(100*time.Millisecond), retry.MaxSleepTime(2*time.Second))

	if storeRetryErr != nil {
		if storeErr == nil {
			storeErr = storeRetryErr
		}
		return false, fmt.Errorf("failed to store condition write enabled result to etcd after retries: %w", storeErr)
	}

	return true, nil
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
	metadataProvider := meta.NewMetadataProvider(ctx, etcdCli, cfg)
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
	// Embed mode reads locally; no direct read from object storage needed
	return openLogUnsafe(ctx, c.Metadata, logName, c.clientPool, c.cfg, c.SelectQuorumNodes, nil)
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
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closeState.Load() {
		return werr.ErrWoodpeckerClientClosed
	}
	return deleteLogUnsafe(ctx, c.Metadata, c.clientPool, c.cfg, logName)
}

// DeleteAllLogs deletes all logs managed by this client.
func (c *woodpeckerEmbedClient) DeleteAllLogs(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closeState.Load() {
		return werr.ErrWoodpeckerClientClosed
	}
	return deleteAllLogsUnsafe(ctx, c.Metadata, c.clientPool, c.cfg)
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
