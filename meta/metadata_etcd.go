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

package meta

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

const (
	CurrentScopeName = "Meta"
)

var _ MetadataProvider = (*metadataProviderEtcd)(nil)

type metadataProviderEtcd struct {
	sync.Mutex
	client *clientv3.Client

	session        *concurrency.Session
	logWriterLocks map[string]*concurrency.Mutex
	lockSessions   map[string]*concurrency.Session
}

func NewMetadataProvider(ctx context.Context, client *clientv3.Client) MetadataProvider {
	return &metadataProviderEtcd{
		client:         client,
		logWriterLocks: make(map[string]*concurrency.Mutex),
		lockSessions:   make(map[string]*concurrency.Session),
	}
}

// InitIfNecessary initializes the metadata provider if necessary.
// It checks if there is logIdGen,instance,quorumIdGen keys in etcd.
// If not, it creates them.
func (e *metadataProviderEtcd) InitIfNecessary(ctx context.Context) error {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "InitIfNecessary")
	defer sp.End()
	startTime := time.Now()
	keys := []string{
		ServiceInstanceKey,
		VersionKey,
		LogIdGeneratorKey,
		QuorumIdGeneratorKey,
	}
	ops := make([]clientv3.Op, 0, 4)
	ops = append(ops, clientv3.OpGet(ServiceInstanceKey))
	ops = append(ops, clientv3.OpGet(VersionKey))
	ops = append(ops, clientv3.OpGet(LogIdGeneratorKey))
	ops = append(ops, clientv3.OpGet(QuorumIdGeneratorKey))
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	log := logger.Ctx(ctx)
	resp, err := e.client.Txn(ctx).If().Then(ops...).Commit()
	if err != nil || !resp.Succeeded {
		log.Error("init service metadata failed", zap.Error(err))
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("init_if_necessary", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("init_if_necessary", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataRead.WithCauseErr(err)
	}
	sp.AddEvent("GetServiceMetaCompleted", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startTime).Milliseconds())))
	initOps := make([]clientv3.Op, 0, 4)
	for index, rp := range resp.Responses {
		if rp.GetResponseRange().Kvs == nil || len(rp.GetResponseRange().Kvs) == 0 {
			if index == 0 {
				// instance initial value is a uuid
				initOps = append(initOps, clientv3.OpPut(keys[index], uuid.New().String()))
			} else if index == 1 {
				// meta version initial value
				v := &proto.Version{
					Major: VersionMajor,
					Minor: VersionMinor,
					Patch: VersionPatch,
				}
				data, encodeErr := pb.Marshal(v)
				if encodeErr != nil {
					log.Error("encode version failed", zap.Error(encodeErr))
					metrics.WpEtcdMetaOperationsTotal.WithLabelValues("init_if_necessary", "error").Inc()
					metrics.WpEtcdMetaOperationLatency.WithLabelValues("init_if_necessary", "error").Observe(float64(time.Since(startTime).Milliseconds()))
					return werr.ErrMetadataEncode.WithCauseErr(encodeErr)
				}
				initOps = append(initOps, clientv3.OpPut(keys[index], string(data)))
			} else {
				// idgen initial value is 0
				initOps = append(initOps, clientv3.OpPut(keys[index], "0"))
			}
			log.Warn("init service metadata warning, key not found", zap.String("key", keys[index]))
		}
	}
	if len(initOps) == 0 {
		// cluster already initialized successfully
		log.Debug("cluster already initialized, skipping initialization")
		return nil
	} else if len(initOps) != len(keys) {
		// cluster already initialized partially, but not all
		err = werr.ErrMetadataInit.WithCauseErrMsg("some keys already exists")
		log.Error("init operation failed, some keys already exists", zap.Error(err))
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("init_if_necessary", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("init_if_necessary", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return err
	}
	// cluster not initialized, initialize it
	_, initErr := e.client.Txn(ctx).If().Then(initOps...).Commit()
	if initErr != nil || !resp.Succeeded {
		err = werr.ErrMetadataInit.WithCauseErr(initErr)
		log.Error("init operation failed", zap.Error(err))
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("init_if_necessary", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("init_if_necessary", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return err
	}
	sp.AddEvent("InitServiceMetaCompleted", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startTime).Milliseconds())))
	// cluster initialized successfully
	log.Info("cluster initialized successfully")
	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("init_if_necessary", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("init_if_necessary", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

func (e *metadataProviderEtcd) GetVersionInfo(ctx context.Context) (*proto.Version, error) {
	startTime := time.Now()
	getResp, getErr := e.client.Get(ctx, VersionKey)
	if getErr != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_version_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_version_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, werr.ErrMetadataRead.WithCauseErr(getErr)
	}
	if len(getResp.Kvs) == 0 {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_version_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_version_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, werr.ErrMetadataRead.WithCauseErrMsg("version not found")
	}
	expectedVersion := &proto.Version{}
	decodedErr := pb.Unmarshal(getResp.Kvs[0].Value, expectedVersion)
	if decodedErr != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_version_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_version_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, werr.ErrMetadataDecode.WithCauseErr(decodedErr)
	}
	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_version_info", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_version_info", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return expectedVersion, nil
}

func (e *metadataProviderEtcd) CreateLog(ctx context.Context, logName string) error {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "CreateLog")
	defer sp.End()
	startTime := time.Now()
	e.Lock()
	defer e.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get the current id value from logIdGenerator
	resp, err := e.client.Get(ctx, LogIdGeneratorKey)
	sp.AddEvent("GetIdGen", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startTime).Milliseconds())))
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("create_log", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("create_log", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataRead.WithCauseErr(err)
	}

	if len(resp.Kvs) == 0 {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("create_log", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("create_log", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataRead.WithCauseErrMsg(fmt.Sprintf("%s key not found", LogIdGeneratorKey))
	}

	// check if logName exists
	exists, err := e.CheckExists(ctx, logName)
	sp.AddEvent("CheckExists", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startTime).Milliseconds())))
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("create_log", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("create_log", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataRead.WithCauseErr(err)
	}
	if exists {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("create_log", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("create_log", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrLogAlreadyExists.WithCauseErrMsg(fmt.Sprintf("%s already exists", logName))
	}

	// create a New Log with default Options
	currentIdStr := string(resp.Kvs[0].Value)
	currentID, err := atoi(currentIdStr)
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("create_log", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("create_log", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataDecode.WithCauseErr(err)
	}
	nextID := currentID + 1
	logMeta := &proto.LogMeta{
		LogId:                     int64(nextID),
		MaxSegmentRollTimeSeconds: 60,
		MaxSegmentRollSizeBytes:   1024 * 1024 * 1024, // 1GB
		CompactionBufferSizeBytes: 128 * 1024 * 1024,  // 128MB
		MaxCompactionFileCount:    600,
		CreationTimestamp:         uint64(time.Now().Unix()),
		ModificationTimestamp:     uint64(time.Now().Unix()),
		TruncatedSegmentId:        -1,
		TruncatedEntryId:          -1,
	}
	// Serialize to binary
	logMetaValue, err := pb.Marshal(logMeta)
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("create_log", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("create_log", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrCreateLogMetadata.WithCauseErr(err)
	}
	// Start a transaction
	txn := e.client.Txn(ctx)

	// Create logs/<logName>  and update logs/idgen atomically
	txnResp, err := txn.If(
		// Ensure logs/idgen has not changed since we read it
		clientv3.Compare(clientv3.Value(LogIdGeneratorKey), "=", fmt.Sprintf("%d", currentID)),
	).Then(
		// Create logs/<logName> with logValue
		clientv3.OpPut(BuildLogKey(logName), string(logMetaValue)),
		// Update logs/idgen to nextID
		clientv3.OpPut(LogIdGeneratorKey, fmt.Sprintf("%d", nextID)),
	).Commit()
	sp.AddEvent("Committed", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startTime).Milliseconds())))
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("create_log", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("create_log", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrCreateLogMetadata.WithCauseErr(err)
	}

	if !txnResp.Succeeded {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("create_log", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("create_log", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrCreateLogMetadataTxn.WithCauseErrMsg("transaction failed due to idgen mismatch, please try again")
	}
	logger.Ctx(ctx).Info("log created successfully", zap.String("logName", logName), zap.Int64("logId", logMeta.LogId))
	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("create_log", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("create_log", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

// atoi converts string to int with error handling
func atoi(s string) (int, error) {
	value, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func (e *metadataProviderEtcd) GetLogMeta(ctx context.Context, logName string) (*proto.LogMeta, error) {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "GetLogMeta")
	defer sp.End()
	startTime := time.Now()

	// Get log meta for the path = logs/<logName>
	logResp, err := e.client.Get(ctx, BuildLogKey(logName))
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_log_meta", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_log_meta", "err").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, werr.ErrMetadataRead.WithCauseErr(err)
	}
	if len(logResp.Kvs) == 0 {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_log_meta", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_log_meta", "err").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, werr.ErrMetadataRead.WithCauseErrMsg(fmt.Sprintf("log not found: %s", logName))
	}
	logMeta := &proto.LogMeta{}
	if err = pb.Unmarshal(logResp.Kvs[0].Value, logMeta); err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_log_meta", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_log_meta", "err").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, werr.ErrMetadataDecode.WithCauseErr(err)
	}
	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_log_meta", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_log_meta", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return logMeta, nil
}

func (e *metadataProviderEtcd) UpdateLogMeta(ctx context.Context, logName string, logMeta *proto.LogMeta) error {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "UpdateLogMeta")
	defer sp.End()
	startTime := time.Now()
	logKey := BuildLogKey(logName)
	logMetaValue, err := pb.Marshal(logMeta)
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("update_log_meta", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("update_log_meta", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataEncode.WithCauseErr(err)
	}

	// Start a transaction to update the log metadata
	txn := e.client.Txn(ctx)

	// Update the log metadata if it exists
	txnResp, err := txn.If(
		// Ensure the log exists
		clientv3.Compare(clientv3.CreateRevision(logKey), ">", 0),
	).Then(
		// Update the log metadata
		clientv3.OpPut(logKey, string(logMetaValue)),
	).Commit()

	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("update_log_meta", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("update_log_meta", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataRead.WithCauseErr(err)
	}

	if !txnResp.Succeeded {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("update_log_meta", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("update_log_meta", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataRead.WithCauseErrMsg(fmt.Sprintf("log not found: %s", logName))
	}

	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("update_log_meta", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("update_log_meta", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

func (e *metadataProviderEtcd) OpenLog(ctx context.Context, logName string) (*proto.LogMeta, map[int64]*proto.SegmentMetadata, error) {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "OpenLog")
	defer sp.End()
	startTime := time.Now()

	// Get log meta for the path = logs/<logName>
	logResp, err := e.GetLogMeta(ctx, logName)
	sp.AddEvent("GetLogMeta", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startTime).Milliseconds())))
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("open_log", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("open_log", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, nil, err
	}

	// Get segments meta with prefix = logs/<logName>/segments/<segmentId>
	segmentMetaList, err := e.GetAllSegmentMetadata(ctx, logName)
	sp.AddEvent("GetAllSegmentMetadata", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startTime).Milliseconds())))
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("open_log", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("open_log", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, nil, err
	}

	logger.Ctx(ctx).Info("log opened successfully", zap.String("logName", logName), zap.Int64("logId", logResp.LogId))
	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("open_log", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("open_log", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return logResp, segmentMetaList, nil
}

func (e *metadataProviderEtcd) CheckExists(ctx context.Context, logName string) (bool, error) {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "CheckExists")
	defer sp.End()
	startTime := time.Now()
	// Get log meta for the path = logs/<logName>
	logResp, err := e.client.Get(ctx, BuildLogKey(logName))
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("check_exists", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("check_exists", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return false, werr.ErrMetadataRead.WithCauseErr(err)
	}
	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("check_exists", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("check_exists", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	if len(logResp.Kvs) == 0 {
		return false, nil
	}
	return true, nil
}

func (e *metadataProviderEtcd) ListLogs(ctx context.Context) ([]string, error) {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "ListLogs")
	defer sp.End()
	startTime := time.Now()

	list, err := e.ListLogsWithPrefix(ctx, "")
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("check_exists", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("check_exists", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, err
	}
	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("check_exists", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("check_exists", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return list, err
}

func (e *metadataProviderEtcd) ListLogsWithPrefix(ctx context.Context, logNamePrefix string) ([]string, error) {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "ListLogsWithPrefix")
	defer sp.End()
	startTime := time.Now()
	logResp, err := e.client.Get(ctx, BuildLogKey(logNamePrefix), clientv3.WithPrefix())
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("list_logs_with_prefix", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("list_logs_with_prefix", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, werr.ErrMetadataRead.WithCauseErr(err)
	}
	if len(logResp.Kvs) == 0 {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("list_logs_with_prefix", "success").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("list_logs_with_prefix", "success").Observe(float64(time.Since(startTime).Milliseconds()))
		return []string{}, nil
	}
	logs := make(map[string]int, 0)
	for _, path := range logResp.Kvs {
		logName, extractErr := extractLogName(string(path.Key))
		if extractErr != nil {
			metrics.WpEtcdMetaOperationsTotal.WithLabelValues("list_logs_with_prefix", "error").Inc()
			metrics.WpEtcdMetaOperationLatency.WithLabelValues("list_logs_with_prefix", "error").Observe(float64(time.Since(startTime).Milliseconds()))
			return nil, werr.ErrMetadataRead.WithCauseErr(extractErr)
		}
		logs[logName] = 1
	}
	logNames := make([]string, 0)
	for logName := range logs {
		logNames = append(logNames, logName)
	}
	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("list_logs_with_prefix", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("list_logs_with_prefix", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return logNames, nil
}

func extractLogName(path string) (string, error) {
	// Split the path by '/'
	parts := strings.Split(path, "/")

	// Check if the path has at least 4 parts
	if len(parts) < 3 {
		return "", werr.ErrMetadataDecode.WithCauseErrMsg(
			fmt.Sprintf("extract logName failed, invalid path format: %s", path))
	}

	// Return the third part
	return parts[2], nil
}

func (e *metadataProviderEtcd) AcquireLogWriterLock(ctx context.Context, logName string) (*concurrency.Session, error) {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "AcquireLogWriterLock")
	defer sp.End()
	startTime := time.Now()
	e.Lock()
	defer e.Unlock()

	// Check if lock already exists for this log
	if l, exists := e.logWriterLocks[logName]; exists {
		// We can't directly check session status through mutex, so try lock
		// If it fails with a lease-related error, we'll create a new one
		err := l.TryLock(ctx)
		if err == nil {
			// Find the associated session and return its keepAlive channel
			if session, hasSession := e.lockSessions[logName]; hasSession && session != nil {
				metrics.WpEtcdMetaOperationsTotal.WithLabelValues("acquire_log_writer_lock", "success").Inc()
				metrics.WpEtcdMetaOperationLatency.WithLabelValues("acquire_log_writer_lock", "success").Observe(float64(time.Since(startTime).Milliseconds()))
				return session, nil
			}
		}

		// Assume session might be invalid, clean up and create new
		logger.Ctx(ctx).Warn("Failed to acquire existing lock, creating new lock",
			zap.String("logName", logName),
			zap.Error(err))
		delete(e.logWriterLocks, logName)

		// Close the old session if it exists
		if session, hasSession := e.lockSessions[logName]; hasSession && session != nil {
			_ = session.Close()
			delete(e.lockSessions, logName)
		}
	}

	// Create a new session specifically for this lock with TTL
	newSession, err := concurrency.NewSession(e.client, concurrency.WithTTL(10))
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("acquire_log_writer_lock", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("acquire_log_writer_lock", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, fmt.Errorf("failed to create session for lock: %w", err)
	}

	// Create a new lock with this session
	lockKey := BuildLogLockKey(logName)
	lock := concurrency.NewMutex(newSession, lockKey)

	// Try to acquire the lock
	err = lock.TryLock(ctx)
	if err != nil {
		// If we can't acquire the lock, clean up the session
		_ = newSession.Close()
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("acquire_log_writer_lock", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("acquire_log_writer_lock", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}

	// Store the lock and session for future reference
	e.logWriterLocks[logName] = lock
	e.lockSessions[logName] = newSession
	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("acquire_log_writer_lock", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("acquire_log_writer_lock", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return newSession, nil
}

func (e *metadataProviderEtcd) ReleaseLogWriterLock(ctx context.Context, logName string) error {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "ReleaseLogWriterLock")
	defer sp.End()
	startTime := time.Now()
	e.Lock()
	defer e.Unlock()
	if l, exists := e.logWriterLocks[logName]; exists {
		// First unlock the mutex
		err := l.Unlock(ctx)
		if err != nil {
			logger.Ctx(ctx).Warn("Failed to unlock writer lock",
				zap.String("logName", logName),
				zap.Error(err))
			metrics.WpEtcdMetaOperationsTotal.WithLabelValues("release_log_writer_lock", "error").Inc()
			metrics.WpEtcdMetaOperationLatency.WithLabelValues("release_log_writer_lock", "error").Observe(float64(time.Since(startTime).Milliseconds()))
			return err
		}

		// Close the associated session
		if session, hasSession := e.lockSessions[logName]; hasSession && session != nil {
			closeErr := session.Close()
			if closeErr != nil {
				logger.Ctx(ctx).Warn("Failed to close lock session",
					zap.String("logName", logName),
					zap.Error(closeErr))
				metrics.WpEtcdMetaOperationsTotal.WithLabelValues("release_log_writer_lock", "error").Inc()
				metrics.WpEtcdMetaOperationLatency.WithLabelValues("release_log_writer_lock", "error").Observe(float64(time.Since(startTime).Milliseconds()))
			}
		}

		// Remove the lock and session from our maps
		delete(e.logWriterLocks, logName)
		delete(e.lockSessions, logName)
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("release_log_writer_lock", "success").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("release_log_writer_lock", "success").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil
	}
	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("release_log_writer_lock", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("release_log_writer_lock", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

func (e *metadataProviderEtcd) StoreSegmentMetadata(ctx context.Context, logName string, metadata *proto.SegmentMetadata) error {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "StoreSegmentMetadata")
	defer sp.End()
	startTime := time.Now()
	e.Lock()
	defer e.Unlock()
	segmentKey := BuildSegmentInstanceKey(logName, fmt.Sprintf("%d", metadata.GetSegNo()))
	segmentMetadata, err := pb.Marshal(metadata)
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("store_segment_metadata", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("store_segment_metadata", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataEncode.WithCauseErr(err)
	}
	// Start a transaction
	txn := e.client.Txn(ctx)

	// Create segmentKey if it does not already exist
	txnResp, err := txn.If(
		// Ensure segmentKey does not exist
		clientv3.Compare(clientv3.CreateRevision(segmentKey), "=", 0),
	).Then(
		// Create segmentKey with segmentMetadata
		clientv3.OpPut(segmentKey, string(segmentMetadata)),
	).Commit()

	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("store_segment_metadata", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("store_segment_metadata", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrCreateSegmentMetadata.WithCauseErr(err)
	}

	if !txnResp.Succeeded {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("store_segment_metadata", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("store_segment_metadata", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrCreateSegmentMetadata.WithCauseErrMsg(
			fmt.Sprintf("segment metadata already exists for logName:%s segmentId:%d", logName, metadata.SegNo))
	}

	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("store_segment_metadata", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("store_segment_metadata", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

func (e *metadataProviderEtcd) UpdateSegmentMetadata(ctx context.Context, logName string, metadata *proto.SegmentMetadata) error {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "UpdateSegmentMetadata")
	defer sp.End()
	startTime := time.Now()
	e.Lock()
	defer e.Unlock()
	segmentKey := BuildSegmentInstanceKey(logName, fmt.Sprintf("%d", metadata.SegNo))
	segmentMetadata, err := pb.Marshal(metadata)
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("update_segment_metadata", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("update_segment_metadata", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataEncode.WithCauseErr(err)
	}
	// Start a transaction
	txn := e.client.Txn(ctx)

	// Update segmentKey if it exists
	txnResp, err := txn.If(
		// Ensure segmentKey exists
		clientv3.Compare(clientv3.ModRevision(segmentKey), ">", 0), // TODO: check if this is correct
	).Then(
		// Update segmentKey with segmentMetadata
		clientv3.OpPut(segmentKey, string(segmentMetadata)),
	).Commit()

	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("update_segment_metadata", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("update_segment_metadata", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return err
	}

	if !txnResp.Succeeded {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("update_segment_metadata", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("update_segment_metadata", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrUpdateSegmentMetadata.WithCauseErrMsg(
			fmt.Sprintf("segment metadata not found for logName:%s segmentId:%d", logName, metadata.SegNo))
	}
	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("update_segment_metadata", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("update_segment_metadata", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

func (e *metadataProviderEtcd) GetSegmentMetadata(ctx context.Context, logName string, segmentId int64) (*proto.SegmentMetadata, error) {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "GetSegmentMetadata")
	defer sp.End()
	startTime := time.Now()
	segmentKey := BuildSegmentInstanceKey(logName, fmt.Sprintf("%d", segmentId))
	getResp, getErr := e.client.Get(ctx, segmentKey)
	if getErr != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_segment_metadata", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_segment_metadata", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, getErr
	}
	if len(getResp.Kvs) == 0 {
		return nil, werr.ErrSegmentNotFound.WithCauseErrMsg(
			fmt.Sprintf("segment meta not found for log:%s segment:%d", logName, segmentId))
	}
	segmentMetadata := &proto.SegmentMetadata{}
	if err := pb.Unmarshal(getResp.Kvs[0].Value, segmentMetadata); err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_segment_metadata", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_segment_metadata", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, werr.ErrMetadataDecode.WithCauseErr(err)
	}
	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_segment_metadata", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_segment_metadata", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return segmentMetadata, nil
}

func (e *metadataProviderEtcd) GetAllSegmentMetadata(ctx context.Context, logName string) (map[int64]*proto.SegmentMetadata, error) {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "GetAllSegmentMetadata")
	defer sp.End()
	startTime := time.Now()
	segmentKey := BuildSegmentInstanceKey(logName, "")
	getResp, getErr := e.client.Get(ctx, segmentKey, clientv3.WithPrefix())
	if getErr != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_all_segment_metadata", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_all_segment_metadata", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, getErr
	}

	segmentMetaMap := make(map[int64]*proto.SegmentMetadata)
	if len(getResp.Kvs) == 0 {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_all_segment_metadata", "success").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_all_segment_metadata", "success").Observe(float64(time.Since(startTime).Milliseconds()))
		return segmentMetaMap, nil
	}

	segIds := make([]int64, 0)
	for _, kv := range getResp.Kvs {
		segmentMeta := &proto.SegmentMetadata{}
		if err := pb.Unmarshal(kv.Value, segmentMeta); err != nil {
			metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_all_segment_metadata", "error").Inc()
			metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_all_segment_metadata", "error").Observe(float64(time.Since(startTime).Milliseconds()))
			return nil, werr.ErrMetadataDecode.WithCauseErr(err)
		}
		segmentMetaMap[segmentMeta.SegNo] = segmentMeta
		segIds = append(segIds, segmentMeta.SegNo)
	}
	logger.Ctx(ctx).Debug("GetAllSegmentMetadata", zap.String("logName", logName), zap.Int("segmentCount", len(segmentMetaMap)), zap.Int64s("segmentIds", segIds))
	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_all_segment_metadata", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_all_segment_metadata", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return segmentMetaMap, nil
}

func (e *metadataProviderEtcd) CheckSegmentExists(ctx context.Context, logName string, segmentId int64) (bool, error) {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "CheckSegmentExists")
	defer sp.End()
	startTime := time.Now()
	segmentResp, err := e.client.Get(ctx, BuildSegmentInstanceKey(logName, fmt.Sprintf("%d", segmentId)))
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("check_segment_exists", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("check_segment_exists", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return false, err
	}
	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("check_segment_exists", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("check_segment_exists", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	if len(segmentResp.Kvs) == 0 {
		return false, nil
	}
	return true, nil
}

// DeleteSegmentMetadata deletes a segment metadata entry.
// It returns an error if the segment does not exist or if the deletion fails.
func (e *metadataProviderEtcd) DeleteSegmentMetadata(ctx context.Context, logName string, segmentId int64) error {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "DeleteSegmentMetadata")
	defer sp.End()
	startTime := time.Now()
	e.Lock()
	defer e.Unlock()

	segmentKey := BuildSegmentInstanceKey(logName, fmt.Sprintf("%d", segmentId))

	// Start a transaction
	txn := e.client.Txn(ctx)

	// Delete the segment metadata if it exists
	txnResp, err := txn.If(
		// Ensure the segment exists
		clientv3.Compare(clientv3.CreateRevision(segmentKey), ">", 0),
	).Then(
		// Delete the segment metadata
		clientv3.OpDelete(segmentKey),
	).Commit()

	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("delete_segment_metadata", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("delete_segment_metadata", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataWrite.WithCauseErr(err)
	}

	if !txnResp.Succeeded {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("delete_segment_metadata", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("delete_segment_metadata", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrSegmentNotFound.WithCauseErrMsg(
			fmt.Sprintf("segment not found for logName:%s segmentId:%d", logName, segmentId))
	}

	logger.Ctx(ctx).Debug("Deleted segment metadata",
		zap.String("logName", logName),
		zap.Int64("segmentId", segmentId))

	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("delete_segment_metadata", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("delete_segment_metadata", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

func (e *metadataProviderEtcd) StoreQuorumInfo(ctx context.Context, info *proto.QuorumInfo) error {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "StoreQuorumInfo")
	defer sp.End()
	startTime := time.Now()
	quorumKey := BuildQuorumInfoKey(fmt.Sprintf("%d", info.Id))
	quorumInfoValue, err := pb.Marshal(info)
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("store_quorum_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("store_quorum_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataEncode.WithCauseErr(err)
	}

	// Start a transaction
	txn := e.client.Txn(ctx)

	// Create quorum info if it does not already exist
	txnResp, err := txn.If(
		// Ensure quorumKey does not exist
		clientv3.Compare(clientv3.CreateRevision(quorumKey), "=", 0),
	).Then(
		// Create quorumKey with quorumInfoValue
		clientv3.OpPut(quorumKey, string(quorumInfoValue)),
	).Commit()

	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("store_quorum_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("store_quorum_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrUpdateQuorumInfoMetadata.WithCauseErr(err)
	}

	if !txnResp.Succeeded {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("store_quorum_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("store_quorum_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrUpdateQuorumInfoMetadata.WithCauseErrMsg(
			fmt.Sprintf("quorum info already exists for id:%d", info.Id))
	}

	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("store_quorum_info", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("store_quorum_info", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

func (e *metadataProviderEtcd) GetQuorumInfo(ctx context.Context, quorumId int64) (*proto.QuorumInfo, error) {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "GetQuorumInfo")
	defer sp.End()
	startTime := time.Now()
	quorumKey := BuildQuorumInfoKey(fmt.Sprintf("%d", quorumId))
	getResp, getErr := e.client.Get(ctx, quorumKey)
	if getErr != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_quorum_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_quorum_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, werr.ErrMetadataEncode.WithCauseErr(getErr)
	}
	if len(getResp.Kvs) == 0 {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_quorum_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_quorum_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, werr.ErrMetadataEncode.WithCauseErrMsg(fmt.Sprintf("quorum info not found for id:%d", quorumId))
	}

	quorumInfo := &proto.QuorumInfo{}
	if err := pb.Unmarshal(getResp.Kvs[0].Value, quorumInfo); err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_quorum_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_quorum_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, werr.ErrMetadataDecode.WithCauseErr(err)
	}
	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_quorum_info", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_quorum_info", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return quorumInfo, nil
}

func (e *metadataProviderEtcd) CreateReaderTempInfo(ctx context.Context, readerName string, logId int64, fromSegmentId int64, fromEntryId int64) error {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "CreateReaderTempInfo")
	defer sp.End()
	startTime := time.Now()
	// Create a key path for the reader temporary information
	readerKey := BuildLogReaderTempInfoKey(logId, readerName)

	// Create reader info structure
	ts := uint64(time.Now().UnixMilli())
	readerInfo := &proto.ReaderTempInfo{
		ReaderName:          readerName,
		OpenTimestamp:       ts,
		LogId:               logId,
		OpenSegmentId:       fromSegmentId,
		OpenEntryId:         fromEntryId,
		RecentReadSegmentId: fromSegmentId,
		RecentReadEntryId:   fromEntryId,
		RecentReadTimestamp: ts,
	}

	// Serialize to binary
	readerInfoValue, err := pb.Marshal(readerInfo)
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("create_reader_temp_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("create_reader_temp_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataEncode.WithCauseErr(err)
	}

	// Create a lease with TTL of 60 seconds
	lease, err := e.client.Grant(ctx, 60)
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("create_reader_temp_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("create_reader_temp_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataWrite.WithCauseErr(err)
	}

	// Put reader info in etcd with the lease
	_, err = e.client.Put(ctx, readerKey, string(readerInfoValue), clientv3.WithLease(lease.ID))
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("create_reader_temp_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("create_reader_temp_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataWrite.WithCauseErr(err)
	}

	logger.Ctx(ctx).Debug("Created reader temporary information with lease",
		zap.String("readerName", readerName),
		zap.Int64("logId", logId),
		zap.Int64("openSegmentId", fromSegmentId),
		zap.Int64("openEntryId", fromEntryId),
		zap.Int64("leaseTTL", 60),
		zap.Int64("leaseID", int64(lease.ID)))

	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("create_reader_temp_info", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("create_reader_temp_info", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

// GetReaderTempInfo returns the temporary information for a specific reader
func (e *metadataProviderEtcd) GetReaderTempInfo(ctx context.Context, logId int64, readerName string) (*proto.ReaderTempInfo, error) {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "GetReaderTempInfo")
	defer sp.End()
	startTime := time.Now()
	// Create the key path for the reader temporary information
	readerKey := BuildLogReaderTempInfoKey(logId, readerName)

	// Get reader info from etcd
	resp, err := e.client.Get(ctx, readerKey)
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_reader_temp_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_reader_temp_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, werr.ErrMetadataRead.WithCauseErr(err)
	}

	// Check if reader info exists
	if len(resp.Kvs) == 0 {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_reader_temp_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_reader_temp_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, werr.ErrMetadataRead.WithCauseErrMsg(fmt.Sprintf("reader temp info not found for logId:%d readerName:%s", logId, readerName))
	}

	// Decode reader info
	readerInfo := &proto.ReaderTempInfo{}
	if err := pb.Unmarshal(resp.Kvs[0].Value, readerInfo); err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_reader_temp_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_reader_temp_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, werr.ErrMetadataDecode.WithCauseErr(err)
	}

	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_reader_temp_info", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_reader_temp_info", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return readerInfo, nil
}

// GetAllReaderTempInfoForLog returns all reader temporary information for a given log
func (e *metadataProviderEtcd) GetAllReaderTempInfoForLog(ctx context.Context, logId int64) ([]*proto.ReaderTempInfo, error) {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "GetAllReaderTempInfoForLog")
	defer sp.End()
	startTime := time.Now()
	// Create the prefix for all readers of this log
	readerPrefix := BuildLogAllReaderTempInfosKey(logId)

	// Get all reader infos with this prefix
	resp, err := e.client.Get(ctx, readerPrefix, clientv3.WithPrefix())
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_all_reader_temp_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_all_reader_temp_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, werr.ErrMetadataRead.WithCauseErr(err)
	}

	// Create result slice
	readers := make([]*proto.ReaderTempInfo, 0, len(resp.Kvs))

	// Decode each reader info
	for _, kv := range resp.Kvs {
		readerInfo := &proto.ReaderTempInfo{}
		if err := pb.Unmarshal(kv.Value, readerInfo); err != nil {
			logger.Ctx(ctx).Warn("Failed to decode reader temp info",
				zap.String("key", string(kv.Key)),
				zap.Error(err))
			continue
		}
		readers = append(readers, readerInfo)
	}

	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_all_reader_temp_info", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_all_reader_temp_info", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return readers, nil
}

// UpdateReaderTempInfo updates the reader's recent read position
func (e *metadataProviderEtcd) UpdateReaderTempInfo(ctx context.Context, logId int64, readerName string, recentReadSegmentId int64, recentReadEntryId int64) error {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "UpdateReaderTempInfo")
	defer sp.End()
	startTime := time.Now()
	// Create the key path for the reader temporary information
	readerKey := BuildLogReaderTempInfoKey(logId, readerName)

	// Get the current reader info
	resp, err := e.client.Get(ctx, readerKey)
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("update_reader_temp_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("update_reader_temp_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataRead.WithCauseErr(err)
	}
	if len(resp.Kvs) == 0 {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("update_reader_temp_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("update_reader_temp_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataRead.WithCauseErrMsg(fmt.Sprintf("reader temp info not found for logId:%d readerName:%s", logId, readerName))
	}

	// Decode reader info
	readerInfo := &proto.ReaderTempInfo{}
	if err := pb.Unmarshal(resp.Kvs[0].Value, readerInfo); err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("update_reader_temp_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("update_reader_temp_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataDecode.WithCauseErr(err)
	}

	// Only update the read position related fields
	readerInfo.RecentReadSegmentId = recentReadSegmentId
	readerInfo.RecentReadEntryId = recentReadEntryId
	readerInfo.RecentReadTimestamp = uint64(time.Now().UnixMilli())

	// Marshal the updated reader info
	bytes, err := pb.Marshal(readerInfo)
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("update_reader_temp_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("update_reader_temp_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataEncode.WithCauseErr(err)
	}

	// Get the lease ID from the current key
	leaseID := resp.Kvs[0].Lease

	// Update the reader info with the existing lease
	if leaseID != 0 {
		_, err = e.client.Put(ctx, readerKey, string(bytes), clientv3.WithLease(clientv3.LeaseID(leaseID)))
	} else {
		// If no lease is attached (shouldn't happen normally), just update the value
		_, err = e.client.Put(ctx, readerKey, string(bytes))
	}

	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("update_reader_temp_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("update_reader_temp_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataWrite.WithCauseErr(err)
	}

	logger.Ctx(ctx).Debug("Updated reader temporary information",
		zap.String("readerName", readerName),
		zap.Int64("logId", logId),
		zap.Int64("recentReadSegmentId", recentReadSegmentId),
		zap.Int64("recentReadEntryId", recentReadEntryId))

	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("update_reader_temp_info", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("update_reader_temp_info", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

// DeleteReaderTempInfo deletes the temporary information for a reader when it closes
func (e *metadataProviderEtcd) DeleteReaderTempInfo(ctx context.Context, logId int64, readerName string) error {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "DeleteReaderTempInfo")
	defer sp.End()
	startTime := time.Now()
	// Create the key path for the reader temporary information
	readerKey := BuildLogReaderTempInfoKey(logId, readerName)

	// Delete the reader information
	resp, err := e.client.Delete(ctx, readerKey)
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("delete_reader_temp_info", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("delete_reader_temp_info", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return werr.ErrMetadataWrite.WithCauseErr(err)
	}

	// Check if the key existed
	if resp.Deleted == 0 {
		logger.Ctx(ctx).Warn("Reader temp info not found during deletion",
			zap.String("readerName", readerName),
			zap.Int64("logId", logId))
		// We don't return an error here since the end result is the same - the reader info doesn't exist
	} else {
		logger.Ctx(ctx).Debug("Deleted reader temporary information",
			zap.String("readerName", readerName),
			zap.Int64("logId", logId))
	}

	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("delete_reader_temp_info", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("delete_reader_temp_info", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

func (e *metadataProviderEtcd) Close() error {
	startTime := time.Now()
	e.Lock()
	defer e.Unlock()

	// Close all individual writer locks and their sessions
	for logName, lock := range e.logWriterLocks {
		if lock == nil {
			continue
		}

		// First unlock the mutex
		err := lock.Unlock(context.Background())
		if err != nil {
			logger.Ctx(context.Background()).Warn("Failed to unlock writer lock during close",
				zap.String("logName", logName),
				zap.Error(err))
		}

		// Close the associated session
		if session, hasSession := e.lockSessions[logName]; hasSession && session != nil {
			closeErr := session.Close()
			if closeErr != nil {
				logger.Ctx(context.Background()).Warn("Failed to close lock session during close",
					zap.String("logName", logName),
					zap.Error(closeErr))
			}
		}
	}

	// Clear the maps
	e.logWriterLocks = make(map[string]*concurrency.Mutex)
	e.lockSessions = make(map[string]*concurrency.Session)

	// Close the main session if it exists (should be removed in the new implementation)
	if e.session != nil {
		err := e.session.Close()
		if err != nil {
			logger.Ctx(context.Background()).Warn("Failed to close main etcd session", zap.Error(err))
			metrics.WpEtcdMetaOperationsTotal.WithLabelValues("close", "error").Inc()
			metrics.WpEtcdMetaOperationLatency.WithLabelValues("close", "error").Observe(float64(time.Since(startTime).Milliseconds()))
			return err
		}
		e.session = nil
	}

	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("close", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("close", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

// CreateSegmentCleanupStatus creates a new segment cleanup status record
func (e *metadataProviderEtcd) CreateSegmentCleanupStatus(ctx context.Context, status *proto.SegmentCleanupStatus) error {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "CreateSegmentCleanupStatus")
	defer sp.End()
	startTime := time.Now()
	key := BuildSegmentCleanupStatusKey(status.LogId, status.SegmentId)
	bytes, err := proto.MarshalSegmentCleanupStatus(status)
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("create_segment_cleanup_status", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("create_segment_cleanup_status", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return fmt.Errorf("failed to marshal segment cleanup status: %w", err)
	}

	// Start a new transaction
	txn := e.client.Txn(ctx)

	// First check if it already exists
	cmp := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	put := clientv3.OpPut(key, string(bytes))

	resp, err := txn.If(cmp).Then(put).Commit()
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("create_segment_cleanup_status", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("create_segment_cleanup_status", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return fmt.Errorf("failed to create segment cleanup status: %w", err)
	}

	if !resp.Succeeded {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("create_segment_cleanup_status", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("create_segment_cleanup_status", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return fmt.Errorf("segment cleanup status already exists: %s", key)
	}

	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("create_segment_cleanup_status", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("create_segment_cleanup_status", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

// UpdateSegmentCleanupStatus updates an existing segment cleanup status
func (e *metadataProviderEtcd) UpdateSegmentCleanupStatus(ctx context.Context, status *proto.SegmentCleanupStatus) error {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "UpdateSegmentCleanupStatus")
	defer sp.End()
	startTime := time.Now()
	key := BuildSegmentCleanupStatusKey(status.LogId, status.SegmentId)
	bytes, err := proto.MarshalSegmentCleanupStatus(status)
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("update_segment_cleanup_status", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("update_segment_cleanup_status", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return fmt.Errorf("failed to marshal segment cleanup status: %w", err)
	}

	// Start a new transaction
	txn := e.client.Txn(ctx)

	// Check if it exists
	cmp := clientv3.Compare(clientv3.CreateRevision(key), ">", 0)
	put := clientv3.OpPut(key, string(bytes))

	resp, err := txn.If(cmp).Then(put).Commit()
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("update_segment_cleanup_status", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("update_segment_cleanup_status", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return fmt.Errorf("failed to update segment cleanup status: %w", err)
	}

	if !resp.Succeeded {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("update_segment_cleanup_status", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("update_segment_cleanup_status", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return fmt.Errorf("segment cleanup status does not exist: %s", key)
	}

	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("update_segment_cleanup_status", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("update_segment_cleanup_status", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

// GetSegmentCleanupStatus retrieves the cleanup status for a segment
func (e *metadataProviderEtcd) GetSegmentCleanupStatus(ctx context.Context, logId, segmentId int64) (*proto.SegmentCleanupStatus, error) {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "GetSegmentCleanupStatus")
	defer sp.End()
	startTime := time.Now()
	key := BuildSegmentCleanupStatusKey(logId, segmentId)

	resp, err := e.client.Get(ctx, key)
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_segment_cleanup_status", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_segment_cleanup_status", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, fmt.Errorf("failed to get segment cleanup status: %w", err)
	}

	if len(resp.Kvs) == 0 {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_segment_cleanup_status", "success").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_segment_cleanup_status", "success").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, nil
	}

	status := &proto.SegmentCleanupStatus{}
	err = proto.UnmarshalSegmentCleanupStatus(resp.Kvs[0].Value, status)
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_segment_cleanup_status", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_segment_cleanup_status", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, fmt.Errorf("failed to unmarshal segment cleanup status: %w", err)
	}

	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("get_segment_cleanup_status", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("get_segment_cleanup_status", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return status, nil
}

// DeleteSegmentCleanupStatus deletes the cleanup status for a segment
func (e *metadataProviderEtcd) DeleteSegmentCleanupStatus(ctx context.Context, logId, segmentId int64) error {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "DeleteSegmentCleanupStatus")
	defer sp.End()
	startTime := time.Now()
	key := BuildSegmentCleanupStatusKey(logId, segmentId)

	_, err := e.client.Delete(ctx, key)
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("delete_segment_cleanup_status", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("delete_segment_cleanup_status", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return fmt.Errorf("failed to delete segment cleanup status: %w", err)
	}

	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("delete_segment_cleanup_status", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("delete_segment_cleanup_status", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

// ListSegmentCleanupStatus lists all cleanup statuses for a log
func (e *metadataProviderEtcd) ListSegmentCleanupStatus(ctx context.Context, logId int64) ([]*proto.SegmentCleanupStatus, error) {
	ctx, sp := otel.Tracer(CurrentScopeName).Start(ctx, "ListSegmentCleanupStatus")
	defer sp.End()
	startTime := time.Now()
	// Create a prefix key for the log to retrieve all segments
	prefix := BuildAllSegmentsCleanupStatusKey(logId)

	resp, err := e.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("list_segment_cleanup_status", "error").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("list_segment_cleanup_status", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil, fmt.Errorf("failed to list segment cleanup statuses: %w", err)
	}

	if len(resp.Kvs) == 0 {
		metrics.WpEtcdMetaOperationsTotal.WithLabelValues("list_segment_cleanup_status", "success").Inc()
		metrics.WpEtcdMetaOperationLatency.WithLabelValues("list_segment_cleanup_status", "success").Observe(float64(time.Since(startTime).Milliseconds()))
		return []*proto.SegmentCleanupStatus{}, nil
	}

	statuses := make([]*proto.SegmentCleanupStatus, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		status := &proto.SegmentCleanupStatus{}
		err = proto.UnmarshalSegmentCleanupStatus(kv.Value, status)
		if err != nil {
			metrics.WpEtcdMetaOperationsTotal.WithLabelValues("list_segment_cleanup_status", "error").Inc()
			metrics.WpEtcdMetaOperationLatency.WithLabelValues("list_segment_cleanup_status", "error").Observe(float64(time.Since(startTime).Milliseconds()))
			return nil, fmt.Errorf("failed to unmarshal segment cleanup status: %w", err)
		}
		statuses = append(statuses, status)
	}

	metrics.WpEtcdMetaOperationsTotal.WithLabelValues("list_segment_cleanup_status", "success").Inc()
	metrics.WpEtcdMetaOperationLatency.WithLabelValues("list_segment_cleanup_status", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return statuses, nil
}
