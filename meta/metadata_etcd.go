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
	"go.uber.org/zap"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

var _ MetadataProvider = (*metadataProviderEtcd)(nil)

type metadataProviderEtcd struct {
	sync.Mutex
	client *clientv3.Client

	session        *concurrency.Session
	logWriterLocks map[string]*concurrency.Mutex
}

func NewMetadataProvider(ctx context.Context, client *clientv3.Client) MetadataProvider {
	return &metadataProviderEtcd{
		client:         client,
		logWriterLocks: make(map[string]*concurrency.Mutex),
	}
}

// InitIfNecessary initializes the metadata provider if necessary.
// It checks if there is logIdGen,instance,quorumIdGen keys in etcd.
// If not, it creates them.
func (e *metadataProviderEtcd) InitIfNecessary(ctx context.Context) error {
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
		return werr.ErrMetadataRead.WithCauseErr(err)
	}
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
		return err
	}
	// cluster not initialized, initialize it
	_, initErr := e.client.Txn(ctx).If().Then(initOps...).Commit()
	if initErr != nil || !resp.Succeeded {
		err = werr.ErrMetadataInit.WithCauseErr(initErr)
		log.Error("init operation failed", zap.Error(err))
		return err
	}
	// cluster initialized successfully
	log.Info("cluster initialized successfully")
	return nil
}

func (e *metadataProviderEtcd) GetVersionInfo(ctx context.Context) (*proto.Version, error) {
	getResp, getErr := e.client.Get(ctx, VersionKey)
	if getErr != nil {
		return nil, werr.ErrMetadataRead.WithCauseErr(getErr)
	}
	if len(getResp.Kvs) == 0 {
		return nil, werr.ErrMetadataRead.WithCauseErrMsg("version not found")
	}
	expectedVersion := &proto.Version{}
	decodedErr := pb.Unmarshal(getResp.Kvs[0].Value, expectedVersion)
	if decodedErr != nil {
		return nil, werr.ErrMetadataDecode.WithCauseErr(decodedErr)
	}
	return expectedVersion, nil
}

func (e *metadataProviderEtcd) CreateLog(ctx context.Context, logName string) error {
	e.Lock()
	defer e.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get the current id value from logIdGenerator
	resp, err := e.client.Get(ctx, LogIdGeneratorKey)
	if err != nil {
		return werr.ErrMetadataRead.WithCauseErr(err)
	}

	if len(resp.Kvs) == 0 {
		return werr.ErrMetadataRead.WithCauseErrMsg(fmt.Sprintf("%s key not found", LogIdGeneratorKey))
	}

	// check if logName exists
	exists, err := e.CheckExists(ctx, logName)
	if err != nil {
		return werr.ErrMetadataRead.WithCauseErr(err)
	}
	if exists {
		return werr.ErrLogAlreadyExists.WithCauseErrMsg(fmt.Sprintf("%s already exists", logName))
	}

	// create a New Log with default Options
	currentIdStr := string(resp.Kvs[0].Value)
	currentID, err := atoi(currentIdStr)
	if err != nil {
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

	if err != nil {
		return werr.ErrCreateLogMetadata.WithCauseErr(err)
	}

	if !txnResp.Succeeded {
		return werr.ErrCreateLogMetadata.WithCauseErrMsg("transaction failed due to idgen mismatch")
	}
	logger.Ctx(ctx).Info("log created successfully", zap.String("logName", logName), zap.Int64("logId", logMeta.LogId))
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
	// Get log meta for the path = logs/<logName>
	logResp, err := e.client.Get(ctx, BuildLogKey(logName))
	if err != nil {
		return nil, werr.ErrMetadataRead.WithCauseErr(err)
	}
	if len(logResp.Kvs) == 0 {
		return nil, werr.ErrMetadataRead.WithCauseErrMsg(fmt.Sprintf("log not found: %s", logName))
	}
	logMeta := &proto.LogMeta{}
	if err = pb.Unmarshal(logResp.Kvs[0].Value, logMeta); err != nil {
		return nil, werr.ErrMetadataDecode.WithCauseErr(err)
	}
	return logMeta, nil
}

func (e *metadataProviderEtcd) UpdateLogMeta(ctx context.Context, logName string, logMeta *proto.LogMeta) error {
	logKey := BuildLogKey(logName)
	logMetaValue, err := pb.Marshal(logMeta)
	if err != nil {
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
		return werr.ErrMetadataRead.WithCauseErr(err)
	}

	if !txnResp.Succeeded {
		return werr.ErrMetadataRead.WithCauseErrMsg(fmt.Sprintf("log not found: %s", logName))
	}

	return nil
}

func (e *metadataProviderEtcd) OpenLog(ctx context.Context, logName string) (*proto.LogMeta, map[int64]*proto.SegmentMetadata, error) {
	// Get log meta for the path = logs/<logName>
	logResp, err := e.GetLogMeta(ctx, logName)
	if err != nil {
		return nil, nil, err
	}

	// Get segments meta with prefix = logs/<logName>/segments/<segmentId>
	segmentMetaList, err := e.GetAllSegmentMetadata(ctx, logName)
	if err != nil {
		return nil, nil, err
	}

	//
	return logResp, segmentMetaList, nil
}

func (e *metadataProviderEtcd) CheckExists(ctx context.Context, logName string) (bool, error) {
	// Get log meta for the path = logs/<logName>
	logResp, err := e.client.Get(ctx, BuildLogKey(logName))
	if err != nil {
		return false, werr.ErrMetadataRead.WithCauseErr(err)
	}
	if len(logResp.Kvs) == 0 {
		return false, nil
	}
	return true, nil
}

func (e *metadataProviderEtcd) ListLogs(ctx context.Context) ([]string, error) {
	return e.ListLogsWithPrefix(ctx, "")
}

func (e *metadataProviderEtcd) ListLogsWithPrefix(ctx context.Context, logNamePrefix string) ([]string, error) {
	logResp, err := e.client.Get(ctx, BuildLogKey(logNamePrefix), clientv3.WithPrefix())
	if err != nil {
		return nil, werr.ErrMetadataRead.WithCauseErr(err)
	}
	if len(logResp.Kvs) == 0 {
		return []string{}, nil
	}
	logs := make(map[string]int, 0)
	for _, path := range logResp.Kvs {
		logName, extractErr := extractLogName(string(path.Key))
		if extractErr != nil {
			return nil, werr.ErrMetadataRead.WithCauseErr(extractErr)
		}
		logs[logName] = 1
	}
	logNames := make([]string, 0)
	for logName := range logs {
		logNames = append(logNames, logName)
	}
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

func (e *metadataProviderEtcd) AcquireLogWriterLock(ctx context.Context, logName string) error {
	e.Lock()
	defer e.Unlock()
	if e.session == nil { // TODO 要判断session 还活着，不活着的话，直接掉线close掉这个writer，或者自己保活。
		// keep a session for this metadata cli
		newSession, err := concurrency.NewSession(e.client, concurrency.WithTTL(5))
		if err != nil {
			return err
		}
		e.session = newSession
	}
	// try lock if the lock already exists
	if l, exists := e.logWriterLocks[logName]; exists {
		return l.TryLock(ctx)
	}
	// create a new lock
	lockKey := BuildLogLockKey(logName)
	lock := concurrency.NewMutex(e.session, lockKey)
	e.logWriterLocks[logName] = lock
	//
	return lock.TryLock(ctx)
}

func (e *metadataProviderEtcd) ReleaseLogWriterLock(ctx context.Context, logName string) error {
	e.Lock()
	defer e.Unlock()
	if l, exists := e.logWriterLocks[logName]; exists {
		delete(e.logWriterLocks, logName)
		return l.Unlock(ctx)
	}
	return nil
}

func (e *metadataProviderEtcd) StoreSegmentMetadata(ctx context.Context, logName string, metadata *proto.SegmentMetadata) error {
	e.Lock()
	defer e.Unlock()
	segmentKey := BuildSegmentInstanceKey(logName, fmt.Sprintf("%d", metadata.GetSegNo()))
	segmentMetadata, err := pb.Marshal(metadata)
	if err != nil {
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
		return werr.ErrCreateSegmentMetadata.WithCauseErr(err)
	}

	if !txnResp.Succeeded {
		return werr.ErrCreateSegmentMetadata.WithCauseErrMsg(
			fmt.Sprintf("segment metadata already exists for logName:%s segmentId:%d", logName, metadata.SegNo))
	}

	return nil
}

func (e *metadataProviderEtcd) UpdateSegmentMetadata(ctx context.Context, logName string, metadata *proto.SegmentMetadata) error {
	e.Lock()
	defer e.Unlock()
	segmentKey := BuildSegmentInstanceKey(logName, fmt.Sprintf("%d", metadata.SegNo))
	segmentMetadata, err := pb.Marshal(metadata)
	if err != nil {
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
		return err
	}

	if !txnResp.Succeeded {
		return werr.ErrUpdateSegmentMetadata.WithCauseErrMsg(
			fmt.Sprintf("segment metadata not found for logName:%s segmentId:%d", logName, metadata.SegNo))
	}
	return nil
}

func (e *metadataProviderEtcd) GetSegmentMetadata(ctx context.Context, logName string, segmentId int64) (*proto.SegmentMetadata, error) {
	segmentKey := BuildSegmentInstanceKey(logName, fmt.Sprintf("%d", segmentId))
	getResp, getErr := e.client.Get(ctx, segmentKey)
	if getErr != nil {
		return nil, getErr
	}
	if len(getResp.Kvs) == 0 {
		return nil, werr.ErrSegmentNotFound.WithCauseErrMsg(
			fmt.Sprintf("segment meta not found for log:%s segment:%d", logName, segmentId))
	}
	segmentMetadata := &proto.SegmentMetadata{}
	if err := pb.Unmarshal(getResp.Kvs[0].Value, segmentMetadata); err != nil {
		return nil, werr.ErrMetadataDecode.WithCauseErr(err)
	}
	return segmentMetadata, nil
}

func (e *metadataProviderEtcd) GetAllSegmentMetadata(ctx context.Context, logName string) (map[int64]*proto.SegmentMetadata, error) {
	segmentKey := BuildSegmentInstanceKey(logName, "")
	getResp, getErr := e.client.Get(ctx, segmentKey, clientv3.WithPrefix())
	if getErr != nil {
		return nil, getErr
	}

	segmentMetaMap := make(map[int64]*proto.SegmentMetadata)
	if len(getResp.Kvs) == 0 {
		return segmentMetaMap, nil
	}

	for _, kv := range getResp.Kvs {
		segmentMeta := &proto.SegmentMetadata{}
		if err := pb.Unmarshal(kv.Value, segmentMeta); err != nil {
			return nil, werr.ErrMetadataDecode.WithCauseErr(err)
		}
		segmentMetaMap[segmentMeta.SegNo] = segmentMeta
	}
	return segmentMetaMap, nil
}

func (e *metadataProviderEtcd) CheckSegmentExists(ctx context.Context, logName string, segmentId int64) (bool, error) {
	segmentResp, err := e.client.Get(ctx, BuildSegmentInstanceKey(logName, fmt.Sprintf("%d", segmentId)))
	if err != nil {
		return false, err
	}
	if len(segmentResp.Kvs) == 0 {
		return false, nil
	}
	return true, nil
}

// DeleteSegmentMetadata deletes a segment metadata entry.
// It returns an error if the segment does not exist or if the deletion fails.
func (e *metadataProviderEtcd) DeleteSegmentMetadata(ctx context.Context, logName string, segmentId int64) error {
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
		return werr.ErrMetadataWrite.WithCauseErr(err)
	}

	if !txnResp.Succeeded {
		return werr.ErrSegmentNotFound.WithCauseErrMsg(
			fmt.Sprintf("segment not found for logName:%s segmentId:%d", logName, segmentId))
	}

	logger.Ctx(ctx).Debug("Deleted segment metadata",
		zap.String("logName", logName),
		zap.Int64("segmentId", segmentId))

	return nil
}

func (e *metadataProviderEtcd) StoreQuorumInfo(ctx context.Context, info *proto.QuorumInfo) error {
	quorumKey := BuildQuorumInfoKey(fmt.Sprintf("%d", info.Id))
	quorumInfoValue, err := pb.Marshal(info)
	if err != nil {
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
		return werr.ErrUpdateQuorumInfoMetadata.WithCauseErr(err)
	}

	if !txnResp.Succeeded {
		return werr.ErrUpdateQuorumInfoMetadata.WithCauseErrMsg(
			fmt.Sprintf("quorum info already exists for id:%d", info.Id))
	}

	return nil
}

func (e *metadataProviderEtcd) GetQuorumInfo(ctx context.Context, quorumId int64) (*proto.QuorumInfo, error) {
	quorumKey := BuildQuorumInfoKey(fmt.Sprintf("%d", quorumId))
	getResp, getErr := e.client.Get(ctx, quorumKey)
	if getErr != nil {
		return nil, werr.ErrMetadataEncode.WithCauseErr(getErr)
	}
	if len(getResp.Kvs) == 0 {
		return nil, werr.ErrMetadataEncode.WithCauseErrMsg(fmt.Sprintf("quorum info not found for id:%d", quorumId))
	}

	quorumInfo := &proto.QuorumInfo{}
	if err := pb.Unmarshal(getResp.Kvs[0].Value, quorumInfo); err != nil {
		return nil, werr.ErrMetadataDecode.WithCauseErr(err)
	}
	return quorumInfo, nil
}

func (e *metadataProviderEtcd) CreateReaderTempInfo(ctx context.Context, readerName string, logId int64, fromSegmentId int64, fromEntryId int64) error {
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
		return werr.ErrMetadataEncode.WithCauseErr(err)
	}

	// Create a lease with TTL of 60 seconds
	lease, err := e.client.Grant(ctx, 60)
	if err != nil {
		return werr.ErrMetadataWrite.WithCauseErr(err)
	}

	// Put reader info in etcd with the lease
	_, err = e.client.Put(ctx, readerKey, string(readerInfoValue), clientv3.WithLease(lease.ID))
	if err != nil {
		return werr.ErrMetadataWrite.WithCauseErr(err)
	}

	logger.Ctx(ctx).Debug("Created reader temporary information with lease",
		zap.String("readerName", readerName),
		zap.Int64("logId", logId),
		zap.Int64("openSegmentId", fromSegmentId),
		zap.Int64("openEntryId", fromEntryId),
		zap.Int64("leaseTTL", 60),
		zap.Int64("leaseID", int64(lease.ID)))

	return nil
}

// GetReaderTempInfo returns the temporary information for a specific reader
func (e *metadataProviderEtcd) GetReaderTempInfo(ctx context.Context, logId int64, readerName string) (*proto.ReaderTempInfo, error) {
	// Create the key path for the reader temporary information
	readerKey := BuildLogReaderTempInfoKey(logId, readerName)

	// Get reader info from etcd
	resp, err := e.client.Get(ctx, readerKey)
	if err != nil {
		return nil, werr.ErrMetadataRead.WithCauseErr(err)
	}

	// Check if reader info exists
	if len(resp.Kvs) == 0 {
		return nil, werr.ErrMetadataRead.WithCauseErrMsg(fmt.Sprintf("reader temp info not found for logId:%d readerName:%s", logId, readerName))
	}

	// Decode reader info
	readerInfo := &proto.ReaderTempInfo{}
	if err := pb.Unmarshal(resp.Kvs[0].Value, readerInfo); err != nil {
		return nil, werr.ErrMetadataDecode.WithCauseErr(err)
	}

	return readerInfo, nil
}

// GetAllReaderTempInfoForLog returns all reader temporary information for a given log
func (e *metadataProviderEtcd) GetAllReaderTempInfoForLog(ctx context.Context, logId int64) ([]*proto.ReaderTempInfo, error) {
	// Create the prefix for all readers of this log
	readerPrefix := BuildLogAllReaderTempInfosKey(logId)

	// Get all reader infos with this prefix
	resp, err := e.client.Get(ctx, readerPrefix, clientv3.WithPrefix())
	if err != nil {
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

	return readers, nil
}

// UpdateReaderTempInfo updates the reader's recent read position
func (e *metadataProviderEtcd) UpdateReaderTempInfo(ctx context.Context, logId int64, readerName string, recentReadSegmentId int64, recentReadEntryId int64) error {
	// Create the key path for the reader temporary information
	readerKey := BuildLogReaderTempInfoKey(logId, readerName)

	// Get the current reader info
	resp, err := e.client.Get(ctx, readerKey)
	if err != nil {
		return werr.ErrMetadataRead.WithCauseErr(err)
	}
	if len(resp.Kvs) == 0 {
		return werr.ErrMetadataRead.WithCauseErrMsg(fmt.Sprintf("reader temp info not found for logId:%d readerName:%s", logId, readerName))
	}

	// Decode reader info
	readerInfo := &proto.ReaderTempInfo{}
	if err := pb.Unmarshal(resp.Kvs[0].Value, readerInfo); err != nil {
		return werr.ErrMetadataDecode.WithCauseErr(err)
	}

	// Only update the read position related fields
	readerInfo.RecentReadSegmentId = recentReadSegmentId
	readerInfo.RecentReadEntryId = recentReadEntryId
	readerInfo.RecentReadTimestamp = uint64(time.Now().UnixMilli())

	// Marshal the updated reader info
	bytes, err := pb.Marshal(readerInfo)
	if err != nil {
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
		return werr.ErrMetadataWrite.WithCauseErr(err)
	}

	logger.Ctx(ctx).Debug("Updated reader temporary information",
		zap.String("readerName", readerName),
		zap.Int64("logId", logId),
		zap.Int64("recentReadSegmentId", recentReadSegmentId),
		zap.Int64("recentReadEntryId", recentReadEntryId))

	return nil
}

// DeleteReaderTempInfo deletes the temporary information for a reader when it closes
func (e *metadataProviderEtcd) DeleteReaderTempInfo(ctx context.Context, logId int64, readerName string) error {
	// Create the key path for the reader temporary information
	readerKey := BuildLogReaderTempInfoKey(logId, readerName)

	// Delete the reader information
	resp, err := e.client.Delete(ctx, readerKey)
	if err != nil {
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

	return nil
}

func (e *metadataProviderEtcd) Close() error {
	e.Lock()
	defer e.Unlock()
	for logName, lock := range e.logWriterLocks {
		if lock == nil {
			continue
		}
		err := lock.Unlock(context.Background())
		if err != nil {
			logger.Ctx(context.Background()).Warn("Failed to unlock writer lock", zap.String("logName", logName), zap.Error(err))
		}
	}
	if e.session != nil {
		err := e.session.Close()
		if err != nil {
			logger.Ctx(context.Background()).Warn("Failed to close etcd session", zap.Error(err))
		}
	}
	return nil
}

// CreateSegmentCleanupStatus creates a new segment cleanup status record
func (e *metadataProviderEtcd) CreateSegmentCleanupStatus(ctx context.Context, status *proto.SegmentCleanupStatus) error {
	key := BuildSegmentCleanupStatusKey(status.LogId, status.SegmentId)
	bytes, err := proto.MarshalSegmentCleanupStatus(status)
	if err != nil {
		return fmt.Errorf("failed to marshal segment cleanup status: %w", err)
	}

	// Start a new transaction
	txn := e.client.Txn(ctx)

	// First check if it already exists
	cmp := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	put := clientv3.OpPut(key, string(bytes))

	resp, err := txn.If(cmp).Then(put).Commit()
	if err != nil {
		return fmt.Errorf("failed to create segment cleanup status: %w", err)
	}

	if !resp.Succeeded {
		return fmt.Errorf("segment cleanup status already exists: %s", key)
	}

	return nil
}

// UpdateSegmentCleanupStatus updates an existing segment cleanup status
func (e *metadataProviderEtcd) UpdateSegmentCleanupStatus(ctx context.Context, status *proto.SegmentCleanupStatus) error {
	key := BuildSegmentCleanupStatusKey(status.LogId, status.SegmentId)
	bytes, err := proto.MarshalSegmentCleanupStatus(status)
	if err != nil {
		return fmt.Errorf("failed to marshal segment cleanup status: %w", err)
	}

	// Start a new transaction
	txn := e.client.Txn(ctx)

	// Check if it exists
	cmp := clientv3.Compare(clientv3.CreateRevision(key), ">", 0)
	put := clientv3.OpPut(key, string(bytes))

	resp, err := txn.If(cmp).Then(put).Commit()
	if err != nil {
		return fmt.Errorf("failed to update segment cleanup status: %w", err)
	}

	if !resp.Succeeded {
		return fmt.Errorf("segment cleanup status does not exist: %s", key)
	}

	return nil
}

// GetSegmentCleanupStatus retrieves the cleanup status for a segment
func (e *metadataProviderEtcd) GetSegmentCleanupStatus(ctx context.Context, logId, segmentId int64) (*proto.SegmentCleanupStatus, error) {
	key := BuildSegmentCleanupStatusKey(logId, segmentId)

	resp, err := e.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get segment cleanup status: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	status := &proto.SegmentCleanupStatus{}
	err = proto.UnmarshalSegmentCleanupStatus(resp.Kvs[0].Value, status)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal segment cleanup status: %w", err)
	}

	return status, nil
}

// DeleteSegmentCleanupStatus deletes the cleanup status for a segment
func (e *metadataProviderEtcd) DeleteSegmentCleanupStatus(ctx context.Context, logId, segmentId int64) error {
	key := BuildSegmentCleanupStatusKey(logId, segmentId)

	_, err := e.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete segment cleanup status: %w", err)
	}

	return nil
}

// ListSegmentCleanupStatus lists all cleanup statuses for a log
func (e *metadataProviderEtcd) ListSegmentCleanupStatus(ctx context.Context, logId int64) ([]*proto.SegmentCleanupStatus, error) {
	// Create a prefix key for the log to retrieve all segments
	prefix := BuildAllSegmentsCleanupStatusKey(logId)

	resp, err := e.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to list segment cleanup statuses: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return []*proto.SegmentCleanupStatus{}, nil
	}

	statuses := make([]*proto.SegmentCleanupStatus, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		status := &proto.SegmentCleanupStatus{}
		err = proto.UnmarshalSegmentCleanupStatus(kv.Value, status)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal segment cleanup status: %w", err)
		}
		statuses = append(statuses, status)
	}

	return statuses, nil
}
