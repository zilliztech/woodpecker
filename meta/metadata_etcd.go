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
	if e.session == nil {
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
