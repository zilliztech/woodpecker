package meta

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/milvus-io/woodpecker/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"
)

func NewMetadataProviderEtcd(ctx context.Context, client *clientv3.Client) MetadataProvider {
	return &metadataProviderEtcd{
		client: client,
	}
}

var _ MetadataProvider = (*metadataProviderEtcd)(nil)

type metadataProviderEtcd struct {
	sync.Mutex

	segmentsMetadata map[string][]*proto.SegmentMetadata
	quorumInfo       map[int64]*proto.QuorumInfo
	client           *clientv3.Client
}

func (e *metadataProviderEtcd) StoreSegmentMetadata(ctx context.Context, logName string, metadata *proto.SegmentMetadata) error {
	segmentKey := BuildSegmentInstancePath(logName, fmt.Sprintf("%d", metadata.GetSegNo()))
	segmentMetadata, err := pb.Marshal(metadata)
	if err != nil {
		return err
	}
	resp, err := e.client.Txn(ctx).If().Then(clientv3.OpPut(segmentKey, string(segmentMetadata))).Commit()
	if err != nil || !resp.Succeeded {
		return err
	}
	return nil
}

func (e *metadataProviderEtcd) StoreQuorumInfo(ctx context.Context, info *proto.QuorumInfo) error {
	//TODO implement me
	panic("implement me")
}

func (e *metadataProviderEtcd) StoreVersionInfo(ctx context.Context, version *proto.Version) error {
	//TODO implement me
	panic("implement me")
}

func (e *metadataProviderEtcd) Close() error {
	//TODO implement me
	panic("implement me")
}

func (e *metadataProviderEtcd) GetSegmentMetadata(ctx context.Context, logName string, segmentId int64) (*proto.SegmentMetadata, error) {
	//TODO implement me
	panic("implement me")
}

func (e *metadataProviderEtcd) GetAllSegmentMetadata(ctx context.Context, logName string) ([]*proto.SegmentMetadata, error) {
	//TODO implement me
	panic("implement me")
}

func (e *metadataProviderEtcd) GetQuorumInfo(ctx context.Context, quorumId int64) (*proto.QuorumInfo, error) {
	quorumKey := BuildQuorumInfoPath(fmt.Sprintf("%d", quorumId))
	resp, err := e.client.Txn(ctx).If().Then(clientv3.OpGet(quorumKey)).Commit()
	if err != nil || !resp.Succeeded {
		return nil, err
	}
	if resp.Responses[0].GetResponseRange().Kvs == nil || len(resp.Responses[0].GetResponseRange().Kvs) == 0 {
		return nil, errors.New("quorum info not found")
	}
	quorumInfo := &proto.QuorumInfo{}
	if err := pb.Unmarshal(resp.Responses[0].GetResponseRange().Kvs[0].Value, quorumInfo); err != nil {
		return nil, err
	}
	return quorumInfo, nil
}

func (e *metadataProviderEtcd) GetVersionInfo(ctx context.Context) (*proto.Version, error) {
	//TODO implement me
	panic("implement me")
}

// InitIfNecessary initializes the metadata provider if necessary.
// It checks if there is logIdGen,instance,quorumIdGen keys in etcd.
// If not, it creates them.
func (e *metadataProviderEtcd) InitIfNecessary(ctx context.Context) error {
	keys := []string{
		ServiceInstance,
		logIdGenerator,
		QuorumIdGenerator,
	}
	ops := make([]clientv3.Op, 0, 3)
	ops = append(ops, clientv3.OpGet(ServiceInstance))
	ops = append(ops, clientv3.OpGet(logIdGenerator))
	ops = append(ops, clientv3.OpGet(QuorumIdGenerator))
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := e.client.Txn(ctx).If().Then(ops...).Commit()
	if err != nil || !resp.Succeeded {
		return err
	}
	initOps := make([]clientv3.Op, 0, 3)
	for index, rp := range resp.Responses {
		if rp.GetResponseRange().Kvs == nil || len(rp.GetResponseRange().Kvs) == 0 {
			if index == 0 {
				// instance initial value is a uuid
				initOps = append(initOps, clientv3.OpPut(keys[index], uuid.New().String()))
			} else {
				// idgen initial value is 0
				initOps = append(initOps, clientv3.OpPut(keys[index], "0"))
			}
			log.Printf("%s not found \n", keys[index])
		}
	}
	if len(initOps) == 0 {
		// cluster already initialized successfully
		log.Printf("cluster already initialized, skipping initialization")
		return nil
	} else if len(initOps) != len(keys) {
		// cluster already initialized partially, but not all
		log.Printf("init operation failed, some keys already exists")
		return errors.New("init operation failed, some keys already exists")
	}
	// cluster not initialized, initialize it
	_, initErr := e.client.Txn(ctx).If().Then(initOps...).Commit()
	if initErr != nil || !resp.Succeeded {
		log.Printf("init operation failed, %v", initErr)
		return initErr
	}
	// cluster initialized successfully
	log.Printf("cluster initialized successfully")
	return nil
}

func (e *metadataProviderEtcd) CreateLog(ctx context.Context, logName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get the current id value from logIdGenerator
	resp, err := e.client.Get(ctx, logIdGenerator)
	if err != nil {
		return fmt.Errorf("failed to get next logId: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return fmt.Errorf("%s key not found", logIdGenerator)
	}

	// check if logName exists
	checkExistsResp, err := e.client.Get(ctx, BuildLogPath(logName), clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to get next logId: %w", err)
	}
	if len(checkExistsResp.Kvs) > 0 {
		return errors.New(fmt.Sprintf("%s already exists", logName))
	}

	// create a New Log
	currentID := string(resp.Kvs[0].Value)
	nextID := atoi(currentID) + 1
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
		log.Fatalf("Failed to serialize LogMeta: %v", err)
	}

	// Start a transaction
	txn := e.client.Txn(ctx)

	// Create logs/<logName>  and update logs/idgen atomically
	txnResp, err := txn.If(
		// Ensure logs/idgen has not changed since we read it
		clientv3.Compare(clientv3.Value(logIdGenerator), "=", currentID),
	).Then(
		// Create logs/<logName> with logValue
		clientv3.OpPut(BuildLogPath(logName), string(logMetaValue)),
		// Update logs/idgen to nextID
		clientv3.OpPut(logIdGenerator, fmt.Sprintf("%d", nextID)),
	).Commit()

	if err != nil {
		return fmt.Errorf("transaction failed: %w", err)
	}

	if !txnResp.Succeeded {
		return fmt.Errorf("transaction failed due to idgen mismatch")
	}
	return nil
}

// atoi converts string to int with error handling
func atoi(s string) int {
	value, err := strconv.Atoi(s)
	if err != nil {
		log.Fatalf("failed to convert string to int: %v", err)
	}
	return value
}

func (e *metadataProviderEtcd) OpenLog(ctx context.Context, logName string) (*proto.LogMeta, []*proto.SegmentMetadata, error) {
	// Get log meta for the path = logs/<logName>
	logResp, err := e.client.Get(ctx, BuildLogPath(logName))
	if err != nil {
		return nil, nil, err
	}
	if len(logResp.Kvs) == 0 {
		return nil, nil, fmt.Errorf("log not found: %s", logName)
	}
	logMeta := &proto.LogMeta{}
	if err := pb.Unmarshal(logResp.Kvs[0].Value, logMeta); err != nil {
		return nil, nil, err
	}

	// Get segments meta with prefix = logs/<logName>/segments/<segmentId>
	segResp, err := e.client.Get(ctx, BuildSegmentInstancePath(logName, ""), clientv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}
	segmentsMetadata := make([]*proto.SegmentMetadata, len(segResp.Kvs))
	for _, kv := range segResp.Kvs {
		key := string(kv.Key)
		value := kv.Value

		segmentMeta := &proto.SegmentMetadata{}
		if err := pb.Unmarshal(value, segmentMeta); err != nil {
			log.Printf("failed to unmarshal segment metadata for key %s: %v", key, err)
			return nil, nil, err
		}
		segmentsMetadata = append(segmentsMetadata, segmentMeta)
	}

	//
	return logMeta, segmentsMetadata, nil
}
