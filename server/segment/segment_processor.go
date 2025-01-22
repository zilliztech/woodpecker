package segment

import (
	"context"
	"fmt"
	"github.com/milvus-io/woodpecker/server/storage"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
)

// SegmentProcessor for segment processing in server side
type SegmentProcessor interface {
	GetLogId() int64
	GetSegmentId() int64
	AddEntry(context.Context, *SegmentEntry) (int64, error)
	ReadEntry(context.Context, int64) (*SegmentEntry, error)
}

func NewSegmentProcessor(ctx context.Context, logId int64, segId int64, etcdCli *clientv3.Client) SegmentProcessor {
	log.Printf("new segment processor with logId: %d, segId: %d", logId, segId)
	return &segmentProcessor{
		logId:   logId,
		segId:   segId,
		etcdCli: etcdCli,
	}
}

var _ SegmentProcessor = (*segmentProcessor)(nil)

type segmentProcessor struct {
	logId   int64
	segId   int64
	etcdCli *clientv3.Client

	currentLogFileId     uint64
	currentLogFileWriter storage.LogFile

	minioClient *minio.Client
}

func (s *segmentProcessor) GetLogId() int64 {
	return s.logId
}

func (s *segmentProcessor) GetSegmentId() int64 {
	return s.segId
}

func (s *segmentProcessor) AddEntry(ctx context.Context, entry *SegmentEntry) (int64, error) {
	logFileWriter, err := s.getOrCreateLogFileWriter(ctx)
	if err != nil {
		return -1, err
	}
	// TODO
	err = logFileWriter.Append(ctx, entry.Data)
	if err != nil {
		return -1, err
	}
	// TODO
	return entry.EntryId, nil
}

func (s *segmentProcessor) ReadEntry(ctx context.Context, i int64) (*SegmentEntry, error) {
	//TODO implement me
	panic("implement me")
}

func (s *segmentProcessor) getOrCreateLogFileWriter(ctx context.Context) (storage.LogFile, error) {
	if s.currentLogFileWriter == nil {
		// get logfile id from meta/storage
		s.currentLogFileId = 0
		s.currentLogFileWriter = storage.NewObjectStorageLogFile(s.currentLogFileId, s.getSegmentKeyPrefix(), s.getInstanceBucket(), s.getMinioClient())
		log.Printf("createLogFileWriter with logId: %d, segId: %d", s.logId, s.segId)
	}
	return s.currentLogFileWriter, nil
}

// TODO move to common package for config
func (s *segmentProcessor) getInstanceBucket() string {
	// TODO get from instance id from meta
	return fmt.Sprintf("woodpecker")
}

func (s *segmentProcessor) getSegmentKeyPrefix() string {
	return fmt.Sprintf("%d/%d", s.logId, s.segId)
}

func (s *segmentProcessor) getMinioClient() *minio.Client {
	if s.minioClient == nil {
		newMinioCli, err := s.newMinioClient(context.Background())
		if err != nil {
			panic(err)
		}
		s.minioClient = newMinioCli
	}
	return s.minioClient
}

// TODO move to common package
func (s *segmentProcessor) newMinioClient(ctx context.Context) (*minio.Client, error) {
	var creds *credentials.Credentials
	creds = credentials.NewStaticV4("minioadmin", "minioadmin", "")
	minioClient, err := minio.New("localhost:9000", &minio.Options{
		Creds:  creds,
		Secure: false,
	})
	// options nil or invalid formatted endpoint, don't need to retry
	if err != nil {
		return nil, err
	}

	var bucketExists bool
	// check valid in first query
	checkBucketFn := func() error {
		bucketExists, err = minioClient.BucketExists(ctx, s.getInstanceBucket())
		if err != nil {
			return err
		}
		if !bucketExists {
			err := minioClient.MakeBucket(ctx, s.getInstanceBucket(), minio.MakeBucketOptions{})
			if err != nil {
				return err
			}
		}
		return nil
	}
	// check and create root bucket if not exists
	err = checkBucketFn()
	if err != nil {
		return nil, err
	}
	return minioClient, nil
}
