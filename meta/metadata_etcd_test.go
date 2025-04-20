package meta

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/proto"
)

func TestAll(t *testing.T) {
	err := etcd.InitEtcdServer(true, "", "/tmp/testMetadata", "/tmp/testMetadata.log", "info")
	assert.NoError(t, err)
	defer etcd.StopEtcdServer()
	t.Run("test meta init", testInitIfNecessary)
	t.Run("test create log and open", testCreateLogAndOpen)
	t.Run("test check log exists", testCheckExists)
	t.Run("test store quorum", testStoreQuorumInfo)
	t.Run("test store segment meta", testStoreSegmentMeta)
	t.Run("test logWrite lock", testLogWriterLock)
	t.Run("test update segment meta", testUpdateSegmentMeta)
	// TODO add update truncate logMeta test
	// TODO add create reader temp info test
}

func testInitIfNecessary(t *testing.T) {
	// get etcd client
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	assert.NoError(t, err)
	assert.NotNil(t, etcdCli)

	// clear metadata first
	deleteResp, err := etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
	assert.NoError(t, err)
	if len(deleteResp.PrevKvs) > 0 {
		t.Logf("clear metadata success, following keys have bean deleted")
		for _, kv := range deleteResp.PrevKvs {
			t.Logf("%s %s", string(kv.Key), string(kv.Value))
		}
	}

	// Init metadata
	metadataProvider := NewMetadataProvider(context.Background(), etcdCli)
	initErr := metadataProvider.InitIfNecessary(context.Background())
	assert.NoError(t, initErr)

	// check init
	{
		getResp, getErr := etcdCli.Get(context.Background(), ServiceInstanceKey)
		assert.NoErrorf(t, getErr, "get %s failed:", ServiceInstanceKey)
		assert.Equalf(t, 1, len(getResp.Kvs), "expected 1 kv，but got %d ", len(getResp.Kvs))
	}
	expectedVersion := &proto.Version{
		Major: VersionMajor,
		Minor: VersionMinor,
		Patch: VersionPatch,
	}
	expectedVersionData, _ := pb.Marshal(expectedVersion)
	expectedVersionStr := string(expectedVersionData)
	{
		getResp, getErr := etcdCli.Get(context.Background(), VersionKey)
		assert.NoErrorf(t, getErr, "get %s failed:", VersionKey)
		assert.Equalf(t, 1, len(getResp.Kvs), "expected 1 kv，but got %d ", len(getResp.Kvs))
		assert.Equalf(t, expectedVersionStr, string(getResp.Kvs[0].Value), "expected %s but got %s ", expectedVersionStr, string(getResp.Kvs[0].Value))
	}
	{
		actualVersion, getVersionErr := metadataProvider.GetVersionInfo(context.Background())
		actualVersionData, _ := pb.Marshal(actualVersion)
		assert.NoError(t, getVersionErr)
		assert.Equal(t, expectedVersionStr, string(actualVersionData))
	}
	{
		getResp, getErr := etcdCli.Get(context.Background(), LogIdGeneratorKey)
		assert.NoErrorf(t, getErr, "get %s failed:", LogIdGeneratorKey)
		assert.Equalf(t, 1, len(getResp.Kvs), "expected 1 kv，but got %d ", len(getResp.Kvs))
		assert.Equal(t, "0", string(getResp.Kvs[0].Value), "get %s value does not equal to '0'", LogIdGeneratorKey)
	}
	{
		getResp, getErr := etcdCli.Get(context.Background(), QuorumIdGeneratorKey)
		assert.NoErrorf(t, getErr, "get %s failed:", QuorumIdGeneratorKey)
		assert.Equalf(t, 1, len(getResp.Kvs), "expected 1 kv，but got %d ", len(getResp.Kvs))
		assert.Equalf(t, "0", string(getResp.Kvs[0].Value), "get %s value does not equal to '0'", QuorumIdGeneratorKey)
	}

	t.Logf("clear finished")
}

// TestCreateLog tests the CreateLog method of metadataProviderEtcd
func testCreateLogAndOpen(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	assert.NoError(t, err)
	assert.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	assert.NoError(t, err)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli)
	err = provider.InitIfNecessary(context.Background())
	assert.NoError(t, err)

	// create log
	logName := "test_log" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	// check idgen update to 1
	idGenResp, err := etcdCli.Get(context.Background(), LogIdGeneratorKey)
	require.NoError(t, err, "get LogIdGeneratorKey failed")
	assert.Equal(t, "1", string(idGenResp.Kvs[0].Value))

	// check test_log exists
	{
		logPath := BuildLogKey(logName)
		logResp, err := etcdCli.Get(context.Background(), logPath)
		require.NoError(t, err, "get log failed")
		require.Len(t, logResp.Kvs, 1)
		// deserialize LogMeta
		logMeta := &proto.LogMeta{}
		err = pb.Unmarshal(logResp.Kvs[0].Value, logMeta)
		require.NoError(t, err, "deserialize LogMeta failed")
		// check LogMeta content
		assert.Equal(t, int64(1), logMeta.LogId, "Unexpected LogId")
		assert.Equal(t, int64(60), logMeta.MaxSegmentRollTimeSeconds, "Unexpected MaxSegmentRollTimeSeconds")
		assert.Equal(t, int64(1024*1024*1024), logMeta.MaxSegmentRollSizeBytes, "Unexpected MaxSegmentRollSizeBytes")
		assert.Equal(t, int64(128*1024*1024), logMeta.CompactionBufferSizeBytes, "Unexpected CompactionBufferSizeBytes")
		assert.Equal(t, int64(600), logMeta.MaxCompactionFileCount, "Unexpected MaxCompactionFileCount")
		assert.Greater(t, logMeta.CreationTimestamp, uint64(0), "Unexpected CreationTimestamp")
		assert.Greater(t, logMeta.ModificationTimestamp, uint64(0), "Unexpected ModificationTimestamp")
	}

	// create again, expect already error
	{
		err = provider.CreateLog(context.Background(), logName)
		require.Error(t, err, "expected error because log already exists")
	}

	// test open log
	{
		logMeta, segMetas, err := provider.OpenLog(context.Background(), logName)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), logMeta.LogId, "Unexpected LogId")
		assert.Equal(t, int64(60), logMeta.MaxSegmentRollTimeSeconds, "Unexpected MaxSegmentRollTimeSeconds")
		assert.Equal(t, int64(1024*1024*1024), logMeta.MaxSegmentRollSizeBytes, "Unexpected MaxSegmentRollSizeBytes")
		assert.Equal(t, int64(128*1024*1024), logMeta.CompactionBufferSizeBytes, "Unexpected CompactionBufferSizeBytes")
		assert.NotNil(t, segMetas, "Unexpected nil segMetas")
		assert.Equal(t, 0, len(segMetas), "Unexpected segMetas length")
	}

	// test check exists
	{
		exists, err := provider.CheckExists(context.Background(), logName)
		assert.NoError(t, err)
		assert.True(t, exists)
	}

	// test list logs
	{
		printDirContents(t, context.Background(), etcdCli, ServicePrefix, "")
		logNames, err := provider.ListLogs(context.Background())
		assert.NoError(t, err)
		assert.Equalf(t, 1, len(logNames), "%v", logNames)
		assert.Equal(t, logName, logNames[0])
	}

	// test list log with prefix
	{
		printDirContents(t, context.Background(), etcdCli, ServicePrefix, "")
		logNames, err := provider.ListLogsWithPrefix(context.Background(), "test_log")
		assert.NoError(t, err)
		assert.Equal(t, 1, len(logNames))
		assert.Equal(t, logName, logNames[0])
	}
}

func testCheckExists(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	assert.NoError(t, err)
	assert.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	assert.NoError(t, err)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli)
	err = provider.InitIfNecessary(context.Background())
	assert.NoError(t, err)

	// create log
	logName := "test_log_11"
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	// check idgen update to 1
	idGenResp, err := etcdCli.Get(context.Background(), LogIdGeneratorKey)
	require.NoError(t, err, "get LogIdGeneratorKey failed")
	assert.Equal(t, "1", string(idGenResp.Kvs[0].Value))

	{
		exists, err := provider.CheckExists(context.Background(), "test_log_11")
		assert.NoError(t, err)
		assert.True(t, exists)
	}

	{
		exists, err := provider.CheckExists(context.Background(), "test_log_1")
		assert.NoError(t, err)
		assert.False(t, exists)
	}

	{
		exists, err := provider.CheckExists(context.Background(), "test_log_110")
		assert.NoError(t, err)
		assert.False(t, exists)
	}
}

// printDirContents show all exists keys
func printDirContents(t *testing.T, ctx context.Context, cli *clientv3.Client, prefix string, indent string) {
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("fatal %v", err)
	}

	for _, kv := range resp.Kvs {
		fmt.Printf("%s%s: %s\n", indent, string(kv.Key), string(kv.Value))

		if strings.HasSuffix(string(kv.Key), "/") {
			newPrefix := string(kv.Key)
			printDirContents(t, ctx, cli, newPrefix, indent+"  ")
		}
	}
}

// testStoreQuorumInfo tests the StoreQuorumInfo & GetQuorumInfo methods
func testStoreQuorumInfo(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	assert.NoError(t, err)
	assert.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	assert.NoError(t, err)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli)
	err = provider.InitIfNecessary(context.Background())
	assert.NoError(t, err)

	// store quorumInfo
	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Es:    2,
		Wq:    2,
		Aq:    2,
		Nodes: []string{"node1_address", "node2_address"},
	}
	storeErr := provider.StoreQuorumInfo(context.Background(), quorumInfo)
	assert.NoError(t, storeErr)
	getQuorumInfo, getErr := provider.GetQuorumInfo(context.Background(), 1)
	assert.NoError(t, getErr)
	assert.NotNil(t, getQuorumInfo)
	assert.Equal(t, quorumInfo.Id, getQuorumInfo.Id)
	assert.Equal(t, quorumInfo.Es, getQuorumInfo.Es)
	assert.Equal(t, quorumInfo.Wq, getQuorumInfo.Wq)
	assert.Equal(t, quorumInfo.Aq, getQuorumInfo.Aq)
	assert.Equal(t, quorumInfo.Nodes, getQuorumInfo.Nodes)
}

// testStoreSegmentMeta tests the StoreSegmentMeta & GetSegmentMeta methods
func testStoreSegmentMeta(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	assert.NoError(t, err)
	assert.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	assert.NoError(t, err)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli)
	err = provider.InitIfNecessary(context.Background())
	assert.NoError(t, err)

	logName := "test_log" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	// store segment meta
	segmentMeta := &proto.SegmentMetadata{
		SegNo:       1,
		EntryOffset: []int32{1, 2, 3, 4, 5, 6},
	}
	storeErr := provider.StoreSegmentMetadata(context.Background(), logName, segmentMeta)
	assert.NoError(t, storeErr)

	// test get segmentMeta
	{
		getSegmentMeta, getErr := provider.GetSegmentMetadata(context.Background(), logName, 1)
		assert.NoError(t, getErr)
		assert.NotNil(t, getSegmentMeta)
		assert.Equal(t, segmentMeta.SegNo, getSegmentMeta.SegNo)
		assert.Equal(t, segmentMeta.EntryOffset, getSegmentMeta.EntryOffset)
	}

	// test get all segmentMetas of the log
	{
		segmentMetaList, listErr := provider.GetAllSegmentMetadata(context.Background(), logName)
		assert.NoError(t, listErr)
		assert.Equal(t, 1, len(segmentMetaList))
		assert.Equal(t, segmentMeta.SegNo, segmentMetaList[segmentMeta.SegNo].SegNo)
		assert.Equal(t, segmentMeta.EntryOffset, segmentMetaList[segmentMeta.SegNo].EntryOffset)
	}
}

// testUpdateSegmentMeta tests the UpdateSegmentMeta
func testUpdateSegmentMeta(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	assert.NoError(t, err)
	assert.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	assert.NoError(t, err)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli)
	err = provider.InitIfNecessary(context.Background())
	assert.NoError(t, err)

	logName := "test_log" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	// test store
	segmentMeta := &proto.SegmentMetadata{
		SegNo: 1,
		State: proto.SegmentState_Active,
	}
	storeErr := provider.StoreSegmentMetadata(context.Background(), logName, segmentMeta)
	assert.NoError(t, storeErr)
	// test get segmentMeta
	{
		getSegmentMeta, getErr := provider.GetSegmentMetadata(context.Background(), logName, 1)
		assert.NoError(t, getErr)
		assert.NotNil(t, getSegmentMeta)
		assert.Equal(t, segmentMeta.SegNo, getSegmentMeta.SegNo)
		assert.Equal(t, segmentMeta.State, getSegmentMeta.State)
		assert.Empty(t, getSegmentMeta.EntryOffset)
	}

	// test update
	segmentMeta.EntryOffset = []int32{1, 2, 3, 4, 5, 6, 7, 8, 9}
	segmentMeta.State = proto.SegmentState_Sealed
	updateErr := provider.UpdateSegmentMetadata(context.Background(), logName, segmentMeta)
	assert.NoError(t, updateErr)
	{
		getSegmentMeta, getErr := provider.GetSegmentMetadata(context.Background(), logName, 1)
		assert.NoError(t, getErr)
		assert.NotNil(t, getSegmentMeta)
		assert.Equal(t, segmentMeta.SegNo, getSegmentMeta.SegNo)
		assert.Equal(t, segmentMeta.EntryOffset, getSegmentMeta.EntryOffset)
		assert.Equal(t, segmentMeta.State, getSegmentMeta.State)
	}
}

func testLogWriterLock(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	assert.NoError(t, err)
	assert.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	assert.NoError(t, err)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli)
	defer provider.Close()
	err = provider.InitIfNecessary(context.Background())
	assert.NoError(t, err)
	logName := "test_log" + time.Now().Format("20060102150405")

	// lock success
	getLockErr := provider.AcquireLogWriterLock(context.Background(), logName)
	assert.NoError(t, getLockErr)

	// reentrant lock success
	getLockErr = provider.AcquireLogWriterLock(context.Background(), logName)
	assert.NoError(t, getLockErr)

	// test lock fail from another session
	{
		newSession, newSessionErr := concurrency.NewSession(etcdCli, concurrency.WithTTL(5))
		assert.NoError(t, newSessionErr)
		lockKey := BuildLogLockKey(logName)
		mutex1 := concurrency.NewMutex(newSession, lockKey)
		lockErr := mutex1.TryLock(context.Background())
		assert.Error(t, lockErr)
		assert.ErrorContainsf(t, lockErr, "Locked by another session", "unexpected error: %s", lockErr.Error())
		newSession.Close()
	}

	// release lock
	releaseLockErr := provider.ReleaseLogWriterLock(context.Background(), logName)
	assert.NoError(t, releaseLockErr)

	// test lock success from another session after release
	{
		newSession, newSessionErr := concurrency.NewSession(etcdCli, concurrency.WithTTL(5))
		assert.NoError(t, newSessionErr)
		lockKey := BuildLogLockKey(logName)
		mutex1 := concurrency.NewMutex(newSession, lockKey)
		lockErr := mutex1.TryLock(context.Background())
		assert.NoError(t, lockErr)
		newSession.Close()
	}
}
