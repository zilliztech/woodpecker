package meta

import (
	"context"
	"fmt"
	"sort"
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
	t.Run("test update truncate logMeta", testUpdateLogMetaForTruncation)
	t.Run("test create reader temp info", testCreateReaderTempInfo)
	t.Run("test get reader temp info", testGetReaderTempInfo)
	t.Run("test get all reader temp info for log", testGetAllReaderTempInfoForLog)
	t.Run("test update reader temp info", testUpdateReaderTempInfo)
	t.Run("test delete reader temp info", testDeleteReaderTempInfo)
	t.Run("test create segment cleanup status", testCreateSegmentCleanupStatus)
	t.Run("test update segment cleanup status", testUpdateSegmentCleanupStatus)
	t.Run("test list segment cleanup status", testListSegmentCleanupStatus)
	t.Run("test failed segment cleanup status", testFailedCleanupStatus)
	t.Run("test delete segment cleanup status", testDeleteSegmentCleanupStatus)
	t.Run("test non existent segment cleanup status", testNonExistentStatus)
	t.Run("test empty list for non existent log", testEmptyListForNonExistentLog)
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

func testUpdateLogMetaForTruncation(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	assert.NoError(t, err)
	assert.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	assert.NoError(t, err)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli)
	err = provider.InitIfNecessary(context.Background())
	assert.NoError(t, err)

	// Create a test log
	logName := "truncate_test_log_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	// Get the initial log metadata
	initialLogMeta, err := provider.GetLogMeta(context.Background(), logName)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), initialLogMeta.TruncatedSegmentId)
	assert.Equal(t, int64(-1), initialLogMeta.TruncatedEntryId)

	// Update truncation point
	truncatedSegmentId := int64(5)
	truncatedEntryId := int64(100)
	initialLogMeta.TruncatedSegmentId = truncatedSegmentId
	initialLogMeta.TruncatedEntryId = truncatedEntryId
	initialLogMeta.ModificationTimestamp = uint64(time.Now().Unix())

	// Update the log metadata
	err = provider.UpdateLogMeta(context.Background(), logName, initialLogMeta)
	assert.NoError(t, err)

	// Get the updated log metadata
	updatedLogMeta, err := provider.GetLogMeta(context.Background(), logName)
	assert.NoError(t, err)

	// Verify truncation point is updated
	assert.Equal(t, truncatedSegmentId, updatedLogMeta.TruncatedSegmentId)
	assert.Equal(t, truncatedEntryId, updatedLogMeta.TruncatedEntryId)
	assert.GreaterOrEqual(t, updatedLogMeta.ModificationTimestamp, initialLogMeta.ModificationTimestamp)
}

func testCreateReaderTempInfo(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	assert.NoError(t, err)
	assert.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	assert.NoError(t, err)

	// Create etcd session with a short TTL for testing
	session, err := concurrency.NewSession(etcdCli, concurrency.WithTTL(3))
	assert.NoError(t, err)

	// Create a metadata provider with the session
	provider := &metadataProviderEtcd{
		client:         etcdCli,
		session:        session,
		logWriterLocks: make(map[string]*concurrency.Mutex),
	}

	err = provider.InitIfNecessary(context.Background())
	assert.NoError(t, err)

	// Create a test log
	logName := "reader_temp_info_test_log_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	// Get log metadata to get the logId
	logMeta, err := provider.GetLogMeta(context.Background(), logName)
	assert.NoError(t, err)

	// Create reader temp info
	readerName := "test-reader-" + time.Now().Format("20060102150405")
	segmentId := int64(3)
	entryId := int64(42)

	// Create the reader temp info - this will use the session's lease
	err = provider.CreateReaderTempInfo(context.Background(), readerName, logMeta.LogId, segmentId, entryId)
	assert.NoError(t, err)

	// Verify the reader temp info was created
	readerKey := BuildLogReaderTempInfoKey(logMeta.LogId, readerName)
	resp, err := etcdCli.Get(context.Background(), readerKey)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.Kvs), "Reader temp info should exist")

	// Decode the reader temp info
	readerInfo := &proto.ReaderTempInfo{}
	err = pb.Unmarshal(resp.Kvs[0].Value, readerInfo)
	assert.NoError(t, err)

	// Verify the reader temp info content
	assert.Equal(t, readerName, readerInfo.ReaderName)
	assert.Equal(t, logMeta.LogId, readerInfo.LogId)
	assert.Equal(t, segmentId, readerInfo.OpenSegmentId)
	assert.Equal(t, entryId, readerInfo.OpenEntryId)
	assert.Equal(t, segmentId, readerInfo.RecentReadSegmentId)
	assert.Equal(t, entryId, readerInfo.RecentReadEntryId)
	assert.Greater(t, readerInfo.OpenTimestamp, uint64(0))
	assert.Greater(t, readerInfo.RecentReadTimestamp, uint64(0))

	// Verify the key is attached to the session's lease
	leaseInfo, err := etcdCli.TimeToLive(context.Background(), clientv3.LeaseID(resp.Kvs[0].Lease))
	assert.NoError(t, err)
	assert.True(t, leaseInfo.TTL > 0, "Key should have a TTL")
	assert.True(t, leaseInfo.TTL <= 60, "TTL should be 60 seconds or less")

	t.Logf("Reader temp info is created with TTL of %d seconds", leaseInfo.TTL)

	// Close the etcd session to simulate reader disconnection
	t.Log("Closing the session to simulate reader disconnection...")
	err = session.Close()
	assert.NoError(t, err)

	// Wait for the lease to expire (a bit more than the TTL)
	t.Log("Waiting for session lease to expire...")
	time.Sleep(65 * time.Second)

	// Check if the key has been automatically removed after session ended
	checkResp, err := etcdCli.Get(context.Background(), readerKey)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(checkResp.Kvs), "Reader temp info should be automatically removed after session ends")

	t.Log("Reader temp info was automatically removed after session ended as expected")
}

func testGetReaderTempInfo(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	assert.NoError(t, err)
	assert.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	assert.NoError(t, err)

	// Create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli)
	err = provider.InitIfNecessary(context.Background())
	assert.NoError(t, err)

	// Create a test log
	logName := "get_reader_temp_info_test_log_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	// Get log metadata to get the logId
	logMeta, err := provider.GetLogMeta(context.Background(), logName)
	assert.NoError(t, err)

	// Create reader temp info
	readerName := "test-reader-" + time.Now().Format("20060102150405")
	segmentId := int64(3)
	entryId := int64(42)

	// Create the reader temp info
	err = provider.CreateReaderTempInfo(context.Background(), readerName, logMeta.LogId, segmentId, entryId)
	assert.NoError(t, err)

	// Get the reader temp info
	readerInfo, err := provider.GetReaderTempInfo(context.Background(), logMeta.LogId, readerName)
	assert.NoError(t, err)
	assert.NotNil(t, readerInfo)

	// Verify reader temp info content
	assert.Equal(t, readerName, readerInfo.ReaderName)
	assert.Equal(t, logMeta.LogId, readerInfo.LogId)
	assert.Equal(t, segmentId, readerInfo.OpenSegmentId)
	assert.Equal(t, entryId, readerInfo.OpenEntryId)
	assert.Equal(t, segmentId, readerInfo.RecentReadSegmentId)
	assert.Equal(t, entryId, readerInfo.RecentReadEntryId)
	assert.Greater(t, readerInfo.OpenTimestamp, uint64(0))
	assert.Greater(t, readerInfo.RecentReadTimestamp, uint64(0))

	// Test getting non-existent reader
	nonExistentReaderName := "non-existent-reader"
	_, err = provider.GetReaderTempInfo(context.Background(), logMeta.LogId, nonExistentReaderName)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reader temp info not found")
}

// Test updating reader temporary information
func testUpdateReaderTempInfo(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	assert.NoError(t, err)
	assert.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	assert.NoError(t, err)

	// Create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli)
	err = provider.InitIfNecessary(context.Background())
	assert.NoError(t, err)

	// Create a test log
	logName := "update_reader_temp_info_test_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	// Get log metadata to get the logId
	logMeta, err := provider.GetLogMeta(context.Background(), logName)
	assert.NoError(t, err)

	// Create reader temp info
	readerName := "test-reader-update-" + time.Now().Format("20060102150405")
	initialSegmentId := int64(3)
	initialEntryId := int64(42)

	// Create the reader temp info
	err = provider.CreateReaderTempInfo(context.Background(), readerName, logMeta.LogId, initialSegmentId, initialEntryId)
	assert.NoError(t, err)

	// Get the reader temp info
	initialReader, err := provider.GetReaderTempInfo(context.Background(), logMeta.LogId, readerName)
	assert.NoError(t, err)
	assert.NotNil(t, initialReader)
	assert.Equal(t, initialSegmentId, initialReader.RecentReadSegmentId)
	assert.Equal(t, initialEntryId, initialReader.RecentReadEntryId)

	// Update the read position
	updatedSegmentId := int64(4)
	updatedEntryId := int64(10)

	// Update reader temp info
	err = provider.UpdateReaderTempInfo(context.Background(), logMeta.LogId, readerName, updatedSegmentId, updatedEntryId)
	assert.NoError(t, err)

	// Get the updated reader temp info
	updatedReader, err := provider.GetReaderTempInfo(context.Background(), logMeta.LogId, readerName)
	assert.NoError(t, err)
	assert.NotNil(t, updatedReader)

	// Verify updated fields
	assert.Equal(t, updatedSegmentId, updatedReader.RecentReadSegmentId)
	assert.Equal(t, updatedEntryId, updatedReader.RecentReadEntryId)
	assert.Greater(t, updatedReader.RecentReadTimestamp, initialReader.RecentReadTimestamp)

	// Original initial positions should remain unchanged
	assert.Equal(t, initialSegmentId, updatedReader.OpenSegmentId)
	assert.Equal(t, initialEntryId, updatedReader.OpenEntryId)

	// Test updating non-existent reader
	err = provider.UpdateReaderTempInfo(context.Background(), logMeta.LogId, "non-existent-reader", 1, 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reader temp info not found")

	// Test updating with different logId
	err = provider.UpdateReaderTempInfo(context.Background(), 999, readerName, 1, 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reader temp info not found")

	// Verify lease persistence (reader temporary info should still exist with a TTL)
	readerKey := BuildLogReaderTempInfoKey(logMeta.LogId, readerName)
	resp, err := etcdCli.Get(context.Background(), readerKey)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.Kvs))

	// Verify the lease ID is non-zero
	assert.NotEqual(t, int64(0), resp.Kvs[0].Lease)

	// Verify we can get the TTL info
	leaseInfo, err := etcdCli.TimeToLive(context.Background(), clientv3.LeaseID(resp.Kvs[0].Lease))
	assert.NoError(t, err)
	assert.True(t, leaseInfo.TTL > 0, "Key should have a TTL")
}

// Test deleting reader temporary information
func testDeleteReaderTempInfo(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	assert.NoError(t, err)
	assert.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	assert.NoError(t, err)

	// Create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli)
	err = provider.InitIfNecessary(context.Background())
	assert.NoError(t, err)

	// Create a test log
	logName := "delete_reader_temp_info_test_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	// Get log metadata to get the logId
	logMeta, err := provider.GetLogMeta(context.Background(), logName)
	assert.NoError(t, err)

	// Create multiple reader temp infos
	readerCount := 3
	readerNames := make([]string, readerCount)
	segmentIds := []int64{1, 2, 3}
	entryIds := []int64{10, 20, 30}

	for i := 0; i < readerCount; i++ {
		readerNames[i] = fmt.Sprintf("test-reader-%d-%s", i, time.Now().Format("20060102150405"))
		err = provider.CreateReaderTempInfo(context.Background(), readerNames[i], logMeta.LogId, segmentIds[i], entryIds[i])
		assert.NoError(t, err)
	}

	// Verify all readers exist
	readers, err := provider.GetAllReaderTempInfoForLog(context.Background(), logMeta.LogId)
	assert.NoError(t, err)
	assert.Equal(t, readerCount, len(readers))

	// Test 1: Delete a specific reader
	err = provider.DeleteReaderTempInfo(context.Background(), logMeta.LogId, readerNames[1])
	assert.NoError(t, err)

	// Verify reader was deleted
	readers, err = provider.GetAllReaderTempInfoForLog(context.Background(), logMeta.LogId)
	assert.NoError(t, err)
	assert.Equal(t, readerCount-1, len(readers))

	// Try to get the deleted reader - should fail
	_, err = provider.GetReaderTempInfo(context.Background(), logMeta.LogId, readerNames[1])
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reader temp info not found")

	// Test 2: Delete non-existent reader - should not error
	err = provider.DeleteReaderTempInfo(context.Background(), logMeta.LogId, "non-existent-reader")
	assert.NoError(t, err)

	// Test 3: Delete with incorrect logId - should not error but won't delete anything
	err = provider.DeleteReaderTempInfo(context.Background(), 999, readerNames[0])
	assert.NoError(t, err)

	// Verify remaining reader still exists
	readers, err = provider.GetAllReaderTempInfoForLog(context.Background(), logMeta.LogId)
	assert.NoError(t, err)
	assert.Equal(t, readerCount-1, len(readers))

	// Delete last readers
	for i := 0; i < readerCount; i++ {
		if i != 1 { // Skip the one we already deleted
			err = provider.DeleteReaderTempInfo(context.Background(), logMeta.LogId, readerNames[i])
			assert.NoError(t, err)
		}
	}

	// Verify all readers are gone
	readers, err = provider.GetAllReaderTempInfoForLog(context.Background(), logMeta.LogId)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(readers))
}

func testGetAllReaderTempInfoForLog(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	assert.NoError(t, err)
	assert.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	assert.NoError(t, err)

	// Create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli)
	err = provider.InitIfNecessary(context.Background())
	assert.NoError(t, err)

	// Create a test log
	logName := "get_all_readers_temp_info_test_log_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	// Get log metadata to get the logId
	logMeta, err := provider.GetLogMeta(context.Background(), logName)
	assert.NoError(t, err)

	// Initially there should be no readers
	readers, err := provider.GetAllReaderTempInfoForLog(context.Background(), logMeta.LogId)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(readers))

	// Create multiple reader temp infos
	readerCount := 3
	readerNames := make([]string, readerCount)
	segmentIds := []int64{1, 2, 3}
	entryIds := []int64{10, 20, 30}

	for i := 0; i < readerCount; i++ {
		readerNames[i] = fmt.Sprintf("test-reader-%d-%s", i, time.Now().Format("20060102150405"))
		err = provider.CreateReaderTempInfo(context.Background(), readerNames[i], logMeta.LogId, segmentIds[i], entryIds[i])
		assert.NoError(t, err)
	}

	// Get all reader temp infos for the log
	allReaders, err := provider.GetAllReaderTempInfoForLog(context.Background(), logMeta.LogId)
	assert.NoError(t, err)
	assert.Equal(t, readerCount, len(allReaders))

	// Verify reader names match what we expected
	foundReaders := make(map[string]bool)
	for _, reader := range allReaders {
		foundReaders[reader.ReaderName] = true

		// Find the index of this reader in our original list
		var idx int
		for i, name := range readerNames {
			if name == reader.ReaderName {
				idx = i
				break
			}
		}

		// Verify reader info matches what we created
		assert.Equal(t, logMeta.LogId, reader.LogId)
		assert.Equal(t, segmentIds[idx], reader.OpenSegmentId)
		assert.Equal(t, entryIds[idx], reader.OpenEntryId)
		assert.Equal(t, segmentIds[idx], reader.RecentReadSegmentId)
		assert.Equal(t, entryIds[idx], reader.RecentReadEntryId)
	}

	// Verify all readers we created are found
	for _, name := range readerNames {
		assert.True(t, foundReaders[name], "Reader %s was not found", name)
	}

	// Test getting readers for non-existent log
	nonExistentLogId := int64(9999)
	nonExistentReaders, err := provider.GetAllReaderTempInfoForLog(context.Background(), nonExistentLogId)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(nonExistentReaders))
}

// Helper function to create a test log and return provider, logName, and logId
func setupSegmentCleanupTest(t *testing.T) (MetadataProvider, string, int64) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	assert.NoError(t, err)
	assert.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	assert.NoError(t, err)

	// Create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli)
	err = provider.InitIfNecessary(context.Background())
	assert.NoError(t, err)

	// Create a test log
	logName := "segment_cleanup_test_log_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	// Get log metadata to get the logId
	logMeta, err := provider.GetLogMeta(context.Background(), logName)
	assert.NoError(t, err)

	return provider, logName, logMeta.LogId
}

// Test creating segment cleanup status
func testCreateSegmentCleanupStatus(t *testing.T) {
	provider, _, logId := setupSegmentCleanupTest(t)

	segmentId := int64(1)
	nowMs := uint64(time.Now().UnixMilli())

	// Create cleanup status
	status := &proto.SegmentCleanupStatus{
		LogId:          logId,
		SegmentId:      segmentId,
		State:          proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		StartTime:      nowMs,
		LastUpdateTime: nowMs,
		QuorumCleanupStatus: map[string]bool{
			"node1": false,
			"node2": false,
		},
		ErrorMessage: "test error",
	}

	err := provider.CreateSegmentCleanupStatus(context.Background(), status)
	assert.NoError(t, err)

	// Verify that the status was created
	storedStatus, err := provider.GetSegmentCleanupStatus(context.Background(), logId, segmentId)
	assert.NoError(t, err)
	assert.NotNil(t, storedStatus)
	assert.Equal(t, logId, storedStatus.LogId)
	assert.Equal(t, segmentId, storedStatus.SegmentId)
	assert.Equal(t, proto.SegmentCleanupState_CLEANUP_IN_PROGRESS, storedStatus.State)
	assert.Equal(t, nowMs, storedStatus.StartTime)
	assert.Equal(t, nowMs, storedStatus.LastUpdateTime)
	assert.Equal(t, 2, len(storedStatus.QuorumCleanupStatus))
	assert.False(t, storedStatus.QuorumCleanupStatus["node1"])
	assert.False(t, storedStatus.QuorumCleanupStatus["node2"])
	assert.Equal(t, "test error", storedStatus.ErrorMessage)
}

// Test updating segment cleanup status
func testUpdateSegmentCleanupStatus(t *testing.T) {
	provider, _, logId := setupSegmentCleanupTest(t)

	segmentId := int64(1)
	nowMs := uint64(time.Now().UnixMilli())

	// Create initial status
	status := &proto.SegmentCleanupStatus{
		LogId:          logId,
		SegmentId:      segmentId,
		State:          proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		StartTime:      nowMs,
		LastUpdateTime: nowMs,
		QuorumCleanupStatus: map[string]bool{
			"node1": false,
			"node2": false,
		},
		ErrorMessage: "",
	}

	err := provider.CreateSegmentCleanupStatus(context.Background(), status)
	assert.NoError(t, err)

	// Update status
	status.State = proto.SegmentCleanupState_CLEANUP_COMPLETED
	status.LastUpdateTime = uint64(time.Now().UnixMilli())
	status.QuorumCleanupStatus["node1"] = true
	status.QuorumCleanupStatus["node2"] = true

	err = provider.UpdateSegmentCleanupStatus(context.Background(), status)
	assert.NoError(t, err)

	// Verify update was successful
	updatedStatus, err := provider.GetSegmentCleanupStatus(context.Background(), logId, segmentId)
	assert.NoError(t, err)
	assert.NotNil(t, updatedStatus)
	assert.Equal(t, proto.SegmentCleanupState_CLEANUP_COMPLETED, updatedStatus.State)
	assert.True(t, updatedStatus.QuorumCleanupStatus["node1"])
	assert.True(t, updatedStatus.QuorumCleanupStatus["node2"])
}

// Test listing segment cleanup statuses
func testListSegmentCleanupStatus(t *testing.T) {
	provider, _, logId := setupSegmentCleanupTest(t)

	// Create cleanup status for first segment
	segmentId1 := int64(1)
	nowMs := uint64(time.Now().UnixMilli())

	status1 := &proto.SegmentCleanupStatus{
		LogId:          logId,
		SegmentId:      segmentId1,
		State:          proto.SegmentCleanupState_CLEANUP_COMPLETED,
		StartTime:      nowMs,
		LastUpdateTime: nowMs,
		QuorumCleanupStatus: map[string]bool{
			"node1": true,
			"node2": true,
		},
		ErrorMessage: "",
	}

	err := provider.CreateSegmentCleanupStatus(context.Background(), status1)
	assert.NoError(t, err)

	// Create cleanup status for a second segment
	segmentId2 := int64(2)

	status2 := &proto.SegmentCleanupStatus{
		LogId:          logId,
		SegmentId:      segmentId2,
		State:          proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		StartTime:      nowMs,
		LastUpdateTime: nowMs,
		QuorumCleanupStatus: map[string]bool{
			"node1": false,
			"node3": false,
		},
		ErrorMessage: "",
	}

	err = provider.CreateSegmentCleanupStatus(context.Background(), status2)
	assert.NoError(t, err)

	// List all cleanup statuses for the log
	statuses, err := provider.ListSegmentCleanupStatus(context.Background(), logId)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(statuses))

	// Sort statuses by segment ID to ensure consistent order
	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].SegmentId < statuses[j].SegmentId
	})

	// Verify first status
	assert.Equal(t, segmentId1, statuses[0].SegmentId)
	assert.Equal(t, proto.SegmentCleanupState_CLEANUP_COMPLETED, statuses[0].State)

	// Verify second status
	assert.Equal(t, segmentId2, statuses[1].SegmentId)
	assert.Equal(t, proto.SegmentCleanupState_CLEANUP_IN_PROGRESS, statuses[1].State)
}

// Test failed cleanup status
func testFailedCleanupStatus(t *testing.T) {
	provider, _, logId := setupSegmentCleanupTest(t)

	segmentId := int64(3)
	nowMs := uint64(time.Now().UnixMilli())

	status := &proto.SegmentCleanupStatus{
		LogId:          logId,
		SegmentId:      segmentId,
		State:          proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		StartTime:      nowMs,
		LastUpdateTime: nowMs,
		QuorumCleanupStatus: map[string]bool{
			"node1": false,
			"node2": false,
		},
		ErrorMessage: "",
	}

	err := provider.CreateSegmentCleanupStatus(context.Background(), status)
	assert.NoError(t, err)

	// Update to failed state
	status.State = proto.SegmentCleanupState_CLEANUP_FAILED
	status.LastUpdateTime = uint64(time.Now().UnixMilli())
	status.ErrorMessage = "Nodes unavailable during cleanup"

	err = provider.UpdateSegmentCleanupStatus(context.Background(), status)
	assert.NoError(t, err)

	// Verify failed status
	failedStatus, err := provider.GetSegmentCleanupStatus(context.Background(), logId, segmentId)
	assert.NoError(t, err)
	assert.NotNil(t, failedStatus)
	assert.Equal(t, proto.SegmentCleanupState_CLEANUP_FAILED, failedStatus.State)
	assert.Equal(t, "Nodes unavailable during cleanup", failedStatus.ErrorMessage)
}

// Test deleting segment cleanup status
func testDeleteSegmentCleanupStatus(t *testing.T) {
	provider, _, logId := setupSegmentCleanupTest(t)

	segmentId := int64(1)
	nowMs := uint64(time.Now().UnixMilli())

	// Create initial status
	status := &proto.SegmentCleanupStatus{
		LogId:          logId,
		SegmentId:      segmentId,
		State:          proto.SegmentCleanupState_CLEANUP_COMPLETED,
		StartTime:      nowMs,
		LastUpdateTime: nowMs,
		QuorumCleanupStatus: map[string]bool{
			"node1": true,
			"node2": true,
		},
		ErrorMessage: "",
	}

	err := provider.CreateSegmentCleanupStatus(context.Background(), status)
	assert.NoError(t, err)

	// Create a second status to verify list count later
	segmentId2 := int64(2)
	status2 := &proto.SegmentCleanupStatus{
		LogId:          logId,
		SegmentId:      segmentId2,
		State:          proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		StartTime:      nowMs,
		LastUpdateTime: nowMs,
		QuorumCleanupStatus: map[string]bool{
			"node1": false,
		},
		ErrorMessage: "",
	}

	err = provider.CreateSegmentCleanupStatus(context.Background(), status2)
	assert.NoError(t, err)

	// Delete the first status
	err = provider.DeleteSegmentCleanupStatus(context.Background(), logId, segmentId)
	assert.NoError(t, err)

	// Verify deletion
	status1, err := provider.GetSegmentCleanupStatus(context.Background(), logId, segmentId)
	assert.NoError(t, err)
	assert.Nil(t, status1)

	// List statuses again to verify count decreased
	statuses, err := provider.ListSegmentCleanupStatus(context.Background(), logId)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(statuses))
	assert.Equal(t, segmentId2, statuses[0].SegmentId)
}

// Test handling of non-existent cleanup status
func testNonExistentStatus(t *testing.T) {
	provider, _, logId := setupSegmentCleanupTest(t)

	nonExistentSegmentId := int64(999)

	// Try to get non-existent status
	notExistsStatus, err := provider.GetSegmentCleanupStatus(context.Background(), logId, nonExistentSegmentId)
	assert.NoError(t, err)
	assert.Nil(t, notExistsStatus, "segment cleanup status should be nil")

	// Try to delete non-existent status (should not error)
	err = provider.DeleteSegmentCleanupStatus(context.Background(), logId, nonExistentSegmentId)
	assert.NoError(t, err)
}

// Test empty list for non-existent log
func testEmptyListForNonExistentLog(t *testing.T) {
	provider, _, _ := setupSegmentCleanupTest(t)

	nonExistentLogId := int64(9999)

	statuses, err := provider.ListSegmentCleanupStatus(context.Background(), nonExistentLogId)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(statuses))
}
