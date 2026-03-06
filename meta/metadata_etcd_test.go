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
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc/metadata"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

func testMetaCfg(t *testing.T) *config.Configuration {
	cfg, err := config.NewConfiguration()
	require.NoError(t, err, "failed to create test configuration")
	return cfg
}

func TestAll(t *testing.T) {
	tmpDir := t.TempDir()
	err := etcd.InitEtcdServer(true, "", tmpDir, tmpDir+"/etcd.log", "info")
	require.NoError(t, err)
	defer etcd.StopEtcdServer()
	t.Run("test init if necessary", testInitIfNecessary)
	t.Run("test create log and open", testCreateLogAndOpen)
	t.Run("test check exists", testCheckExists)
	t.Run("test store quorum info", testStoreQuorumInfo)
	t.Run("test store segment meta", testStoreSegmentMeta)
	t.Run("test update segment meta", testUpdateSegmentMeta)
	t.Run("test delete segment meta", testDeleteSegmentMeta)
	t.Run("test log writer lock", testLogWriterLock)
	t.Run("test update log meta for truncation", testUpdateLogMetaForTruncation)
	t.Run("test create reader temp info", testCreateReaderTempInfo)
	t.Run("test get reader temp info", testGetReaderTempInfo)
	t.Run("test update reader temp info", testUpdateReaderTempInfo)
	t.Run("test get all reader temp info for log", testGetAllReaderTempInfoForLog)
	t.Run("test delete reader temp info", testDeleteReaderTempInfo)
	t.Run("test create segment cleanup status", testCreateSegmentCleanupStatus)
	t.Run("test update segment cleanup status", testUpdateSegmentCleanupStatus)
	t.Run("test list segment cleanup status", testListSegmentCleanupStatus)
	t.Run("test failed cleanup status", testFailedCleanupStatus)
	t.Run("test delete segment cleanup status", testDeleteSegmentCleanupStatus)
	t.Run("test non existent segment cleanup status", testNonExistentStatus)
	t.Run("test empty list for non existent log", testEmptyListForNonExistentLog)
	t.Run("test update log meta with wrong revision", testUpdateLogMetaWithWrongRevision)
	t.Run("test update segment meta with wrong revision", testUpdateSegmentMetaWithWrongRevision)
	t.Run("test store or get condition write result", testStoreOrGetConditionWriteResult)
	t.Run("test get condition write result", testGetConditionWriteResult)
	t.Run("test session lock with etcd", testSessionLockWithEtcd)
	t.Run("test check session lock alive nil", testCheckSessionLockAliveNil)
	t.Run("test close empty provider", testCloseEmptyProvider)
	t.Run("test get log meta not found", testGetLogMetaNotFound)
	t.Run("test close with active lock", testCloseWithActiveLock)
	t.Run("test release non existent lock", testReleaseNonExistentLock)
	t.Run("test init already initialized", testInitAlreadyInitialized)
	t.Run("test init partial init", testInitPartialInit)
	t.Run("test get version info not found", testGetVersionInfoNotFound)
	t.Run("test get quorum info not found", testGetQuorumInfoNotFound)
	t.Run("test store quorum info already exists", testStoreQuorumInfoAlreadyExists)
	t.Run("test open log not found", testOpenLogNotFound)
	t.Run("test list logs with prefix no match", testListLogsWithPrefixNoMatch)
	t.Run("test new metadata provider default timeout", testNewMetadataProviderDefaultTimeout)
	t.Run("test get context with timeout grpc metadata", testGetContextWithTimeoutGRPCMetadata)
	t.Run("test store segment meta already exists", testStoreSegmentMetaAlreadyExists)
	t.Run("test create cleanup status already exists", testCreateCleanupStatusAlreadyExists)
	t.Run("test update cleanup status not exists", testUpdateCleanupStatusNotExists)
	t.Run("test check segment not exists", testCheckSegmentNotExists)
	t.Run("test cancelled context etcd errors", testCancelledContextEtcdErrors)
	t.Run("test corrupted protobuf data", testCorruptedProtobufData)
	t.Run("test create log edge cases", testCreateLogEdgeCases)
	t.Run("test acquire lock edge cases", testAcquireLockEdgeCases)
	t.Run("test update reader temp info without lease", testUpdateReaderTempInfoWithoutLease)
	t.Run("test close with nil lock entry", testCloseWithNilLockEntry)
	t.Run("test open log with corrupted segment data", testOpenLogWithCorruptedSegmentData)
}

func testInitIfNecessary(t *testing.T) {
	// get etcd client
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	require.NotNil(t, etcdCli)

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
	metadataProvider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	initErr := metadataProvider.InitIfNecessary(context.Background())
	require.NoError(t, initErr)

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
	require.NoError(t, err)
	require.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

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
		assert.Equal(t, int64(256*1024*1024), logMeta.MaxSegmentRollSizeBytes, "Unexpected MaxSegmentRollSizeBytes")
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
		assert.Equal(t, int64(1), logMeta.Metadata.LogId, "Unexpected LogId")
		assert.Equal(t, int64(60), logMeta.Metadata.MaxSegmentRollTimeSeconds, "Unexpected MaxSegmentRollTimeSeconds")
		assert.Equal(t, int64(256*1024*1024), logMeta.Metadata.MaxSegmentRollSizeBytes, "Unexpected MaxSegmentRollSizeBytes")
		assert.Equal(t, int64(128*1024*1024), logMeta.Metadata.CompactionBufferSizeBytes, "Unexpected CompactionBufferSizeBytes")
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
	require.NoError(t, err)
	require.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

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
		t.Logf("%s%s: %s\n", indent, string(kv.Key), string(kv.Value))

		if strings.HasSuffix(string(kv.Key), "/") {
			newPrefix := string(kv.Key)
			printDirContents(t, ctx, cli, newPrefix, indent+"  ")
		}
	}
}

// testStoreQuorumInfo tests the StoreQuorumInfo & GetQuorumInfo methods
func testStoreQuorumInfo(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	require.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

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
	require.NoError(t, err)
	require.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	logName := "test_log" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	// store segment meta
	segmentMeta := &SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1,
		},
	}
	storeErr := provider.StoreSegmentMetadata(context.Background(), logName, int64(1), segmentMeta)
	assert.NoError(t, storeErr)

	// test get segmentMeta
	{
		getSegmentMeta, getErr := provider.GetSegmentMetadata(context.Background(), logName, 1)
		assert.NoError(t, getErr)
		assert.NotNil(t, getSegmentMeta)
		assert.Equal(t, segmentMeta.Metadata.SegNo, getSegmentMeta.Metadata.SegNo)
	}

	// test get all segmentMetas of the log
	{
		segmentMetaList, listErr := provider.GetAllSegmentMetadata(context.Background(), logName)
		assert.NoError(t, listErr)
		assert.Equal(t, 1, len(segmentMetaList))
		assert.Equal(t, segmentMeta.Metadata.SegNo, segmentMetaList[segmentMeta.Metadata.SegNo].Metadata.SegNo)
	}
}

// testUpdateSegmentMeta tests the UpdateSegmentMeta
func testUpdateSegmentMeta(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	require.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	logName := "test_log" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	// test store
	segmentMeta := &SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1,
			State: proto.SegmentState_Active,
		},
	}
	storeErr := provider.StoreSegmentMetadata(context.Background(), logName, int64(1), segmentMeta)
	assert.NoError(t, storeErr)
	// test get segmentMeta
	{
		getSegmentMeta, getErr := provider.GetSegmentMetadata(context.Background(), logName, 1)
		assert.NoError(t, getErr)
		assert.NotNil(t, getSegmentMeta)
		assert.Equal(t, segmentMeta.Metadata.SegNo, getSegmentMeta.Metadata.SegNo)
		assert.Equal(t, segmentMeta.Metadata.State, getSegmentMeta.Metadata.State)
	}

	// test update
	// Get the current segment meta to get the revision
	currentSegmentMeta, getErr := provider.GetSegmentMetadata(context.Background(), logName, 1)
	assert.NoError(t, getErr)

	// Update the state and use the current revision
	currentSegmentMeta.Metadata.State = proto.SegmentState_Sealed
	updateErr := provider.UpdateSegmentMetadata(context.Background(), logName, int64(1), currentSegmentMeta, proto.SegmentState_Active)
	assert.NoError(t, updateErr)
	{
		getSegmentMeta, getErr := provider.GetSegmentMetadata(context.Background(), logName, 1)
		assert.NoError(t, getErr)
		assert.NotNil(t, getSegmentMeta)
		assert.Equal(t, currentSegmentMeta.Metadata.SegNo, getSegmentMeta.Metadata.SegNo)
		assert.Equal(t, currentSegmentMeta.Metadata.State, getSegmentMeta.Metadata.State)
	}
}

// testDeleteSegmentMeta tests the DeleteSegmentMetadata method
func testDeleteSegmentMeta(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	require.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	logName := "test_log" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	// Create segment metadata
	segmentMeta := &SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1,
			State: proto.SegmentState_Active,
		},
	}

	storeErr := provider.StoreSegmentMetadata(context.Background(), logName, int64(1), segmentMeta)
	assert.NoError(t, storeErr)

	// Verify segment exists
	exists, err := provider.CheckSegmentExists(context.Background(), logName, segmentMeta.Metadata.SegNo)
	assert.NoError(t, err)
	assert.True(t, exists)

	// Delete the segment
	deleteErr := provider.DeleteSegmentMetadata(context.Background(), logName, int64(1), segmentMeta.Metadata.SegNo, proto.SegmentState_Active)
	assert.NoError(t, deleteErr)

	// Verify segment no longer exists
	exists, err = provider.CheckSegmentExists(context.Background(), logName, segmentMeta.Metadata.SegNo)
	assert.NoError(t, err)
	assert.False(t, exists)

	// Attempt to get the deleted segment should result in error
	_, getErr := provider.GetSegmentMetadata(context.Background(), logName, segmentMeta.Metadata.SegNo)
	assert.Error(t, getErr)
	assert.True(t, werr.ErrSegmentNotFound.Is(getErr))

	// Attempt to delete a non-existent segment should result in error
	deleteErr = provider.DeleteSegmentMetadata(context.Background(), logName, int64(1), 999, proto.SegmentState_Truncated)
	assert.Error(t, deleteErr)
	assert.True(t, werr.ErrSegmentNotFound.Is(deleteErr))

	// Test deleting from a non-existent log
	nonExistentLogName := "non_existent_log"
	deleteErr = provider.DeleteSegmentMetadata(context.Background(), nonExistentLogName, int64(1), 1, proto.SegmentState_Truncated)
	assert.Error(t, deleteErr)
	assert.True(t, werr.ErrSegmentNotFound.Is(deleteErr))
}

func testLogWriterLock(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	require.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	defer provider.Close()
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)
	logName := "test_log" + time.Now().Format("20060102150405")

	// lock success
	_, getLockErr := provider.AcquireLogWriterLock(context.Background(), logName)
	assert.NoError(t, getLockErr)

	// reentrant lock success
	_, getLockErr = provider.AcquireLogWriterLock(context.Background(), logName)
	assert.NoError(t, getLockErr)

	// test lock fail from another session
	{
		newSession, newSessionErr := concurrency.NewSession(etcdCli, concurrency.WithTTL(5))
		require.NoError(t, newSessionErr)
		defer newSession.Close()
		lockKey := BuildLogLockKey(logName)
		mutex1 := concurrency.NewMutex(newSession, lockKey)
		lockErr := mutex1.TryLock(context.Background())
		assert.Error(t, lockErr)
	}

	// release lock
	releaseLockErr := provider.ReleaseLogWriterLock(context.Background(), logName)
	assert.NoError(t, releaseLockErr)

	// test lock success from another session after release
	{
		newSession, newSessionErr := concurrency.NewSession(etcdCli, concurrency.WithTTL(5))
		require.NoError(t, newSessionErr)
		defer newSession.Close()
		lockKey := BuildLogLockKey(logName)
		mutex1 := concurrency.NewMutex(newSession, lockKey)
		lockErr := mutex1.TryLock(context.Background())
		assert.NoError(t, lockErr)
	}
}

func testUpdateLogMetaForTruncation(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	require.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	// Create a test log
	logName := "truncate_test_log_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	// Get the initial log metadata
	initialLogMeta, err := provider.GetLogMeta(context.Background(), logName)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), initialLogMeta.Metadata.TruncatedSegmentId)
	assert.Equal(t, int64(-1), initialLogMeta.Metadata.TruncatedEntryId)

	// Update truncation point
	truncatedSegmentId := int64(5)
	truncatedEntryId := int64(100)
	initialLogMeta.Metadata.TruncatedSegmentId = truncatedSegmentId
	initialLogMeta.Metadata.TruncatedEntryId = truncatedEntryId
	initialLogMeta.Metadata.ModificationTimestamp = uint64(time.Now().Unix())

	// Update the log metadata
	err = provider.UpdateLogMeta(context.Background(), logName, initialLogMeta)
	assert.NoError(t, err)

	// Get the updated log metadata
	updatedLogMeta, err := provider.GetLogMeta(context.Background(), logName)
	assert.NoError(t, err)

	// Verify truncation point is updated
	assert.Equal(t, truncatedSegmentId, updatedLogMeta.Metadata.TruncatedSegmentId)
	assert.Equal(t, truncatedEntryId, updatedLogMeta.Metadata.TruncatedEntryId)
	assert.GreaterOrEqual(t, updatedLogMeta.Metadata.ModificationTimestamp, initialLogMeta.Metadata.ModificationTimestamp)
}

func testCreateReaderTempInfo(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	require.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	// Create a metadata provider with the session
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))

	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	// Create a test log
	logName := "reader_temp_info_test_log_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	require.NoError(t, err)

	// Get log metadata to get the logId
	logMeta, err := provider.GetLogMeta(context.Background(), logName)
	require.NoError(t, err)

	// Create reader temp info
	readerName := "test-reader-" + time.Now().Format("20060102150405")
	segmentId := int64(3)
	entryId := int64(42)

	// Create the reader temp info - this will use the session's lease
	err = provider.CreateReaderTempInfo(context.Background(), readerName, logMeta.Metadata.LogId, segmentId, entryId)
	assert.NoError(t, err)

	// Verify the reader temp info was created
	readerKey := BuildLogReaderTempInfoKey(logMeta.Metadata.LogId, readerName)
	resp, err := etcdCli.Get(context.Background(), readerKey)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.Kvs), "Reader temp info should exist")

	// Decode the reader temp info
	readerInfo := &proto.ReaderTempInfo{}
	err = pb.Unmarshal(resp.Kvs[0].Value, readerInfo)
	assert.NoError(t, err)

	// Verify the reader temp info content
	assert.Equal(t, readerName, readerInfo.ReaderName)
	assert.Equal(t, logMeta.Metadata.LogId, readerInfo.LogId)
	assert.Equal(t, segmentId, readerInfo.OpenSegmentId)
	assert.Equal(t, entryId, readerInfo.OpenEntryId)
	assert.Equal(t, segmentId, readerInfo.RecentReadSegmentId)
	assert.Equal(t, entryId, readerInfo.RecentReadEntryId)
	assert.Greater(t, readerInfo.OpenTimestamp, uint64(0))
	assert.Greater(t, readerInfo.RecentReadTimestamp, uint64(0))

	// Verify the key is attached to the session's lease
	leaseID := clientv3.LeaseID(resp.Kvs[0].Lease)
	leaseInfo, err := etcdCli.TimeToLive(context.Background(), leaseID)
	require.NoError(t, err)
	assert.True(t, leaseInfo.TTL > 0, "Key should have a TTL")
	assert.True(t, leaseInfo.TTL <= 60, "TTL should be 60 seconds or less")

	// Revoke the lease to simulate expiry without waiting 65 seconds
	_, err = etcdCli.Revoke(context.Background(), leaseID)
	require.NoError(t, err)

	// Check if the key has been automatically removed after lease revoked
	checkResp, err := etcdCli.Get(context.Background(), readerKey)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(checkResp.Kvs), "Reader temp info should be automatically removed after lease revoked")
}

func testGetReaderTempInfo(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	require.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	// Create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	// Create a test log
	logName := "get_reader_temp_info_test_log_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	require.NoError(t, err)

	// Get log metadata to get the logId
	logMeta, err := provider.GetLogMeta(context.Background(), logName)
	require.NoError(t, err)

	// Create reader temp info
	readerName := "test-reader-" + time.Now().Format("20060102150405")
	segmentId := int64(3)
	entryId := int64(42)

	// Create the reader temp info
	err = provider.CreateReaderTempInfo(context.Background(), readerName, logMeta.Metadata.LogId, segmentId, entryId)
	assert.NoError(t, err)

	// Get the reader temp info
	readerInfo, err := provider.GetReaderTempInfo(context.Background(), logMeta.Metadata.LogId, readerName)
	assert.NoError(t, err)
	assert.NotNil(t, readerInfo)

	// Verify reader temp info content
	assert.Equal(t, readerName, readerInfo.ReaderName)
	assert.Equal(t, logMeta.Metadata.LogId, readerInfo.LogId)
	assert.Equal(t, segmentId, readerInfo.OpenSegmentId)
	assert.Equal(t, entryId, readerInfo.OpenEntryId)
	assert.Equal(t, segmentId, readerInfo.RecentReadSegmentId)
	assert.Equal(t, entryId, readerInfo.RecentReadEntryId)
	assert.Greater(t, readerInfo.OpenTimestamp, uint64(0))
	assert.Greater(t, readerInfo.RecentReadTimestamp, uint64(0))

	// Test getting non-existent reader
	nonExistentReaderName := "non-existent-reader"
	_, err = provider.GetReaderTempInfo(context.Background(), logMeta.Metadata.LogId, nonExistentReaderName)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reader temp info not found")
}

// Test updating reader temporary information
func testUpdateReaderTempInfo(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	require.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	// Create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	// Create a test log
	logName := "update_reader_temp_info_test_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	require.NoError(t, err)

	// Get log metadata to get the logId
	logMeta, err := provider.GetLogMeta(context.Background(), logName)
	require.NoError(t, err)

	// Create reader temp info
	readerName := "test-reader-update-" + time.Now().Format("20060102150405")
	initialSegmentId := int64(3)
	initialEntryId := int64(42)

	// Create the reader temp info
	err = provider.CreateReaderTempInfo(context.Background(), readerName, logMeta.Metadata.LogId, initialSegmentId, initialEntryId)
	assert.NoError(t, err)

	// Get the reader temp info
	initialReader, err := provider.GetReaderTempInfo(context.Background(), logMeta.Metadata.LogId, readerName)
	assert.NoError(t, err)
	assert.NotNil(t, initialReader)
	assert.Equal(t, initialSegmentId, initialReader.RecentReadSegmentId)
	assert.Equal(t, initialEntryId, initialReader.RecentReadEntryId)

	// Update the read position
	updatedSegmentId := int64(4)
	updatedEntryId := int64(10)

	// Update reader temp info
	err = provider.UpdateReaderTempInfo(context.Background(), logMeta.Metadata.LogId, readerName, updatedSegmentId, updatedEntryId)
	assert.NoError(t, err)

	// Get the updated reader temp info
	updatedReader, err := provider.GetReaderTempInfo(context.Background(), logMeta.Metadata.LogId, readerName)
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
	err = provider.UpdateReaderTempInfo(context.Background(), logMeta.Metadata.LogId, "non-existent-reader", 1, 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reader temp info not found")

	// Test updating with different logId
	err = provider.UpdateReaderTempInfo(context.Background(), 999, readerName, 1, 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reader temp info not found")

	// Verify lease persistence (reader temporary info should still exist with a TTL)
	readerKey := BuildLogReaderTempInfoKey(logMeta.Metadata.LogId, readerName)
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
	require.NoError(t, err)
	require.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	// Create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	// Create a test log
	logName := "delete_reader_temp_info_test_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	require.NoError(t, err)

	// Get log metadata to get the logId
	logMeta, err := provider.GetLogMeta(context.Background(), logName)
	require.NoError(t, err)

	// Create multiple reader temp infos
	readerCount := 3
	readerNames := make([]string, readerCount)
	segmentIds := []int64{1, 2, 3}
	entryIds := []int64{10, 20, 30}

	for i := 0; i < readerCount; i++ {
		readerNames[i] = fmt.Sprintf("test-reader-%d-%s", i, time.Now().Format("20060102150405"))
		err = provider.CreateReaderTempInfo(context.Background(), readerNames[i], logMeta.Metadata.LogId, segmentIds[i], entryIds[i])
		assert.NoError(t, err)
	}

	// Verify all readers exist
	readers, err := provider.GetAllReaderTempInfoForLog(context.Background(), logMeta.Metadata.LogId)
	assert.NoError(t, err)
	assert.Equal(t, readerCount, len(readers))

	// Test 1: Delete a specific reader
	err = provider.DeleteReaderTempInfo(context.Background(), logMeta.Metadata.LogId, readerNames[1])
	assert.NoError(t, err)

	// Verify reader was deleted
	readers, err = provider.GetAllReaderTempInfoForLog(context.Background(), logMeta.Metadata.LogId)
	assert.NoError(t, err)
	assert.Equal(t, readerCount-1, len(readers))

	// Try to get the deleted reader - should fail
	_, err = provider.GetReaderTempInfo(context.Background(), logMeta.Metadata.LogId, readerNames[1])
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reader temp info not found")

	// Test 2: Delete non-existent reader - should not error
	err = provider.DeleteReaderTempInfo(context.Background(), logMeta.Metadata.LogId, "non-existent-reader")
	assert.NoError(t, err)

	// Test 3: Delete with incorrect logId - should not error but won't delete anything
	err = provider.DeleteReaderTempInfo(context.Background(), 999, readerNames[0])
	assert.NoError(t, err)

	// Verify remaining reader still exists
	readers, err = provider.GetAllReaderTempInfoForLog(context.Background(), logMeta.Metadata.LogId)
	assert.NoError(t, err)
	assert.Equal(t, readerCount-1, len(readers))

	// Delete last readers
	for i := 0; i < readerCount; i++ {
		if i != 1 { // Skip the one we already deleted
			err = provider.DeleteReaderTempInfo(context.Background(), logMeta.Metadata.LogId, readerNames[i])
			assert.NoError(t, err)
		}
	}

	// Verify all readers are gone
	readers, err = provider.GetAllReaderTempInfoForLog(context.Background(), logMeta.Metadata.LogId)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(readers))
}

func testGetAllReaderTempInfoForLog(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	require.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	// Create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	// Create a test log
	logName := "get_all_readers_temp_info_test_log_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	require.NoError(t, err)

	// Get log metadata to get the logId
	logMeta, err := provider.GetLogMeta(context.Background(), logName)
	require.NoError(t, err)

	// Initially there should be no readers
	readers, err := provider.GetAllReaderTempInfoForLog(context.Background(), logMeta.Metadata.LogId)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(readers))

	// Create multiple reader temp infos
	readerCount := 3
	readerNames := make([]string, readerCount)
	segmentIds := []int64{1, 2, 3}
	entryIds := []int64{10, 20, 30}

	for i := 0; i < readerCount; i++ {
		readerNames[i] = fmt.Sprintf("test-reader-%d-%s", i, time.Now().Format("20060102150405"))
		err = provider.CreateReaderTempInfo(context.Background(), readerNames[i], logMeta.Metadata.LogId, segmentIds[i], entryIds[i])
		assert.NoError(t, err)
	}

	// Get all reader temp infos for the log
	allReaders, err := provider.GetAllReaderTempInfoForLog(context.Background(), logMeta.Metadata.LogId)
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
		assert.Equal(t, logMeta.Metadata.LogId, reader.LogId)
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
	require.NoError(t, err)
	require.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	// Create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	// Create a test log
	logName := "segment_cleanup_test_log_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	require.NoError(t, err)

	// Get log metadata to get the logId
	logMeta, err := provider.GetLogMeta(context.Background(), logName)
	require.NoError(t, err)

	return provider, logName, logMeta.Metadata.LogId
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

// Test updating logMeta with wrong revision (optimistic lock failure)
func testUpdateLogMetaWithWrongRevision(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	require.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	// Create a test log
	logName := "wrong_revision_test_log_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	// Get the initial log metadata
	initialLogMeta, err := provider.GetLogMeta(context.Background(), logName)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), initialLogMeta.Metadata.TruncatedSegmentId)
	assert.Equal(t, int64(-1), initialLogMeta.Metadata.TruncatedEntryId)

	// Create a logMeta with wrong revision (using a different revision number)
	wrongRevisionLogMeta := &LogMeta{
		Metadata: &proto.LogMeta{
			LogId:                     initialLogMeta.Metadata.LogId,
			MaxSegmentRollTimeSeconds: initialLogMeta.Metadata.MaxSegmentRollTimeSeconds,
			MaxSegmentRollSizeBytes:   initialLogMeta.Metadata.MaxSegmentRollSizeBytes,
			CompactionBufferSizeBytes: initialLogMeta.Metadata.CompactionBufferSizeBytes,
			MaxCompactionFileCount:    initialLogMeta.Metadata.MaxCompactionFileCount,
			CreationTimestamp:         initialLogMeta.Metadata.CreationTimestamp,
			ModificationTimestamp:     initialLogMeta.Metadata.ModificationTimestamp,
			TruncatedSegmentId:        int64(100),
			TruncatedEntryId:          int64(200),
		},
		Revision: initialLogMeta.Revision + 1, // Wrong revision
	}

	// Try to update with wrong revision - should fail
	err = provider.UpdateLogMeta(context.Background(), logName, wrongRevisionLogMeta)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "log metadata revision is invalid or outdated")

	// Verify the log metadata was not updated
	unchangedLogMeta, err := provider.GetLogMeta(context.Background(), logName)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), unchangedLogMeta.Metadata.TruncatedSegmentId)
	assert.Equal(t, int64(-1), unchangedLogMeta.Metadata.TruncatedEntryId)

	// Now try with correct revision - should succeed
	initialLogMeta.Metadata.TruncatedSegmentId = int64(100)
	initialLogMeta.Metadata.TruncatedEntryId = int64(200)
	initialLogMeta.Metadata.ModificationTimestamp = uint64(time.Now().Unix())

	err = provider.UpdateLogMeta(context.Background(), logName, initialLogMeta)
	assert.NoError(t, err)

	// Verify the update was successful
	updatedLogMeta, err := provider.GetLogMeta(context.Background(), logName)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), updatedLogMeta.Metadata.TruncatedSegmentId)
	assert.Equal(t, int64(200), updatedLogMeta.Metadata.TruncatedEntryId)
}

// Test updating segmentMeta with wrong revision (optimistic lock failure)
func testUpdateSegmentMetaWithWrongRevision(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	require.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	logName := "wrong_revision_segment_test_log_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	// Create initial segment metadata
	initialSegmentMeta := &SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1,
			State: proto.SegmentState_Active,
		},
	}

	storeErr := provider.StoreSegmentMetadata(context.Background(), logName, int64(1), initialSegmentMeta)
	assert.NoError(t, storeErr)

	// Get the current segment metadata to get the correct revision
	currentSegmentMeta, err := provider.GetSegmentMetadata(context.Background(), logName, 1)
	assert.NoError(t, err)
	assert.Equal(t, proto.SegmentState_Active, currentSegmentMeta.Metadata.State)

	// Create a segmentMeta with wrong revision
	wrongRevisionSegmentMeta := &SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1,
			State: proto.SegmentState_Sealed,
		},
		Revision: currentSegmentMeta.Revision + 1, // Wrong revision
	}

	// Try to update with wrong revision - should fail
	err = provider.UpdateSegmentMetadata(context.Background(), logName, int64(1), wrongRevisionSegmentMeta, proto.SegmentState_Active)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "segment metadata revision is invalid or outdated")

	// Verify the segment metadata was not updated
	unchangedSegmentMeta, err := provider.GetSegmentMetadata(context.Background(), logName, 1)
	assert.NoError(t, err)
	assert.Equal(t, proto.SegmentState_Active, unchangedSegmentMeta.Metadata.State)

	// Now try with correct revision - should succeed
	currentSegmentMeta.Metadata.State = proto.SegmentState_Sealed
	err = provider.UpdateSegmentMetadata(context.Background(), logName, int64(1), currentSegmentMeta, proto.SegmentState_Active)
	assert.NoError(t, err)

	// Verify the update was successful
	updatedSegmentMeta, err := provider.GetSegmentMetadata(context.Background(), logName, 1)
	assert.NoError(t, err)
	assert.Equal(t, proto.SegmentState_Sealed, updatedSegmentMeta.Metadata.State)
}

func testStoreOrGetConditionWriteResult(t *testing.T) {
	// get etcd client
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	require.NotNil(t, etcdCli)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))

	// Clear the condition write key first
	_, err = etcdCli.Delete(context.Background(), ConditionWriteKey)
	require.NoError(t, err)

	// Test Case 1: First node stores true, should get true back
	result1, err := provider.StoreOrGetConditionWriteResult(context.Background(), true)
	assert.NoError(t, err)
	assert.True(t, result1, "First node should get its own detection result (true)")

	// Test Case 2: Second node tries to store false, but should get true (from first node)
	result2, err := provider.StoreOrGetConditionWriteResult(context.Background(), false)
	assert.NoError(t, err)
	assert.True(t, result2, "Second node should get the first node's result (true), not its own (false)")

	// Test Case 3: Third node tries to store true, should still get true (consistent)
	result3, err := provider.StoreOrGetConditionWriteResult(context.Background(), true)
	assert.NoError(t, err)
	assert.True(t, result3, "Third node should get the first node's result (true)")

	// Verify the value is actually stored in etcd
	resp, err := etcdCli.Get(context.Background(), ConditionWriteKey)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.Kvs))
	assert.Equal(t, "true", string(resp.Kvs[0].Value))

	// Test Case 4: Clear and test with false as first value
	_, err = etcdCli.Delete(context.Background(), ConditionWriteKey)
	require.NoError(t, err)

	// First node stores false
	result4, err := provider.StoreOrGetConditionWriteResult(context.Background(), false)
	assert.NoError(t, err)
	assert.False(t, result4, "First node should get its own detection result (false)")

	// Second node tries to store true, but should get false (from first node)
	result5, err := provider.StoreOrGetConditionWriteResult(context.Background(), true)
	assert.NoError(t, err)
	assert.False(t, result5, "Second node should get the first node's result (false), not its own (true)")

	// Verify the value is false in etcd
	resp, err = etcdCli.Get(context.Background(), ConditionWriteKey)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.Kvs))
	assert.Equal(t, "false", string(resp.Kvs[0].Value))
}

func testGetConditionWriteResult(t *testing.T) {
	// get etcd client
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	require.NotNil(t, etcdCli)

	// create metadata provider
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))

	// Test Case 1: Key doesn't exist, should return error
	_, err = etcdCli.Delete(context.Background(), ConditionWriteKey)
	require.NoError(t, err)

	result, err := provider.GetConditionWriteResult(context.Background())
	assert.Error(t, err)
	assert.False(t, result)
	assert.ErrorIs(t, err, werr.ErrMetadataKeyNotExists, "Error should be ErrMetadataKeyNotExists")

	// Test Case 2: Store true and retrieve it
	_, err = etcdCli.Put(context.Background(), ConditionWriteKey, "true")
	require.NoError(t, err)

	result, err = provider.GetConditionWriteResult(context.Background())
	assert.NoError(t, err)
	assert.True(t, result, "Should retrieve true value")

	// Test Case 3: Store false and retrieve it
	_, err = etcdCli.Put(context.Background(), ConditionWriteKey, "false")
	require.NoError(t, err)

	result, err = provider.GetConditionWriteResult(context.Background())
	assert.NoError(t, err)
	assert.False(t, result, "Should retrieve false value")

	// Test Case 4: Integration with StoreOrGetConditionWriteResult
	// Clear the key first
	_, err = etcdCli.Delete(context.Background(), ConditionWriteKey)
	require.NoError(t, err)

	// Use StoreOrGetConditionWriteResult to store a value
	stored, err := provider.StoreOrGetConditionWriteResult(context.Background(), true)
	assert.NoError(t, err)
	assert.True(t, stored)

	// Now use GetConditionWriteResult to retrieve it
	retrieved, err := provider.GetConditionWriteResult(context.Background())
	assert.NoError(t, err)
	assert.True(t, retrieved, "Should retrieve the same value that was stored")
	assert.Equal(t, stored, retrieved, "Stored and retrieved values should match")
}

// testInitAlreadyInitialized tests InitIfNecessary when all keys already exist
func testInitAlreadyInitialized(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))

	// First init — creates all keys
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	// Second init — all keys already exist, should hit "already initialized" path
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)
}

// testInitPartialInit tests InitIfNecessary when only some keys exist
func testInitPartialInit(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	// Manually create only ServiceInstanceKey (partial init)
	_, err = etcdCli.Put(context.Background(), ServiceInstanceKey, "partial-instance")
	assert.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))

	// Should fail because only some keys exist
	err = provider.InitIfNecessary(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "some keys already exists")
}

// testGetVersionInfoNotFound tests GetVersionInfo when version key doesn't exist
func testGetVersionInfoNotFound(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)

	// Delete version key
	_, err = etcdCli.Delete(context.Background(), VersionKey)
	assert.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	version, err := provider.GetVersionInfo(context.Background())
	assert.Error(t, err)
	assert.Nil(t, version)
	assert.Contains(t, err.Error(), "version not found")
}

// testGetQuorumInfoNotFound tests GetQuorumInfo for non-existent quorum
func testGetQuorumInfoNotFound(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	quorum, err := provider.GetQuorumInfo(context.Background(), 99999)
	assert.Error(t, err)
	assert.Nil(t, quorum)
	assert.Contains(t, err.Error(), "quorum info not found")
}

// testStoreQuorumInfoAlreadyExists tests StoreQuorumInfo when quorum already exists
func testStoreQuorumInfoAlreadyExists(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	quorum := &proto.QuorumInfo{Id: 100, Es: 1, Wq: 1, Aq: 1, Nodes: []string{"node1"}}

	// First store should succeed
	err = provider.StoreQuorumInfo(context.Background(), quorum)
	assert.NoError(t, err)

	// Second store should fail — already exists
	err = provider.StoreQuorumInfo(context.Background(), quorum)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "quorum info already exists")
}

// testOpenLogNotFound tests OpenLog for non-existent log
func testOpenLogNotFound(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	logMeta, segMetas, err := provider.OpenLog(context.Background(), "nonexistent_log")
	assert.Error(t, err)
	assert.Nil(t, logMeta)
	assert.Nil(t, segMetas)
}

// testListLogsWithPrefixNoMatch tests ListLogsWithPrefix when no logs match
func testListLogsWithPrefixNoMatch(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	logNames, err := provider.ListLogsWithPrefix(context.Background(), "nonexistent_prefix")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(logNames))
}

// testNewMetadataProviderDefaultTimeout tests NewMetadataProvider with zero request timeout
func testNewMetadataProviderDefaultTimeout(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)

	cfg := testMetaCfg(t)
	// Zero value DurationMilliseconds → Milliseconds() returns 0 → defaults to 10000ms
	cfg.Etcd.RequestTimeout = config.DurationMilliseconds{}

	provider := NewMetadataProvider(context.Background(), etcdCli, cfg)
	assert.NotNil(t, provider)

	// Should still work with default timeout (10s)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)
}

// testGetContextWithTimeoutGRPCMetadata tests getContextWithTimeout with gRPC metadata in context
func testGetContextWithTimeoutGRPCMetadata(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	p := provider.(*metadataProviderEtcd)

	// Create context with gRPC incoming metadata including auth keys
	md := metadata.Pairs("authorization", "Bearer token123", "token", "abc", "other-key", "value")
	ctxWithMd := metadata.NewIncomingContext(context.Background(), md)

	newCtx, cancel := p.getContextWithTimeout(ctxWithMd)
	defer cancel()

	// Verify context is valid and has timeout
	assert.NotNil(t, newCtx)
	deadline, hasDeadline := newCtx.Deadline()
	assert.True(t, hasDeadline)
	assert.True(t, deadline.After(time.Now()))

	// Verify auth-related keys are removed from the new context
	newMd, ok := metadata.FromIncomingContext(newCtx)
	require.True(t, ok, "new context should have incoming metadata")
	assert.Empty(t, newMd.Get("authorization"), "authorization should be removed")
	assert.Empty(t, newMd.Get("token"), "token should be removed")
	assert.Equal(t, []string{"value"}, newMd.Get("other-key"), "non-auth keys should be preserved")

	// Verify original context metadata is NOT mutated
	origMd, ok := metadata.FromIncomingContext(ctxWithMd)
	require.True(t, ok, "original context should still have incoming metadata")
	assert.NotEmpty(t, origMd.Get("authorization"), "original authorization should not be mutated")
	assert.NotEmpty(t, origMd.Get("token"), "original token should not be mutated")
}

// testStoreSegmentMetaAlreadyExists tests StoreSegmentMetadata when segment already exists
func testStoreSegmentMetaAlreadyExists(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	logName := "dup_seg_test_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	seg := &SegmentMeta{Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active}}
	err = provider.StoreSegmentMetadata(context.Background(), logName, int64(1), seg)
	assert.NoError(t, err)

	// Store again — should fail
	seg2 := &SegmentMeta{Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active}}
	err = provider.StoreSegmentMetadata(context.Background(), logName, int64(1), seg2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "segment metadata already exists")
}

// testCreateCleanupStatusAlreadyExists tests CreateSegmentCleanupStatus when status already exists
func testCreateCleanupStatusAlreadyExists(t *testing.T) {
	provider, _, logId := setupSegmentCleanupTest(t)

	status := &proto.SegmentCleanupStatus{
		LogId:               logId,
		SegmentId:           int64(1),
		State:               proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		StartTime:           uint64(time.Now().UnixMilli()),
		LastUpdateTime:      uint64(time.Now().UnixMilli()),
		QuorumCleanupStatus: map[string]bool{"node1": false},
	}

	err := provider.CreateSegmentCleanupStatus(context.Background(), status)
	assert.NoError(t, err)

	// Create again — should fail
	err = provider.CreateSegmentCleanupStatus(context.Background(), status)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

// testUpdateCleanupStatusNotExists tests UpdateSegmentCleanupStatus when status doesn't exist
func testUpdateCleanupStatusNotExists(t *testing.T) {
	provider, _, logId := setupSegmentCleanupTest(t)

	status := &proto.SegmentCleanupStatus{
		LogId:               logId,
		SegmentId:           int64(999),
		State:               proto.SegmentCleanupState_CLEANUP_COMPLETED,
		StartTime:           uint64(time.Now().UnixMilli()),
		LastUpdateTime:      uint64(time.Now().UnixMilli()),
		QuorumCleanupStatus: map[string]bool{},
	}

	err := provider.UpdateSegmentCleanupStatus(context.Background(), status)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

// testCheckSegmentNotExists tests CheckSegmentExists for non-existent segment
func testCheckSegmentNotExists(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	logName := "check_seg_test_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	assert.NoError(t, err)

	// Check non-existent segment
	exists, err := provider.CheckSegmentExists(context.Background(), logName, 999)
	assert.NoError(t, err)
	assert.False(t, exists)
}

// === etcd-dependent SessionLock / Close / Release tests (moved from constant_test.go) ===

func testSessionLockWithEtcd(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	mp := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	assert.NotNil(t, mp)

	// Acquire a lock
	lock, err := mp.AcquireLogWriterLock(context.Background(), "test-session-lock")
	assert.NoError(t, err)
	assert.NotNil(t, lock)
	assert.True(t, lock.IsValid())
	assert.NotNil(t, lock.GetSession())

	// Check lock alive
	alive, err := mp.CheckSessionLockAlive(context.Background(), lock)
	assert.NoError(t, err)
	assert.True(t, alive)

	// Release the lock
	err = mp.ReleaseLogWriterLock(context.Background(), "test-session-lock")
	assert.NoError(t, err)
}

func testCheckSessionLockAliveNil(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	mp := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))

	// Test nil session lock
	alive, err := mp.CheckSessionLockAlive(context.Background(), nil)
	assert.Error(t, err)
	assert.False(t, alive)
	assert.Contains(t, err.Error(), "not properly initialized")

	// Test session lock with nil session
	sl := &SessionLock{}
	alive, err = mp.CheckSessionLockAlive(context.Background(), sl)
	assert.Error(t, err)
	assert.False(t, alive)
}

func testCloseEmptyProvider(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	mp := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))

	// Close with no locks should succeed
	err = mp.Close()
	assert.NoError(t, err)
}

func testGetLogMetaNotFound(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	mp := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))

	// GetLogMeta for non-existent log
	logMeta, err := mp.GetLogMeta(context.Background(), "nonexistent-log")
	assert.Error(t, err)
	assert.Nil(t, logMeta)
}

func testCloseWithActiveLock(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	mp := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))

	// Acquire a lock
	_, err = mp.AcquireLogWriterLock(context.Background(), "close-test-log")
	assert.NoError(t, err)

	// Close should release the lock
	err = mp.Close()
	assert.NoError(t, err)
}

func testReleaseNonExistentLock(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	mp := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))

	// Release a lock that was never acquired should succeed (no-op)
	err = mp.ReleaseLogWriterLock(context.Background(), "never-acquired-log")
	assert.NoError(t, err)
}

// testCancelledContextEtcdErrors covers error branches when etcd operations fail due to cancelled context
func testCancelledContextEtcdErrors(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	// InitIfNecessary
	err = provider.InitIfNecessary(cancelledCtx)
	assert.Error(t, err)

	// GetVersionInfo
	_, err = provider.GetVersionInfo(cancelledCtx)
	assert.Error(t, err)

	// CreateLog
	err = provider.CreateLog(cancelledCtx, "cancelled-log")
	assert.Error(t, err)

	// GetLogMeta
	_, err = provider.GetLogMeta(cancelledCtx, "cancelled-log")
	assert.Error(t, err)

	// UpdateLogMeta - marshal succeeds, txn fails
	err = provider.UpdateLogMeta(cancelledCtx, "cancelled-log", &LogMeta{
		Metadata: &proto.LogMeta{LogId: 1},
		Revision: 1,
	})
	assert.Error(t, err)

	// OpenLog
	_, _, err = provider.OpenLog(cancelledCtx, "cancelled-log")
	assert.Error(t, err)

	// CheckExists
	_, err = provider.CheckExists(cancelledCtx, "cancelled-log")
	assert.Error(t, err)

	// ListLogs (also covers ListLogsWithPrefix error path within ListLogs)
	_, err = provider.ListLogs(cancelledCtx)
	assert.Error(t, err)

	// ListLogsWithPrefix
	_, err = provider.ListLogsWithPrefix(cancelledCtx, "test")
	assert.Error(t, err)

	// StoreSegmentMetadata - marshal succeeds, txn fails
	err = provider.StoreSegmentMetadata(cancelledCtx, "cancelled-log", 1, &SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active},
	})
	assert.Error(t, err)

	// UpdateSegmentMetadata - marshal succeeds, txn fails
	err = provider.UpdateSegmentMetadata(cancelledCtx, "cancelled-log", 1, &SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Sealed},
		Revision: 1,
	}, proto.SegmentState_Active)
	assert.Error(t, err)

	// GetSegmentMetadata
	_, err = provider.GetSegmentMetadata(cancelledCtx, "cancelled-log", 1)
	assert.Error(t, err)

	// GetAllSegmentMetadata
	_, err = provider.GetAllSegmentMetadata(cancelledCtx, "cancelled-log")
	assert.Error(t, err)

	// CheckSegmentExists
	_, err = provider.CheckSegmentExists(cancelledCtx, "cancelled-log", 1)
	assert.Error(t, err)

	// DeleteSegmentMetadata - txn fails
	err = provider.DeleteSegmentMetadata(cancelledCtx, "cancelled-log", 1, 1, proto.SegmentState_Active)
	assert.Error(t, err)

	// StoreQuorumInfo - marshal succeeds, txn fails
	err = provider.StoreQuorumInfo(cancelledCtx, &proto.QuorumInfo{Id: 1})
	assert.Error(t, err)

	// GetQuorumInfo
	_, err = provider.GetQuorumInfo(cancelledCtx, 1)
	assert.Error(t, err)

	// CreateReaderTempInfo - grant fails
	err = provider.CreateReaderTempInfo(cancelledCtx, "reader1", 1, 0, 0)
	assert.Error(t, err)

	// GetReaderTempInfo
	_, err = provider.GetReaderTempInfo(cancelledCtx, 1, "reader1")
	assert.Error(t, err)

	// GetAllReaderTempInfoForLog
	_, err = provider.GetAllReaderTempInfoForLog(cancelledCtx, 1)
	assert.Error(t, err)

	// UpdateReaderTempInfo
	err = provider.UpdateReaderTempInfo(cancelledCtx, 1, "reader1", 0, 0)
	assert.Error(t, err)

	// DeleteReaderTempInfo
	err = provider.DeleteReaderTempInfo(cancelledCtx, 1, "reader1")
	assert.Error(t, err)

	// CreateSegmentCleanupStatus - marshal succeeds, txn fails
	err = provider.CreateSegmentCleanupStatus(cancelledCtx, &proto.SegmentCleanupStatus{
		LogId: 1, SegmentId: 1, State: proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		StartTime: 1, LastUpdateTime: 1,
	})
	assert.Error(t, err)

	// UpdateSegmentCleanupStatus - marshal succeeds, txn fails
	err = provider.UpdateSegmentCleanupStatus(cancelledCtx, &proto.SegmentCleanupStatus{
		LogId: 1, SegmentId: 1, State: proto.SegmentCleanupState_CLEANUP_COMPLETED,
		StartTime: 1, LastUpdateTime: 1,
	})
	assert.Error(t, err)

	// GetSegmentCleanupStatus
	_, err = provider.GetSegmentCleanupStatus(cancelledCtx, 1, 1)
	assert.Error(t, err)

	// DeleteSegmentCleanupStatus
	err = provider.DeleteSegmentCleanupStatus(cancelledCtx, 1, 1)
	assert.Error(t, err)

	// ListSegmentCleanupStatus
	_, err = provider.ListSegmentCleanupStatus(cancelledCtx, 1)
	assert.Error(t, err)

	// StoreOrGetConditionWriteResult
	_, err = provider.StoreOrGetConditionWriteResult(cancelledCtx, true)
	assert.Error(t, err)

	// GetConditionWriteResult
	_, err = provider.GetConditionWriteResult(cancelledCtx)
	assert.Error(t, err)
}

// testCorruptedProtobufData covers unmarshal error branches by writing invalid data to etcd
func testCorruptedProtobufData(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))

	// Invalid protobuf: field 1, length-delimited, claims length=10 but no data follows
	invalidProto := string([]byte{0x0a, 0x0a})

	// GetVersionInfo unmarshal error
	_, err = etcdCli.Put(context.Background(), VersionKey, invalidProto)
	require.NoError(t, err)
	_, err = provider.GetVersionInfo(context.Background())
	assert.Error(t, err)

	// GetLogMeta unmarshal error
	logKey := BuildLogKey("corrupted-log")
	_, err = etcdCli.Put(context.Background(), logKey, invalidProto)
	require.NoError(t, err)
	_, err = provider.GetLogMeta(context.Background(), "corrupted-log")
	assert.Error(t, err)

	// GetSegmentMetadata unmarshal error
	segKey := BuildSegmentInstanceKey("corrupted-log", "1")
	_, err = etcdCli.Put(context.Background(), segKey, invalidProto)
	require.NoError(t, err)
	_, err = provider.GetSegmentMetadata(context.Background(), "corrupted-log", 1)
	assert.Error(t, err)

	// GetAllSegmentMetadata unmarshal error (reuses corrupted segment key from above)
	_, err = provider.GetAllSegmentMetadata(context.Background(), "corrupted-log")
	assert.Error(t, err)

	// GetQuorumInfo unmarshal error
	quorumKey := BuildQuorumInfoKey("999")
	_, err = etcdCli.Put(context.Background(), quorumKey, invalidProto)
	require.NoError(t, err)
	_, err = provider.GetQuorumInfo(context.Background(), 999)
	assert.Error(t, err)

	// GetReaderTempInfo unmarshal error
	readerKey := BuildLogReaderTempInfoKey(1, "corrupted-reader")
	_, err = etcdCli.Put(context.Background(), readerKey, invalidProto)
	require.NoError(t, err)
	_, err = provider.GetReaderTempInfo(context.Background(), 1, "corrupted-reader")
	assert.Error(t, err)

	// GetAllReaderTempInfoForLog with corrupted data (should skip bad entries via continue)
	readers, err := provider.GetAllReaderTempInfoForLog(context.Background(), 1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(readers))

	// UpdateReaderTempInfo unmarshal error (reading existing corrupted data)
	err = provider.UpdateReaderTempInfo(context.Background(), 1, "corrupted-reader", 0, 0)
	assert.Error(t, err)

	// GetSegmentCleanupStatus unmarshal error
	cleanupKey := BuildSegmentCleanupStatusKey(1, 1)
	_, err = etcdCli.Put(context.Background(), cleanupKey, invalidProto)
	require.NoError(t, err)
	_, err = provider.GetSegmentCleanupStatus(context.Background(), 1, 1)
	assert.Error(t, err)

	// ListSegmentCleanupStatus unmarshal error
	_, err = provider.ListSegmentCleanupStatus(context.Background(), 1)
	assert.Error(t, err)
}

// testCreateLogEdgeCases covers CreateLog error branches for missing/invalid idgen
func testCreateLogEdgeCases(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)

	// Test 1: LogIdGeneratorKey does not exist
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.CreateLog(context.Background(), "test-log-no-idgen")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key not found")

	// Test 2: LogIdGeneratorKey has non-numeric value
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	_, err = etcdCli.Put(context.Background(), LogIdGeneratorKey, "not-a-number")
	require.NoError(t, err)

	provider2 := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider2.CreateLog(context.Background(), "test-log-bad-idgen")
	assert.Error(t, err)
}

// testAcquireLockEdgeCases covers AcquireLogWriterLock and ReleaseLogWriterLock edge cases
func testAcquireLockEdgeCases(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))

	// Test 1: Re-acquire with invalid existing lock
	logName1 := "invalid-lock-test-" + time.Now().Format("20060102150405")
	lock, err := provider.AcquireLogWriterLock(context.Background(), logName1)
	require.NoError(t, err)
	require.NotNil(t, lock)

	lock.MarkInvalid()

	newLock, err := provider.AcquireLogWriterLock(context.Background(), logName1)
	assert.NoError(t, err)
	assert.NotNil(t, newLock)
	assert.True(t, newLock.IsValid())

	// Test 2: Acquire with cancelled context on existing valid lock
	// (covers TryLock error on existing lock → MarkInvalid → cleanup → new session → TryLock error)
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = provider.AcquireLogWriterLock(cancelledCtx, logName1)
	assert.Error(t, err)

	// Test 3: Acquire with cancelled context on fresh log name (no existing lock)
	logName2 := "lock-cancel-fresh-" + time.Now().Format("20060102150405")
	_, err = provider.AcquireLogWriterLock(cancelledCtx, logName2)
	assert.Error(t, err)

	// Test 4: Release with cancelled context (covers Unlock error branch)
	logName3 := "release-err-test-" + time.Now().Format("20060102150405")
	_, err = provider.AcquireLogWriterLock(context.Background(), logName3)
	require.NoError(t, err)

	err = provider.ReleaseLogWriterLock(cancelledCtx, logName3)
	assert.NoError(t, err) // function always returns nil, errors are logged

	// Test 5: CheckSessionLockAlive with cancelled context (covers KeepAliveOnce error)
	logName4 := "alive-check-err-" + time.Now().Format("20060102150405")
	aliveLock, err := provider.AcquireLogWriterLock(context.Background(), logName4)
	require.NoError(t, err)

	alive, err := provider.CheckSessionLockAlive(cancelledCtx, aliveLock)
	assert.Error(t, err)
	assert.False(t, alive)

	// Cleanup
	_ = provider.Close()
}

// testUpdateReaderTempInfoWithoutLease covers the no-lease update branch
func testUpdateReaderTempInfoWithoutLease(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	logName := "no_lease_reader_test_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	require.NoError(t, err)

	logMeta, err := provider.GetLogMeta(context.Background(), logName)
	require.NoError(t, err)

	// Write reader temp info directly to etcd WITHOUT a lease
	readerName := "reader-no-lease"
	readerKey := BuildLogReaderTempInfoKey(logMeta.Metadata.LogId, readerName)
	readerInfo := &proto.ReaderTempInfo{
		ReaderName:          readerName,
		OpenTimestamp:       uint64(time.Now().UnixMilli()),
		LogId:               logMeta.Metadata.LogId,
		OpenSegmentId:       0,
		OpenEntryId:         0,
		RecentReadSegmentId: 0,
		RecentReadEntryId:   0,
		RecentReadTimestamp: uint64(time.Now().UnixMilli()),
	}
	readerBytes, err := pb.Marshal(readerInfo)
	require.NoError(t, err)
	_, err = etcdCli.Put(context.Background(), readerKey, string(readerBytes)) // no lease
	require.NoError(t, err)

	// Verify the key has no lease
	resp, err := etcdCli.Get(context.Background(), readerKey)
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.Kvs))
	assert.Equal(t, int64(0), resp.Kvs[0].Lease, "Key should have no lease")

	// Update reader - should hit the no-lease (else) branch
	err = provider.UpdateReaderTempInfo(context.Background(), logMeta.Metadata.LogId, readerName, 5, 100)
	assert.NoError(t, err)

	// Verify update worked
	updatedReader, err := provider.GetReaderTempInfo(context.Background(), logMeta.Metadata.LogId, readerName)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), updatedReader.RecentReadSegmentId)
	assert.Equal(t, int64(100), updatedReader.RecentReadEntryId)
}

// testCloseWithNilLockEntry covers Close with nil and empty lock entries in the map
func testCloseWithNilLockEntry(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	p := provider.(*metadataProviderEtcd)

	// Store a nil SessionLock pointer in the locks map
	p.logWriterLocks.Store("nil-lock-log", (*SessionLock)(nil))

	// Store a SessionLock with nil mutex and nil session
	p.logWriterLocks.Store("empty-lock-log", &SessionLock{})

	// Close should handle nil/empty lock entries gracefully
	err = provider.Close()
	assert.NoError(t, err)
}

// testOpenLogWithCorruptedSegmentData covers OpenLog when GetAllSegmentMetadata fails
func testOpenLogWithCorruptedSegmentData(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	_, err = etcdCli.Delete(context.Background(), ServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	err = provider.InitIfNecessary(context.Background())
	require.NoError(t, err)

	logName := "open_log_corrupted_seg_" + time.Now().Format("20060102150405")
	err = provider.CreateLog(context.Background(), logName)
	require.NoError(t, err)

	// Put corrupted segment data so GetAllSegmentMetadata fails during OpenLog
	invalidProto := string([]byte{0x0a, 0x0a})
	segKey := BuildSegmentInstanceKey(logName, "1")
	_, err = etcdCli.Put(context.Background(), segKey, invalidProto)
	require.NoError(t, err)

	// OpenLog should fail due to corrupted segment metadata
	logMeta, segMetas, err := provider.OpenLog(context.Background(), logName)
	assert.Error(t, err)
	assert.Nil(t, logMeta)
	assert.Nil(t, segMetas)
}
