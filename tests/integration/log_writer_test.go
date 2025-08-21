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

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// TestClientLogWriterSessionExpiry tests the behavior of LogWriter when its session expires
// It uses the client-based approach for setup and verifies proper error handling
func TestClientLogWriterSessionExpiryByManuallyRelease(t *testing.T) {
	// Create client
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err, "Failed to create configuration")
	cfg.Log.Level = "debug"
	cfg.Woodpecker.Logstore.FencePolicy.ConditionWrite = "disable"

	client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
	assert.NoError(t, err, "Failed to create client")

	// Create log if not exists
	logName := "test-client-session-expiry-log_" + time.Now().Format("20060102150405")
	err = client.CreateLog(context.Background(), logName)
	if err != nil && !werr.ErrLogHandleLogAlreadyExists.Is(err) {
		assert.NoError(t, err, "Failed to create log")
	}

	// Open log
	logHandle, err := client.OpenLog(context.Background(), logName)
	assert.NoError(t, err, "Failed to open log")

	// Open writer
	writer, err := logHandle.OpenLogWriter(context.Background())
	assert.NoError(t, err, "Failed to open log writer")

	// 1. Normal write should succeed
	msg := &log.WriteMessage{
		Payload: []byte("test message 1"),
	}
	result := writer.Write(context.Background(), msg)
	assert.NoError(t, result.Err, "Initial write should succeed")
	assert.NotNil(t, result.LogMessageId, "Successful write should return a valid LogMessageId")
	firstMsgID := result.LogMessageId

	// Access metadataProvider to manually expire the session
	metadataProvider := client.GetMetadataProvider()

	// Manually release the lock but keep the writer connected, simulating session expiry
	err = metadataProvider.ReleaseLogWriterLock(context.Background(), logName)
	assert.NoError(t, err, "Failed to release log writer lock")

	// Wait for session monitor to detect expiry
	time.Sleep(100 * time.Millisecond)

	// 2. After session expiry, write should fail
	msg = &log.WriteMessage{
		Payload: []byte("test message 2 - should fail"),
	}
	result = writer.Write(context.Background(), msg)
	assert.Error(t, result.Err, "Write after session expiry should fail")
	assert.True(t, werr.ErrLogWriterLockLost.Is(result.Err), "Error should be ErrWriterLockLost")

	// 3. Close the expired writer
	err = writer.Close(context.Background())
	assert.NoError(t, err, "Closing writer should succeed")

	// 4. Open a new writer
	newWriter, err := logHandle.OpenLogWriter(context.Background())
	assert.NoError(t, err, "Opening a new log writer should succeed")
	defer newWriter.Close(context.Background())

	// 5. Writing with the new writer should succeed
	msg = &log.WriteMessage{
		Payload: []byte("test message 3 - new writer"),
	}
	result = newWriter.Write(context.Background(), msg)
	assert.NoError(t, result.Err, "Write with new writer should succeed")
	assert.NotNil(t, result.LogMessageId, "Successful write should return a valid LogMessageId")

	// wait for sync before read
	flushInterval := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval.Milliseconds()
	time.Sleep(time.Duration(1000 + flushInterval*int(time.Millisecond)))

	// 6. Verify data integrity with a reader
	reader, err := logHandle.OpenLogReader(context.Background(), &log.LogMessageId{SegmentId: firstMsgID.SegmentId, EntryId: firstMsgID.EntryId}, "test-reader")
	assert.NoError(t, err, "Failed to open reader")
	defer reader.Close(context.Background())

	// Read first message
	message1, err := reader.ReadNext(context.Background())
	assert.NoError(t, err, "Failed to read first message")
	assert.Equal(t, "test message 1", string(message1.Payload), "First message content doesn't match")

	// Read second message (third message, since second failed)
	message2, err := reader.ReadNext(context.Background())
	assert.NoError(t, err, "Failed to read second message")
	assert.Equal(t, "test message 3 - new writer", string(message2.Payload), "Second message content doesn't match")
}

func TestClientLogWriterSessionExpiry(t *testing.T) {
	// Create client
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err, "Failed to create configuration")
	cfg.Woodpecker.Logstore.FencePolicy.ConditionWrite = "disable"

	client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
	assert.NoError(t, err, "Failed to create client")

	// Create log if not exists
	logName := "test-client-session-expiry-log_" + time.Now().Format("20060102150405")
	err = client.CreateLog(context.Background(), logName)
	if err != nil && !werr.ErrLogHandleLogAlreadyExists.Is(err) {
		assert.NoError(t, err, "Failed to create log")
	}

	// Open log
	logHandle, err := client.OpenLog(context.Background(), logName)
	assert.NoError(t, err, "Failed to open log")

	// Open writer
	writer, err := logHandle.OpenLogWriter(context.Background())
	assert.NoError(t, err, "Failed to open log writer")

	// 1. Normal write should succeed
	msg := &log.WriteMessage{
		Payload: []byte("test message 1"),
	}
	result := writer.Write(context.Background(), msg)
	assert.NoError(t, result.Err, "Initial write should succeed")
	assert.NotNil(t, result.LogMessageId, "Successful write should return a valid LogMessageId")
	firstMsgID := result.LogMessageId

	// Access writer to manually expire the session directly
	writerSession, convertOk := writer.(interface {
		GetWriterSessionForTest() *concurrency.Session
	})
	assert.True(t, convertOk)
	assert.NotNil(t, writerSession)
	se := writerSession.GetWriterSessionForTest()
	e := se.Close()
	assert.NoError(t, e)

	// Wait for session monitor to detect expiry
	time.Sleep(100 * time.Millisecond)

	// 2. After session expiry, write should fail
	msg = &log.WriteMessage{
		Payload: []byte("test message 2 - should fail"),
	}
	result = writer.Write(context.Background(), msg)
	assert.Error(t, result.Err, "Write after session expiry should fail")
	assert.True(t, werr.ErrLogWriterLockLost.Is(result.Err), "Error should be ErrWriterLockLost")

	// 3. Close the expired writer
	err = writer.Close(context.Background())
	assert.NoError(t, err, "Closing writer should succeed")

	// 4. Open a new writer
	newWriter, err := logHandle.OpenLogWriter(context.Background())
	assert.NoError(t, err, "Opening a new log writer should succeed")
	defer newWriter.Close(context.Background())

	// 5. Writing with the new writer should succeed
	msg = &log.WriteMessage{
		Payload: []byte("test message 3 - new writer"),
	}
	result = newWriter.Write(context.Background(), msg)
	assert.NoError(t, result.Err, "Write with new writer should succeed")
	assert.NotNil(t, result.LogMessageId, "Successful write should return a valid LogMessageId")

	// 6. Verify data integrity with a reader
	reader, err := logHandle.OpenLogReader(context.Background(), &log.LogMessageId{SegmentId: firstMsgID.SegmentId, EntryId: firstMsgID.EntryId}, "test-reader")
	assert.NoError(t, err, "Failed to open reader")
	defer reader.Close(context.Background())

	// Read first message
	message1, err := reader.ReadNext(context.Background())
	assert.NoError(t, err, "Failed to read first message")
	assert.Equal(t, "test message 1", string(message1.Payload), "First message content doesn't match")

	// Read second message (third message, since second failed)
	message2, err := reader.ReadNext(context.Background())
	assert.NoError(t, err, "Failed to read second message")
	assert.Equal(t, "test message 3 - new writer", string(message2.Payload), "Second message content doesn't match")
}

// TestClientLogWriterEtcdFailure tests that LogWriter actively expires session when etcd is unreachable
// After 5 consecutive TTL check failures (about 15 seconds), the session should be marked as invalid
func TestClientLogWriterEtcdFailure(t *testing.T) {
	// Create client with a separate etcd instance for this test
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err, "Failed to create configuration")
	cfg.Woodpecker.Logstore.FencePolicy.ConditionWrite = "disable"
	testEtcdBaseDir := "TestClientLogWriterEtcdFailure" + time.Now().Format("20060102150405")

	// start etcd server
	err = etcd.InitEtcdServer(true, "", "/tmp/"+testEtcdBaseDir, "/tmp/"+testEtcdBaseDir+".log", "info")
	assert.NoError(t, err)
	time.Sleep(1000 * time.Millisecond)

	// Start a separate etcd instance for this test
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	assert.NoError(t, err, "Failed to create etcd client")
	defer etcdCli.Close()

	// Create storage client if needed
	var storageCli storageclient.ObjectStorage
	if cfg.Woodpecker.Storage.IsStorageMinio() {
		ctx := context.Background()
		storageCli, err = storageclient.NewObjectStorage(ctx, cfg)
		assert.NoError(t, err, "Failed to create storage client")
	}

	// Create client with the separate etcd instance
	client, err := woodpecker.NewEmbedClient(context.Background(), cfg, etcdCli, storageCli, true)
	assert.NoError(t, err, "Failed to create client")
	//defer client.Close(context.Background())

	// Create log if not exists
	logName := "test-etcd-failure-log_" + time.Now().Format("20060102150405")
	err = client.CreateLog(context.Background(), logName)
	if err != nil && !werr.ErrLogHandleLogAlreadyExists.Is(err) {
		assert.NoError(t, err, "Failed to create log")
	}

	// Open log
	logHandle, err := client.OpenLog(context.Background(), logName)
	assert.NoError(t, err, "Failed to open log")

	// Open writer
	writer, err := logHandle.OpenLogWriter(context.Background())
	assert.NoError(t, err, "Failed to open log writer")

	// 1. Normal write should succeed
	msg := &log.WriteMessage{
		Payload: []byte("test message before etcd failure"),
	}
	result := writer.Write(context.Background(), msg)
	assert.NoError(t, result.Err, "Initial write should succeed")
	assert.NotNil(t, result.LogMessageId, "Successful write should return a valid LogMessageId")
	firstMsgID := result.LogMessageId

	// 2. Stop etcd server and close etcd client to simulate etcd failure
	// For embed etcd, we need to close the client connection first to ensure
	// keepalive requests fail immediately, similar to how remote etcd behaves
	// when the service is stopped
	etcdCli.Close()
	// Then stop the embed etcd server
	etcd.StopEtcdServer()

	// 3. Wait for TTL check failures to accumulate
	// After 5 consecutive failures (each 3 seconds apart), session should be marked invalid
	// Wait a bit longer to ensure the monitor goroutine has time to detect and mark as invalid
	// 5 failures * 3 seconds = 15 seconds, plus some buffer for goroutine scheduling
	waitTime := 50 * time.Second
	t.Logf("Waiting %v for session to be marked invalid due to etcd connectivity issues", waitTime)
	time.Sleep(waitTime)

	// 4. After etcd failure and TTL check failures, write should fail
	msg = &log.WriteMessage{
		Payload: []byte("test message after etcd failure - should fail2"),
	}
	result = writer.Write(context.Background(), msg)
	assert.Error(t, result.Err, "Write after etcd failure should fail")
	assert.True(t, werr.ErrLogWriterLockLost.Is(result.Err), "Error should be ErrLogWriterLockLost")

	// 5. Close the expired writer
	err = writer.Close(context.Background())
	assert.Error(t, err, "Closing writer should Failed due to etcd connection lose")

	// 6. Restart etcd to allow opening a new writer
	// Note: We can't easily restart the same etcd instance, so we'll create a new client
	// In a real scenario, etcd would be restarted and the application would reconnect
	err = etcd.StartEtcdServerUnsafe(true, "", "/tmp/"+testEtcdBaseDir, "/tmp/"+testEtcdBaseDir+".log", "info")
	assert.NoError(t, err, "Failed to restart etcd")
	time.Sleep(20000 * time.Millisecond) // embed server， sleep 20s to wait simulate lease expired after startup
	etcdCli2, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	assert.NoError(t, err, "Failed to create etcd client")
	defer etcdCli2.Close()

	// Create a new client with the restarted etcd
	client2, err := woodpecker.NewEmbedClient(context.Background(), cfg, etcdCli2, storageCli, true)
	assert.NoError(t, err, "Failed to create new client")
	defer client2.Close(context.Background())

	// Open log again with new client
	logHandle2, err := client2.OpenLog(context.Background(), logName)
	assert.NoError(t, err, "Failed to open log with new client")

	// 7. Open a new writer with the restarted etcd
	newWriter, err := logHandle2.OpenLogWriter(context.Background())
	assert.NoError(t, err, "Opening a new log writer should succeed")
	defer newWriter.Close(context.Background())

	// 8. Writing with the new writer should succeed
	msg = &log.WriteMessage{
		Payload: []byte("test message with new writer after etcd restart"),
	}
	result = newWriter.Write(context.Background(), msg)
	assert.NoError(t, result.Err, "Write with new writer should succeed")
	assert.NotNil(t, result.LogMessageId, "Successful write should return a valid LogMessageId")

	// 9. Verify data integrity with a reader
	// Wait for sync before read
	flushInterval := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval
	time.Sleep(time.Duration(1000 + flushInterval*int(time.Millisecond)))

	reader, err := logHandle2.OpenLogReader(context.Background(), &log.LogMessageId{SegmentId: firstMsgID.SegmentId, EntryId: firstMsgID.EntryId}, "test-reader")
	assert.NoError(t, err, "Failed to open reader")
	defer reader.Close(context.Background())

	// Read first message
	message1, err := reader.ReadNext(context.Background())
	assert.NoError(t, err, "Failed to read first message")
	assert.Equal(t, "test message before etcd failure", string(message1.Payload), "First message content doesn't match")

	// Read second message (third message, since second failed)
	message2, err := reader.ReadNext(context.Background())
	assert.NoError(t, err, "Failed to read second message")
	assert.Equal(t, "test message with new writer after etcd restart", string(message2.Payload), "Second message content doesn't match")
}

func TestClientLogWriterRealEtcdServiceFailureManually(t *testing.T) {
	t.Skip("Test with external real ETCD Service manually")
	// Create client with a separate etcd instance for this test
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err, "Failed to create configuration")
	cfg.Woodpecker.Logstore.FencePolicy.ConditionWrite = "disable"

	// Start a separate etcd instance for this test
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	assert.NoError(t, err, "Failed to create etcd client")
	defer etcdCli.Close()

	// Create storage client if needed
	var storageCli storageclient.ObjectStorage
	if cfg.Woodpecker.Storage.IsStorageMinio() {
		ctx := context.Background()
		storageCli, err = storageclient.NewObjectStorage(ctx, cfg)
		assert.NoError(t, err, "Failed to create storage client")
	}

	// Create client with the separate etcd instance
	client, err := woodpecker.NewEmbedClient(context.Background(), cfg, etcdCli, storageCli, true)
	assert.NoError(t, err, "Failed to create client")

	// Create log if not exists
	logName := "test-etcd-failure-manually-log_" + time.Now().Format("20060102150405")
	err = client.CreateLog(context.Background(), logName)
	if err != nil && !werr.ErrLogHandleLogAlreadyExists.Is(err) {
		assert.NoError(t, err, "Failed to create log")
	}

	// Open log
	logHandle, err := client.OpenLog(context.Background(), logName)
	assert.NoError(t, err, "Failed to open log")

	// Open writer
	writer, err := logHandle.OpenLogWriter(context.Background())
	assert.NoError(t, err, "Failed to open log writer")

	// 1. Normal write should succeed
	msg := &log.WriteMessage{
		Payload: []byte("test message before etcd failure"),
	}
	result := writer.Write(context.Background(), msg)
	assert.NoError(t, result.Err, "Initial write should succeed")
	assert.NotNil(t, result.LogMessageId, "Successful write should return a valid LogMessageId")
	firstMsgID := result.LogMessageId

	// 2. stop etcd server to simulate etcd failure
	// 3. Wait for TTL check failures to accumulate
	waitTime := 50 * time.Second
	t.Logf("=========== Waiting %v for KILL ETCD Manually ===========", waitTime)
	time.Sleep(waitTime)

	// 4. After etcd failure and TTL check failures, write should fail
	msg = &log.WriteMessage{
		Payload: []byte("test message after etcd failure - should fail"),
	}
	result = writer.Write(context.Background(), msg)
	assert.Error(t, result.Err, "Write after etcd failure should fail")
	assert.True(t, werr.ErrLogWriterLockLost.Is(result.Err), "Error should be ErrLogWriterLockLost")

	// 5. Close the expired writer
	err = writer.Close(context.Background())
	assert.Error(t, err, "Closing writer should Failed due to etcd connection lose")

	// 6. Restart etcd to allow opening a new writer
	t.Logf("=========== Waiting %v for START ETCD Manually ===========", waitTime)
	time.Sleep(waitTime)

	//err = etcd.StartEtcdServerUnsafe(true, "", "/tmp/"+testEtcdBaseDir, "/tmp/"+testEtcdBaseDir+".log", "info")
	//assert.NoError(t, err, "Failed to restart etcd")
	etcdCli2, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	assert.NoError(t, err, "Failed to create etcd client")
	defer etcdCli2.Close()

	// Create a new client with the restarted etcd
	client2, err := woodpecker.NewEmbedClient(context.Background(), cfg, etcdCli2, storageCli, true)
	assert.NoError(t, err, "Failed to create new client")
	defer client2.Close(context.Background())

	// Open log again with new client
	logHandle2, err := client2.OpenLog(context.Background(), logName)
	assert.NoError(t, err, "Failed to open log with new client")

	// 7. Open a new writer with the restarted etcd
	newWriter, err := logHandle2.OpenLogWriter(context.Background())
	assert.NoError(t, err, "Opening a new log writer should succeed")
	defer newWriter.Close(context.Background())

	// 8. Writing with the new writer should succeed
	msg = &log.WriteMessage{
		Payload: []byte("test message with new writer after etcd restart"),
	}
	result = newWriter.Write(context.Background(), msg)
	assert.NoError(t, result.Err, "Write with new writer should succeed")
	assert.NotNil(t, result.LogMessageId, "Successful write should return a valid LogMessageId")

	// 9. Verify data integrity with a reader
	// Wait for sync before read
	flushInterval := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval
	time.Sleep(time.Duration(1000 + flushInterval*int(time.Millisecond)))

	reader, err := logHandle2.OpenLogReader(context.Background(), &log.LogMessageId{SegmentId: firstMsgID.SegmentId, EntryId: firstMsgID.EntryId}, "test-reader")
	assert.NoError(t, err, "Failed to open reader")
	defer reader.Close(context.Background())

	// Read first message
	message1, err := reader.ReadNext(context.Background())
	assert.NoError(t, err, "Failed to read first message")
	assert.Equal(t, "test message before etcd failure", string(message1.Payload), "First message content doesn't match")

	// Read second message (third message, since second failed)
	message2, err := reader.ReadNext(context.Background())
	assert.NoError(t, err, "Failed to read second message")
	assert.Equal(t, "test message with new writer after etcd restart", string(message2.Payload), "Second message content doesn't match")
}
