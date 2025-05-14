package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// TestClientLogWriterSessionExpiry tests the behavior of LogWriter when its session expires
// It uses the client-based approach for setup and verifies proper error handling
func TestClientLogWriterSessionExpiryByManuallyRelease(t *testing.T) {
	// Create client
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err, "Failed to create configuration")

	client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
	assert.NoError(t, err, "Failed to create client")

	// Create log if not exists
	logName := "test-client-session-expiry-log_" + time.Now().Format("20060102150405")
	err = client.CreateLog(context.Background(), logName)
	if err != nil && !werr.ErrLogAlreadyExists.Is(err) {
		assert.NoError(t, err, "Failed to create log")
	}

	// Open log
	logHandle, err := client.OpenLog(context.Background(), logName)
	assert.NoError(t, err, "Failed to open log")

	// Open writer
	writer, err := logHandle.OpenLogWriter(context.Background())
	assert.NoError(t, err, "Failed to open log writer")

	// 1. Normal write should succeed
	msg := &log.WriterMessage{
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
	msg = &log.WriterMessage{
		Payload: []byte("test message 2 - should fail"),
	}
	result = writer.Write(context.Background(), msg)
	assert.Error(t, result.Err, "Write after session expiry should fail")
	assert.Equal(t, "fenced", result.Err.Error(), "Error should be ErrWriterLockLost")

	// 3. Close the expired writer
	err = writer.Close(context.Background())
	assert.NoError(t, err, "Closing writer should succeed")

	// 4. Open a new writer
	newWriter, err := logHandle.OpenLogWriter(context.Background())
	assert.NoError(t, err, "Opening a new log writer should succeed")
	defer newWriter.Close(context.Background())

	// 5. Writing with the new writer should succeed
	msg = &log.WriterMessage{
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

func TestClientLogWriterSessionExpiry(t *testing.T) {
	// Create client
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err, "Failed to create configuration")

	client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
	assert.NoError(t, err, "Failed to create client")

	// Create log if not exists
	logName := "test-client-session-expiry-log_" + time.Now().Format("20060102150405")
	err = client.CreateLog(context.Background(), logName)
	if err != nil && !werr.ErrLogAlreadyExists.Is(err) {
		assert.NoError(t, err, "Failed to create log")
	}

	// Open log
	logHandle, err := client.OpenLog(context.Background(), logName)
	assert.NoError(t, err, "Failed to open log")

	// Open writer
	writer, err := logHandle.OpenLogWriter(context.Background())
	assert.NoError(t, err, "Failed to open log writer")

	// 1. Normal write should succeed
	msg := &log.WriterMessage{
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
	msg = &log.WriterMessage{
		Payload: []byte("test message 2 - should fail"),
	}
	result = writer.Write(context.Background(), msg)
	assert.Error(t, result.Err, "Write after session expiry should fail")
	assert.Equal(t, "fenced", result.Err.Error(), "Error should be ErrWriterLockLost")

	// 3. Close the expired writer
	err = writer.Close(context.Background())
	assert.NoError(t, err, "Closing writer should succeed")

	// 4. Open a new writer
	newWriter, err := logHandle.OpenLogWriter(context.Background())
	assert.NoError(t, err, "Opening a new log writer should succeed")
	defer newWriter.Close(context.Background())

	// 5. Writing with the new writer should succeed
	msg = &log.WriterMessage{
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
