package logger_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	"github.com/zilliztech/woodpecker/server/storage/objectstorage"
)

func TestMinioAppendWithSaturatedDiagnosticOutput(t *testing.T) {
	logger.SaturateDiagnosticOutputForTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	cfg.Woodpecker.Logstore.FencePolicy.ConditionWrite = "enable"
	store := mocks_objectstorage.NewObjectStorage(t)
	store.EXPECT().PutObjectIfNoneMatch(mock.Anything, "bucket", "wal/1/0/write.lock", mock.Anything, mock.Anything, "bucket/wal", "1").Return(nil).Once()
	store.EXPECT().RemoveObject(mock.Anything, "bucket", "wal/1/0/write.lock", "bucket/wal", "1").Return(nil).Once()
	uploadStarted, allowPersistence := make(chan struct{}), make(chan struct{})
	releaseUpload := sync.OnceFunc(func() { close(allowPersistence) })
	store.On("PutObjectIfNoneMatch", mock.Anything, "bucket", "wal/1/0/0.blk", mock.Anything, mock.Anything, "bucket/wal", "1").
		Run(func(mock.Arguments) { close(uploadStarted); <-allowPersistence }).Return(nil).Once()

	writer, err := objectstorage.NewMinioFileWriter(ctx, "bucket", "wal", 1, 0, store, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		releaseUpload()
		closeCtx, done := context.WithTimeout(context.Background(), 3*time.Second)
		defer done()
		require.NoError(t, writer.Close(closeCtx))
	})
	ack := channel.NewLocalResultChannel("1/0/0")
	_, err = writer.WriteDataAsync(ctx, 0, []byte("entry"), ack)
	require.NoError(t, err)
	require.NoError(t, writer.Sync(ctx))
	select {
	case <-uploadStarted:
	case <-ctx.Done():
		t.Fatal("WAL upload did not start while diagnostic output was blocked")
	}
	waiting, stopWaiting := context.WithTimeout(ctx, 30*time.Millisecond)
	_, err = ack.ReadResult(waiting)
	stopWaiting()
	require.ErrorIs(t, err, context.DeadlineExceeded, "the write has not reached storage yet")
	releaseUpload()
	result, err := ack.ReadResult(ctx)
	require.NoError(t, err)
	require.NoError(t, result.Err)
	require.EqualValues(t, 0, result.SyncedId)
}
