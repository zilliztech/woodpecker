package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/meta"
)

func TestLogWriterLock(t *testing.T) {
	logName := "test_log_for_metadata_it"
	etcdCli1, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	etcdCli2, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	assert.NoError(t, err)
	metaProvider := meta.NewMetadataProvider(context.Background(), etcdCli1)
	defer metaProvider.Close()
	metaProvider2 := meta.NewMetadataProvider(context.Background(), etcdCli2)
	defer metaProvider2.Close()
	_ = metaProvider.InitIfNecessary(context.TODO())
	exists, err := metaProvider.CheckExists(context.Background(), logName)
	assert.NoError(t, err)
	assert.False(t, exists)

	// lock success
	getLockErr := metaProvider.AcquireLogWriterLock(context.Background(), logName)
	assert.NoError(t, getLockErr)

	// reentrant lock success
	getLockErr = metaProvider.AcquireLogWriterLock(context.Background(), logName)
	assert.NoError(t, getLockErr)

	// get lock fail from another session
	getLockErr = metaProvider2.AcquireLogWriterLock(context.Background(), logName)
	assert.Error(t, getLockErr)

	// release lock
	releaseLockErr := metaProvider.ReleaseLogWriterLock(context.Background(), logName)
	assert.NoError(t, releaseLockErr)

	// get lock success from another session after release by the first session
	getLockErr = metaProvider2.AcquireLogWriterLock(context.Background(), logName)
	assert.NoError(t, getLockErr)

	// session 1 try again
	getLockErr = metaProvider.AcquireLogWriterLock(context.Background(), logName)
	assert.Error(t, getLockErr)

	// session 2 close
	closeErr := metaProvider2.Close()
	assert.NoError(t, closeErr)

	// session 1 try lock success
	getLockErr = metaProvider.AcquireLogWriterLock(context.Background(), logName)
	assert.NoError(t, getLockErr)
}
