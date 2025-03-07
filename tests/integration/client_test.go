package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/woodpecker"
)

func TestOpenWriterMultiTimesInSingleClient(t *testing.T) {
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
	assert.NoError(t, err)

	// CreateLog if not exists
	createErr := client.CreateLog(context.Background(), "test_log_single")
	if createErr != nil {
		assert.True(t, werr.ErrLogAlreadyExists.Is(createErr))
	}
	logHandle, openErr := client.OpenLog(context.Background(), "test_log_single")
	assert.NoError(t, openErr)

	logWriter1, openWriterErr1 := logHandle.OpenLogWriter(context.Background())
	assert.NoError(t, openWriterErr1)
	assert.NotNil(t, logWriter1)
	logWriter2, openWriterErr2 := logHandle.OpenLogWriter(context.Background())
	assert.NoError(t, openWriterErr2)
	assert.NotNil(t, logWriter2)
}

func TestOpenWriterMultiTimesInMultiClient(t *testing.T) {
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	client1, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
	assert.NoError(t, err)
	client2, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
	assert.NoError(t, err)

	createErr := client1.CreateLog(context.Background(), "test_log_multi")
	if createErr != nil {
		assert.True(t, werr.ErrLogAlreadyExists.Is(createErr))
	}

	logHandle1, openErr := client1.OpenLog(context.Background(), "test_log_multi")
	assert.NoError(t, openErr)
	logHandle2, openErr := client2.OpenLog(context.Background(), "test_log_multi")
	assert.NoError(t, openErr)

	// client1 get writer, client2 get fail
	logWriter1, openWriterErr1 := logHandle1.OpenLogWriter(context.Background())
	assert.NoError(t, openWriterErr1)
	assert.NotNil(t, logWriter1)
	logWriter2, openWriterErr2 := logHandle2.OpenLogWriter(context.Background())
	assert.Error(t, openWriterErr2)
	assert.Nil(t, logWriter2)

	// client1 release, client 2 get writer
	releaseErr := logWriter1.Close(context.Background())
	assert.NoError(t, releaseErr)
	logWriter3, openWriterErr3 := logHandle2.OpenLogWriter(context.Background())
	assert.NoError(t, openWriterErr3)
	assert.NotNil(t, logWriter3)
}
