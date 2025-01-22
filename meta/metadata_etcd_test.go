package meta

import (
	"context"
	"github.com/milvus-io/woodpecker/common/etcd"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEtcdMeta(t *testing.T) {
	err := etcd.InitEtcdServer(true, "", "/tmp/data", "stdout", "info")
	assert.NoError(t, err)
	defer etcd.StopEtcdServer()

	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	assert.NoError(t, err)
	metadata := NewMetadataProviderEtcd(context.TODO(), etcdCli)
	err = metadata.InitIfNecessary(context.TODO())
	assert.NoError(t, err)
}
