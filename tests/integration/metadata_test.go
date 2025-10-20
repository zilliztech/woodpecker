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
	metaProvider := meta.NewMetadataProvider(context.Background(), etcdCli1, 10000)
	defer metaProvider.Close()
	metaProvider2 := meta.NewMetadataProvider(context.Background(), etcdCli2, 10000)
	defer metaProvider2.Close()
	_ = metaProvider.InitIfNecessary(context.TODO())
	exists, err := metaProvider.CheckExists(context.Background(), logName)
	assert.NoError(t, err)
	assert.False(t, exists)

	// lock success
	se, getLockErr := metaProvider.AcquireLogWriterLock(context.Background(), logName)
	assert.NoError(t, getLockErr)
	assert.NotNil(t, se)

	// reentrant lock success
	se, getLockErr = metaProvider.AcquireLogWriterLock(context.Background(), logName)
	assert.NoError(t, getLockErr)
	assert.NotNil(t, se)

	// get lock fail from another session
	se, getLockErr = metaProvider2.AcquireLogWriterLock(context.Background(), logName)
	assert.Error(t, getLockErr)
	assert.Nil(t, se)

	// release lock
	releaseLockErr := metaProvider.ReleaseLogWriterLock(context.Background(), logName)
	assert.NoError(t, releaseLockErr)

	// get lock success from another session after release by the first session
	se, getLockErr = metaProvider2.AcquireLogWriterLock(context.Background(), logName)
	assert.NoError(t, getLockErr)
	assert.NotNil(t, se)

	// session 1 try again
	se, getLockErr = metaProvider.AcquireLogWriterLock(context.Background(), logName)
	assert.Error(t, getLockErr)
	assert.Nil(t, se)

	// session 2 close
	closeErr := metaProvider2.Close()
	assert.NoError(t, closeErr)

	// session 1 try lock success
	se, getLockErr = metaProvider.AcquireLogWriterLock(context.Background(), logName)
	assert.NoError(t, getLockErr)
	assert.NotNil(t, se)
}
