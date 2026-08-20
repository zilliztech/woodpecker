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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
)

// Regression tests for milvus-io/milvus#52341: reader temp info used to be
// created with a fixed-TTL lease that nothing ever renewed (an etcd Put with
// WithLease does NOT refresh the lease TTL), so every reader older than the
// TTL silently lost its metadata and every subsequent UpdateReaderTempInfo
// failed with "reader temp info not found" forever. The metadata protects
// not-yet-consumed truncated segments from cleanup, so losing it silently
// disables that protection for lagging readers.

type readerSessionTestEnv struct {
	etcdCli    *clientv3.Client
	provider   MetadataProvider
	logId      int64
	readerName string
	readerKey  string
	// session is what an open reader holds as a field for its whole lifetime
	session ReaderTempInfoSession
}

func setupReaderSessionTest(t *testing.T, logName string, readerName string, fromSegmentId int64, fromEntryId int64) *readerSessionTestEnv {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	_, err = etcdCli.Delete(context.Background(), LegacyServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	require.NoError(t, provider.InitIfNecessary(context.Background()))

	require.NoError(t, provider.CreateLog(context.Background(), logName))
	logMeta, err := provider.GetLogMeta(context.Background(), logName)
	require.NoError(t, err)
	logId := logMeta.Metadata.LogId

	session, err := provider.CreateReaderTempInfo(context.Background(), readerName, logId, fromSegmentId, fromEntryId)
	require.NoError(t, err)
	require.NotNil(t, session)
	require.Equal(t, logId, session.LogId())
	require.Equal(t, readerName, session.ReaderName())
	require.True(t, session.IsActive())

	return &readerSessionTestEnv{
		etcdCli:    etcdCli,
		provider:   provider,
		logId:      logId,
		readerName: readerName,
		readerKey:  legacyKeyBuilder().BuildLogReaderTempInfoKey(logId, readerName),
		session:    session,
	}
}

// An open reader that reads nothing (idle or slow consumer) must keep its temp
// info alive well past the lease TTL. This is exactly the reader the cleanup
// protection exists for.
func testReaderTempInfoOutlivesLeaseTTLWhileIdle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-TTL idle wait in short mode")
	}
	restoreTTL := readerTempInfoSessionTTLSeconds
	readerTempInfoSessionTTLSeconds = 5
	defer func() { readerTempInfoSessionTTLSeconds = restoreTTL }()

	env := setupReaderSessionTest(t, "reader_session_idle_test_"+time.Now().Format("20060102150405"), "idle-reader", 1, 10)
	defer env.provider.Close()

	// Wait far past the lease TTL without any update activity.
	time.Sleep(time.Duration(readerTempInfoSessionTTLSeconds)*time.Second*5/2 + time.Second)

	info, err := env.provider.GetReaderTempInfo(context.Background(), env.logId, env.readerName)
	require.NoError(t, err, "idle reader temp info must not expire while the reader is open")
	assert.Equal(t, int64(1), info.RecentReadSegmentId)
	assert.Equal(t, int64(10), info.RecentReadEntryId)

	// The key must still be protected by a live lease (auto-cleanup on crash).
	resp, err := env.etcdCli.Get(context.Background(), env.readerKey)
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.Kvs))
	require.NotEqual(t, int64(0), resp.Kvs[0].Lease, "key must stay lease-protected")
	ttlResp, err := env.etcdCli.TimeToLive(context.Background(), clientv3.LeaseID(resp.Kvs[0].Lease))
	require.NoError(t, err)
	assert.Greater(t, ttlResp.TTL, int64(0), "lease must still be alive")
}

// If the lease is lost anyway (e.g. etcd partition outliving the TTL), the next
// update from the still-open reader must recreate the temp info instead of
// failing forever.
func testUpdateReaderTempInfoSelfHealsAfterLeaseLoss(t *testing.T) {
	env := setupReaderSessionTest(t, "reader_session_heal_test_"+time.Now().Format("20060102150405"), "healing-reader", 2, 20)
	defer env.provider.Close()

	// Simulate lease expiry: revoke it so etcd removes the key.
	resp, err := env.etcdCli.Get(context.Background(), env.readerKey)
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.Kvs))
	_, err = env.etcdCli.Revoke(context.Background(), clientv3.LeaseID(resp.Kvs[0].Lease))
	require.NoError(t, err)
	gone, err := env.etcdCli.Get(context.Background(), env.readerKey)
	require.NoError(t, err)
	require.Equal(t, 0, len(gone.Kvs), "key should vanish with the revoked lease")

	// The reader is still open; its next progress update must self-heal.
	err = env.provider.UpdateReaderTempInfo(context.Background(), env.session, 7, 77)
	require.NoError(t, err, "update must recreate reader temp info lost with its lease")

	info, err := env.provider.GetReaderTempInfo(context.Background(), env.logId, env.readerName)
	require.NoError(t, err)
	assert.Equal(t, int64(7), info.RecentReadSegmentId)
	assert.Equal(t, int64(77), info.RecentReadEntryId)
	assert.Equal(t, int64(2), info.OpenSegmentId, "open position from creation must be preserved")
	assert.Equal(t, int64(20), info.OpenEntryId, "open position from creation must be preserved")

	// The recreated key must be protected by a live lease again.
	resp, err = env.etcdCli.Get(context.Background(), env.readerKey)
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.Kvs))
	require.NotEqual(t, int64(0), resp.Kvs[0].Lease)
	ttlResp, err := env.etcdCli.TimeToLive(context.Background(), clientv3.LeaseID(resp.Kvs[0].Lease))
	require.NoError(t, err)
	assert.Greater(t, ttlResp.TTL, int64(0))
}

// A late update racing with reader close must not resurrect the temp info:
// a resurrected key would pin the cleanup low-watermark forever.
func testUpdateAfterDeleteDoesNotResurrectReaderTempInfo(t *testing.T) {
	env := setupReaderSessionTest(t, "reader_session_close_test_"+time.Now().Format("20060102150405"), "closed-reader", 3, 30)
	defer env.provider.Close()

	require.NoError(t, env.provider.DeleteReaderTempInfo(context.Background(), env.session))

	err := env.provider.UpdateReaderTempInfo(context.Background(), env.session, 5, 50)
	require.Error(t, err, "update after close must not recreate the temp info")
	assert.Contains(t, err.Error(), "reader temp info not found")

	resp, err := env.etcdCli.Get(context.Background(), env.readerKey)
	require.NoError(t, err)
	assert.Equal(t, 0, len(resp.Kvs), "closed reader temp info must stay deleted")
}

// A Put that fails for a reason unrelated to the lease (caller cancelled, etcd
// slow) must not rotate the session: closing a session revokes its lease, and
// etcd then deletes every key attached to it — including the live reader's own
// temp info.
func testUpdatePutFailureDoesNotRevokeReaderTempInfoLease(t *testing.T) {
	env := setupReaderSessionTest(t, "reader_session_putfail_test_"+time.Now().Format("20060102150405"), "putfail-reader", 4, 40)
	defer env.provider.Close()

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err := env.provider.UpdateReaderTempInfo(cancelledCtx, env.session, 5, 50)
	require.Error(t, err, "update with a dead caller context must fail")

	resp, err := env.etcdCli.Get(context.Background(), env.readerKey)
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.Kvs), "an unrelated Put failure must not delete the reader temp info")
	require.NotEqual(t, int64(0), resp.Kvs[0].Lease)
	ttlResp, err := env.etcdCli.TimeToLive(context.Background(), clientv3.LeaseID(resp.Kvs[0].Lease))
	require.NoError(t, err)
	assert.Greater(t, ttlResp.TTL, int64(0), "the healthy lease must not be revoked")

	// The reader keeps working once the caller context is healthy again.
	require.NoError(t, env.provider.UpdateReaderTempInfo(context.Background(), env.session, 6, 60))
}

// A reader still holding its session after the provider closed must not
// resurrect its temp info either: provider Close() stops every keepalive, and
// a recreated one would be tracked by nothing and outlive the process's
// metadata layer, pinning the truncated-segment cleanup low-watermark forever.
func testUpdateAfterProviderCloseDoesNotResurrectReaderTempInfo(t *testing.T) {
	env := setupReaderSessionTest(t, "reader_session_provclose_test_"+time.Now().Format("20060102150405"), "orphan-reader", 5, 50)

	require.NoError(t, env.provider.Close())
	assert.False(t, env.session.IsActive(), "provider close must retire the reader session")

	err := env.provider.UpdateReaderTempInfo(context.Background(), env.session, 6, 66)
	require.Error(t, err, "an update after provider close must not resurrect the key")
	assert.Contains(t, err.Error(), "reader temp info not found")

	resp, err := env.etcdCli.Get(context.Background(), env.readerKey)
	require.NoError(t, err)
	assert.Equal(t, 0, len(resp.Kvs), "the retired reader temp info must stay deleted")

	// A reader closing after the provider did is a no-op, not an error
	require.NoError(t, env.provider.DeleteReaderTempInfo(context.Background(), env.session))
}

// A session this provider never handed out carries no ownership proof and must
// be refused rather than adopted: adopting one would let any caller write (and
// keep alive) reader temp info for a reader that is gone.
func testReaderTempInfoRejectsForeignSession(t *testing.T) {
	env := setupReaderSessionTest(t, "reader_session_foreign_test_"+time.Now().Format("20060102150405"), "foreign-reader", 6, 60)
	defer env.provider.Close()

	err := env.provider.UpdateReaderTempInfo(context.Background(), nil, 7, 70)
	require.Error(t, err, "update with a nil session must be refused")
	err = env.provider.DeleteReaderTempInfo(context.Background(), nil)
	require.Error(t, err, "delete with a nil session must be refused")

	err = env.provider.UpdateReaderTempInfo(context.Background(), foreignReaderTempInfoSession{logId: env.logId, readerName: env.readerName}, 7, 70)
	require.Error(t, err, "update with a foreign session type must be refused")

	// The real reader is untouched by any of it
	info, err := env.provider.GetReaderTempInfo(context.Background(), env.logId, env.readerName)
	require.NoError(t, err)
	assert.Equal(t, int64(6), info.RecentReadSegmentId)
	assert.True(t, env.session.IsActive())
}

// foreignReaderTempInfoSession implements the interface without coming from
// this provider, standing in for a session handed out by another provider (or
// another process). Even when it names a real reader it carries no ownership.
type foreignReaderTempInfoSession struct {
	logId      int64
	readerName string
}

func (s foreignReaderTempInfoSession) LogId() int64       { return s.logId }
func (s foreignReaderTempInfoSession) ReaderName() string { return s.readerName }
func (foreignReaderTempInfoSession) IsActive() bool       { return true }

// CreateReaderTempInfo must fail within the caller's deadline when etcd is
// unreachable instead of blocking on the etcd client's own lifetime context.
func testCreateReaderTempInfoHonorsCallerDeadlineWhenEtcdUnreachable(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:1"},
		DialTimeout: time.Second,
	})
	require.NoError(t, err)
	defer cli.Close()

	cfg := testMetaCfg(t)
	cfg.Etcd.RequestTimeout = config.DurationMilliseconds{Duration: config.NewDuration(time.Second, time.Millisecond)}
	provider := NewMetadataProvider(context.Background(), cli, cfg)
	defer provider.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, createErr := provider.CreateReaderTempInfo(ctx, "unreachable-reader", 1, 0, 0)
		done <- createErr
	}()
	select {
	case createErr := <-done:
		require.Error(t, createErr, "create must fail when etcd is unreachable")
	case <-time.After(15 * time.Second):
		t.Fatal("CreateReaderTempInfo ignored the caller's deadline while etcd is unreachable")
	}
}
