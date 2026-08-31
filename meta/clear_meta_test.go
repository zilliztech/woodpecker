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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/common/etcd"
)

// resp builds the four Get responses InitIfNecessary inspects, in its key order:
// instance, version, logidgen, quorumidgen. A true entry means the key exists.
func resp(present ...bool) []*etcdserverpb.ResponseOp {
	out := make([]*etcdserverpb.ResponseOp, 0, len(present))
	for _, p := range present {
		r := &etcdserverpb.RangeResponse{}
		if p {
			r.Kvs = []*mvccpb.KeyValue{{Key: []byte("k"), Value: []byte("v")}}
		}
		out = append(out, &etcdserverpb.ResponseOp{
			Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: r},
		})
	}
	return out
}

// TestOnlyLogIdGenSurvives pins the exception that makes ClearMetaExceptLogIdGen usable: a
// cleared instance keeps exactly one of the four instance-level keys, and startup must accept
// that. Every other partial combination stays an error — it means something outside ClearMeta
// wrote or removed these keys, and guessing at the intent is worse than refusing.
func TestOnlyLogIdGenSurvives(t *testing.T) {
	// instance, version, logidgen, quorumidgen
	assert.True(t, onlyLogIdGenSurvives(resp(false, false, true, false)),
		"the state ClearMetaExceptLogIdGen leaves behind must be accepted")

	assert.False(t, onlyLogIdGenSurvives(resp(false, false, false, false)), "nothing present is a fresh cluster")
	assert.False(t, onlyLogIdGenSurvives(resp(true, true, true, true)), "all present is an initialised cluster")
	assert.False(t, onlyLogIdGenSurvives(resp(true, false, true, false)), "instance also present")
	assert.False(t, onlyLogIdGenSurvives(resp(false, true, true, false)), "version also present")
	assert.False(t, onlyLogIdGenSurvives(resp(false, false, true, true)), "quorumidgen also present")
	assert.False(t, onlyLogIdGenSurvives(resp(true, false, false, false)), "only instance present")

	assert.False(t, onlyLogIdGenSurvives(resp(false, false, true)), "short response is not trusted")
	assert.False(t, onlyLogIdGenSurvives(nil), "empty response is not trusted")
}

// ── integration: idempotency and interruption ──────────────────────────────────

func setupClearMetaTest(t *testing.T) (MetadataProvider, *clientv3.Client, *KeyBuilder) {
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	_, err = etcdCli.Delete(context.Background(), LegacyServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	require.NoError(t, provider.InitIfNecessary(context.Background()))
	return provider, etcdCli, NewKeyBuilder(LegacyServicePrefix)
}

func readKey(t *testing.T, cli *clientv3.Client, key string) (string, bool) {
	t.Helper()
	resp, err := cli.Get(context.Background(), key)
	require.NoError(t, err)
	if len(resp.Kvs) == 0 {
		return "", false
	}
	return string(resp.Kvs[0].Value), true
}

func countPrefix(t *testing.T, cli *clientv3.Client, prefix string) int64 {
	t.Helper()
	resp, err := cli.Get(context.Background(), prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	require.NoError(t, err)
	return resp.Count
}

// TestClearMetaExceptLogIdGen_PreservesCounterAndIsRepeatable is the property the whole design
// rests on: content goes away, the log id counter does not, and running it again changes
// nothing further. A counter that reset here would let a new log reuse a directory an old log
// may still hold objects in.
func testClearMetaPreservesCounterAndIsRepeatable(t *testing.T) {
	ctx := context.Background()
	provider, cli, kb := setupClearMetaTest(t)

	require.NoError(t, provider.CreateLog(ctx, "log-a"))
	require.NoError(t, provider.CreateLog(ctx, "log-b"))
	counterBefore, ok := readKey(t, cli, kb.LogIdGeneratorKey())
	require.True(t, ok)
	require.NotEqual(t, "0", counterBefore, "creating logs must have advanced the counter")

	require.NoError(t, provider.ClearMeta(ctx, false))

	assert.Zero(t, countPrefix(t, cli, kb.LogsPrefix()), "logs must be gone")
	counterAfter, ok := readKey(t, cli, kb.LogIdGeneratorKey())
	require.True(t, ok, "logidgen must survive")
	assert.Equal(t, counterBefore, counterAfter, "the counter must not move")

	for _, key := range []string{kb.ServiceInstanceKey(), kb.VersionKey(), kb.QuorumIdGeneratorKey()} {
		_, present := readKey(t, cli, key)
		assert.True(t, present, "instance-level key must be re-seeded, not dropped: %s", key)
	}

	// Repeat: an already-clear instance is a no-op, and the counter still holds.
	require.NoError(t, provider.ClearMeta(ctx, false))
	counterAgain, ok := readKey(t, cli, kb.LogIdGeneratorKey())
	require.True(t, ok)
	assert.Equal(t, counterBefore, counterAgain)

	// And the instance is usable again: the next log must get an id above the old watermark,
	// which is the whole point of preserving the counter.
	require.NoError(t, provider.CreateLog(ctx, "log-c"))
	newMeta, err := provider.GetLogMeta(ctx, "log-c")
	require.NoError(t, err)
	previous, convErr := strconv.ParseInt(counterBefore, 10, 64)
	require.NoError(t, convErr)
	assert.Greater(t, newMeta.Metadata.GetLogId(), previous,
		"a log created after the clear must not reuse an id an earlier log already had")
}

// TestClearMeta_ResumesAfterInterruption simulates a run cut short partway through: some
// content prefixes deleted, the instance keys not yet touched. Calling it again must finish
// the job, and the interrupted state must not be one the node refuses to start from.
func testClearMetaResumesAfterInterruption(t *testing.T) {
	ctx := context.Background()
	provider, cli, kb := setupClearMetaTest(t)

	require.NoError(t, provider.CreateLog(ctx, "log-a"))
	counterBefore, _ := readKey(t, cli, kb.LogIdGeneratorKey())

	// Interrupt: only the logs prefix was removed before the process died.
	_, err := cli.Delete(ctx, kb.LogsPrefix(), clientv3.WithPrefix())
	require.NoError(t, err)

	// That half-done state must still be startable — the instance keys were never dropped.
	require.NoError(t, provider.InitIfNecessary(ctx))

	// Resuming completes the clear and leaves the counter alone.
	require.NoError(t, provider.ClearMeta(ctx, false))
	counterAfter, ok := readKey(t, cli, kb.LogIdGeneratorKey())
	require.True(t, ok)
	assert.Equal(t, counterBefore, counterAfter)
	assert.Zero(t, countPrefix(t, cli, kb.LogsPrefix()))
}

// TestClearMeta_OnlyLogIdGenLeft_StillStarts covers the state ClearMetaExceptLogIdGen is
// allowed to leave if the re-seed transaction itself is interrupted: one instance-level key
// present, three absent. Without the InitIfNecessary exception this is the state that makes a
// node refuse to start after a legitimate clear.
func testClearMetaOnlyLogIdGenLeftStillStarts(t *testing.T) {
	ctx := context.Background()
	provider, cli, kb := setupClearMetaTest(t)
	require.NoError(t, provider.CreateLog(ctx, "log-a"))
	counterBefore, _ := readKey(t, cli, kb.LogIdGeneratorKey())

	for _, key := range []string{kb.ServiceInstanceKey(), kb.VersionKey(), kb.QuorumIdGeneratorKey()} {
		_, err := cli.Delete(ctx, key)
		require.NoError(t, err)
	}

	assert.NoError(t, provider.InitIfNecessary(ctx),
		"a cluster left with only logidgen must still initialise")
	counterAfter, ok := readKey(t, cli, kb.LogIdGeneratorKey())
	require.True(t, ok)
	assert.Equal(t, counterBefore, counterAfter, "initialising must not reset the counter")
}

// TestClearMeta_WithLogIdGen_ResetsCounter is the opposite corner: the dangerous form does what
// it says, so callers who ask for it get a genuinely fresh instance.
func testClearMetaWithLogIdGenResetsCounter(t *testing.T) {
	ctx := context.Background()
	provider, cli, kb := setupClearMetaTest(t)
	require.NoError(t, provider.CreateLog(ctx, "log-a"))

	require.NoError(t, provider.ClearMeta(ctx, true))
	counter, ok := readKey(t, cli, kb.LogIdGeneratorKey())
	require.True(t, ok)
	assert.Equal(t, "0", counter)
}
