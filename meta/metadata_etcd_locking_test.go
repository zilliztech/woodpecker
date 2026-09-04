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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/proto"
)

// A single provider is shared by every log a client hosts, so any lock it holds
// across an etcd round-trip serializes metadata writes for all of them: one slow
// write stalls every log, not just the one that issued it. These two tests pin
// the property that replaced that lock.

func newLockingTestProvider(t *testing.T) (MetadataProvider, *metadataProviderEtcd) {
	t.Helper()
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	require.NotNil(t, etcdCli)
	_, err = etcdCli.Delete(context.Background(), LegacyServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	require.NoError(t, provider.InitIfNecessary(context.Background()))
	return provider, provider.(*metadataProviderEtcd)
}

// testSegmentMetadataWritesTakeNoProviderLock holds every mutex the provider has
// and requires each per-log metadata write to complete anyway.
//
// The struct has exactly two, so holding both is an exhaustive statement: no
// per-log write sits behind provider-level state. That is what makes a
// regression detectable here — re-introducing a shared lock on any of these four
// methods, whichever mutex it reached for, deadlocks this test rather than
// quietly restoring the old blast radius.
func testSegmentMetadataWritesTakeNoProviderLock(t *testing.T) {
	provider, e := newLockingTestProvider(t)
	ctx := context.Background()

	logName := "test_log_no_provider_lock" + time.Now().Format("20060102150405")
	require.NoError(t, provider.CreateLog(ctx, logName))
	logMeta, err := provider.GetLogMeta(ctx, logName)
	require.NoError(t, err)
	logId := logMeta.Metadata.LogId

	// Stored before the locks are taken, so Update and Delete below have a
	// subject that already exists.
	existing := &SegmentMeta{Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active}}
	require.NoError(t, provider.StoreSegmentMetadata(ctx, logName, logId, existing))

	e.stateMu.Lock()
	e.instanceMu.Lock()
	defer func() {
		e.instanceMu.Unlock()
		e.stateMu.Unlock()
	}()

	mustFinish := func(name string, fn func() error) {
		t.Helper()
		done := make(chan error, 1)
		go func() { done <- fn() }()
		select {
		case callErr := <-done:
			assert.NoError(t, callErr, name)
		case <-time.After(20 * time.Second):
			t.Fatalf("%s blocked while a provider mutex was held", name)
		}
	}

	mustFinish("StoreSegmentMetadata", func() error {
		return provider.StoreSegmentMetadata(ctx, logName, logId,
			&SegmentMeta{Metadata: &proto.SegmentMetadata{SegNo: 2, State: proto.SegmentState_Active}})
	})
	mustFinish("UpdateSegmentMetadata", func() error {
		existing.Metadata.State = proto.SegmentState_Completed
		return provider.UpdateSegmentMetadata(ctx, logName, logId, existing, proto.SegmentState_Active)
	})
	mustFinish("DeleteSegmentMetadata", func() error {
		return provider.DeleteSegmentMetadata(ctx, logName, logId, 2, proto.SegmentState_Active)
	})
	mustFinish("DeleteLogMetadata", func() error {
		return provider.DeleteLogMetadata(ctx, logName, false)
	})
}

// testConcurrentSegmentMetadataWritesAcrossLogs drives one provider the way a
// client with many logs drives it: every log writing segment metadata at once.
//
// Nothing here is timing sensitive — it asserts outcomes, not speed. Its job is
// to show that the etcd transactions carry the correctness the removed mutex was
// assumed to be providing, and to give the race detector the concurrency it
// needs to say so.
func testConcurrentSegmentMetadataWritesAcrossLogs(t *testing.T) {
	provider, _ := newLockingTestProvider(t)
	ctx := context.Background()

	const logCount = 8
	const segmentsPerLog = 5

	prefix := "test_log_concurrent" + time.Now().Format("20060102150405")
	names := make([]string, logCount)
	logIds := make([]int64, logCount)
	for i := 0; i < logCount; i++ {
		names[i] = fmt.Sprintf("%s_%d", prefix, i)
		require.NoError(t, provider.CreateLog(ctx, names[i]))
		logMeta, err := provider.GetLogMeta(ctx, names[i])
		require.NoError(t, err)
		logIds[i] = logMeta.Metadata.LogId
	}

	// One slot per goroutine: each writes only its own index, so the results
	// need no synchronisation of their own.
	errs := make([]error, logCount)
	var wg sync.WaitGroup
	for i := 0; i < logCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for segNo := 0; segNo < segmentsPerLog; segNo++ {
				segmentMeta := &SegmentMeta{Metadata: &proto.SegmentMetadata{
					SegNo: int64(segNo),
					State: proto.SegmentState_Active,
				}}
				if err := provider.StoreSegmentMetadata(ctx, names[i], logIds[i], segmentMeta); err != nil {
					errs[i] = fmt.Errorf("store seg %d: %w", segNo, err)
					return
				}
				// StoreSegmentMetadata records the revision the update below
				// compares against, so this is a genuine read-modify-write.
				segmentMeta.Metadata.State = proto.SegmentState_Completed
				if err := provider.UpdateSegmentMetadata(ctx, names[i], logIds[i], segmentMeta, proto.SegmentState_Active); err != nil {
					errs[i] = fmt.Errorf("update seg %d: %w", segNo, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "log %s", names[i])
	}

	// Every log must hold exactly the segments its own goroutine wrote, in the
	// state that goroutine left them: no write lost, none applied to a
	// neighbour's log.
	for i := 0; i < logCount; i++ {
		all, err := provider.GetAllSegmentMetadata(ctx, names[i])
		require.NoError(t, err, names[i])
		require.Len(t, all, segmentsPerLog, names[i])
		for segNo, segmentMeta := range all {
			assert.Equal(t, proto.SegmentState_Completed, segmentMeta.Metadata.State,
				"log %s segment %d", names[i], segNo)
		}
	}
}
