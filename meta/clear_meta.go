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
	"time"

	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

// ClearMeta removes an instance's metadata so it can be started again from empty.
//
// Everything that describes content — logs, segments, quorums, node registrations, reader
// sessions, cleanup and compacted-mark records — is removed unconditionally. The four
// instance-level keys are handled by clearLogIdGen:
//
//	false (the safe default, use ClearMetaExceptLogIdGen)
//	    logidgen keeps its value; instance/version/quorumidgen are re-seeded.
//	true
//	    all four are re-seeded, so logIds restart at 1.
//
// Why logidgen is special: logId appears in the data paths of both storage tiers —
// {minio.rootPath}/{logId}/{segmentId}/… in object storage and
// {storage.root}/{bucket}/{rootPath}/{logId}/ on node disks. Restarting the counter makes a
// new log reuse a directory that an old log may still have objects in, and residue cannot be
// ruled out: a node may be permanently down when its log is deleted, a node holding data for
// an already-truncated segment is not even discoverable from metadata, and nodes get replaced.
// Keeping the counter monotonic makes collisions structurally impossible instead of relying
// on a deletion having been complete.
//
// Clear it only when the instance's object storage is known to be empty — typically because
// every log was deleted synchronously first — or when the instance is being abandoned and
// nothing will ever be restored onto the same bucket/rootPath.
//
// The instance-level keys are re-seeded rather than deleted, never dropped, so the cluster is
// never observed with fewer than four of them and InitIfNecessary always sees something it can
// work with.
//
// Idempotency and interruption: every step is either a prefix delete (a no-op once the prefix
// is empty), an unconditional overwrite, or a create-if-absent, so re-running is safe and an
// interrupted run resumes simply by being called again. An interruption cannot leave a state
// that fails to start either: the content prefixes are cleared before the instance keys are
// touched, and those keys are only ever overwritten, so a run cut short anywhere leaves a
// cluster that is at worst partially emptied — never partially initialised.
//
// The one value that is not stable across runs is the instance UUID, which is regenerated each
// time. Nothing reads it for behaviour; it exists as an "initialised" marker and for legacy
// prefix detection.
func (e *metadataProviderEtcd) ClearMeta(ctx context.Context, clearLogIdGen bool) error {
	// Shared with CreateLog: both write the instance-level keys, the log id
	// generator among them.
	e.instanceMu.Lock()
	defer e.instanceMu.Unlock()
	start := time.Now()
	log := logger.Ctx(ctx)

	// Content prefixes. Order does not matter: nothing here is read after the instance-level
	// keys are re-seeded, and a partial failure is retried by simply running again.
	prefixes := []string{
		e.keyBuilder.LogsPrefix(),
		e.keyBuilder.QuorumsPrefix(),
		e.keyBuilder.NodesPrefix(),
		e.keyBuilder.ReaderTempInfoPrefix(),
		e.keyBuilder.SegmentCleanupStatusPrefix(),
		e.keyBuilder.SegmentCompactedNotifyStatusPrefix(),
		e.keyBuilder.LogDeletedPrefix(),
	}
	for _, prefix := range prefixes {
		ctxDel, cancel := e.getContextWithTimeout(ctx)
		_, err := e.client.Delete(ctxDel, prefix, clientv3.WithPrefix())
		cancel()
		if err != nil {
			log.Warn("ClearMeta: failed to delete prefix", zap.String("prefix", prefix), zap.Error(err))
			return werr.ErrMetadataWrite.WithCauseErr(err)
		}
	}

	ctxDel, cancelDel := e.getContextWithTimeout(ctx)
	_, err := e.client.Delete(ctxDel, e.keyBuilder.ConditionWriteKey())
	cancelDel()
	if err != nil {
		return werr.ErrMetadataWrite.WithCauseErr(err)
	}

	// Re-seed the instance-level keys in one transaction so InitIfNecessary never observes a
	// partially initialised cluster.
	versionValue, marshalErr := pb.Marshal(&proto.Version{
		Major: VersionMajor, Minor: VersionMinor, Patch: VersionPatch,
	})
	if marshalErr != nil {
		return werr.ErrMetadataEncode.WithCauseErr(marshalErr)
	}
	ops := []clientv3.Op{
		clientv3.OpPut(e.keyBuilder.ServiceInstanceKey(), uuid.New().String()),
		clientv3.OpPut(e.keyBuilder.VersionKey(), string(versionValue)),
		clientv3.OpPut(e.keyBuilder.QuorumIdGeneratorKey(), "0"),
	}
	if clearLogIdGen {
		ops = append(ops, clientv3.OpPut(e.keyBuilder.LogIdGeneratorKey(), "0"))
	}
	ctxTxn, cancelTxn := e.getContextWithTimeout(ctx)
	resp, txnErr := e.client.Txn(ctxTxn).If().Then(ops...).Commit()
	cancelTxn()
	if txnErr != nil || !resp.Succeeded {
		log.Warn("ClearMeta: failed to re-seed instance keys", zap.Error(txnErr))
		return werr.ErrMetadataWrite.WithCauseErrMsg("failed to re-seed instance-level keys")
	}

	// A preserved logidgen must still exist, otherwise the cluster is left partially
	// initialised. Seed it only when it is genuinely absent — a never-initialised instance.
	//
	// Create-if-absent, not read-then-write: a Get followed by a Put would race a concurrent
	// CreateLog that set the counter in between, and writing "0" over it is exactly the
	// regression this whole path exists to prevent. The compare makes the write a no-op the
	// moment the key exists.
	if !clearLogIdGen {
		key := e.keyBuilder.LogIdGeneratorKey()
		ctxSeed, cancelSeed := e.getContextWithTimeout(ctx)
		_, seedErr := e.client.Txn(ctxSeed).
			If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, "0")).
			Commit()
		cancelSeed()
		if seedErr != nil {
			return werr.ErrMetadataWrite.WithCauseErr(seedErr)
		}
	}

	log.Info("ClearMeta done",
		zap.String("prefix", e.keyBuilder.Prefix()),
		zap.Bool("clearLogIdGen", clearLogIdGen),
		zap.Duration("elapsed", time.Since(start)))
	return nil
}
