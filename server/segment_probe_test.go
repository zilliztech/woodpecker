// Copyright (C) 2025 Zilliz. All rights reserved.
//
// This file is part of the Woodpecker project.
//
// Woodpecker is dual-licensed under the GNU Affero General Public License v3.0
// (AGPLv3) and the Server Side Public License v1 (SSPLv1). You may use this
// file under either license, at your option.
//
// AGPLv3 License: https://www.gnu.org/licenses/agpl-3.0.html
// SSPLv1 License: https://www.mongodb.com/licensing/server-side-public-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the license texts for specific language governing permissions and
// limitations under the licenses.

package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_server/mocks_segment"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/processor"
)

// batchOf builds one read result covering [from, from+count).
func batchOf(from, count int64) *proto.BatchReadResult {
	entries := make([]*proto.LogEntry, 0, count)
	for i := int64(0); i < count; i++ {
		entries = append(entries, &proto.LogEntry{SegId: 3, EntryId: from + i})
	}
	return &proto.BatchReadResult{Entries: entries, LastReadState: &proto.LastReadState{SegmentId: 3}}
}

// TestProbeRead_StopsAtTheCap covers the bound that keeps a probe from turning into a full segment
// scan: the reason for stopping is the cap, which is not a statement about the data.
func TestProbeRead_StopsAtTheCap(t *testing.T) {
	read := func(_ context.Context, from, max int64, _ *proto.LastReadState) (*proto.BatchReadResult, error) {
		return batchOf(from, max), nil
	}
	got := probeRead(context.Background(), read, 100, 250)

	require.Equal(t, int64(100), got.FirstEntry)
	require.Equal(t, int64(349), got.LastEntry)
	require.Equal(t, int64(250), got.EntriesRead, "the cap is a count of entries, not of batches")
	require.Equal(t, probeStopCapReached, got.StopReason)
	require.Empty(t, got.Error)
}

// TestProbeRead_NotYetWrittenIsNotAFault is the distinction the whole command exists for. A tail
// read that finds nothing is the steady state of a caught-up reader, and reporting it as an error
// would make every healthy log look damaged.
func TestProbeRead_NotYetWrittenIsNotAFault(t *testing.T) {
	read := func(_ context.Context, from, _ int64, _ *proto.LastReadState) (*proto.BatchReadResult, error) {
		if from == 100 {
			return batchOf(100, 10), nil
		}
		return nil, werr.ErrEntryNotFound
	}
	got := probeRead(context.Background(), read, 100, 1000)

	require.Equal(t, int64(109), got.LastEntry)
	require.Equal(t, probeStopNotYetWritten, got.StopReason)
	require.Empty(t, got.Error, "nothing failed: the data is not there yet")
}

// TestProbeRead_EndOfSegmentIsNotAFault covers a sealed segment read to its end. It is a different
// answer from "not yet written": nothing more will ever arrive here.
func TestProbeRead_EndOfSegmentIsNotAFault(t *testing.T) {
	read := func(_ context.Context, from, _ int64, _ *proto.LastReadState) (*proto.BatchReadResult, error) {
		if from == 0 {
			return batchOf(0, 42), nil
		}
		return nil, werr.ErrFileReaderEndOfFile
	}
	got := probeRead(context.Background(), read, 0, 1000)

	require.Equal(t, int64(41), got.LastEntry)
	require.Equal(t, probeStopEndOfSegment, got.StopReason)
	require.Empty(t, got.Error)
}

// TestProbeRead_ReportsWhereItBroke covers the case a reader waits on forever: the replica served
// entries and then failed. Both halves matter -- the last entry it could serve, and the failure.
func TestProbeRead_ReportsWhereItBroke(t *testing.T) {
	read := func(_ context.Context, from, _ int64, _ *proto.LastReadState) (*proto.BatchReadResult, error) {
		if from == 0 {
			return batchOf(0, 500), nil
		}
		return nil, fmt.Errorf("crc mismatch in block 7")
	}
	got := probeRead(context.Background(), read, 0, 10_000)

	require.Equal(t, int64(499), got.LastEntry, "what it could serve is as important as the failure")
	require.Equal(t, probeStopError, got.StopReason)
	require.Contains(t, got.Error, "crc mismatch in block 7")
}

// TestProbeRead_FailingOnTheFirstEntryServesNothing covers a replica that cannot serve the starting
// position at all. Reporting entry 0 as served would put a position in the report that no reader
// could obtain.
func TestProbeRead_FailingOnTheFirstEntryServesNothing(t *testing.T) {
	read := func(_ context.Context, _, _ int64, _ *proto.LastReadState) (*proto.BatchReadResult, error) {
		return nil, fmt.Errorf("block index unreadable")
	}
	got := probeRead(context.Background(), read, 4821, 1000)

	require.Equal(t, int64(-1), got.FirstEntry)
	require.Equal(t, int64(-1), got.LastEntry)
	require.Zero(t, got.EntriesRead)
	require.Equal(t, probeStopError, got.StopReason)
}

// TestProbeRead_EmptyBatchIsNotAnInfiniteLoop covers a read that answers without entries and without
// an error. Treating it as progress would spin until the cap with nothing to show.
func TestProbeRead_EmptyBatchIsNotAnInfiniteLoop(t *testing.T) {
	calls := 0
	read := func(_ context.Context, from, _ int64, _ *proto.LastReadState) (*proto.BatchReadResult, error) {
		calls++
		if calls > 5 {
			// Ends a loop that should already have stopped, so a regression fails here instead of
			// running until the test binary's timeout.
			return nil, fmt.Errorf("asked %d times for entries after an empty answer", calls)
		}
		return &proto.BatchReadResult{}, nil
	}
	got := probeRead(context.Background(), read, 7, 1_000_000)

	require.LessOrEqual(t, calls, 2, "an empty answer means there is nothing more to ask for")
	require.Equal(t, int64(-1), got.LastEntry)
	require.Equal(t, probeStopNotYetWritten, got.StopReason)
}

// probeStore builds a logStore holding nothing but the configuration the resolution and source
// helpers read.
func probeStore(t *testing.T, cfg *config.Configuration) *logStore {
	t.Helper()
	return &logStore{cfg: cfg, segmentProcessors: make(map[string]map[int64]processor.SegmentProcessor)}
}

// TestLocalInstancesHoldingSegment_FindsTheInstanceOwningTheDirectory covers a node asked about a
// segment it holds but serves no writer for -- the case that matters, since a stuck reader's
// segment often has no live processor.
func TestLocalInstancesHoldingSegment_FindsTheInstanceOwningTheDirectory(t *testing.T) {
	cfg := serviceModeCfg(t)
	l := probeStore(t, cfg)
	// <root>/<bucket>/<instance root, nested>/<logId>/<segId>
	writeSegmentData(t, filepath.Join(cfg.Woodpecker.Storage.RootPath, "bkt", "inst/a", "7", "3"), "data")

	got := l.localInstancesHoldingSegment(7, 3)

	require.Len(t, got, 1)
	require.Equal(t, "bkt", got[0].bucket)
	require.Equal(t, "inst/a", got[0].rootPath, "an instance root with a slash in it is still one root")
}

// TestLocalInstancesHoldingSegment_DoesNotMatchASegmentOfAnotherLog keeps a directory named like the
// segment under a different log from being taken for this one.
func TestLocalInstancesHoldingSegment_DoesNotMatchASegmentOfAnotherLog(t *testing.T) {
	cfg := serviceModeCfg(t)
	l := probeStore(t, cfg)
	writeSegmentData(t, filepath.Join(cfg.Woodpecker.Storage.RootPath, "bkt", "inst", "8", "3"), "data")

	require.Empty(t, l.localInstancesHoldingSegment(7, 3))
}

// TestResolveProbeInstance_AmbiguityIsReportedNotPicked covers two instances holding the same log
// and segment id. Picking one would report another instance's data under the name of this one.
func TestResolveProbeInstance_AmbiguityIsReportedNotPicked(t *testing.T) {
	cfg := serviceModeCfg(t)
	l := probeStore(t, cfg)
	writeSegmentData(t, filepath.Join(cfg.Woodpecker.Storage.RootPath, "bkt-a", "inst", "7", "3"), "data")
	writeSegmentData(t, filepath.Join(cfg.Woodpecker.Storage.RootPath, "bkt-b", "inst", "7", "3"), "data")

	_, _, err := l.resolveProbeInstance(SegmentProbeRequest{LogID: 7, SegmentID: 3})

	require.Error(t, err)
	require.Contains(t, err.Error(), "bkt-a/inst")
	require.Contains(t, err.Error(), "bkt-b/inst")
}

// TestResolveProbeInstance_SaysWhenItHoldsNothing covers a node with no local copy and no writer: it
// cannot probe object storage without being told which instance, and saying so is actionable.
func TestResolveProbeInstance_SaysWhenItHoldsNothing(t *testing.T) {
	l := probeStore(t, serviceModeCfg(t))

	_, _, err := l.resolveProbeInstance(SegmentProbeRequest{LogID: 7, SegmentID: 3})

	require.Error(t, err)
	require.Contains(t, err.Error(), "bucket_name")
}

// TestResolveProbeInstance_CallerThatKnowsIsBelieved covers an explicit instance, which is how a
// node with no local copy can still be asked to read the shared copy.
func TestResolveProbeInstance_CallerThatKnowsIsBelieved(t *testing.T) {
	l := probeStore(t, serviceModeCfg(t))

	bucket, rootPath, err := l.resolveProbeInstance(SegmentProbeRequest{
		Bucket: "bkt", RootPath: "inst", LogID: 7, SegmentID: 3,
	})

	require.NoError(t, err)
	require.Equal(t, "bkt", bucket)
	require.Equal(t, "inst", rootPath)
}

// TestProbeSource_SharedCopyIsNamedAsSuch is the honesty requirement. Once the local copy is gone
// the segment is served from object storage, which every replica reads: three such answers are one
// copy answering three times.
func TestProbeSource_SharedCopyIsNamedAsSuch(t *testing.T) {
	cfg := serviceModeCfg(t)
	l := probeStore(t, cfg)
	dir := filepath.Join(cfg.Woodpecker.Storage.RootPath, "bkt", "inst", "7", "3")

	writeSegmentData(t, dir, "staged bytes")
	require.Equal(t, probeSourceLocalStaged, l.probeSource("bkt", "inst", 7, 3))

	// Compacted and reclaimed: the directory and its mark can remain, data.log does not.
	require.NoError(t, os.Remove(filepath.Join(dir, "data.log")))
	require.Equal(t, probeSourceObjectStore, l.probeSource("bkt", "inst", 7, 3),
		"a reclaimed local copy means the answer comes from the copy every replica shares")
}

// TestProbeSegment_UsesTheNodesOwnBoundWhenNoneWasAsked keeps an unbounded request from scanning a
// whole segment: the caller passing zero gets the node's default, not no limit at all.
func TestProbeSegment_UsesTheNodesOwnBoundWhenNoneWasAsked(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	store.cfg.Woodpecker.Storage.Type = "service"
	store.cfg.Woodpecker.Storage.RootPath = t.TempDir()
	mp := mocks_segment.NewSegmentProcessor(t)
	mp.EXPECT().GetLogId().Return(testLogId).Maybe()
	store.segmentProcessors[GetLogKey(testBucketName, testRootPath, testLogId)] = map[int64]processor.SegmentProcessor{29: mp}

	var asked []int64
	mp.EXPECT().ReadBatchEntriesAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, from, max int64, _ *proto.LastReadState) (*proto.BatchReadResult, error) {
			asked = append(asked, max)
			return batchOf(from, max), nil
		})

	got, err := store.ProbeSegment(context.Background(), SegmentProbeRequest{LogID: testLogId, SegmentID: 29})

	require.NoError(t, err)
	require.Equal(t, probeStopCapReached, got.StopReason)
	require.Equal(t, ProbeMaxEntriesDefault, got.EntriesRead,
		"a request with no bound stops at the node's default, not at no bound at all")
	require.NotEmpty(t, asked)
}

// TestProbeSegment_ResolvesTheInstanceFromALiveProcessor covers the primary resolution route: the
// instance is recovered from the key the node already files the segment under, so a caller that
// does not know the bucket and root path still gets an answer about the right instance.
func TestProbeSegment_ResolvesTheInstanceFromALiveProcessor(t *testing.T) {
	store := createTestLogStore()
	store.stopped.Store(false)
	store.cfg.Woodpecker.Storage.Type = "service"
	store.cfg.Woodpecker.Storage.RootPath = t.TempDir()
	mp := mocks_segment.NewSegmentProcessor(t)
	mp.EXPECT().GetLogId().Return(testLogId).Maybe()
	// A nested instance root: the key is bucket/root/logId, so recovering the two parts from it
	// cannot simply split on every slash.
	const nestedRoot = "inst/a/b"
	store.segmentProcessors[GetLogKey(testBucketName, nestedRoot, testLogId)] = map[int64]processor.SegmentProcessor{29: mp}
	mp.EXPECT().ReadBatchEntriesAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, werr.ErrEntryNotFound).Maybe()

	got, err := store.ProbeSegment(context.Background(), SegmentProbeRequest{LogID: testLogId, SegmentID: 29})

	require.NoError(t, err)
	require.Equal(t, testBucketName, got.Bucket)
	require.Equal(t, nestedRoot, got.RootPath, "an instance root with slashes must survive the round trip")
	require.Equal(t, probeStopNotYetWritten, got.StopReason)
}

// TestProbeSource_NamesTheStoreEachDeploymentReads covers the deployments that are not service mode,
// where there is no staged copy to distinguish.
func TestProbeSource_NamesTheStoreEachDeploymentReads(t *testing.T) {
	for storageType, want := range map[string]string{
		"local": probeSourceLocalDisk,
		"minio": probeSourceObjectStore,
	} {
		cfg := serviceModeCfg(t)
		cfg.Woodpecker.Storage.Type = storageType
		l := probeStore(t, cfg)
		require.Equal(t, want, l.probeSource("bkt", "inst", 7, 3), "storage type %q", storageType)
	}
}
