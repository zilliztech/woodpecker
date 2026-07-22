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

package stagedstorage

// Segment-state lifecycle tests for the staged reader.
//
// A staged segment, seen from the reader, moves through these states:
//
//	S1 local incomplete (tailing)      -> S2 local finalized (footer in data.log)
//	                                   -> compacted in object storage (footer.blk uploaded)
//	S3 data.log + data.compacted mark  (drop pending; reads still local)
//	S4 tombstone (mark only)           (reads served from object storage, no re-caching)
//	S5 nothing here                    (fast ErrEntryNotFound, no object-storage call)
//	S6 mark but no footer              (anomaly: ErrEntryNotFound)
//
// The transitions can happen BETWEEN two reads of one live reader
// (tryParseFooterAndIndexesIfExists re-probes on every stateless read), so this file pins the
// in-flight transitions and coexistence states that the per-state tests in reader_impl_test.go
// do not cover:
//
//	T5  tailing growth: read -> writer appends -> read again picks up the new entries
//	T2  finalize between reads: the reader flips to the completed path and reports EOF past end
//	T3  compaction between reads while data.log still exists: the reader flips to object storage
//	S3  mark + data.log coexist: reads keep being served locally, mark is invisible
//	T4' compacted-provenance resume state replayed into a LOCAL reader resolves by entry id

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// newTailingWriter creates a staged writer with entryCount entries synced but NOT finalized,
// leaving the segment in the local-incomplete (tailing) state.
func newTailingWriter(t *testing.T, dir string, logId, segId int64, entryCount int64) *StagedFileWriter {
	t.Helper()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg)
	require.NoError(t, err)
	for i := int64(0); i < entryCount; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test data"), nil)
		require.NoError(t, err)
	}
	require.NoError(t, writer.Sync(context.Background()))
	time.Sleep(200 * time.Millisecond)
	return writer
}

// compactedSingleBlockFixture returns (footerData, blockData) for a compacted segment with one
// block holding entries 0..4.
func compactedSingleBlockFixture(t *testing.T) ([]byte, []byte) {
	t.Helper()
	blockIndex := &codec.IndexRecord{BlockNumber: 0, StartOffset: 0, BlockSize: 200, FirstEntryID: 0, LastEntryID: 4}
	footer := &codec.FooterRecord{
		TotalBlocks: 1, TotalRecords: 5, TotalSize: 200,
		IndexLength: uint32(codec.RecordHeaderSize + codec.IndexRecordSize),
		Version:     codec.FormatVersion, Flags: codec.SetCompacted(0), LAC: 4,
	}
	var footerBuf bytes.Buffer
	footerBuf.Write(codec.EncodeRecord(blockIndex))
	footerBuf.Write(codec.EncodeRecord(footer))

	var blockBuf bytes.Buffer
	blockBuf.Write(codec.EncodeRecord(&codec.HeaderRecord{Version: codec.FormatVersion, Flags: codec.SetCompacted(0), FirstEntryID: 0}))
	dataRecords := make([]codec.Record, 0, 5)
	for i := 0; i < 5; i++ {
		dataRecords = append(dataRecords, &codec.DataRecord{Payload: []byte("test data")})
	}
	blockDataOnly := encodeRecordList(dataRecords)
	blockBuf.Write(codec.EncodeRecord(&codec.BlockHeaderRecord{
		BlockNumber: 0, FirstEntryID: 0, LastEntryID: 4,
		BlockLength: uint32(len(blockDataOnly)), BlockCrc: crc32.ChecksumIEEE(blockDataOnly),
	}))
	blockBuf.Write(blockDataOnly)
	return footerBuf.Bytes(), blockBuf.Bytes()
}

// TestStagedReaderStates_TailingProgressiveRead covers T5: one live reader on an incomplete
// segment reads, waits (ErrEntryNotFound past the LAC), and picks up entries the writer
// appends afterwards — the everyday tailing loop, read progressively rather than as a static
// snapshot.
func TestStagedReaderStates_TailingProgressiveRead(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	logId, segId := int64(200), int64(200)

	writer := newTailingWriter(t, dir, logId, segId, 3) // entries 0..2 synced, not finalized
	defer writer.Close(ctx)

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", dir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)
	require.True(t, reader.isIncompleteFile.Load(), "precondition: tailing state")

	// Phase 1: read what's confirmed so far.
	require.NoError(t, reader.UpdateLastAddConfirmed(ctx, 2))
	result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}, nil)
	require.NoError(t, err)
	require.Equal(t, 3, len(result.Entries))
	assert.Equal(t, int64(0), result.Entries[0].EntryId)
	assert.Equal(t, int64(2), result.Entries[2].EntryId)

	// Phase 2: nothing new yet — tailing read waits via ErrEntryNotFound (never EOF: the
	// segment can still grow).
	_, err = reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 3, MaxBatchEntries: 10}, nil)
	require.Error(t, err)
	assert.True(t, werr.ErrEntryNotFound.Is(err), "an incomplete segment must signal wait, not EOF; got %v", err)

	// Phase 3: the writer appends entries 3..4; the SAME reader picks them up.
	for i := int64(3); i < 5; i++ {
		_, err = writer.WriteDataAsync(ctx, i, []byte("test data"), nil)
		require.NoError(t, err)
	}
	require.NoError(t, writer.Sync(ctx))
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, reader.UpdateLastAddConfirmed(ctx, 4))

	result, err = reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 3, MaxBatchEntries: 10}, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(result.Entries), "the reader must see the newly appended entries")
	assert.Equal(t, int64(3), result.Entries[0].EntryId)
	assert.Equal(t, int64(4), result.Entries[1].EntryId)
}

// TestStagedReaderStates_FinalizePickedUpBetweenReads covers T2: a reader opened on an
// incomplete segment must, after the writer finalizes BETWEEN two reads, flip to the
// completed path — a past-end read turns from "wait" (ErrEntryNotFound) into EOF so the
// consumer can advance to the next segment, and in-range reads keep working.
func TestStagedReaderStates_FinalizePickedUpBetweenReads(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	logId, segId := int64(201), int64(201)

	writer := newTailingWriter(t, dir, logId, segId, 5) // entries 0..4, not finalized

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", dir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)
	require.True(t, reader.isIncompleteFile.Load())

	// While incomplete: past-end means "wait".
	require.NoError(t, reader.UpdateLastAddConfirmed(ctx, 4))
	_, err = reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 5, MaxBatchEntries: 10}, nil)
	require.Error(t, err)
	assert.True(t, werr.ErrEntryNotFound.Is(err))

	// The writer finalizes and closes between two reads.
	_, err = writer.Finalize(ctx, 4)
	require.NoError(t, err)
	require.NoError(t, writer.Close(ctx))

	// Past-end now means EOF: the same reader picks the local footer up on its next stateless
	// read and flips to the completed path.
	_, err = reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 5, MaxBatchEntries: 10}, nil)
	require.Error(t, err)
	assert.True(t, werr.ErrFileReaderEndOfFile.Is(err),
		"after finalize the reader must report EOF past the end, not keep the consumer waiting; got %v", err)
	assert.False(t, reader.isIncompleteFile.Load(), "the reader flipped to the completed state")

	// In-range reads still work on the flipped reader.
	result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}, nil)
	require.NoError(t, err)
	require.Equal(t, 5, len(result.Entries))
	assert.Equal(t, int64(4), result.Entries[4].EntryId)
}

// TestStagedReaderStates_CompactionFlipsLocalReaderToMinio covers T3: a reader on a still-
// present local incomplete data.log flips to the compacted (object-storage) path once the
// compacted footer appears in minio between two reads — reads continue correctly against the
// compacted block numbering, the emitted resume state carries compacted provenance, and the
// local data.log is untouched.
func TestStagedReaderStates_CompactionFlipsLocalReaderToMinio(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	logId, segId := int64(202), int64(202)

	writer := newTailingWriter(t, dir, logId, segId, 5) // local incomplete, entries 0..4
	defer writer.Close(ctx)

	footerData, blockData := compactedSingleBlockFixture(t)
	footerKey := fmt.Sprintf("test-root/%d/%d/footer.blk", logId, segId)
	blockKey := fmt.Sprintf("test-root/%d/%d/m_0.blk", logId, segId)

	// The mock flips from "not compacted yet" to "footer present" via this switch, emulating
	// compaction completing between two reads.
	var compacted atomic.Bool
	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	mockStorage.EXPECT().IsObjectNotExistsError(mock.Anything).RunAndReturn(minioHandler.IsObjectNotExists).Maybe()
	mockStorage.EXPECT().StatObject(mock.Anything, "test-bucket", footerKey, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _, _, _, _ string) (int64, bool, error) {
			if compacted.Load() {
				return int64(len(footerData)), false, nil
			}
			return 0, false, minio.ErrorResponse{Code: "NoSuchKey"}
		}).Maybe()
	mockStorage.EXPECT().GetObject(mock.Anything, "test-bucket", footerKey, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _, _ string, _, _ int64, _, _ string) (minioHandler.FileReader, error) {
			return &readerMockFileReader{data: footerData}, nil
		}).Maybe()
	mockStorage.EXPECT().GetObject(mock.Anything, "test-bucket", blockKey, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _, _ string, _, _ int64, _, _ string) (minioHandler.FileReader, error) {
			return &readerMockFileReader{data: blockData}, nil
		}).Maybe()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", dir, logId, segId, mockStorage, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)
	require.False(t, reader.isCompacted.Load(), "precondition: local reader before compaction")

	// Phase 1 (pre-compaction): local read works.
	require.NoError(t, reader.UpdateLastAddConfirmed(ctx, 4))
	result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}, nil)
	require.NoError(t, err)
	require.Equal(t, 5, len(result.Entries))

	// Phase 2: compaction completes (footer becomes visible). The next stateless read flips
	// the SAME reader to the compacted path and serves from object storage.
	compacted.Store(true)
	result, err = reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}, nil)
	require.NoError(t, err)
	require.Equal(t, 5, len(result.Entries), "reads continue correctly after the in-place flip")
	assert.Equal(t, int64(0), result.Entries[0].EntryId)
	assert.Equal(t, int64(4), result.Entries[4].EntryId)
	assert.True(t, reader.isCompacted.Load(), "the reader flipped to the compacted state")
	require.NotNil(t, result.LastReadState)
	assert.True(t, codec.IsCompacted(uint16(result.LastReadState.Flags)),
		"post-flip resume state must carry compacted provenance (block numbering changed)")

	// The flip never touches the local file: dropping it is exclusively the cleanup task's job.
	_, statErr := os.Stat(getSegmentFilePath(dir, logId, segId))
	assert.NoError(t, statErr, "data.log must be untouched by the read-side flip")
}

// TestStagedReaderStates_MarkWithDataLogStillServesLocally covers S3 (drop pending): a
// data.compacted mark next to a still-present data.log must not affect reads — the reader
// opens as a local reader and serves locally, with zero object-storage involvement (nil
// client) and local (non-compacted) provenance on the emitted state.
func TestStagedReaderStates_MarkWithDataLogStillServesLocally(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	logId, segId := int64(203), int64(203)

	// Finalized local segment with 5 entries, then the compacted mark lands (push arrived
	// before the drop).
	first := createTestReaderFromWriter(t, dir, logId, segId, 5, 4)
	require.NoError(t, first.Close(ctx))
	segDir := getSegmentDir(dir, logId, segId)
	require.NoError(t, os.WriteFile(filepath.Join(segDir, CompactedMarkFileName), nil, 0o644))

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	// nil object-storage client: any attempt to consult minio would fail the read.
	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", dir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	assert.False(t, reader.isCompacted.Load(), "a present data.log wins over the mark")
	result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}, nil)
	require.NoError(t, err)
	require.Equal(t, 5, len(result.Entries))
	require.NotNil(t, result.LastReadState)
	assert.False(t, codec.IsCompacted(uint16(result.LastReadState.Flags)),
		"local serving in the drop-pending state emits local provenance")
}

// TestStagedReaderStates_ReclaimedSegment_BackendNotFoundIsNotFound pins the predicate
// DISPATCH: absence must be recognized via the client's IsObjectNotExistsError, not the
// MinIO-only minioHandler.IsObjectNotExists. The mock returns a non-MinIO-shaped error
// (like Azure's raw *azcore.ResponseError 404, which minio.ToErrorResponse can never match)
// while its dispatched predicate says "not found" — the reader must classify it as a
// definitive ErrEntryNotFound, not fall into the transient branch and retry unboundedly.
func TestStagedReaderStates_ReclaimedSegment_BackendNotFoundIsNotFound(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	logId, segId := int64(209), int64(209)

	segDir := getSegmentDir(dir, logId, segId)
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(segDir, CompactedMarkFileName), nil, 0o644))

	azureLike404 := fmt.Errorf("GET https://acct.blob.core.windows.net/c/footer.blk: 404 BlobNotFound")
	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	// consulted twice: the helper's absent branch and the caller's re-check
	mockStorage.EXPECT().IsObjectNotExistsError(azureLike404).Return(true)
	mockStorage.EXPECT().StatObject(mock.Anything, "test-bucket", fmt.Sprintf("test-root/%d/%d/footer.blk", logId, segId), mock.Anything, mock.Anything).
		Return(int64(0), false, azureLike404).Once()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	_, err = NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", dir, logId, segId, mockStorage, cfg)
	require.Error(t, err)
	assert.True(t, werr.ErrEntryNotFound.Is(err),
		"a backend-recognized 404 is a definitive absence and must map to ErrEntryNotFound, got %v", err)
}

// TestStagedReaderStates_ParseAfterClose_ReturnsAlreadyClosed pins the under-lock closed
// check on the parse path: tryParseFooterAndIndexesIfExists runs BEFORE ReadNextBatchAdv's
// read lock and takes the writer lock itself, so a read racing an eviction-triggered Close
// can reach it after the file was nilled. It must report the rebuildable
// ErrFileReaderAlreadyClosed — pre-fix it fell through to the nil-file guard and masqueraded
// as ErrEntryNotFound, which nothing rebuilds.
func TestStagedReaderStates_ParseAfterClose_ReturnsAlreadyClosed(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	logId, segId := int64(207), int64(207)

	writer := newTailingWriter(t, dir, logId, segId, 3) // local incomplete: parse path stays live
	defer writer.Close(ctx)

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", dir, logId, segId, nil, cfg)
	require.NoError(t, err)
	require.True(t, reader.isIncompleteFile.Load(), "precondition: incomplete, so the parse path does real work")

	require.NoError(t, reader.Close(ctx))

	err = reader.tryParseFooterAndIndexesIfExists(ctx)
	require.Error(t, err)
	assert.True(t, werr.ErrFileReaderAlreadyClosed.Is(err),
		"parse on a closed reader must be the rebuildable AlreadyClosed, got %v", err)
	assert.False(t, werr.ErrEntryNotFound.Is(err), "must not masquerade as ErrEntryNotFound")
}

// TestStagedReaderStates_ConcurrentCloseAndRead_OnlyAlreadyClosedSurfaces races the
// eviction-triggered Close (drop path: EvictSegmentReader -> InvalidateReader -> Close)
// against in-flight reads on a COMPACTED (pool-backed) reader — the flavor whose lost race
// used to submit to a released pool and surface ants' ErrPoolClosed verbatim to the
// application. With the under-lock closed re-check, every read outcome must be either a
// successful batch or ErrFileReaderAlreadyClosed; anything else (pool-closed, EntryNotFound,
// EOF) fails the test. Run under -race in the gates, this also exercises memory safety.
func TestStagedReaderStates_ConcurrentCloseAndRead_OnlyAlreadyClosedSurfaces(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	logId, segId := int64(208), int64(208)

	// Reclaimed layout: tombstone mark only — the reader opens on the R1 path, compacted.
	segDir := getSegmentDir(dir, logId, segId)
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(segDir, CompactedMarkFileName), nil, 0o644))

	footerData, blockData := compactedSingleBlockFixture(t)
	footerKey := fmt.Sprintf("test-root/%d/%d/footer.blk", logId, segId)
	blockKey := fmt.Sprintf("test-root/%d/%d/m_0.blk", logId, segId)

	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	mockStorage.EXPECT().IsObjectNotExistsError(mock.Anything).RunAndReturn(minioHandler.IsObjectNotExists).Maybe()
	mockStorage.EXPECT().StatObject(mock.Anything, "test-bucket", footerKey, mock.Anything, mock.Anything).
		Return(int64(len(footerData)), false, nil).Maybe()
	mockStorage.EXPECT().GetObject(mock.Anything, "test-bucket", footerKey, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _, _ string, _, _ int64, _, _ string) (minioHandler.FileReader, error) {
			return &readerMockFileReader{data: footerData}, nil
		}).Maybe()
	mockStorage.EXPECT().GetObject(mock.Anything, "test-bucket", blockKey, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _, _ string, _, _ int64, _, _ string) (minioHandler.FileReader, error) {
			return &readerMockFileReader{data: blockData}, nil
		}).Maybe()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	for i := 0; i < 60; i++ {
		reader, openErr := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", dir, logId, segId, mockStorage, cfg)
		require.NoError(t, openErr)
		require.True(t, reader.isCompacted.Load(), "precondition: compacted (pool-backed) reader")

		done := make(chan error, 1)
		go func() {
			for j := 0; j < 5; j++ {
				_, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}, nil)
				if readErr != nil {
					done <- readErr
					return
				}
			}
			done <- nil
		}()

		runtime.Gosched()
		require.NoError(t, reader.Close(ctx))

		if raceErr := <-done; raceErr != nil {
			require.True(t, werr.ErrFileReaderAlreadyClosed.Is(raceErr),
				"a read racing Close may only observe AlreadyClosed (rebuildable), got: %v", raceErr)
		}
	}
}

// TestStagedReaderStates_ReclaimedSegment_TransientMinioErrorSurfaces pins the
// absent-vs-error split on the R1 fallback: for a reclaimed segment (tombstone mark, no
// data.log) object storage is the ONLY source, so a transient StatObject failure
// (throttle/5xx/timeout) must surface as a retriable error — NOT be collapsed into
// ErrEntryNotFound, which callers deliberately treat as the silent caught-up steady state
// (the collapse would turn an S3 blip into an unlogged read stall).
func TestStagedReaderStates_ReclaimedSegment_TransientMinioErrorSurfaces(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	logId, segId := int64(205), int64(205)

	segDir := getSegmentDir(dir, logId, segId)
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(segDir, CompactedMarkFileName), nil, 0o644))

	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	mockStorage.EXPECT().StatObject(mock.Anything, "test-bucket", fmt.Sprintf("test-root/%d/%d/footer.blk", logId, segId), mock.Anything, mock.Anything).
		Return(int64(0), false, fmt.Errorf("i/o timeout")).Once()
	mockStorage.EXPECT().IsObjectNotExistsError(mock.Anything).Return(false).Maybe()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	_, err = NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", dir, logId, segId, mockStorage, cfg)
	require.Error(t, err)
	assert.False(t, werr.ErrEntryNotFound.Is(err),
		"a transient object-storage failure must stay a retriable error, got %v", err)
	assert.Contains(t, err.Error(), "i/o timeout")
}

// TestStagedReaderStates_ReclaimedSegment_FooterGenuinelyAbsentIsNotFound is the companion:
// a CONFIRMED absent footer (NoSuchKey) on the same tombstone-only layout still maps to
// ErrEntryNotFound (the S6 anomaly semantics are unchanged by the absent-vs-error split).
func TestStagedReaderStates_ReclaimedSegment_FooterGenuinelyAbsentIsNotFound(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	logId, segId := int64(206), int64(206)

	segDir := getSegmentDir(dir, logId, segId)
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(segDir, CompactedMarkFileName), nil, 0o644))

	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	mockStorage.EXPECT().IsObjectNotExistsError(mock.Anything).RunAndReturn(minioHandler.IsObjectNotExists).Maybe()
	mockStorage.EXPECT().StatObject(mock.Anything, "test-bucket", fmt.Sprintf("test-root/%d/%d/footer.blk", logId, segId), mock.Anything, mock.Anything).
		Return(int64(0), false, minio.ErrorResponse{Code: "NoSuchKey"}).Once()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	_, err = NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", dir, logId, segId, mockStorage, cfg)
	require.Error(t, err)
	assert.True(t, werr.ErrEntryNotFound.Is(err), "confirmed absence still maps to ErrEntryNotFound, got %v", err)
}

// TestStagedReaderStates_CompactedStateIntoLocalReader_ResolvesByEntryId covers T4', the
// mirror of the local->compacted provenance test: a COMPACTED-provenance resume state (with a
// block id meaningless in local numbering) replayed into a LOCAL reader must not reuse the
// block id — it resolves by entry id and emits local provenance.
func TestStagedReaderStates_CompactedStateIntoLocalReader_ResolvesByEntryId(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	reader := createTestReaderFromWriter(t, dir, 204, 204, 5, 4) // finalized local, entries 0..4
	defer reader.Close(ctx)
	require.False(t, reader.isCompacted.Load())

	staleCompactedState := &proto.LastReadState{
		SegmentId:   204,
		Flags:       uint32(codec.SetCompacted(0)),
		Version:     uint32(codec.FormatVersion),
		LastBlockId: 99, // meaningless in local block numbering
		BlockOffset: 0,
	}
	result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 2, MaxBatchEntries: 10}, staleCompactedState)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 3, len(result.Entries), "the resume must resolve by entry id, not the stale compacted block id")
	assert.Equal(t, int64(2), result.Entries[0].EntryId)
	assert.Equal(t, int64(4), result.Entries[2].EntryId)
	require.NotNil(t, result.LastReadState)
	assert.False(t, codec.IsCompacted(uint16(result.LastReadState.Flags)),
		"the local reader's emitted state must carry local provenance even after absorbing compacted flags")
}
