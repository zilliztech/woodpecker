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
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// writeSegmentFile lays out a segment the way the writer does — a file header, then a block header
// followed by its data records per block — so a survey of it is a survey of the real format.
func writeSegmentFile(t *testing.T, dir string, blocks int) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	out := codec.EncodeRecord(&codec.HeaderRecord{Version: codec.FormatVersion})
	entryID := int64(0)
	for block := 0; block < blocks; block++ {
		data := codec.EncodeRecord(&codec.DataRecord{Payload: []byte{byte(entryID)}})
		out = append(out, codec.EncodeRecord(&codec.BlockHeaderRecord{
			BlockNumber:  int32(block),
			FirstEntryID: entryID,
			LastEntryID:  entryID,
			BlockLength:  uint32(len(data)),
			BlockCrc:     crc32.ChecksumIEEE(data),
		})...)
		out = append(out, data...)
		entryID++
	}
	path := filepath.Join(dir, "data.log")
	require.NoError(t, os.WriteFile(path, out, 0o644))
	return path
}

func inspectTestStore(t *testing.T) *logStore {
	t.Helper()
	store := createTestLogStore()
	store.stopped.Store(false)
	store.cfg.Woodpecker.Storage.Type = "service"
	store.cfg.Woodpecker.Storage.RootPath = t.TempDir()
	return store
}

// TestInspectSegment_WalksTheLocalFile covers the ordinary case end to end: a real segment file on
// disk, surveyed block by block.
func TestInspectSegment_WalksTheLocalFile(t *testing.T) {
	store := inspectTestStore(t)
	writeSegmentFile(t, store.localSegmentDir(testBucketName, testRootPath, testLogId, 29), 3)

	got, err := store.InspectSegment(context.Background(), SegmentInspectRequest{
		Bucket: testBucketName, RootPath: testRootPath, LogID: testLogId, SegmentID: 29,
	})

	require.NoError(t, err)
	require.Len(t, got.Survey.Blocks, 3)
	for i, block := range got.Survey.Blocks {
		require.Equal(t, codec.BlockOK, block.Status, "block %d", i)
	}
	require.True(t, got.Local.DataLog)
	require.Equal(t, probeSourceLocalStaged, got.Source)
}

// TestInspectSegment_DamageDoesNotHideTheRest is the reason this exists. A read stops at the first
// bad block; the survey has to report what lies beyond it, because that is the difference between
// skipping a few entries and skipping the rest of the segment.
func TestInspectSegment_DamageDoesNotHideTheRest(t *testing.T) {
	store := inspectTestStore(t)
	path := writeSegmentFile(t, store.localSegmentDir(testBucketName, testRootPath, testLogId, 29), 4)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	// Flip a byte inside the second block's data, leaving every header intact.
	blockSpan := codec.RecordHeaderSize + codec.BlockHeaderRecordSize + codec.RecordHeaderSize + 1
	fileHeader := codec.RecordHeaderSize + codec.HeaderRecordSize
	raw[fileHeader+blockSpan+codec.RecordHeaderSize+codec.BlockHeaderRecordSize+codec.RecordHeaderSize] ^= 0xFF
	require.NoError(t, os.WriteFile(path, raw, 0o644))

	got, err := store.InspectSegment(context.Background(), SegmentInspectRequest{
		Bucket: testBucketName, RootPath: testRootPath, LogID: testLogId, SegmentID: 29,
	})

	require.NoError(t, err)
	require.Len(t, got.Survey.Blocks, 4, "the survey must not stop where a read would")
	require.Equal(t, codec.BlockChecksumFailed, got.Survey.Blocks[1].Status)
	require.Equal(t, codec.BlockOK, got.Survey.Blocks[2].Status, "the damage is bounded, and that is the finding")
	require.Equal(t, codec.BlockOK, got.Survey.Blocks[3].Status)
}

// TestInspectSegment_NoLocalFileOpensNothing keeps a survey of a segment this node does not hold
// from creating the directory that decides where the next one resolves to.
func TestInspectSegment_NoLocalFileOpensNothing(t *testing.T) {
	store := inspectTestStore(t)

	got, err := store.InspectSegment(context.Background(), SegmentInspectRequest{
		Bucket: testBucketName, RootPath: testRootPath, LogID: testLogId, SegmentID: 29,
	})

	require.NoError(t, err)
	require.Empty(t, got.Survey.Blocks)
	require.Equal(t, surveyStopNoLocalBlocks, got.Survey.StopReason)
	require.NoDirExists(t, store.localSegmentDir(testBucketName, testRootPath, testLogId, 29))
}

// TestInspectSegment_ShutdownIsNotASurvey covers a probe during a rolling restart: a node on its way
// down has said nothing about the segment's blocks.
func TestInspectSegment_ShutdownIsNotASurvey(t *testing.T) {
	store := inspectTestStore(t)
	writeSegmentFile(t, store.localSegmentDir(testBucketName, testRootPath, testLogId, 29), 2)
	store.stopped.Store(true)

	got, err := store.InspectSegment(context.Background(), SegmentInspectRequest{
		Bucket: testBucketName, RootPath: testRootPath, LogID: testLogId, SegmentID: 29,
	})

	require.Error(t, err)
	require.True(t, werr.ErrLogStoreShutdown.Is(err), "got %v", err)
	require.Nil(t, got)
}

// TestInspectBound keeps a survey within bounds the node sets, whatever was asked for.
func TestInspectBound(t *testing.T) {
	require.Equal(t, InspectMaxBlocksDefault, inspectBound(0))
	require.Equal(t, InspectMaxBlocksDefault, inspectBound(-1))
	require.Equal(t, int64(8), inspectBound(8))
	require.Equal(t, InspectMaxBlocksLimit, inspectBound(1_000_000),
		"a caller cannot ask a node to read a segment whole")
}
