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

package storage

import (
	"context"

	"github.com/zilliztech/woodpecker/common/channel"
)

// Writer defines the interface for writing log entries to different storage backends
//
//go:generate mockery --dir=./server/storage --name=Writer --structname=Writer --output=mocks/mocks_server/mocks_storage --filename=mock_writer.go --with-expecter=true  --outpkg=mocks_storage
type Writer interface {
	// WriteDataAsync writes a log entry to the storage asynchronously
	WriteDataAsync(ctx context.Context, entryId int64, data []byte, resultCh channel.ResultChannel) (int64, error)

	// Sync ensures all buffered data is written to persistent storage
	Sync(ctx context.Context) error

	// GetFirstEntryId returns the first entry ID written
	GetFirstEntryId(ctx context.Context) int64

	// GetLastEntryId returns the last entry ID written
	GetLastEntryId(ctx context.Context) int64

	// GetBlockCount returns the number of blocks in the segment
	GetBlockCount(ctx context.Context) int64

	// Finalize write indexes and footer, return last entry ID
	Finalize(ctx context.Context, lac int64) (int64, error)

	// Close finalizes the writer and releases resources
	Close(ctx context.Context) error

	// Fence returns the last entry ID
	Fence(ctx context.Context) (int64, error)

	// CompactionMemoryEstimate reports the peak memory Compact will hold for this segment, so a
	// node-wide budget can admit it before the work starts.
	//
	// It is planned, not assumed. Compaction groups the segment's blocks into merge tasks and runs
	// maxParallelUploads of them at a time, each holding the source bytes it read and the merged
	// block it assembles -- so the peak is twice the largest few task spans, and the segment's own
	// size is the ceiling on that. Charging a fixed worst case instead would bill a segment holding
	// a few hundred KB the same as a full one, and a node draining a backlog of small segments
	// would run a fraction of the compactions its memory could actually carry.
	//
	// Zero means "nothing to charge": an empty segment, an already-compacted one, or a backend
	// whose Compact is a no-op.
	CompactionMemoryEstimate(expectedLastEntryId int64) int64

	// Compact merges small blocks into larger ones and seals the segment in object storage.
	// expectedLastEntryId is the exact coordinator-confirmed last entry id the compacted
	// segment must publish. In staged mode, non-empty segments require a non-negative
	// expectedLastEntryId and must refuse if local data cannot cover it. The value -1 is
	// valid only for a genuinely empty segment whose last entry id is also -1.
	Compact(ctx context.Context, expectedLastEntryId int64) (int64, error)

	// Snapshot returns a lightweight state summary of this writer.
	Snapshot() WriterSnapshot

	// SnapshotDetailed returns full state including buffer and flush queue info.
	SnapshotDetailed() WriterSnapshotDetailed
}
