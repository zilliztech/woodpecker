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

	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// Reader defines the interface for reading log entries from different storage backends
type Reader interface {
	// ReadNextBatchAdv returns the next batch of entries in the log according to the Reader's direction
	ReadNextBatchAdv(ctx context.Context, opt ReaderOpt) (*Batch, error)

	// GetBlockIndexes returns all block indexes
	GetBlockIndexes() []*codec.IndexRecord

	// GetLastEntryID returns the last entry ID written
	GetLastEntryID(ctx context.Context) (int64, error)

	// GetFooter returns the footer record
	GetFooter() *codec.FooterRecord

	// GetTotalRecords returns the total number of records
	GetTotalRecords() uint32

	// GetTotalBlocks returns the total number of blocks
	GetTotalBlocks() int32

	Close(ctx context.Context) error
}

// ReaderOpt represents the options for creating a reader. which read entries in [start,end).
type ReaderOpt struct {
	// StartEntryID is the entryID to start reading from.
	StartEntryID int64

	// EndEntryID is the entryID to stop reading at.
	EndEntryID int64

	// BatchSize is the maxSize of entries to read in a batch.
	BatchSize int64
}

// Batch represents a batch of entries, and its block infos using for next batch reading position hint.
type Batch struct {
	// last block info of this batch
	LastBatchInfo *BatchInfo
	// data entries
	Entries []*proto.LogEntry
}

type BatchInfo struct {
	// last block info of this batch
	Version       uint16
	Flags         uint16
	LastBlockInfo *codec.IndexRecord // TODO only blockId, blockOffset, blockSize are need
}
