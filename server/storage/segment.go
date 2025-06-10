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
	"io"

	"github.com/zilliztech/woodpecker/proto"
)

// ReaderOpt represents the options for creating a reader. which read entries in [start,end).
type ReaderOpt struct {
	// StartSequenceNum is the fileLastOffset to start reading from.
	StartSequenceNum int64

	// EndSequenceNum is the fileLastOffset to stop reading at.
	EndSequenceNum int64
}

// Reader is an interface to read log entries sequentially.
type Reader interface {
	io.Closer
	// ReadNext returns the next entry in the log according to the Reader's direction.
	ReadNext(ctx context.Context) (*proto.LogEntry, error)
	// ReadNextBatch returns the next batch of entries in the log according to the Reader's direction.
	ReadNextBatch(context.Context, int64) ([]*proto.LogEntry, error)
	// HasNext returns true if there is an entry to read.
	HasNext(context.Context) (bool, error)
}

// Segment represents a segment interface with read and write operations.
//
//go:generate mockery --dir=./server/storage --name=Segment --structname=Segment --output=mocks/mocks_server/mocks_storage --filename=mock_Segment.go --with-expecter=true  --outpkg=mocks_storage
type Segment interface {
	// GetId returns the unique segment id.
	GetId() int64
	// Append adds an entry to the log file synchronously.
	// Returns a future that will receive the result of the append operation.
	// Deprecated: Use AppendAsync instead, entryID is pass by client segmentHandle
	Append(ctx context.Context, data []byte) error
	// AppendAsync adds an entry to the log file asynchronously
	AppendAsync(ctx context.Context, entryId int64, data []byte, resultCh chan<- int64) (int64, error)
	// NewReader creates a reader with options for sequential reads.
	NewReader(ctx context.Context, opt ReaderOpt) (Reader, error)
	// LastFragmentId returns the last fragment id of this logFile.
	LastFragmentId() int64
	// GetLastEntryId returns the last entry id of this logFile.
	GetLastEntryId(ctx context.Context) (int64, error)
	// Sync ensures all buffered data is written to persistent storage.
	Sync(ctx context.Context) error
	// Merge the log file fragments.
	Merge(ctx context.Context) ([]Fragment, []int32, []int32, error)
	// Load the segment log file fragments info
	Load(ctx context.Context) (int64, Fragment, error)
	// DeleteFragments delete the segment log file fragments.
	DeleteFragments(ctx context.Context, flag int) error
	// Closer closes the log file.
	Close(ctx context.Context) error
}
