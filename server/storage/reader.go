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
)

// Reader defines the interface for reading log entries from different storage backends
//
//go:generate mockery --dir=./server/storage --name=Reader --structname=Reader --output=mocks/mocks_server/mocks_storage --filename=mock_reader.go --with-expecter=true  --outpkg=mocks_storage
type Reader interface {
	// ReadNextBatchAdv returns the next batch of entries in the log according to the Reader's direction
	ReadNextBatchAdv(ctx context.Context, opt ReaderOpt, lastReadBatchInfo *proto.LastReadState) (*proto.BatchReadResult, error)

	// GetLastEntryID returns the last entry ID written
	GetLastEntryID(ctx context.Context) (int64, error)

	// UpdateLastAddConfirmed sets the last add confirmed entry ID
	UpdateLastAddConfirmed(ctx context.Context, lac int64) error

	Close(ctx context.Context) error
}

// ReaderOpt represents the options for creating a reader. which read entries in [start,end).
type ReaderOpt struct {
	// StartEntryID is the entryID to start reading from.
	StartEntryID int64

	// EndEntryID is the entryID to stop reading at.
	// Note: Reserved field, not used in batch read mode currently
	EndEntryID int64

	// MaxBatchEntries is the maximum number of entries in a batch.
	// Note: this is a suggestion value, not a guarantee. The actual batch size may be less or more than this value.
	MaxBatchEntries int64
}
