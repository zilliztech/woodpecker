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

package commitlog

import (
	"io"

	"github.com/zilliztech/woodpecker/proto"
)

type CommitLog interface {
	// Append appends a LogEntry asynchronously.
	Append(entry *proto.CommitLogEntry) (int64, error)

	// Sync ensures all buffered data is written to persistent storage.
	Sync() error

	// Truncate removes all entries up to a given offset.
	Truncate(offset int64) (int64, error)

	// LastOffset returns the last appended offset.
	LastOffset() int64

	// NewReader creates a new reader to read log entries.
	NewReader(opt ReaderOpt) (Reader, error)

	// Close closes the WAL and releases all resources, including stopping periodic sync.
	Close() error

	// Purge removes all entries from the log.
	Purge() error

	// startSyncPeriodically starts a background goroutine to sync the WAL periodically.
	startSyncPeriodically() error
}

type ReaderOpt struct {
	// StartOffset is the offset to start reading from.
	StartOffset int64

	// EndOffset is the offset to stop reading at.
	EndOffset int64
}

// Reader is an interface to read log entries sequentially.
type Reader interface {
	io.Closer
	// Read returns the next entry in the log according to the Reader's direction.
	// If a forward/reverse WalReader has passed the end/beginning of the log, it returns [ErrorEntryNotFound].
	// To avoid this error, use HasNext.
	Read() (*proto.CommitLogEntry, error)
	// HasNext returns true if there is an entry to read.
	HasNext() bool
}
