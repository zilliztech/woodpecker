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
type Writer interface {
	// WriteDataAsync writes a log entry to the storage asynchronously
	WriteDataAsync(ctx context.Context, entryId int64, data []byte, resultCh channel.ResultChannel) (int64, error)

	// Sync ensures all buffered data is written to persistent storage
	Sync(ctx context.Context) error

	// GetFirstEntryId returns the first entry ID written
	GetFirstEntryId(ctx context.Context) int64

	// GetLastEntryId returns the last entry ID written
	GetLastEntryId(ctx context.Context) int64

	// Finalize write indexes and footer, return last entry ID
	Finalize(ctx context.Context) (int64, error)

	// Close finalizes the writer and releases resources
	Close(ctx context.Context) error

	// Fence returns the last entry ID
	Fence(ctx context.Context) (int64, error)

	// Compact small blocks into larger blocks, return size after compacted
	Compact(ctx context.Context) (int64, error)
}
