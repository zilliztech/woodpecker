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
)

// Fragment interface defines Read and Write operations.
//
//go:generate mockery --dir=./server/storage --name=Fragment --structname=Fragment --output=mocks/mocks_server/mocks_storage --filename=mock_fragment.go --with-expecter=true  --outpkg=mocks_storage
type Fragment interface {
	GetLogId() int64
	GetSegmentId() int64
	GetFragmentId() int64
	GetFragmentKey() string
	Flush(ctx context.Context) error
	Load(ctx context.Context) error
	LoadSizeStateOnly(ctx context.Context) (int64, error)
	AppendToMergeTarget(ctx context.Context, mergeTarget Fragment, baseOffset int64) error
	GetLastEntryId(ctx context.Context) (int64, error)
	GetFirstEntryId(ctx context.Context) (int64, error)
	GetLastModified(ctx context.Context) int64
	GetEntry(ctx context.Context, entryId int64) ([]byte, error)
	GetSize() int64
	GetRawBufSize() int64
	Release(ctx context.Context) error
}

//go:generate mockery --dir=./server/storage --name=AppendableFragment --structname=AppendableFragment --output=mocks/mocks_server/mocks_storage --filename=mock_appendable_fragment.go --with-expecter=true  --outpkg=mocks_storage
type AppendableFragment interface {
	Fragment
	Append(ctx context.Context, data []byte, entryId int64) error
	IsFull(ctx context.Context, requestSize int64) bool
	IsGrowing(ctx context.Context) (bool, error)
	Finalize(ctx context.Context) error
	GetFetchedLastEntryId(ctx context.Context) (int64, error)
	Close(ctx context.Context) error
	IsClosed(ctx context.Context) bool
}
