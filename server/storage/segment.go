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

// Segment represents a segment interface with read and write operations.
//
//go:generate mockery --dir=./server/storage --name=Segment --structname=Segment --output=mocks/mocks_server/mocks_storage --filename=mock_Segment.go --with-expecter=true  --outpkg=mocks_storage
type Segment interface {
	// DeleteFileData delete the segment log file fragments.
	DeleteFileData(ctx context.Context, flag int) (int, error)
}
