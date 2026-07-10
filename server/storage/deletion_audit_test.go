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
	"fmt"
	"testing"
)

// TestLogDeletedObjectKeys exercises the chunk boundaries: empty, exactly one
// chunk, one over a chunk, and multiple chunks. It must not panic or loop
// forever regardless of key-list size.
func TestLogDeletedObjectKeys(t *testing.T) {
	for _, n := range []int{0, 1, deletedKeysChunkSize - 1, deletedKeysChunkSize, deletedKeysChunkSize + 1, 3*deletedKeysChunkSize + 7} {
		keys := make([]string, n)
		for i := range keys {
			keys[i] = fmt.Sprintf("seg/%d.blk", i)
		}
		LogDeletedObjectKeys(context.Background(), "test deleted keys", "seg", keys)
	}
}
