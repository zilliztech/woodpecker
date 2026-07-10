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

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
)

// deletedKeysChunkSize bounds how many object keys go into one audit log line,
// so deleting a large segment cannot produce an unbounded log entry.
const deletedKeysChunkSize = 200

// LogDeletedObjectKeys emits INFO audit lines listing the exact object keys /
// file paths removed by a segment deletion, chunked to keep individual log
// entries bounded. No-op for an empty key list.
func LogDeletedObjectKeys(ctx context.Context, msg string, segmentFileKey string, keys []string) {
	for start := 0; start < len(keys); start += deletedKeysChunkSize {
		end := min(start+deletedKeysChunkSize, len(keys))
		logger.Ctx(ctx).Info(msg,
			zap.String("segmentFileKey", segmentFileKey),
			zap.Strings("deletedKeys", keys[start:end]),
			zap.Int("chunkStart", start),
			zap.Int("totalDeleted", len(keys)))
	}
}
