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

package stagedstorage

import (
	"path/filepath"

	"github.com/zilliztech/woodpecker/server/storage/serde"
)

// =============================================================================
// Staged Storage Legacy Path Helpers
// =============================================================================
//
// This file contains legacy path helpers for the staged storage backend.
// These functions support older formats that may be deprecated in favor
// of newer approaches.

const (
	// LegacyDataFileName is the name of the legacy single-file format (data.log)
	// This is used for backward compatibility with older segment formats.
	LegacyDataFileName = "data.log"
)

// getSegmentFilePath returns the path to the legacy data.log file.
// Format: {baseDir}/{logId}/{segmentId}/data.log
// This is the legacy single-file format used before per-block storage.
func getSegmentFilePath(baseDir string, logId int64, segmentId int64) string {
	return filepath.Join(serde.GetSegmentDir(baseDir, logId, segmentId), LegacyDataFileName)
}
