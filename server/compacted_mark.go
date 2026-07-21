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

package server

import (
	"context"
	"os"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/server/storage/stagedstorage"
)

// compactedMarkFileName is the tombstone marking a staged segment whose data is durably
// compacted in object storage. It authorizes dropping the local data.log and, kept after
// that drop, lets a reader serve the segment from object storage without an object-storage
// HEAD. Distinct from the truncate/delete `.deleted` marker system. The single source of
// truth for the filename lives in the storage layer (stagedstorage.CompactedMarkFileName).
const compactedMarkFileName = stagedstorage.CompactedMarkFileName

func compactedMarkPath(segmentDir string) string {
	return filepath.Join(segmentDir, compactedMarkFileName)
}

func hasCompactedMark(segmentDir string) bool {
	_, err := os.Stat(compactedMarkPath(segmentDir))
	return err == nil
}

// writeCompactedMark creates the mark (create-if-absent, idempotent) as an EMPTY file, then
// fsyncs the parent segment dir so the marker is durable. Durability matters because
// findDataLogSegmentDirs keys off data.log: once data.log is dropped, a lost mark could never
// be re-created by the reconcile pass, leaving the segment unreadable. Only the marker's
// existence is meaningful, so there is no content to write — the create + a single parent-dir
// fsync replaces the old temp-write + rename + double-fsync.
func writeCompactedMark(ctx context.Context, segmentDir string) error {
	p := compactedMarkPath(segmentDir)
	if _, err := os.Stat(p); err == nil {
		return nil // already marked
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(segmentDir, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	// Fsync the parent dir so the new directory entry survives a crash.
	dir, err := os.Open(segmentDir)
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		dir.Close()
		return err
	}
	if err := dir.Close(); err != nil {
		return err
	}
	logger.Ctx(ctx).Info("compacted mark created", zap.String("path", p))
	return nil
}
