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
	"encoding/json"
	"os"
	"path/filepath"
	"time"

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

// writeCompactedMark atomically creates the mark (create-if-absent, idempotent), fsync'd.
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
	data, _ := json.Marshal(map[string]int64{"compactedAt": time.Now().Unix()})
	tmp := p + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, p); err != nil {
		return err
	}
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

func removeCompactedMark(ctx context.Context, segmentDir string) error {
	err := os.Remove(compactedMarkPath(segmentDir))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
