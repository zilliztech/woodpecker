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
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
)

// deleteMarkerDir is the subdirectory under the storage root holding delete markers.
// HasLocalSegmentData skips this dir so markers are never counted as segment data.
const deleteMarkerDir = ".deleted"

// instanceMarkerFile is the marker filename for an instance-level (whole bucket/rootPath) delete.
const instanceMarkerFile = "_instance_.json"

// deleteMarker is the durable, node-local record that a log or instance was marked deleted.
// Identity is taken from the file CONTENT, never parsed from the path.
type deleteMarker struct {
	Bucket    string `json:"bucket"`
	RootPath  string `json:"rootPath"`
	LogId     int64  `json:"logId"`
	Instance  bool   `json:"instance"`  // true => whole bucket/rootPath, LogId ignored
	DeletedAt int64  `json:"deletedAt"` // unix seconds, set once (create-if-absent)
}

// markerPath returns the on-disk path for a marker (directory layout is organizational only).
func markerPath(storageRoot string, m deleteMarker) string {
	base := filepath.Join(storageRoot, deleteMarkerDir, m.Bucket, m.RootPath)
	if m.Instance {
		return filepath.Join(base, instanceMarkerFile)
	}
	return filepath.Join(base, strconv.FormatInt(m.LogId, 10)+".json")
}

// writeDeleteMarker persists a marker. Create-if-absent: if the marker already exists it is
// left untouched (the grace clock / DeletedAt is stable across re-marks). fsync'd for durability.
func writeDeleteMarker(ctx context.Context, storageRoot string, m deleteMarker) error {
	p := markerPath(storageRoot, m)
	if _, err := os.Stat(p); err == nil {
		logger.Ctx(ctx).Info("delete marker already exists, keeping original",
			zap.String("path", p))
		return nil // already marked — keep the original
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp := p + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer os.Remove(tmp) // harmless no-op after a successful rename; cleans up on failure
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
	// fsync the parent directory so the rename (the new directory entry) is durable;
	// without this, a crash right after rename can lose the marker.
	dir, err := os.Open(filepath.Dir(p))
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
	logger.Ctx(ctx).Info("delete marker created",
		zap.String("path", p),
		zap.String("bucket", m.Bucket),
		zap.String("rootPath", m.RootPath),
		zap.Int64("logId", m.LogId),
		zap.Bool("instance", m.Instance),
		zap.Int64("deletedAt", m.DeletedAt))
	return nil
}

// removeDeleteMarker deletes a marker file. Absent file is not an error.
func removeDeleteMarker(ctx context.Context, storageRoot string, m deleteMarker) error {
	p := markerPath(storageRoot, m)
	err := os.Remove(p)
	if os.IsNotExist(err) {
		logger.Ctx(ctx).Info("delete marker already absent", zap.String("path", p))
		return nil
	}
	if err != nil {
		return err
	}
	logger.Ctx(ctx).Info("delete marker removed",
		zap.String("path", p),
		zap.String("bucket", m.Bucket),
		zap.String("rootPath", m.RootPath),
		zap.Int64("logId", m.LogId),
		zap.Bool("instance", m.Instance))
	return nil
}

// scanDeleteMarkers walks the marker dir and returns every marker, parsed from file content.
// A missing marker dir yields an empty slice (not an error). Unreadable or corrupt marker
// files are skipped with a WARN — they would otherwise linger on disk invisibly.
func scanDeleteMarkers(ctx context.Context, storageRoot string) ([]deleteMarker, error) {
	dir := filepath.Join(storageRoot, deleteMarkerDir)
	var markers []deleteMarker
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return filepath.SkipAll
			}
			logger.Ctx(ctx).Warn("delete marker scan: cannot access path, skipping",
				zap.String("path", path), zap.Error(err))
			return nil
		}
		if d.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		// G122: path comes from WalkDir over the node-local, operator-controlled
		// `<storageRoot>/.deleted/` tree, not attacker-writable input — symlink TOCTOU
		// is not a concern here.
		data, rerr := os.ReadFile(path) //nolint:gosec
		if rerr != nil {
			logger.Ctx(ctx).Warn("delete marker scan: unreadable marker file, skipping",
				zap.String("path", path), zap.Error(rerr))
			return nil
		}
		var m deleteMarker
		if uerr := json.Unmarshal(data, &m); uerr != nil {
			logger.Ctx(ctx).Warn("delete marker scan: corrupt marker file, skipping",
				zap.String("path", path), zap.Error(uerr))
			return nil
		}
		markers = append(markers, m)
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("scan delete markers: %w", err)
	}
	return markers, nil
}
