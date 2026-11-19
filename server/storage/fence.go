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
	"os"
	"path/filepath"
)

// FenceMarkerName is a permanent, per-segment prohibition on ordinary appends.
// Only its existence matters; older local writers stored diagnostic text in it.
const FenceMarkerName = "write.fence"

// FenceMarkerExists distinguishes an absent marker from an unreadable or invalid
// marker. Callers must not turn an inspection failure into permission to append.
func FenceMarkerExists(segmentDir string) (bool, error) {
	p := filepath.Join(segmentDir, FenceMarkerName)
	info, err := os.Lstat(p)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat fence marker: %w", err)
	}
	if !info.Mode().IsRegular() {
		return false, fmt.Errorf("fence marker %s is not a regular file", p)
	}
	return true, nil
}

// PersistFenceMarker makes a fence survive both process restart and host crash.
// The segment directory must already exist. Existing markers are never truncated,
// and are synced again: a previous attempt may have created one but failed to sync.
func PersistFenceMarker(ctx context.Context, segmentDir string) error {
	return persistFenceMarker(ctx, segmentDir, (*os.File).Sync)
}

func persistFenceMarker(ctx context.Context, segmentDir string, syncFile func(*os.File) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	segmentDir, err := filepath.Abs(segmentDir)
	if err != nil {
		return fmt.Errorf("resolve fence directory: %w", err)
	}
	exists, err := FenceMarkerExists(segmentDir)
	if err != nil {
		return err
	}
	p := filepath.Join(segmentDir, FenceMarkerName)
	var f *os.File
	if !exists {
		f, err = os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if os.IsExist(err) {
			// Another fence may have created the marker concurrently. Still sync it.
			f, err = os.OpenFile(p, os.O_RDWR, 0)
		}
	} else {
		f, err = os.OpenFile(p, os.O_RDWR, 0)
	}
	if err != nil {
		return fmt.Errorf("open fence marker: %w", err)
	}
	info, statErr := f.Stat()
	if statErr != nil {
		_ = f.Close()
		return fmt.Errorf("stat opened fence marker: %w", statErr)
	}
	if !info.Mode().IsRegular() {
		_ = f.Close()
		return fmt.Errorf("fence marker %s is not a regular file", p)
	}
	syncErr := syncFile(f)
	closeErr := f.Close()
	if syncErr != nil {
		return fmt.Errorf("sync fence marker: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close fence marker: %w", closeErr)
	}

	// Writers create their paths with MkdirAll. Sync every ancestor, bottom up,
	// so a new log/instance directory cannot disappear along with a synced marker.
	// This also repairs retries after an earlier directory sync failed; existence
	// alone cannot tell us which directory entries were durably created.
	for dir := segmentDir; ; dir = filepath.Dir(dir) {
		if err := ctx.Err(); err != nil {
			return err
		}
		d, err := os.Open(dir)
		if err != nil {
			return fmt.Errorf("open fence directory %s: %w", dir, err)
		}
		syncErr := syncFile(d)
		closeErr := d.Close()
		if syncErr != nil {
			return fmt.Errorf("sync fence directory %s: %w", dir, syncErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close fence directory %s: %w", dir, closeErr)
		}
		if filepath.Dir(dir) == dir {
			return ctx.Err()
		}
	}
}
