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

package fileutil

import (
	"fmt"
	"os"
	"path/filepath"
)

// SyncDir persists a directory's entries, including newly created or renamed files.
// Syncing a file does not provide this guarantee for its name in the parent.
func SyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory %s: %w", path, err)
	}
	info, err := dir.Stat()
	if err != nil {
		_ = dir.Close()
		return fmt.Errorf("stat directory %s: %w", path, err)
	}
	if !info.IsDir() {
		_ = dir.Close()
		return fmt.Errorf("%s is not a directory", path)
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return fmt.Errorf("sync directory %s: %w", path, syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close directory %s: %w", path, closeErr)
	}
	return nil
}

// MkdirAllSync creates and durably publishes the complete directory path. Use it
// when later file fsyncs must remain reachable after a host crash. New files still
// require syncing their containing directory after creation.
//
// Existing ancestors are also synced: they may have been created by a reader,
// another process, or a previous attempt that failed during directory sync. Mere
// existence is not proof of durability. This barrier belongs at directory setup,
// not in per-entry flushes or every repeated Fence.
func MkdirAllSync(path string, perm os.FileMode) error {
	return mkdirAllSync(path, perm, SyncDir)
}

func mkdirAllSync(path string, perm os.FileMode, syncDir func(string) error) error {
	if err := os.MkdirAll(path, perm); err != nil {
		return err
	}
	dir, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve directory %s: %w", path, err)
	}
	for {
		if err := syncDir(dir); err != nil {
			return err
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil
		}
		dir = parent
	}
}
