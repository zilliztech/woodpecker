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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// SyncDir persists a directory's entries, including newly created or renamed files.
// Syncing a file does not provide this guarantee for its name in the parent.
func SyncDir(path string) error {
	return openAndSyncDir(path, os.Open)
}

func openAndSyncDir(path string, open func(string) (*os.File, error)) error {
	dir, err := open(path)
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
	if err := os.MkdirAll(path, perm); err != nil {
		return err
	}
	dir, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve directory %s: %w", path, err)
	}
	return syncAncestors(dir, SyncDir)
}

// MkdirAllSyncUnder creates path and syncs its directory chain through the
// pre-provisioned root, inclusive.
func MkdirAllSyncUnder(root, path string, perm os.FileMode) error {
	return mkdirAllSyncUnder(root, path, perm, syncDirUnder)
}

func syncDirUnder(root *os.Root, path string) error {
	return openAndSyncDir(path, root.Open)
}

func mkdirAllSyncUnder(dataRoot, path string, perm os.FileMode, syncDir func(*os.Root, string) error) error {
	if dataRoot == "" || path == "" {
		return fmt.Errorf("empty data root or directory path")
	}
	rootPath, err := filepath.Abs(dataRoot)
	if err != nil {
		return err
	}
	dir, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(rootPath, dir)
	if err != nil || !filepath.IsLocal(rel) {
		return fmt.Errorf("directory %q is outside data root %q", dir, rootPath)
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return fmt.Errorf("open provisioned data root %q: %w", rootPath, err)
	}
	defer root.Close()

	syncPath := func(path string) error { return syncDir(root, path) }
	current := "."
	for component := range strings.SplitSeq(rel, string(filepath.Separator)) {
		next := filepath.Join(current, component)
		info, err := root.Lstat(next)
		if errors.Is(err, os.ErrNotExist) {
			// Create only the named entry. MkdirAll can create the missing
			// target of an intermediate dangling link.
			err = root.Mkdir(next, perm)
			if err == nil {
				current = next
				continue
			}
			if errors.Is(err, os.ErrExist) {
				info, err = root.Lstat(next)
			}
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			next, err = resolveDirectoryLink(root, next)
			if err != nil {
				return err
			}
			// The link entry and its target can have different parents.
			if err := syncAncestors(current, syncPath); err != nil {
				return err
			}
		} else if !info.IsDir() {
			return fmt.Errorf("%s is not a directory", next)
		}
		current = next
	}
	return syncAncestors(current, syncPath)
}

// Resolve an existing target before creating any children. Convert absolute and
// relative links to the same rooted path for both mkdir and directory sync.
func resolveDirectoryLink(root *os.Root, link string) (string, error) {
	base, err := filepath.EvalSymlinks(root.Name())
	if err != nil {
		return "", err
	}
	target, err := filepath.EvalSymlinks(filepath.Join(base, link))
	if err != nil {
		return "", fmt.Errorf("resolve directory link %q: %w", link, err)
	}
	rel, err := filepath.Rel(base, target)
	if err != nil || !filepath.IsLocal(rel) {
		return "", fmt.Errorf("directory link %q resolves outside data root %q", link, root.Name())
	}
	info, err := root.Stat(rel)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("directory link %q targets a non-directory", link)
	}
	return rel, nil
}

func syncAncestors(dir string, syncDir func(string) error) error {
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
