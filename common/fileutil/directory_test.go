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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMkdirAllSyncRepairsExistingPathAfterFailedSync(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "instance", "log", "segment")
	injected := errors.New("parent fsync failed")
	err := mkdirAllSyncUnder(root, dir, 0o755, func(dataRoot *os.Root, path string) error {
		if path == "instance" {
			return injected
		}
		return syncDirUnder(dataRoot, path)
	})
	require.ErrorIs(t, err, injected)
	require.DirExists(t, dir, "MkdirAll can succeed before directory sync fails")

	// Retry must publish ancestors even though all directories now exist. This
	// also covers a path initially created by a reader or another process.
	var synced []string
	require.NoError(t, mkdirAllSyncUnder(root, dir, 0o755, func(dataRoot *os.Root, path string) error {
		synced = append(synced, path)
		return syncDirUnder(dataRoot, path)
	}))
	require.Equal(t, []string{filepath.Join("instance", "log", "segment"), filepath.Join("instance", "log"), "instance", "."}, synced)
}

func TestMkdirAllSyncConcurrentSharedParents(t *testing.T) {
	root := t.TempDir()
	var wg sync.WaitGroup
	errors := make(chan error, 8)
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errors <- MkdirAllSync(filepath.Join(root, "instance", "log", fmt.Sprint(i%2)), 0o755)
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	for i := range 2 {
		require.DirExists(t, filepath.Join(root, "instance", "log", fmt.Sprint(i)))
	}
}

func TestMkdirAllSyncRejectsFileComponents(t *testing.T) {
	p := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(p, nil, 0o600))
	require.Error(t, MkdirAllSync(filepath.Join(p, "child"), 0o755))
	require.Error(t, MkdirAllSync(p, 0o755))
	require.Error(t, MkdirAllSync("", 0o755))
	require.Error(t, SyncDir(p))
	require.ErrorIs(t, SyncDir(filepath.Join(t.TempDir(), "missing")), os.ErrNotExist)
}

func TestMkdirAllSyncCreatesDirectoryWithRequestedMode(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "private")
	require.NoError(t, MkdirAllSync(dir, 0o700))
	info, err := os.Stat(dir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	require.NoError(t, MkdirAllSync(dir, 0o755), "idempotence must preserve existing mode")
	info, err = os.Stat(dir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
}

func TestMkdirAllSyncUnderRejectsSiblingBeforeCreation(t *testing.T) {
	root := t.TempDir()
	outside := root + "-sibling"
	require.Error(t, DataRoot(root).MkdirAllSync(outside, 0o755))
	require.NoDirExists(t, outside)
}

func TestMkdirAllSyncUnderPropagatesRootFailure(t *testing.T) {
	root := t.TempDir()
	fail := errors.New("root sync failed")
	require.ErrorIs(t, mkdirAllSyncUnder(root, root, 0o755, func(_ *os.Root, path string) error {
		require.Equal(t, ".", path)
		return fail
	}), fail)
}

func TestMkdirAllSyncUnderRejectsEscapingSymlink(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "alias")))
	require.Error(t, DataRoot(root).MkdirAllSync(filepath.Join(root, "alias", "segment"), 0o755))
	require.NoDirExists(t, filepath.Join(outside, "segment"))
}

func TestDataRootDirectoryLinks(t *testing.T) {
	for _, absolute := range []bool{false, true} {
		t.Run(fmt.Sprintf("absolute=%t", absolute), func(t *testing.T) {
			realRoot := t.TempDir()
			root := filepath.Join(t.TempDir(), "mounted")
			require.NoError(t, os.Symlink(realRoot, root))
			aliasDir := filepath.Join(root, "aliases")
			require.NoError(t, os.Mkdir(aliasDir, 0o755))
			target := filepath.Join(root, "shards", "current")
			require.NoError(t, os.MkdirAll(target, 0o755))
			linkTarget := filepath.Join("..", "shards", "current")
			if absolute {
				linkTarget = target
			}
			require.NoError(t, os.Symlink(linkTarget, filepath.Join(aliasDir, "instance")))

			var synced []string
			dir := filepath.Join(aliasDir, "instance", "log", "segment")
			require.NoError(t, mkdirAllSyncUnder(root, dir, 0o755, func(r *os.Root, p string) error {
				synced = append(synced, filepath.ToSlash(p))
				return syncDirUnder(r, p)
			}))
			require.DirExists(t, filepath.Join(target, "log", "segment"))
			require.Subset(t, synced, []string{"aliases", "shards/current/log/segment", "shards/current/log", "shards/current", "shards", "."})
		})
	}
}

func TestDataRootRejectsDanglingDirectoryLink(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "shards"), 0o755))
	require.NoError(t, os.Symlink(filepath.Join("shards", "current"), filepath.Join(root, "instance")))
	err := DataRoot(root).MkdirAllSync(filepath.Join(root, "instance", "log", "segment"), 0o755)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.ErrorContains(t, err, "instance")
	require.NoDirExists(t, filepath.Join(root, "shards", "current"))
}

func TestDataRootRequiresExistingDirectory(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing")
	require.ErrorIs(t, DataRoot(root).MkdirAllSync(filepath.Join(root, "segment"), 0o755), os.ErrNotExist)
	require.NoDirExists(t, root)
}
