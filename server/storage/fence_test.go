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
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPersistFenceMarkerSyncFailureAndRetry(t *testing.T) {
	for _, stage := range []string{"file", "segment directory", "parent directory"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			dir := filepath.Join(t.TempDir(), "log", "segment")
			require.NoError(t, os.MkdirAll(dir, 0o755))
			injected := errors.New("injected fsync failure")
			var synced []string
			err := persistFenceMarker(ctx, dir, func(f *os.File) error {
				synced = append(synced, f.Name())
				if (stage == "file" && f.Name() == filepath.Join(dir, FenceMarkerName)) ||
					(stage == "segment directory" && f.Name() == dir) ||
					(stage == "parent directory" && f.Name() == filepath.Dir(dir)) {
					return injected
				}
				return f.Sync()
			})
			require.ErrorIs(t, err, injected)
			require.FileExists(t, filepath.Join(dir, FenceMarkerName))
			require.NotEmpty(t, synced)
			// The file already exists after failure. Retry must still sync it and
			// its directories rather than mistaking existence for durable success.
			require.NoError(t, PersistFenceMarker(ctx, dir))
			exists, err := FenceMarkerExists(dir)
			require.NoError(t, err)
			require.True(t, exists)
		})
	}
}

func TestPersistFenceMarkerPreservesLegacyContentsAndSyncsAncestors(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "instance", "log", "segment")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	p := filepath.Join(dir, FenceMarkerName)
	legacy := []byte("logId=1\nreason=manual_fence\n")
	require.NoError(t, os.WriteFile(p, legacy, 0o644))
	var synced []string
	require.NoError(t, persistFenceMarker(context.Background(), dir, func(f *os.File) error {
		synced = append(synced, f.Name())
		return f.Sync()
	}))
	contents, err := os.ReadFile(p)
	require.NoError(t, err)
	require.Equal(t, legacy, contents)
	require.Equal(t, p, synced[0], "sync the marker before publishing its directory entry")
	require.Contains(t, synced, dir)
	require.Contains(t, synced, filepath.Dir(dir))
	require.Contains(t, synced, filepath.Dir(filepath.Dir(dir)))
}

func TestFenceMarkerInspectionFailsClosed(t *testing.T) {
	dir := t.TempDir()
	exists, err := FenceMarkerExists(dir)
	require.NoError(t, err)
	require.False(t, exists)
	p := filepath.Join(dir, FenceMarkerName)
	require.NoError(t, os.Mkdir(p, 0o755))
	_, err = FenceMarkerExists(dir)
	require.Error(t, err)
	require.Error(t, PersistFenceMarker(context.Background(), dir))
	require.NoError(t, os.Remove(p))
	require.NoError(t, os.Symlink("missing-target", p))
	_, err = FenceMarkerExists(dir)
	require.Error(t, err, "a dangling symlink must not be treated as an absent fence")

	parentFile := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(parentFile, nil, 0o644))
	_, err = FenceMarkerExists(filepath.Join(parentFile, "segment"))
	require.Error(t, err, "inspection errors must not permit appends")
}

func TestPersistFenceMarkerConcurrent(t *testing.T) {
	dir := t.TempDir()
	const callers = 8
	var wg sync.WaitGroup
	errors := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errors <- PersistFenceMarker(context.Background(), dir)
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
}

func TestPersistFenceMarkerCancellation(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, PersistFenceMarker(ctx, dir), context.Canceled)
	require.NoFileExists(t, filepath.Join(dir, FenceMarkerName))

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	err := persistFenceMarker(ctx, dir, func(f *os.File) error {
		cancel()
		return f.Sync()
	})
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, PersistFenceMarker(context.Background(), dir))
}
