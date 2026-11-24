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

package storage_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/fileutil"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/disk"
	"github.com/zilliztech/woodpecker/server/storage/stagedstorage"
)

type fenceWriterConstructor func(context.Context, string, *config.Configuration, bool) (storage.Writer, error)

var fenceBackends = map[string]fenceWriterConstructor{
	"local": func(ctx context.Context, dir string, cfg *config.Configuration, recover bool) (storage.Writer, error) {
		return disk.NewLocalFileWriterWithMode(ctx, dir, 1, 0, cfg, recover, fileutil.MkdirAllSync)
	},
	"service": func(ctx context.Context, dir string, cfg *config.Configuration, recover bool) (storage.Writer, error) {
		return stagedstorage.NewStagedFileWriterWithMode(ctx, "bucket", "root", dir, 1, 0, nil, cfg, recover, fileutil.MkdirAllSync)
	},
}

func TestFenceSurvivesWriterReopen(t *testing.T) {
	for backend, newWriter := range fenceBackends {
		for _, count := range []int{0, 3} {
			for _, removeData := range []bool{false, true} {
				for _, recover := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/entries=%d/removeData=%v/recover=%v", backend, count, removeData, recover), func(t *testing.T) {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						cfg, err := config.NewConfiguration()
						require.NoError(t, err)
						dir := t.TempDir()
						// Explicit recovery avoids local's cross-process lock wait. Service
						// opens normally to permit the original writes before it is fenced.
						w, err := newWriter(ctx, dir, cfg, backend == "local")
						require.NoError(t, err)
						t.Cleanup(func() { _ = w.Close(context.Background()) })
						var acks []*channel.LocalResultChannel
						for entryID := range int64(count) {
							ch := channel.NewLocalResultChannel(fmt.Sprint(entryID))
							_, err := w.WriteDataAsync(ctx, entryID, []byte("durable data"), ch)
							require.NoError(t, err)
							acks = append(acks, ch)
						}
						last, err := w.Fence(ctx)
						require.NoError(t, err)
						require.EqualValues(t, count-1, last)
						for entryID, ch := range acks {
							result, err := ch.ReadResult(ctx)
							require.NoError(t, err)
							require.NoError(t, result.Err)
							require.EqualValues(t, entryID, result.SyncedId)
						}
						require.NoError(t, w.Close(ctx))
						segmentDir := filepath.Join(dir, "1", "0")
						dataPath := filepath.Join(segmentDir, "data.log")
						before, err := os.ReadFile(dataPath)
						require.NoError(t, err)
						if removeData {
							if backend == "service" {
								segment := stagedstorage.NewStagedSegmentImpl(ctx, "bucket", "root", dir, 1, 0, nil, cfg)
								deleted, err := segment.DeleteFileData(ctx, 1)
								require.NoError(t, err)
								// Partial data reclamation must retain the fence tombstone.
								require.Equal(t, 1, deleted)
								require.FileExists(t, filepath.Join(segmentDir, storage.FenceMarkerName))
							} else {
								require.NoError(t, os.Remove(dataPath))
							}
							last = -1
						}
						// Rebuild all writer state without Finalize: no footer can mask
						// loss of the fence on an empty segment.
						rw, err := newWriter(ctx, dir, cfg, recover)
						require.NoError(t, err)
						t.Cleanup(func() { _ = rw.Close(context.Background()) })
						if !removeData {
							after, err := os.ReadFile(dataPath)
							require.NoError(t, err)
							require.Equal(t, before, after, "a stale append must not truncate fenced data")
						}
						_, err = rw.WriteDataAsync(ctx, last+1, []byte("stale append"), nil)
						require.ErrorIs(t, err, werr.ErrSegmentFenced)
						fencedLast, err := rw.Fence(ctx)
						require.NoError(t, err)
						require.Equal(t, last, fencedLast)
						_, err = rw.Finalize(ctx, last)
						require.NoError(t, err)
						require.NoError(t, rw.Close(ctx))
						require.FileExists(t, filepath.Join(segmentDir, storage.FenceMarkerName))
					})
				}
			}
		}
	}
}

func TestFenceMarkerFailureCanBeRetriedAfterDrain(t *testing.T) {
	for backend, newWriter := range fenceBackends {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration()
			require.NoError(t, err)
			dir := t.TempDir()
			w, err := newWriter(ctx, dir, cfg, true)
			require.NoError(t, err)
			defer w.Close(ctx)
			marker := filepath.Join(dir, "1", "0", storage.FenceMarkerName)
			require.NoError(t, os.Mkdir(marker, 0o755))
			_, err = w.Fence(ctx)
			require.Error(t, err, "draining writes must not bypass marker errors")
			_, err = w.WriteDataAsync(ctx, 0, []byte("stale append"), nil)
			require.ErrorIs(t, err, werr.ErrSegmentFenced)
			require.NoError(t, os.Remove(marker))
			_, err = w.Fence(ctx)
			require.NoError(t, err, "retry after drain must persist the marker")
			require.FileExists(t, marker)
		})
	}
}

func TestInvalidFenceMarkerPreventsWriterCreation(t *testing.T) {
	for backend, newWriter := range fenceBackends {
		t.Run(backend, func(t *testing.T) {
			dir := t.TempDir()
			segmentDir := filepath.Join(dir, "1", "0")
			require.NoError(t, os.MkdirAll(filepath.Join(segmentDir, storage.FenceMarkerName), 0o755))
			dataPath := filepath.Join(segmentDir, "data.log")
			data := []byte("must not be truncated")
			require.NoError(t, os.WriteFile(dataPath, data, 0o644))
			cfg, err := config.NewConfiguration()
			require.NoError(t, err)
			_, err = newWriter(context.Background(), dir, cfg, false)
			require.Error(t, err)
			after, err := os.ReadFile(dataPath)
			require.NoError(t, err)
			require.Equal(t, data, after)
		})
	}
}

func TestConcurrentFencesPersistOneMarker(t *testing.T) {
	for backend, newWriter := range fenceBackends {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cfg, err := config.NewConfiguration()
			require.NoError(t, err)
			dir := t.TempDir()
			w, err := newWriter(ctx, dir, cfg, true)
			require.NoError(t, err)
			defer w.Close(context.Background())
			var wg sync.WaitGroup
			errors := make(chan error, 4)
			for range 4 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := w.Fence(ctx)
					errors <- err
				}()
			}
			wg.Wait()
			close(errors)
			for err := range errors {
				require.NoError(t, err)
			}
			require.FileExists(t, filepath.Join(dir, "1", "0", storage.FenceMarkerName))
		})
	}
}

// An open data.log can still be fsynced after its directory was renamed. The
// original directory name can no longer be synced, so neither first-append ACK
// nor empty Finalize may report durable success.
func TestFirstFileSyncRequiresDirectoryPublication(t *testing.T) {
	for backend, newWriter := range fenceBackends {
		for _, operation := range []string{"append", "finalize"} {
			t.Run(backend+"/"+operation, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				cfg, err := config.NewConfiguration()
				require.NoError(t, err)
				cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage = config.NewDurationMillisecondsFromInt(60000)
				cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForService = config.NewDurationMillisecondsFromInt(60000)
				dir := t.TempDir()
				w, err := newWriter(ctx, dir, cfg, backend == "local")
				require.NoError(t, err)
				defer w.Close(context.Background())
				segmentDir := filepath.Join(dir, "1", "0")
				moved := segmentDir + ".moved"
				require.NoError(t, os.Rename(segmentDir, moved))
				defer func() { require.NoError(t, os.Rename(moved, segmentDir)) }()
				if operation == "finalize" {
					_, err = w.Finalize(ctx, -1)
					require.ErrorIs(t, err, os.ErrNotExist)
					return
				}
				ch := channel.NewLocalResultChannel("directory-sync-failure")
				_, err = w.WriteDataAsync(ctx, 0, []byte("must not be acknowledged"), ch)
				require.NoError(t, err)
				require.NoError(t, w.Sync(ctx))
				result, err := ch.ReadResult(ctx)
				require.NoError(t, err)
				require.ErrorIs(t, result.Err, werr.ErrStorageNotWritable)
				require.EqualValues(t, -1, w.GetLastEntryId(ctx))
			})
		}
	}
}

func TestFileConstructorsSyncParentWithMkdirAllSync(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("execute-only parent requires an unprivileged user")
	}
	parent := t.TempDir()
	require.NoError(t, os.Chmod(parent, 0o311))
	t.Cleanup(func() { require.NoError(t, os.Chmod(parent, 0o700)) })
	base := filepath.Join(parent, "new-base")
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	for backend, newWriter := range fenceBackends {
		t.Run(backend, func(t *testing.T) {
			_, err := newWriter(t.Context(), base, cfg, false)
			require.ErrorIs(t, err, os.ErrPermission)
			require.ErrorContains(t, err, parent)
			if backend == "local" {
				_, err = disk.NewLocalFileReaderAdv(t.Context(), base, 1, 0, 1, fileutil.MkdirAllSync)
			} else {
				_, err = stagedstorage.NewStagedFileReaderAdv(t.Context(), "bucket", "root", base, 1, 0, nil, cfg, fileutil.MkdirAllSync)
			}
			require.ErrorIs(t, err, os.ErrPermission)
			require.ErrorContains(t, err, parent)
			require.DirExists(t, filepath.Join(base, "1", "0"))
		})
	}
}
