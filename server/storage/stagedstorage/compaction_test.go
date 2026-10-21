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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
	objectreader "github.com/zilliztech/woodpecker/server/storage/objectstorage"
)

var errCompactionObjectMissing = errors.New("test object not found")

type compactionAttemptKey struct{}

type compactionPut struct {
	attempt string
	key     string
	data    []byte
	allow   chan error
	done    chan struct{}
}

// Each PUT commits atomically, just like the object-store primitive compaction
// relies on. Barriers expose every overwrite boundary without scheduling sleeps.
// The embedded nil interface deliberately rejects unexpected storage operations.
type compactionObjectStore struct {
	storageclient.ObjectStorage
	mu      sync.RWMutex
	objects map[string][]byte
	puts    chan *compactionPut
	read    compactionReadBarrier
}

type compactionReadBarrier struct {
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func newCompactionObjectStore() *compactionObjectStore {
	return &compactionObjectStore{
		objects: make(map[string][]byte),
		read: compactionReadBarrier{
			entered: make(chan struct{}, 1),
			release: make(chan struct{}),
		},
	}
}

func (s *compactionObjectStore) snapshot() map[string][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	objects := make(map[string][]byte, len(s.objects))
	for key, data := range s.objects {
		objects[key] = bytes.Clone(data)
	}
	return objects
}

func (s *compactionObjectStore) StatObject(_ context.Context, _, key, _, _ string) (int64, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, ok := s.objects[key]
	if !ok {
		return 0, false, errCompactionObjectMissing
	}
	return int64(len(data)), false, nil
}

func (s *compactionObjectStore) GetObject(ctx context.Context, _, key string, offset, size int64, _, _ string) (minioHandler.FileReader, error) {
	s.mu.RLock()
	data, ok := s.objects[key]
	data = bytes.Clone(data)
	s.mu.RUnlock()
	if !ok {
		return nil, errCompactionObjectMissing
	}
	if strings.Contains(key, "/m_") && s.read.armed.CompareAndSwap(true, false) {
		s.read.entered <- struct{}{}
		select {
		case <-s.read.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if offset < 0 || offset > int64(len(data)) {
		return nil, fmt.Errorf("invalid offset %d for %s", offset, key)
	}
	end := int64(len(data))
	if size > 0 {
		end = min(end, offset+size)
	}
	return &readerMockFileReader{data: data[offset:end]}, nil
}

func (s *compactionObjectStore) PutObject(ctx context.Context, _, key string, body io.Reader, size int64, _, _ string) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if int64(len(data)) != size {
		return fmt.Errorf("PUT %s size mismatch", key)
	}
	if s.puts != nil {
		attempt, _ := ctx.Value(compactionAttemptKey{}).(string)
		put := &compactionPut{attempt: attempt, key: key, data: data, allow: make(chan error, 1), done: make(chan struct{})}
		defer close(put.done)
		select {
		case s.puts <- put:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case err := <-put.allow:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.objects[key] = data
	s.mu.Unlock()
	return nil
}

func (s *compactionObjectStore) IsObjectNotExistsError(err error) bool {
	return errors.Is(err, errCompactionObjectMissing)
}

func (s *compactionObjectStore) PutObjectIfNoneMatch(context.Context, string, string, io.Reader, int64, string, string) error {
	return errors.New("conditional writes are disabled")
}

func (s *compactionObjectStore) PutFencedObject(context.Context, string, string, string, string) error {
	return errors.New("conditional writes are disabled")
}

func compactionConfig(t *testing.T, target int64) *config.Configuration {
	t.Helper()
	cfg := newTestConfig(t)
	cfg.Woodpecker.Logstore.SegmentCompactionPolicy.MaxBytes = config.NewByteSize(target)
	cfg.Woodpecker.Logstore.SegmentCompactionPolicy.MaxParallelUploads = 3
	// Only explicit Sync calls choose the source layout in these tests.
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForService = config.NewDurationMillisecondsFromInt(3_600_000)
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = config.NewByteSize(16 << 20)
	return cfg
}

func newCompactionReplica(t *testing.T, objects storageclient.ObjectStorage, cfg *config.Configuration, records [][]byte, groups []int, expected int64) *StagedFileWriter {
	t.Helper()
	w, err := NewStagedFileWriter(t.Context(), "test-bucket", "test-root", t.TempDir(), 1, 0, objects, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close(context.Background())) })
	next := 0
	for _, count := range groups {
		durable := channel.NewLocalResultChannel("compaction-fixture")
		for i := 0; i < count; i++ {
			var result channel.ResultChannel
			if i == count-1 {
				result = durable
			}
			_, err := w.WriteDataAsync(t.Context(), int64(next), records[next], result)
			require.NoError(t, err)
			next++
		}
		require.NoError(t, w.Sync(t.Context()))
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		_, err := durable.ReadResult(ctx)
		cancel()
		require.NoError(t, err)
	}
	require.Equal(t, len(records), next)
	_, err = w.Finalize(t.Context(), expected)
	require.NoError(t, err)
	require.Len(t, w.blockIndexes, len(groups), "fixture must really have the requested flush boundaries")
	return w
}

func compactionPayloads(sizes ...int) [][]byte {
	records := make([][]byte, len(sizes))
	for i, size := range sizes {
		records[i] = bytes.Repeat([]byte{byte(i + 1)}, size)
	}
	return records
}

func assertCompactionReads(t *testing.T, reader storage.Reader, want [][]byte, pinned bool) {
	t.Helper()
	var state *proto.LastReadState
	if pinned {
		// Reuse the reader's cached footer/index instead of refreshing it. Entry 0
		// starts before the first block; the compacted flag keeps the same layout.
		state = &proto.LastReadState{Flags: uint32(codec.FooterFlagCompacted), Version: codec.FormatVersion, LastBlockId: -1}
	}
	result, err := reader.ReadNextBatchAdv(t.Context(), storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: int64(len(want) + 1)}, state)
	require.NoError(t, err)
	require.Len(t, result.Entries, len(want))
	for i, entry := range result.Entries {
		require.Equal(t, int64(i), entry.EntryId)
		require.Equal(t, want[i], entry.Values, "entry %d", i)
	}
	_, err = reader.ReadNextBatchAdv(t.Context(), storage.ReaderOpt{StartEntryID: int64(len(want)), MaxBatchEntries: 1}, result.LastReadState)
	require.True(t, werr.ErrFileReaderEndOfFile.Is(err), "read beyond confirmed LAC: %v", err)
}

func newCompactionObjectReader(t *testing.T, objects storageclient.ObjectStorage) *objectreader.MinioFileReaderAdv {
	t.Helper()
	r, err := objectreader.NewMinioFileReaderAdv(t.Context(), "test-bucket", "test-root", 1, 0, objects, 1<<20, 2)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close(context.Background())) })
	return r
}

func TestCompactionDeterministicAcrossFlushBoundaries(t *testing.T) {
	// Encoded lengths 20+20 exactly fill the target. The next record exceeds
	// the target alone. Small records and an unconfirmed suffix are included.
	records := compactionPayloads(11, 11, 60, 1, 3, 15, 7, 200)
	want := records[:7]
	layouts := [][]int{{8}, {1, 1, 1, 1, 1, 1, 1, 1}, {2, 3, 3}, {3, 1, 4}}
	var baseline map[string][]byte
	for _, groups := range layouts {
		t.Run(fmt.Sprint(groups), func(t *testing.T) {
			objects := newCompactionObjectStore()
			w := newCompactionReplica(t, objects, compactionConfig(t, 40), records, groups, 6)
			size, err := w.Compact(t.Context(), 6)
			require.NoError(t, err)
			require.Positive(t, size)
			actual := objects.snapshot()
			if baseline == nil {
				baseline = actual
			} else {
				require.Equal(t, baseline, actual, "every block and footer must be byte-identical")
			}
			footer, err := codec.ParseFooterFromBytes(actual["test-root/1/0/footer.blk"])
			require.NoError(t, err)
			require.Equal(t, int64(6), footer.LAC)
			require.Equal(t, int32(4), footer.TotalBlocks)
			indexes := w.blockIndexes
			require.Equal(t, int64(1), indexes[0].LastEntryID, "exact size stays in the same output block")
			require.Equal(t, int64(2), indexes[1].FirstEntryID)
			require.Equal(t, int64(2), indexes[1].LastEntryID, "oversized record occupies one block")
			assertCompactionReads(t, newCompactionObjectReader(t, objects), want, false)
			// Retrying must reuse the published result, even though local indexes
			// have since been replaced with compacted indexes.
			again, err := w.Compact(t.Context(), 6)
			require.NoError(t, err)
			require.Equal(t, size, again)
			require.Equal(t, actual, objects.snapshot())
		})
	}
}

type compactionRun struct {
	done chan struct{}
	err  error
}

func startCompaction(t *testing.T, ctx context.Context, cancel context.CancelFunc, w *StagedFileWriter, name string, expected int64) *compactionRun {
	t.Helper()
	run := &compactionRun{done: make(chan struct{})}
	go func() {
		defer close(run.done)
		_, run.err = w.Compact(context.WithValue(ctx, compactionAttemptKey{}, name), expected)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-run.done:
		case <-time.After(5 * time.Second):
			t.Error("compaction did not drain its uploads after cancellation")
		}
	})
	return run
}

func allowCompactionPut(t *testing.T, ctx context.Context, put *compactionPut, err error) {
	t.Helper()
	put.allow <- err
	select {
	case <-put.done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestConcurrentCompactionReadsDuringEveryOverwrite(t *testing.T) {
	// Five output blocks, but radically different source layouts. The second
	// attempt starts before publication and can fail before ANY of its five
	// block PUTs or its footer PUT, or finish successfully.
	for failAfter := 0; failAfter <= 6; failAfter++ {
		t.Run(fmt.Sprintf("fail_after_%d_successful_puts", failAfter), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			objects := newCompactionObjectStore()
			objects.puts = make(chan *compactionPut, 32)
			records := compactionPayloads(11, 11, 11, 11, 11, 11, 11, 11, 11, 200)
			want := records[:9]
			cfg := compactionConfig(t, 40)
			a := newCompactionReplica(t, objects, cfg, records, []int{10}, 8)
			b := newCompactionReplica(t, objects, cfg, records, []int{1, 2, 1, 3, 3}, 8)
			localReader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", a.localBaseDir, 1, 0, objects, cfg)
			require.NoError(t, err)
			defer localReader.Close(context.Background())
			assertCompactionReads(t, localReader, want, false)

			bRun := startCompaction(t, ctx, cancel, b, "B", 8)
			var held []*compactionPut
			select {
			case put := <-objects.puts:
				require.Equal(t, "B", put.attempt)
				held = append(held, put)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			aRun := startCompaction(t, ctx, cancel, a, "A", 8)
			published := false
			for !published {
				select {
				case put := <-objects.puts:
					if put.attempt == "B" {
						held = append(held, put)
						continue
					}
					allowCompactionPut(t, ctx, put, nil)
					published = strings.HasSuffix(put.key, "/footer.blk")
					assertCompactionReads(t, localReader, want, false)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			select {
			case <-aRun.done:
				require.NoError(t, aRun.err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			baseline := objects.snapshot()
			require.Len(t, baseline, 6, "five merged blocks and their footer")
			cached := newCompactionObjectReader(t, objects)
			assertCompactionReads(t, cached, want, true)
			// Hold a real block GET with A's object snapshot while B overwrites
			// the published objects. This read spans the entire overwrite phase.
			inFlight := newCompactionObjectReader(t, objects)
			objects.read.armed.Store(true)
			type readResult struct {
				batch *proto.BatchReadResult
				err   error
			}
			readDone := make(chan readResult, 1)
			go func() {
				batch, readErr := inFlight.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 100}, nil)
				readDone <- readResult{batch: batch, err: readErr}
			}()
			select {
			case <-objects.read.entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			injected := errors.New("injected compaction PUT failure")
			committed := 0
			for {
				var put *compactionPut
				if len(held) > 0 {
					put, held = held[0], held[1:]
				} else {
					select {
					case put = <-objects.puts:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				require.Equal(t, "B", put.attempt)
				require.Equal(t, baseline[put.key], put.data, "the pending overwrite must already be byte-identical")
				// Check fresh and cached readers on both sides of every atomic PUT.
				assertCompactionReads(t, cached, want, true)
				fresh := newCompactionObjectReader(t, objects)
				assertCompactionReads(t, fresh, want, false)
				require.NoError(t, fresh.Close(ctx))
				var putErr error
				if committed == failAfter {
					putErr = injected
				}
				allowCompactionPut(t, ctx, put, putErr)
				require.Equal(t, baseline, objects.snapshot())
				assertCompactionReads(t, cached, want, true)
				fresh = newCompactionObjectReader(t, objects)
				assertCompactionReads(t, fresh, want, false)
				require.NoError(t, fresh.Close(ctx))
				if putErr != nil || strings.HasSuffix(put.key, "/footer.blk") {
					break
				}
				committed++
			}
			select {
			case <-bRun.done:
				if failAfter < 6 {
					require.ErrorIs(t, bRun.err, injected)
				} else {
					require.NoError(t, bRun.err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			require.Equal(t, baseline, objects.snapshot())
			assertCompactionReads(t, cached, want, true)
			close(objects.read.release)
			select {
			case result := <-readDone:
				require.NoError(t, result.err)
				require.Len(t, result.batch.Entries, len(want))
				for i, entry := range result.batch.Entries {
					require.Equal(t, int64(i), entry.EntryId)
					require.Equal(t, want[i], entry.Values)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

func TestCompactionRejectsInvalidInputWithoutPublishing(t *testing.T) {
	for _, mode := range []string{"gap", "overlap", "crc", "short_file"} {
		t.Run(mode, func(t *testing.T) {
			objects := newCompactionObjectStore()
			records := compactionPayloads(20, 20, 20, 20)
			w := newCompactionReplica(t, objects, compactionConfig(t, 40), records, []int{2, 2}, 3)
			switch mode {
			case "gap":
				w.blockIndexes[1].FirstEntryID++
			case "overlap":
				w.blockIndexes[1].FirstEntryID--
			case "crc":
				_, err := w.file.WriteAt([]byte{255}, w.blockIndexes[1].StartOffset+int64(codec.RecordHeaderSize+codec.BlockHeaderRecordSize))
				require.NoError(t, err)
			case "short_file":
				require.NoError(t, os.Truncate(w.segmentFilePath, w.blockIndexes[1].StartOffset+1))
			}
			_, err := w.Compact(t.Context(), 3)
			require.Error(t, err)
			_, published := objects.snapshot()["test-root/1/0/footer.blk"]
			require.False(t, published)
		})
	}
}

func TestCompactionAllFlushPartitionsProduceIdenticalObjects(t *testing.T) {
	// Exhaust all 32 partitions of six records, not just a few chosen layouts.
	records := compactionPayloads(1, 1, 29, 2, 90, 6)
	var baseline map[string][]byte
	for mask := 0; mask < 1<<(len(records)-1); mask++ {
		groups := []int{1}
		for i := 1; i < len(records); i++ {
			if mask&(1<<(i-1)) != 0 {
				groups = append(groups, 1)
			} else {
				groups[len(groups)-1]++
			}
		}
		objects := newCompactionObjectStore()
		w := newCompactionReplica(t, objects, compactionConfig(t, 40), records, groups, 5)
		_, err := w.Compact(t.Context(), 5)
		require.NoError(t, err)
		if baseline == nil {
			baseline = objects.snapshot()
		} else {
			require.Equal(t, baseline, objects.snapshot(), "source partition %v", groups)
		}
		// Close per iteration instead of holding all fixture resources to test cleanup.
		require.NoError(t, w.Close(context.Background()))
	}
}

func TestCompactionEmptySegmentHasStableFooter(t *testing.T) {
	var baseline []byte
	for range 2 {
		objects := newCompactionObjectStore()
		w := newCompactionReplica(t, objects, compactionConfig(t, 40), nil, nil, -1)
		_, err := w.Compact(t.Context(), -1)
		require.NoError(t, err)
		actual := objects.snapshot()
		require.Len(t, actual, 1)
		footer := actual["test-root/1/0/footer.blk"]
		if baseline == nil {
			baseline = slices.Clone(footer)
		} else {
			require.Equal(t, baseline, footer)
		}
	}
}

func TestCompactionCancellationDrainsBoundedUploads(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	objects := newCompactionObjectStore()
	objects.puts = make(chan *compactionPut, 16)
	cfg := compactionConfig(t, 20)
	records := compactionPayloads(11, 11, 11, 11, 11, 11, 11, 11)
	w := newCompactionReplica(t, objects, cfg, records, []int{8}, 7)
	run := startCompaction(t, ctx, cancel, w, "cancelled", 7)
	for range cfg.Woodpecker.Logstore.SegmentCompactionPolicy.MaxParallelUploads {
		select {
		case <-objects.puts:
		case <-time.After(5 * time.Second):
			t.Fatal("upload pool did not fill")
		}
	}
	// The next output waits for an upload slot, rather than spawning another
	// uploader. Cancelling must release both this submitter and every PUT.
	cancel()
	select {
	case <-run.done:
		require.ErrorIs(t, run.err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("compaction failed to drain")
	}
	require.Empty(t, objects.snapshot(), "no blocked upload may commit after cancellation")
}
