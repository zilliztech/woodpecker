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

package objectstorage

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/metrics"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
)

func TestCheckpointCloseDrainsPutBeforeDeletion(t *testing.T) {
	for _, commit := range []bool{false, true} {
		for _, closeCanceled := range []bool{false, true} {
			t.Run(fmt.Sprintf("commit_%t/close_canceled_%t", commit, closeCanceled), func(t *testing.T) {
				w := newTestMinioFileWriter()
				baseDir := "checkpoint-close-" + t.Name()
				w.segmentFileKey = getSegmentFileKey(baseDir, w.logId, w.segmentId)
				w.logNs = w.bucket + "/" + baseDir
				w.checkpointThreshold = 1
				w.allUploadingTaskDone.Store(!closeCanceled)
				bytesGauge := metrics.WpObjectStorageStoredBytes.WithLabelValues(metrics.NodeID, w.logNs, w.logIdStr)
				objectsGauge := metrics.WpObjectStorageStoredObjects.WithLabelValues(metrics.NodeID, w.logNs, w.logIdStr)
				t.Cleanup(func() {
					metrics.WpObjectStorageStoredBytes.DeleteLabelValues(metrics.NodeID, w.logNs, w.logIdStr)
					metrics.WpObjectStorageStoredObjects.DeleteLabelValues(metrics.NodeID, w.logNs, w.logIdStr)
				})
				m := mocks_objectstorage.NewObjectStorage(t)
				w.client = m
				key := getCheckpointBlockKey(w.segmentFileKey)
				var storedSize atomic.Int64
				started := make(chan context.Context, 1)
				release := make(chan struct{})
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				putDone := make(chan struct{})
				closeDone := make(chan struct{})
				var closeErr error
				m.EXPECT().PutObject(mock.Anything, w.bucket, key, mock.Anything, mock.Anything, w.logNs, w.logIdStr).RunAndReturn(
					func(ctx context.Context, _, _ string, _ io.Reader, size int64, _, _ string) error {
						defer close(putDone)
						started <- ctx
						// Even a canceled request can already have committed remotely.
						// Hold its result to prove Close joins it instead of just canceling.
						<-release
						if commit {
							storedSize.Store(size)
							return nil
						}
						return context.Canceled
					},
				).Once()
				m.EXPECT().WalkWithObjects(mock.Anything, w.bucket, w.segmentFileKey+"/", false, mock.Anything, w.logNs, w.logIdStr).RunAndReturn(
					func(_ context.Context, _, _ string, _ bool, visit storageclient.ChunkObjectWalkFunc, _, _ string) error {
						if size := storedSize.Load(); size > 0 {
							visit(&storageclient.ChunkObjectInfo{FilePath: key, Size: size})
						}
						return nil
					},
				).Once()
				if commit {
					m.EXPECT().RemoveObject(mock.Anything, w.bucket, key, w.logNs, w.logIdStr).RunAndReturn(
						func(context.Context, string, string, string, string) error {
							storedSize.Store(0)
							return nil
						},
					).Maybe()
				}
				t.Cleanup(func() {
					unblock()
					select {
					case <-closeDone:
					case <-time.After(5 * time.Second):
						t.Error("Close did not exit after releasing checkpoint PUT")
					}
				})
				w.blockIndexes = checkpointTestIndexes(1)
				triggerCtx, cancelTrigger := context.WithCancel(context.Background())
				defer cancelTrigger()
				w.maybeWriteCheckpointAsync(triggerCtx)
				var putCtx context.Context
				select {
				case putCtx = <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("checkpoint PUT did not start")
				}
				cancelTrigger()
				require.NoError(t, putCtx.Err(), "data acknowledgment cancellation must not stop the checkpoint")
				// A queued follow-up snapshot must be dropped on Close.
				w.blockIndexes = checkpointTestIndexes(2)
				w.maybeWriteCheckpointAsync(context.Background())
				closeCtx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if closeCanceled {
					cancel()
				}
				go func() {
					closeErr = w.Close(closeCtx)
					close(closeDone)
				}()

				select {
				case <-putCtx.Done():
				case <-closeDone:
					t.Error("Close returned without canceling or joining the checkpoint PUT")
				case <-time.After(5 * time.Second):
					t.Error("Close did not cancel the checkpoint PUT")
				}
				closedEarly := false
				select {
				case <-closeDone:
					closedEarly = true
					t.Error("Close returned while the checkpoint PUT was still running")
				case <-time.After(50 * time.Millisecond):
				}
				segment := NewSegmentImpl(context.Background(), w.bucket, baseDir, w.logId, w.segmentId, m, newTestConfig())
				deleteObjects := func() {
					_, err := segment.DeleteFileData(context.Background(), 0)
					require.NoError(t, err)
				}
				if closedEarly {
					// Reproduce the old Close -> delete -> late PUT orphan sequence.
					deleteObjects()
				}
				unblock()
				select {
				case <-closeDone:
				case <-time.After(5 * time.Second):
					t.Fatal("Close did not wait for the checkpoint PUT to finish")
				}
				<-putDone
				// Also wait for accounting when testing the old, detached worker.
				require.Eventually(t, func() bool {
					w.checkpointMu.Lock()
					defer w.checkpointMu.Unlock()
					return !w.checkpointRunning
				}, time.Second, time.Millisecond)
				if closeCanceled {
					require.ErrorIs(t, closeErr, context.Canceled)
				} else {
					require.NoError(t, closeErr)
				}
				if !closedEarly {
					deleteObjects()
				}
				require.Zero(t, storedSize.Load(), "checkpoint PUT recreated an object after deletion")
				require.Zero(t, checkpointGaugeValue(t, objectsGauge))
				require.Zero(t, checkpointGaugeValue(t, bytesGauge))
				w.maybeWriteCheckpointAsync(context.Background())
				require.NoError(t, w.Close(context.Background()), "Close remains idempotent")
			})
		}
	}
}

func TestCheckpointTriggerConcurrentWithClose(t *testing.T) {
	for i := 0; i < 32; i++ {
		w := newTestMinioFileWriter()
		w.checkpointThreshold = 1
		w.allUploadingTaskDone.Store(true)
		w.blockIndexes = checkpointTestIndexes(1)
		m := mocks_objectstorage.NewObjectStorage(t)
		w.client = m
		var closeReturned atomic.Bool
		var latePut atomic.Bool
		m.EXPECT().PutObject(mock.Anything, w.bucket, getCheckpointBlockKey(w.segmentFileKey), mock.Anything, mock.Anything, w.logNs, w.logIdStr).RunAndReturn(
			func(ctx context.Context, _, _ string, _ io.Reader, _ int64, _, _ string) error {
				if closeReturned.Load() {
					latePut.Store(true)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(5 * time.Second):
					return fmt.Errorf("Close did not cancel checkpoint PUT")
				}
			},
		).Maybe()
		start := make(chan struct{})
		triggerDone := make(chan struct{})
		go func() {
			<-start
			w.maybeWriteCheckpointAsync(context.Background())
			close(triggerDone)
		}()
		close(start)
		require.NoError(t, w.Close(context.Background()))
		closeReturned.Store(true)
		<-triggerDone
		require.False(t, latePut.Load(), "checkpoint PUT started after Close returned")
		w.checkpointMu.Lock()
		running, pending := w.checkpointRunning, w.checkpointPendingSnapshot
		w.checkpointMu.Unlock()
		require.False(t, running, "Close left a checkpoint worker running")
		require.Nil(t, pending, "Close left a checkpoint snapshot pending")
	}
}
