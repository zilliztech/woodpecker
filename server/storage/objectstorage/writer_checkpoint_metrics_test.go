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
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/metrics"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

func checkpointGaugeValue(t *testing.T, gauge prometheus.Gauge) float64 {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, gauge.Write(metric))
	return metric.GetGauge().GetValue()
}

func TestCheckpointStoredGaugesBalanceOverwritesAndDeletion(t *testing.T) {
	for _, deleteFlag := range []int{0, 1} {
		t.Run(fmt.Sprintf("delete_flag_%d", deleteFlag), func(t *testing.T) {
			ctx := context.Background()
			w := newTestMinioFileWriter()
			t.Cleanup(w.pool.Release)
			baseDir := fmt.Sprintf("checkpoint-metrics-%d", deleteFlag)
			w.segmentFileKey = getSegmentFileKey(baseDir, w.logId, w.segmentId)
			w.logNs = w.bucket + "/" + baseDir
			bytesGauge := metrics.WpObjectStorageStoredBytes.WithLabelValues(metrics.NodeID, w.logNs, w.logIdStr)
			objectsGauge := metrics.WpObjectStorageStoredObjects.WithLabelValues(metrics.NodeID, w.logNs, w.logIdStr)
			t.Cleanup(func() {
				metrics.WpObjectStorageStoredBytes.DeleteLabelValues(metrics.NodeID, w.logNs, w.logIdStr)
				metrics.WpObjectStorageStoredObjects.DeleteLabelValues(metrics.NodeID, w.logNs, w.logIdStr)
			})
			m := mocks_objectstorage.NewObjectStorage(t)
			w.client = m
			key := getCheckpointBlockKey(w.segmentFileKey)
			var storedSize int64
			expectSuccessfulPut := func() {
				m.EXPECT().PutObject(mock.Anything, w.bucket, key, mock.Anything, mock.Anything, w.logNs, w.logIdStr).RunAndReturn(
					func(_ context.Context, _, _ string, reader io.Reader, size int64, _, _ string) error {
						data, err := io.ReadAll(reader)
						require.NoError(t, err)
						require.Len(t, data, int(size))
						storedSize = size
						return nil
					},
				).Once()
			}
			writeSnapshot := func(blocks []*codec.IndexRecord) {
				// Run the actual worker synchronously so metrics checks are independent
				// of goroutine scheduling; asynchronous coalescing is tested separately.
				w.checkpointPendingSnapshot = blocks
				w.checkpointRunning = true
				w.writeCheckpointLoop(ctx)
				require.False(t, w.checkpointRunning)
			}
			assertStored := func() {
				require.Equal(t, float64(1), checkpointGaugeValue(t, objectsGauge))
				require.Equal(t, float64(storedSize), checkpointGaugeValue(t, bytesGauge))
			}

			// A failed first PUT must not count an object that was not persisted.
			failure := errors.New("checkpoint PUT failed")
			m.EXPECT().PutObject(mock.Anything, w.bucket, key, mock.Anything, mock.Anything, w.logNs, w.logIdStr).Return(failure).Once()
			writeSnapshot(checkpointTestIndexes(1))
			require.Zero(t, checkpointGaugeValue(t, objectsGauge))
			require.Zero(t, checkpointGaugeValue(t, bytesGauge))

			expectSuccessfulPut()
			writeSnapshot(checkpointTestIndexes(1))
			assertStored()
			firstSize := storedSize

			// A failed overwrite retains the previously stored object and gauge values.
			m.EXPECT().PutObject(mock.Anything, w.bucket, key, mock.Anything, mock.Anything, w.logNs, w.logIdStr).Return(failure).Once()
			writeSnapshot(checkpointTestIndexes(2))
			require.Equal(t, firstSize, storedSize)
			assertStored()
			require.Equal(t, int32(1), w.lastCheckpointBlockCount.Load())

			// Successful overwrites adjust the size delta without adding object count.
			for _, blocks := range []int{2, 3} {
				expectSuccessfulPut()
				writeSnapshot(checkpointTestIndexes(blocks))
				assertStored()
			}
			// Serialization failure also leaves stored-object bookkeeping unchanged.
			writeSnapshot([]*codec.IndexRecord{nil})
			assertStored()
			require.Equal(t, int32(3), w.lastCheckpointBlockCount.Load())

			// Both whole-segment and regular-block cleanup include checkpoint.blk.
			m.EXPECT().WalkWithObjects(mock.Anything, w.bucket, w.segmentFileKey+"/", false, mock.Anything, w.logNs, w.logIdStr).RunAndReturn(
				func(_ context.Context, _, _ string, _ bool, visit storageclient.ChunkObjectWalkFunc, _, _ string) error {
					visit(&storageclient.ChunkObjectInfo{FilePath: key, Size: storedSize})
					return nil
				},
			).Once()
			m.EXPECT().RemoveObject(mock.Anything, w.bucket, key, w.logNs, w.logIdStr).Return(nil).Once()
			segment := NewSegmentImpl(ctx, w.bucket, baseDir, w.logId, w.segmentId, m, newTestConfig())
			deleted, err := segment.DeleteFileData(ctx, deleteFlag)
			require.NoError(t, err)
			require.Equal(t, 1, deleted)
			require.Zero(t, checkpointGaugeValue(t, objectsGauge))
			require.Zero(t, checkpointGaugeValue(t, bytesGauge))
		})
	}
}
