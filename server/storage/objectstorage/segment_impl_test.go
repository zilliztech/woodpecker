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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/mocks/mocks_minio"
)

// TestNewSegmentImpl tests the NewSegmentImpl function.
func TestNewSegmentImpl(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
					MaxEntries:      1000,
					MaxBytes:        1024 * 1024,
					MaxInterval:     1000,
					MaxFlushThreads: 5,
					MaxFlushSize:    1024 * 1024,
					MaxFlushRetries: 3,
					RetryInterval:   100,
				},
			},
		},
	}

	segmentImpl := NewSegmentImpl(context.TODO(), 1, 0, "test-segment/1/0", "test-bucket", client, cfg).(*SegmentImpl)
	assert.Equal(t, int64(1), segmentImpl.logId)
	assert.Equal(t, int64(0), segmentImpl.segmentId)
	assert.Equal(t, int64(0), segmentImpl.GetId())
	assert.Equal(t, "test-segment/1/0", segmentImpl.segmentFileKey)
	assert.Equal(t, "test-bucket", segmentImpl.bucket)
}

// TestDeleteFileData tests the DeleteFileData function.
func TestDeleteFileData(t *testing.T) {
	t.Run("SuccessfulDeletion", func(t *testing.T) {
		client := mocks_minio.NewMinioHandler(t)
		cfg := &config.Configuration{
			Woodpecker: config.WoodpeckerConfig{
				Logstore: config.LogstoreConfig{
					SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
						MaxEntries:      10,
						MaxBytes:        1024 * 1024,
						MaxInterval:     1000,
						MaxFlushThreads: 5,
						MaxFlushSize:    1024 * 1024,
						MaxFlushRetries: 3,
						RetryInterval:   100,
					},
				},
			},
		}

		// Create a list of mock objects to be returned by ListObjects
		objectCh := make(chan minio.ObjectInfo, 3)
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/0.blk", Size: 1024}
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/1.blk", Size: 2048}
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/m_0.blk", Size: 4096}
		close(objectCh)

		// Create the LogFile
		impl := NewSegmentImpl(context.TODO(), 1, 0, "test-segment/1/0", "test-bucket", client, cfg).(*SegmentImpl)

		objectCh2 := make(chan minio.ObjectInfo, 3)
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/0.blk", Size: 1024}
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/1.blk", Size: 2048}
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/m_0.blk", Size: 4096}
		close(objectCh2)
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(objectCh2).Once()
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/0.blk", mock.Anything).Return(nil)
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/1.blk", mock.Anything).Return(nil)
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/m_0.blk", mock.Anything).Return(nil)
		// Call DeleteFileData
		deleteCount, err := impl.DeleteFileData(context.Background(), 0)
		assert.NoError(t, err)
		assert.Equal(t, deleteCount, 3)
	})

	t.Run("ListObjectsError", func(t *testing.T) {
		client := mocks_minio.NewMinioHandler(t)
		cfg := &config.Configuration{
			Woodpecker: config.WoodpeckerConfig{
				Logstore: config.LogstoreConfig{
					SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
						MaxEntries:      10,
						MaxBytes:        1024 * 1024,
						MaxInterval:     1000,
						MaxFlushThreads: 5,
						MaxFlushSize:    1024 * 1024,
						MaxFlushRetries: 3,
						RetryInterval:   100,
					},
					SegmentCompactionPolicy: config.SegmentCompactionPolicy{
						MaxBytes: 4 * 1024 * 1024,
					},
				},
			},
		}

		// Create an object channel with an error
		emptyObjectCh := make(chan minio.ObjectInfo, 0)
		close(emptyObjectCh)
		// Create the LogFile
		impl := NewSegmentImpl(context.TODO(), 1, 0, "test-segment/1/0", "test-bucket", client, cfg).(*SegmentImpl)

		errorObjectCh := make(chan minio.ObjectInfo, 1)
		errorObjectCh <- minio.ObjectInfo{Err: errors.New("list error")}
		close(errorObjectCh)
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(errorObjectCh).Once()

		// Call DeleteFileData
		deleteCount, err := impl.DeleteFileData(context.Background(), 0)
		assert.Error(t, err)
		assert.Equal(t, 0, deleteCount)
	})

	t.Run("RemoveObjectError", func(t *testing.T) {
		client := mocks_minio.NewMinioHandler(t)
		cfg := &config.Configuration{
			Woodpecker: config.WoodpeckerConfig{
				Logstore: config.LogstoreConfig{
					SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
						MaxEntries:      10,
						MaxBytes:        1024 * 1024,
						MaxInterval:     1000,
						MaxFlushThreads: 5,
						MaxFlushSize:    1024 * 1024,
						MaxFlushRetries: 3,
						RetryInterval:   100,
					},
					SegmentCompactionPolicy: config.SegmentCompactionPolicy{
						MaxBytes: 4 * 1024 * 1024,
					},
				},
			},
		}

		// Create a list of mock objects to be returned by ListObjects
		objectCh := make(chan minio.ObjectInfo, 2)
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/0.blk", Size: 1024}
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/1.blk", Size: 2048}
		close(objectCh)

		// Create the LogFile
		impl := NewSegmentImpl(context.TODO(), 1, 0, "test-segment/1/0", "test-bucket", client, cfg).(*SegmentImpl)

		objectCh2 := make(chan minio.ObjectInfo, 2)
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/0.blk", Size: 1024}
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/1.blk", Size: 2048}
		close(objectCh2)
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(objectCh2).Once()
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/0.blk", mock.Anything).Return(nil)
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/1.blk", mock.Anything).Return(errors.New("remove error"))

		// Call DeleteFileData
		deleteCount, err := impl.DeleteFileData(context.Background(), 0)
		assert.Error(t, err)
		assert.Equal(t, 1, deleteCount)
	})

	t.Run("NoFragmentsToDelete", func(t *testing.T) {
		client := mocks_minio.NewMinioHandler(t)
		//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error")).Times(0)
		cfg := &config.Configuration{
			Woodpecker: config.WoodpeckerConfig{
				Logstore: config.LogstoreConfig{
					SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
						MaxEntries:      10,
						MaxBytes:        1024 * 1024,
						MaxInterval:     1000,
						MaxFlushThreads: 5,
						MaxFlushSize:    1024 * 1024,
						MaxFlushRetries: 3,
						RetryInterval:   100,
					},
					SegmentCompactionPolicy: config.SegmentCompactionPolicy{
						MaxBytes: 4 * 1024 * 1024,
					},
				},
			},
		}

		// Empty object channel
		objectCh := make(chan minio.ObjectInfo)
		close(objectCh)

		// Set up expectations
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(objectCh)

		// Create the LogFile
		impl := NewSegmentImpl(context.TODO(), 1, 0, "test-segment/1/0", "test-bucket", client, cfg).(*SegmentImpl)

		// Call DeleteFileData
		deleteCound, err := impl.DeleteFileData(context.Background(), 0)
		assert.NoError(t, err)
		// Verify internal state is reset
		assert.Equal(t, 0, deleteCound)
	})

	t.Run("SkipNonFragmentFiles", func(t *testing.T) {
		client := mocks_minio.NewMinioHandler(t)
		cfg := &config.Configuration{
			Woodpecker: config.WoodpeckerConfig{
				Logstore: config.LogstoreConfig{
					SegmentSyncPolicy: config.SegmentSyncPolicyConfig{
						MaxEntries:      10,
						MaxBytes:        1024 * 1024,
						MaxInterval:     1000,
						MaxFlushThreads: 5,
						MaxFlushSize:    1024 * 1024,
						MaxFlushRetries: 3,
						RetryInterval:   100,
					},
					SegmentCompactionPolicy: config.SegmentCompactionPolicy{
						MaxBytes: 4 * 1024 * 1024,
					},
				},
			},
		}

		// Create a list of mock objects including non-blocks files
		objectCh := make(chan minio.ObjectInfo, 3)
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/0.blk", Size: 1024}
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/metadata.json", Size: 256} // Not a block
		objectCh <- minio.ObjectInfo{Key: "test-segment/1/0/m_0.blk", Size: 4096}
		close(objectCh)

		// Create the readonly segment impl
		impl := NewSegmentImpl(context.TODO(), 1, 0, "test-segment/1/0", "test-bucket", client, cfg).(*SegmentImpl)

		objectCh2 := make(chan minio.ObjectInfo, 3)
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/0.blk", Size: 1024}
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/metadata.json", Size: 256} // Not a fragment
		objectCh2 <- minio.ObjectInfo{Key: "test-segment/1/0/m_0.blk", Size: 4096}
		close(objectCh2)
		client.EXPECT().ListObjects(mock.Anything, "test-bucket", "test-segment/1/0/", false, mock.Anything).Return(objectCh2).Once()
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/0.blk", mock.Anything).Return(nil)
		client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-segment/1/0/m_0.blk", mock.Anything).Return(nil)
		//client.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything).Return(minio.ObjectInfo{}, errors.New("error")).Times(0)
		// Call DeleteFileData
		deleteCount, err := impl.DeleteFileData(context.Background(), 0)
		assert.NoError(t, err)
		assert.Equal(t, 2, deleteCount)
	})
}
