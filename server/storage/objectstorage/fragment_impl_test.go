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
	"io"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/codec"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_minio"
)

func TestNewFragmentObject(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	fragment := NewFragmentObject(client, "test-bucket", 1, 0, 1, "test-key", [][]byte{[]byte("entry1"), []byte("entry2")}, 100, true, false, true)
	assert.NotNil(t, fragment)
	assert.Equal(t, "test-bucket", fragment.bucket)
	assert.Equal(t, "test-key", fragment.fragmentKey)
	assert.Equal(t, int64(100), fragment.firstEntryId)
	assert.Equal(t, int64(101), fragment.lastEntryId)
	assert.Equal(t, true, fragment.dataLoaded)
	assert.Equal(t, false, fragment.dataUploaded)
}

func TestFragmentObject_Flush(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	fragment := NewFragmentObject(client, "test-bucket", 1, 0, 1, "test-key", [][]byte{[]byte("entry1"), []byte("entry2")}, 100, true, false, true)
	client.EXPECT().PutObject(mock.Anything, "test-bucket", "test-key", mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	err := fragment.Flush(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, true, fragment.dataUploaded)
	client.AssertExpectations(t)
}

func TestFragmentObject_Flush_EmptyFragment(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	fragment := NewFragmentObject(client, "test-bucket", 1, 0, 1, "test-key", [][]byte{}, 100, false, false, false)
	err := fragment.Flush(context.Background())
	assert.Error(t, err)
	assert.True(t, werr.ErrFragmentEmpty.Is(err))
}

func TestFragmentObject_Load(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	fragment := NewFragmentObject(client, "test-bucket", 1, 0, 1, "test-key", [][]byte{}, 100, false, true, false)
	data := make([]byte, 0)
	data = append(data, codec.Int64ToBytes(1)...)
	data = append(data, codec.Int64ToBytes(100)...)
	data = append(data, codec.Int64ToBytes(101)...)
	data = append(data, []byte{0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 6}...)
	data = append(data, []byte("entry1entry2")...)

	lastModifiedTime := time.Now().UnixMilli()
	client.EXPECT().GetObjectDataAndInfo(mock.Anything, "test-bucket", "test-key", mock.Anything).Return(&mockObjectReader{
		value: data,
	}, int64(len(data)), lastModifiedTime, nil)
	err := fragment.Load(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, true, fragment.dataLoaded)
	assert.Equal(t, []byte("entry1entry2"), fragment.entriesData)
	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 6}, fragment.indexes)
	assert.Equal(t, int64(100), fragment.firstEntryId)
	assert.Equal(t, int64(101), fragment.lastEntryId)
	client.AssertExpectations(t)
}

type mockObjectReader struct {
	io.Closer
	io.Reader
	value []byte
}

func (m *mockObjectReader) Read(p []byte) (n int, err error) {
	return copy(p, m.value), io.EOF
}

func (m *mockObjectReader) Close() error {
	return nil
}

func TestFragmentObject_Load_NotUploaded(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	fragment := NewFragmentObject(client, "test-bucket", 1, 0, 1, "test-key", [][]byte{}, 100, false, false, false)
	err := fragment.Load(context.Background())
	assert.Error(t, err)
	assert.True(t, werr.ErrFragmentNotUploaded.Is(err))
}

func TestFragmentObject_GetLastEntryId(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	fragment := NewFragmentObject(client, "test-bucket", 1, 0, 1, "test-key", [][]byte{[]byte("entry1"), []byte("entry2")}, 100, true, false, true)
	lastEntryId, err := fragment.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, int64(101), lastEntryId)
}

func TestFragmentObject_GetEntry(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	fragment := NewFragmentObject(client, "test-bucket", 1, 0, 1, "test-key", [][]byte{[]byte("entry1"), []byte("entry2")}, 100, true, false, true)
	entry, err := fragment.GetEntry(100)
	assert.NoError(t, err)
	assert.Equal(t, []byte("entry1"), entry)

	entry, err = fragment.GetEntry(101)
	assert.NoError(t, err)
	assert.Equal(t, []byte("entry2"), entry)
}

func TestFragmentObject_GetEntry_NotFound(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	fragment := NewFragmentObject(client, "test-bucket", 1, 0, 1, "test-key", [][]byte{[]byte("entry1"), []byte("entry2")}, 100, true, false, true)
	entry, err := fragment.GetEntry(102)
	assert.Error(t, err)
	assert.Equal(t, werr.ErrEntryNotFound, err)
	assert.Nil(t, entry)
}

func TestFragmentObject_Release(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	fragment := NewFragmentObject(client, "test-bucket", 1, 0, 1, "test-key", [][]byte{[]byte("entry1"), []byte("entry2")}, 100, true, false, true)
	err := fragment.Release()
	assert.NoError(t, err)
	assert.Equal(t, false, fragment.dataLoaded)
	assert.Nil(t, fragment.entriesData)
	assert.Nil(t, fragment.indexes)
}

func TestMergeFragmentsAndReleaseAfterCompleted(t *testing.T) {
	client := mocks_minio.NewMinioHandler(t)
	client.EXPECT().PutObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)

	fragment1 := NewFragmentObject(client, "test-bucket", 1, 0, 1, "test-key1", [][]byte{[]byte("entry1")}, 100, true, false, true)
	fragment2 := NewFragmentObject(client, "test-bucket", 1, 0, 2, "test-key2", [][]byte{[]byte("entry2")}, 101, true, false, true)
	fragments := []*FragmentObject{fragment1, fragment2}

	mergedFragment, err := mergeFragmentsAndReleaseAfterCompleted(context.Background(), "merged-key", 3, fragments, false)
	assert.NoError(t, err)
	assert.NotNil(t, mergedFragment)
	firstEntryId, err := mergedFragment.GetFirstEntryId()
	assert.NoError(t, err)
	assert.Equal(t, int64(100), firstEntryId)
	lastEntryId, err := mergedFragment.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, int64(101), lastEntryId)

	entry, err := mergedFragment.GetEntry(100)
	assert.NoError(t, err)
	assert.Equal(t, []byte("entry1"), entry)

	entry, err = mergedFragment.GetEntry(101)
	assert.NoError(t, err)
	assert.Equal(t, []byte("entry2"), entry)

	entry, err = mergedFragment.GetEntry(102)
	assert.Error(t, err)
	assert.Equal(t, werr.ErrEntryNotFound, err)
	assert.Nil(t, entry)
}
