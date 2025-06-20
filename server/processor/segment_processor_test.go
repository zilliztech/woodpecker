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

package processor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_minio"
	"github.com/zilliztech/woodpecker/mocks/mocks_server/mocks_storage"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
	"github.com/zilliztech/woodpecker/server/storage/objectstorage"
)

func TestSegmentProcessor_AddEntry(t *testing.T) {
	ctx := context.Background()
	mockMinio := mocks_minio.NewMinioHandler(t)
	mockLogFile := mocks_storage.NewSegment(t)
	ch := make(chan int64, 1)
	ch <- int64(0)
	close(ch)
	mockLogFile.EXPECT().AppendAsync(mock.Anything, int64(0), mock.Anything, mock.Anything).Return(int64(0), nil)
	mockLogFile.EXPECT().GetLastEntryId(mock.Anything).Return(int64(0), nil)
	mockLogFile.EXPECT().Close(mock.Anything).Return(nil)
	cfg := &config.Configuration{
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
		},
	}
	segProc := NewSegmentProcessorWithLogFile(context.TODO(), cfg, 1, 1, mockMinio, mockLogFile)

	rc := channel.NewLocalResultChannel("1/0/0")
	_ = rc.SendResult(context.TODO(), &channel.AppendResult{
		Err:      nil,
		SyncedId: 0,
	})
	seqNo, err := segProc.AddEntry(ctx, &SegmentEntry{
		EntryId: 0,
		Data:    []byte("data"),
	}, rc)

	assert.NoError(t, err)
	assert.Equal(t, int64(0), seqNo)

	// set fence
	lastEntryId, fencedErr := segProc.SetFenced(context.TODO())
	assert.NoError(t, fencedErr)
	assert.Equal(t, int64(0), lastEntryId)

	rc2 := channel.NewLocalResultChannel("1/0/1")
	_ = rc2.Close(context.TODO())
	seqNo, err = segProc.AddEntry(ctx, &SegmentEntry{
		EntryId: 1,
		Data:    []byte("data"),
	}, rc2)
	assert.Error(t, err)
	assert.True(t, werr.ErrSegmentFenced.Is(err))
	assert.Equal(t, int64(-1), seqNo)
}

func TestSegmentProcessor_ReadEntry(t *testing.T) {
	ctx := context.Background()
	mockMinio := mocks_minio.NewMinioHandler(t)
	mockLogFile := mocks_storage.NewSegment(t)
	mockReader := new(mockLogFileReader)
	mockLogFile.EXPECT().NewReader(mock.Anything, mock.Anything).Return(mockReader, nil)

	// mock reader HasNext/ReadNext
	mockReader.On("HasNext").Return(true, nil).Once()
	mockReader.On("HasNext").Return(false, nil)
	mockReader.On("ReadNext").Return(&proto.LogEntry{
		SegId:   1,
		EntryId: 0,
		Values:  []byte("data"),
	}, nil)

	cfg := &config.Configuration{
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
		},
	}

	segProc := NewSegmentProcessorWithLogFile(ctx, cfg, 1, 1, mockMinio, mockLogFile)

	// read success
	entry, err := segProc.ReadEntry(ctx, 0)
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, int64(0), entry.EntryId)
	assert.Equal(t, []byte("data"), entry.Data)

	// read empty
	entry, err = segProc.ReadEntry(ctx, 1)
	assert.Error(t, err)
	assert.Nil(t, entry)

}

func TestSegmentProcessor_Compact(t *testing.T) {
	ctx := context.Background()
	mockMinio := mocks_minio.NewMinioHandler(t)
	mockLogFile := mocks_storage.NewSegment(t)
	mockLogFile.EXPECT().Merge(mock.Anything).Return([]storage.Fragment{
		objectstorage.NewFragmentObject(context.TODO(), mockMinio, "test-bucket", 1, 0, int64(0), "woodpecker/1/1/m_0.frag",
			[]*cache.BufferEntry{
				{EntryId: 10, Data: []byte("data0"), NotifyChan: nil},
				{EntryId: 11, Data: []byte("data1"), NotifyChan: nil},
				{EntryId: 12, Data: []byte("data2"), NotifyChan: nil},
			},
			int64(10), true, false, true),
		objectstorage.NewFragmentObject(context.TODO(), mockMinio, "test-bucket", 1, 0, int64(1), "woodpecker/1/1/m_1.frag",
			[]*cache.BufferEntry{
				{EntryId: 13, Data: []byte("data3"), NotifyChan: nil},
				{EntryId: 14, Data: []byte("data4"), NotifyChan: nil},
				{EntryId: 15, Data: []byte("data5"), NotifyChan: nil},
			},
			int64(13), true, false, true),
	}, []int32{10, 13}, []int32{1, 5}, nil)
	cfg := &config.Configuration{
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
		},
	}
	segProc := NewSegmentProcessorWithLogFile(context.TODO(), cfg, 1, 1, mockMinio, mockLogFile)
	metadata, err := segProc.Compact(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, metadata)
	assert.Equal(t, proto.SegmentState_Sealed, metadata.State)
	assert.Equal(t, int64(15), metadata.LastEntryId)
	assert.Equal(t, []int32{10, 13}, metadata.EntryOffset)
	assert.Equal(t, []int32{1, 5}, metadata.FragmentOffset)
}

func TestSegmentProcessor_Recover(t *testing.T) {
	ctx := context.Background()
	mockMinio := mocks_minio.NewMinioHandler(t)
	mockLogFile := mocks_storage.NewSegment(t)
	mockLogFile.EXPECT().Load(mock.Anything).Return(
		int64(0),
		objectstorage.NewFragmentObject(context.TODO(), mockMinio, "test-bucket", 1, 0, int64(1), "woodpecker/1/1/1.frag",
			[]*cache.BufferEntry{
				{EntryId: 13, Data: []byte("data3"), NotifyChan: nil},
				{EntryId: 14, Data: []byte("data4"), NotifyChan: nil},
				{EntryId: 15, Data: []byte("data5"), NotifyChan: nil},
			},
			int64(13), true, false, true),
		nil)
	cfg := &config.Configuration{
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
		},
	}
	segProc := NewSegmentProcessorWithLogFile(context.TODO(), cfg, 1, 1, mockMinio, mockLogFile)
	metadata, err := segProc.Recover(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, metadata)
	assert.Equal(t, proto.SegmentState_Completed, metadata.State)
	assert.Equal(t, int64(15), metadata.LastEntryId)
}

var _ storage.Reader = (*mockLogFileReader)(nil)

type mockLogFileReader struct {
	mock.Mock
}

func (m *mockLogFileReader) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockLogFileReader) ReadNext(ctx context.Context) (*proto.LogEntry, error) {
	args := m.Called()
	return args.Get(0).(*proto.LogEntry), args.Error(1)
}

func (m *mockLogFileReader) ReadNextBatch(ctx context.Context, size int64) ([]*proto.LogEntry, error) {
	args := m.Called()
	return args.Get(0).([]*proto.LogEntry), args.Error(1)
}

func (m *mockLogFileReader) HasNext(ctx context.Context) (bool, error) {
	args := m.Called()
	return args.Bool(0), nil
}
