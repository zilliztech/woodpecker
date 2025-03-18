package segment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_minio"
	"github.com/zilliztech/woodpecker/mocks/mocks_server/mocks_storage"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/objectstorage"
)

func TestSegmentProcessor_AddEntry(t *testing.T) {
	ctx := context.Background()
	mockMinio := mocks_minio.NewMinioHandler(t)
	mockLogFile := mocks_storage.NewLogFile(t)
	ch := make(chan int64, 1)
	ch <- int64(0)
	close(ch)
	mockLogFile.EXPECT().AppendAsync(mock.Anything, int64(0), mock.Anything).Return(int64(0), ch, nil)
	mockLogFile.EXPECT().Close().Return(nil)
	cfg := &config.Configuration{
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
		},
	}
	segProc := NewSegmentProcessorWithLogFile(context.TODO(), cfg, 1, 1, mockMinio, mockLogFile)

	seqNo, syncedCh, err := segProc.AddEntry(ctx, &SegmentEntry{
		EntryId: 0,
		Data:    []byte("data"),
	})

	assert.NoError(t, err)
	assert.Equal(t, int64(0), seqNo)
	assert.NotNil(t, syncedCh)
	assert.Equal(t, int64(0), <-syncedCh)

	// set fence
	segProc.SetFenced()
	seqNo, syncedCh, err = segProc.AddEntry(ctx, &SegmentEntry{
		EntryId: 1,
		Data:    []byte("data"),
	})
	assert.Error(t, err)
	assert.True(t, werr.ErrSegmentFenced.Is(err))
	assert.Equal(t, int64(-1), seqNo)
	assert.Nil(t, syncedCh)
}

func TestSegmentProcessor_ReadEntry(t *testing.T) {
	ctx := context.Background()
	mockMinio := mocks_minio.NewMinioHandler(t)
	mockLogFile := mocks_storage.NewLogFile(t)
	mockReader := new(mockLogFileReader)
	mockLogFile.EXPECT().NewReader(mock.Anything, mock.Anything).Return(mockReader, nil)

	// mock reader HasNext/ReadNext
	mockReader.On("HasNext").Return(true).Once()
	mockReader.On("HasNext").Return(false)
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
	mockLogFile := mocks_storage.NewLogFile(t)
	mockLogFile.EXPECT().Merge(mock.Anything).Return([]storage.Fragment{
		objectstorage.NewFragmentObject(mockMinio, "test-bucket", uint64(0), "woodpecker/1/1/0/m_0.frag", [][]byte{[]byte("data0"), []byte("data1"), []byte("data2")}, int64(10), true, false, true),
		objectstorage.NewFragmentObject(mockMinio, "test-bucket", uint64(1), "woodpecker/1/1/0/m_1.frag", [][]byte{[]byte("data3"), []byte("data4"), []byte("data5")}, int64(13), true, false, true),
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
	mockLogFile := mocks_storage.NewLogFile(t)
	mockLogFile.EXPECT().Load(mock.Anything).Return(
		int64(0),
		objectstorage.NewFragmentObject(mockMinio, "test-bucket", uint64(1), "woodpecker/1/1/0/1.frag", [][]byte{[]byte("data3"), []byte("data4"), []byte("data5")}, int64(13), true, false, true),
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

func (m *mockLogFileReader) ReadNext() (*proto.LogEntry, error) {
	args := m.Called()
	return args.Get(0).(*proto.LogEntry), args.Error(1)
}

func (m *mockLogFileReader) HasNext() bool {
	args := m.Called()
	return args.Bool(0)
}
