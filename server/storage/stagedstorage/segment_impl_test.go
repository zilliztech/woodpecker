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
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
)

// --- NewStagedSegmentImpl ---

func TestNewStagedSegmentImpl(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	client := mocks_objectstorage.NewObjectStorage(t)
	seg := NewStagedSegmentImpl(context.Background(), "test-bucket", "test-root", dir, 1, 0, client, cfg)
	require.NotNil(t, seg)

	impl := seg.(*StagedSegmentImpl)
	assert.Equal(t, int64(1), impl.logId)
	assert.Equal(t, int64(0), impl.segmentId)
	assert.Equal(t, "test-bucket", impl.bucket)
	assert.Equal(t, "test-root", impl.rootPath)
	assert.Equal(t, "1/0", impl.segmentFileKey)
	assert.Equal(t, getSegmentDir(dir, 1, 0), impl.segmentDir)
	assert.Equal(t, getSegmentFilePath(dir, 1, 0), impl.segmentFilePath)
}

func TestNewStagedSegmentImpl_NilClient(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	seg := NewStagedSegmentImpl(context.Background(), "bucket", "root", dir, 2, 3, nil, cfg)
	require.NotNil(t, seg)

	impl := seg.(*StagedSegmentImpl)
	assert.Nil(t, impl.client)
	assert.Equal(t, "2/3", impl.segmentFileKey)
}

// --- DeleteFileData ---

func TestDeleteFileData_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(1)
	segId := int64(0)

	// Create the segment directory (empty)
	segmentDir := getSegmentDir(dir, logId, segId)
	err = os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	client := mocks_objectstorage.NewObjectStorage(t)
	// Mock WalkWithObjects with no objects found
	client.EXPECT().WalkWithObjects(mock.Anything, "test-bucket", mock.Anything, false, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	seg := NewStagedSegmentImpl(context.Background(), "test-bucket", "test-root", dir, logId, segId, client, cfg)

	deleteCount, err := seg.DeleteFileData(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, 0, deleteCount)
}

func TestDeleteFileData_NonExistentLocalDir(t *testing.T) {
	dir := t.TempDir()
	nonExistDir := filepath.Join(dir, "nonexist")
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	client := mocks_objectstorage.NewObjectStorage(t)
	// Mock WalkWithObjects with no objects found
	client.EXPECT().WalkWithObjects(mock.Anything, "test-bucket", mock.Anything, false, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	seg := NewStagedSegmentImpl(context.Background(), "test-bucket", "test-root", nonExistDir, 1, 0, client, cfg)

	// Should succeed - non-existent directory is handled gracefully
	deleteCount, err := seg.DeleteFileData(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, 0, deleteCount)
}

func TestDeleteFileData_WithLocalFiles(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(1)
	segId := int64(0)

	// Create the segment directory and files
	segmentDir := getSegmentDir(dir, logId, segId)
	err = os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	// Create test files
	dataFile := filepath.Join(segmentDir, "data.log")
	lockFile := filepath.Join(segmentDir, "1_0.lock")
	fenceFile := filepath.Join(segmentDir, "1_0.fence")
	otherFile := filepath.Join(segmentDir, "other.txt")

	err = os.WriteFile(dataFile, []byte("segment data"), 0o644)
	require.NoError(t, err)
	err = os.WriteFile(lockFile, []byte("lock"), 0o644)
	require.NoError(t, err)
	err = os.WriteFile(fenceFile, []byte("fence"), 0o644)
	require.NoError(t, err)
	err = os.WriteFile(otherFile, []byte("other"), 0o644)
	require.NoError(t, err)

	// No minio client for this test
	seg := NewStagedSegmentImpl(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg)

	// flag=0 deletes .log, .lock, .fence files
	deleteCount, err := seg.DeleteFileData(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, 3, deleteCount)

	// Verify .log, .lock, .fence files are deleted
	_, err = os.Stat(dataFile)
	assert.True(t, os.IsNotExist(err), "data.log should be deleted")
	_, err = os.Stat(lockFile)
	assert.True(t, os.IsNotExist(err), "lock file should be deleted")
	_, err = os.Stat(fenceFile)
	assert.True(t, os.IsNotExist(err), "fence file should be deleted")

	// Other file should NOT be deleted
	_, err = os.Stat(otherFile)
	assert.NoError(t, err, "other.txt should NOT be deleted")
}

func TestDeleteFileData_WithLocalFiles_Flag1(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(1)
	segId := int64(0)

	segmentDir := getSegmentDir(dir, logId, segId)
	err = os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	dataFile := filepath.Join(segmentDir, "data.log")
	lockFile := filepath.Join(segmentDir, "1_0.lock")

	err = os.WriteFile(dataFile, []byte("data"), 0o644)
	require.NoError(t, err)
	err = os.WriteFile(lockFile, []byte("lock"), 0o644)
	require.NoError(t, err)

	seg := NewStagedSegmentImpl(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg)

	// flag=1 or 2 only deletes .log files
	deleteCount, err := seg.DeleteFileData(context.Background(), 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, deleteCount)

	// .log file deleted
	_, err = os.Stat(dataFile)
	assert.True(t, os.IsNotExist(err))

	// .lock file NOT deleted
	_, err = os.Stat(lockFile)
	assert.NoError(t, err)
}

func TestDeleteFileData_WithMinioObjects(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(1)
	segId := int64(0)

	// Create segment directory (even if empty for local)
	segmentDir := getSegmentDir(dir, logId, segId)
	err = os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	client := mocks_objectstorage.NewObjectStorage(t)

	// Mock WalkWithObjects to simulate finding minio objects
	client.EXPECT().WalkWithObjects(mock.Anything, "test-bucket", mock.Anything, false, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).
		Run(func(ctx context.Context, bucket, prefix string, recursive bool, walkFunc storageclient.ChunkObjectWalkFunc, ns string, logIdStr string) {
			walkFunc(&storageclient.ChunkObjectInfo{FilePath: "test-root/1/0/0.blk", Size: 100})
			walkFunc(&storageclient.ChunkObjectInfo{FilePath: "test-root/1/0/1.blk", Size: 200})
			walkFunc(&storageclient.ChunkObjectInfo{FilePath: "test-root/1/0/m_0.blk", Size: 300})
		}).Once()

	client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-root/1/0/0.blk", mock.Anything, mock.Anything).Return(nil)
	client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-root/1/0/1.blk", mock.Anything, mock.Anything).Return(nil)
	client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-root/1/0/m_0.blk", mock.Anything, mock.Anything).Return(nil)

	seg := NewStagedSegmentImpl(context.Background(), "test-bucket", "test-root", dir, logId, segId, client, cfg)

	deleteCount, err := seg.DeleteFileData(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, 3, deleteCount)
}

func TestDeleteFileData_MinioAndLocalCombined(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(1)
	segId := int64(0)

	// Create segment directory and local files
	segmentDir := getSegmentDir(dir, logId, segId)
	err = os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	dataFile := filepath.Join(segmentDir, "data.log")
	lockFile := filepath.Join(segmentDir, "1_0.lock")
	err = os.WriteFile(dataFile, []byte("data"), 0o644)
	require.NoError(t, err)
	err = os.WriteFile(lockFile, []byte("lock"), 0o644)
	require.NoError(t, err)

	client := mocks_objectstorage.NewObjectStorage(t)

	// Mock minio objects
	client.EXPECT().WalkWithObjects(mock.Anything, "test-bucket", mock.Anything, false, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).
		Run(func(ctx context.Context, bucket, prefix string, recursive bool, walkFunc storageclient.ChunkObjectWalkFunc, ns string, logIdStr string) {
			walkFunc(&storageclient.ChunkObjectInfo{FilePath: "test-root/1/0/0.blk", Size: 100})
		}).Once()

	client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-root/1/0/0.blk", mock.Anything, mock.Anything).Return(nil)

	seg := NewStagedSegmentImpl(context.Background(), "test-bucket", "test-root", dir, logId, segId, client, cfg)

	// flag=0: delete all types
	deleteCount, err := seg.DeleteFileData(context.Background(), 0)
	assert.NoError(t, err)
	// 1 minio object + 2 local files (.log + .lock)
	assert.Equal(t, 3, deleteCount)
}

func TestDeleteFileData_MinioWalkError(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(1)
	segId := int64(0)

	segmentDir := getSegmentDir(dir, logId, segId)
	err = os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	client := mocks_objectstorage.NewObjectStorage(t)
	client.EXPECT().WalkWithObjects(mock.Anything, "test-bucket", mock.Anything, false, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("walk error")).Once()

	seg := NewStagedSegmentImpl(context.Background(), "test-bucket", "test-root", dir, logId, segId, client, cfg)

	_, err = seg.DeleteFileData(context.Background(), 0)
	assert.Error(t, err)
}

func TestDeleteFileData_MinioRemoveError(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(1)
	segId := int64(0)

	segmentDir := getSegmentDir(dir, logId, segId)
	err = os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	client := mocks_objectstorage.NewObjectStorage(t)

	client.EXPECT().WalkWithObjects(mock.Anything, "test-bucket", mock.Anything, false, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).
		Run(func(ctx context.Context, bucket, prefix string, recursive bool, walkFunc storageclient.ChunkObjectWalkFunc, ns string, logIdStr string) {
			walkFunc(&storageclient.ChunkObjectInfo{FilePath: "test-root/1/0/0.blk", Size: 100})
			walkFunc(&storageclient.ChunkObjectInfo{FilePath: "test-root/1/0/1.blk", Size: 200})
		}).Once()

	client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-root/1/0/0.blk", mock.Anything, mock.Anything).Return(nil)
	client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-root/1/0/1.blk", mock.Anything, mock.Anything).Return(errors.New("remove error"))

	seg := NewStagedSegmentImpl(context.Background(), "test-bucket", "test-root", dir, logId, segId, client, cfg)

	deleteCount, err := seg.DeleteFileData(context.Background(), 0)
	assert.Error(t, err)
	// One object removed successfully, one failed
	assert.Equal(t, 1, deleteCount)
}

func TestDeleteFileData_Flag1_OnlyRegularBlocks(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(1)
	segId := int64(0)

	segmentDir := getSegmentDir(dir, logId, segId)
	err = os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	client := mocks_objectstorage.NewObjectStorage(t)

	client.EXPECT().WalkWithObjects(mock.Anything, "test-bucket", mock.Anything, false, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).
		Run(func(ctx context.Context, bucket, prefix string, recursive bool, walkFunc storageclient.ChunkObjectWalkFunc, ns string, logIdStr string) {
			// Regular block (no /m_ prefix)
			walkFunc(&storageclient.ChunkObjectInfo{FilePath: "test-root/1/0/0.blk", Size: 100})
			// Merged block (has /m_ prefix) - should NOT be deleted with flag=1
			walkFunc(&storageclient.ChunkObjectInfo{FilePath: "test-root/1/0/m_0.blk", Size: 200})
		}).Once()

	// Only the regular block should be removed
	client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-root/1/0/0.blk", mock.Anything, mock.Anything).Return(nil)

	seg := NewStagedSegmentImpl(context.Background(), "test-bucket", "test-root", dir, logId, segId, client, cfg)

	deleteCount, err := seg.DeleteFileData(context.Background(), 1)
	assert.NoError(t, err)
	// 1 minio regular block + 0 merged block + 1 local .log file (if exists)
	// Since no local files, just 1
	assert.Equal(t, 1, deleteCount)
}

func TestDeleteFileData_Flag2_OnlyMergedBlocks(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(1)
	segId := int64(0)

	segmentDir := getSegmentDir(dir, logId, segId)
	err = os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	client := mocks_objectstorage.NewObjectStorage(t)

	client.EXPECT().WalkWithObjects(mock.Anything, "test-bucket", mock.Anything, false, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).
		Run(func(ctx context.Context, bucket, prefix string, recursive bool, walkFunc storageclient.ChunkObjectWalkFunc, ns string, logIdStr string) {
			// Regular block should NOT be deleted with flag=2
			walkFunc(&storageclient.ChunkObjectInfo{FilePath: "test-root/1/0/0.blk", Size: 100})
			// Merged block should be deleted
			walkFunc(&storageclient.ChunkObjectInfo{FilePath: "test-root/1/0/m_0.blk", Size: 200})
		}).Once()

	// Only the merged block should be removed
	client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-root/1/0/m_0.blk", mock.Anything, mock.Anything).Return(nil)

	seg := NewStagedSegmentImpl(context.Background(), "test-bucket", "test-root", dir, logId, segId, client, cfg)

	deleteCount, err := seg.DeleteFileData(context.Background(), 2)
	assert.NoError(t, err)
	assert.Equal(t, 1, deleteCount)
}

func TestDeleteFileData_SkipNonBlockFiles(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(1)
	segId := int64(0)

	segmentDir := getSegmentDir(dir, logId, segId)
	err = os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	client := mocks_objectstorage.NewObjectStorage(t)

	client.EXPECT().WalkWithObjects(mock.Anything, "test-bucket", mock.Anything, false, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).
		Run(func(ctx context.Context, bucket, prefix string, recursive bool, walkFunc storageclient.ChunkObjectWalkFunc, ns string, logIdStr string) {
			walkFunc(&storageclient.ChunkObjectInfo{FilePath: "test-root/1/0/0.blk", Size: 100})
			walkFunc(&storageclient.ChunkObjectInfo{FilePath: "test-root/1/0/metadata.json", Size: 50}) // Not a .blk file
		}).Once()

	client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-root/1/0/0.blk", mock.Anything, mock.Anything).Return(nil)

	seg := NewStagedSegmentImpl(context.Background(), "test-bucket", "test-root", dir, logId, segId, client, cfg)

	deleteCount, err := seg.DeleteFileData(context.Background(), 0)
	assert.NoError(t, err)
	// Only the .blk file is deleted, not the .json
	assert.Equal(t, 1, deleteCount)
}

func TestDeleteFileData_Flag0_DeletesLockFiles(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(1)
	segId := int64(0)

	segmentDir := getSegmentDir(dir, logId, segId)
	err = os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	client := mocks_objectstorage.NewObjectStorage(t)

	client.EXPECT().WalkWithObjects(mock.Anything, "test-bucket", mock.Anything, false, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).
		Run(func(ctx context.Context, bucket, prefix string, recursive bool, walkFunc storageclient.ChunkObjectWalkFunc, ns string, logIdStr string) {
			walkFunc(&storageclient.ChunkObjectInfo{FilePath: "test-root/1/0/0.blk", Size: 100})
			walkFunc(&storageclient.ChunkObjectInfo{FilePath: "test-root/1/0/fence.lock", Size: 10})
		}).Once()

	client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-root/1/0/0.blk", mock.Anything, mock.Anything).Return(nil)
	client.EXPECT().RemoveObject(mock.Anything, "test-bucket", "test-root/1/0/fence.lock", mock.Anything, mock.Anything).Return(nil)

	seg := NewStagedSegmentImpl(context.Background(), "test-bucket", "test-root", dir, logId, segId, client, cfg)

	deleteCount, err := seg.DeleteFileData(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, 2, deleteCount)
}

func TestDeleteFileData_DefaultFlag_DeletesAllLocal(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(1)
	segId := int64(0)

	segmentDir := getSegmentDir(dir, logId, segId)
	err = os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	// Create various files
	dataFile := filepath.Join(segmentDir, "data.log")
	otherFile := filepath.Join(segmentDir, "other.txt")
	randomFile := filepath.Join(segmentDir, "random.xyz")

	err = os.WriteFile(dataFile, []byte("data"), 0o644)
	require.NoError(t, err)
	err = os.WriteFile(otherFile, []byte("other"), 0o644)
	require.NoError(t, err)
	err = os.WriteFile(randomFile, []byte("random"), 0o644)
	require.NoError(t, err)

	// No minio client
	seg := NewStagedSegmentImpl(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg)

	// flag=99 (default case) should delete ALL files
	deleteCount, err := seg.DeleteFileData(context.Background(), 99)
	assert.NoError(t, err)
	assert.Equal(t, 3, deleteCount)

	// All files should be gone
	_, err = os.Stat(dataFile)
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(otherFile)
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(randomFile)
	assert.True(t, os.IsNotExist(err))
}

func TestDeleteFileData_DefaultMinioFlag_NoDelete(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(1)
	segId := int64(0)

	segmentDir := getSegmentDir(dir, logId, segId)
	err = os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	client := mocks_objectstorage.NewObjectStorage(t)

	// Mock WalkWithObjects returning some objects
	client.EXPECT().WalkWithObjects(mock.Anything, "test-bucket", mock.Anything, false, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).
		Run(func(ctx context.Context, bucket, prefix string, recursive bool, walkFunc storageclient.ChunkObjectWalkFunc, ns string, logIdStr string) {
			walkFunc(&storageclient.ChunkObjectInfo{FilePath: "test-root/1/0/0.blk", Size: 100})
			walkFunc(&storageclient.ChunkObjectInfo{FilePath: "test-root/1/0/m_0.blk", Size: 200})
		}).Once()

	// With flag=99 (default case), shouldDelete = false for minio, so NO RemoveObject calls expected

	seg := NewStagedSegmentImpl(context.Background(), "test-bucket", "test-root", dir, logId, segId, client, cfg)

	// flag=99: minio default case -> no objects deleted, local default case -> all files deleted
	deleteCount, err := seg.DeleteFileData(context.Background(), 99)
	assert.NoError(t, err)
	// Only local files could be deleted (none exist), minio objects not deleted
	assert.Equal(t, 0, deleteCount)
}

func TestDeleteFileData_SubdirectorySkipped(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(1)
	segId := int64(0)

	segmentDir := getSegmentDir(dir, logId, segId)
	err = os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	// Create a subdirectory inside segment dir
	subDir := filepath.Join(segmentDir, "subdir")
	err = os.MkdirAll(subDir, 0o755)
	require.NoError(t, err)

	// Create a data file
	dataFile := filepath.Join(segmentDir, "data.log")
	err = os.WriteFile(dataFile, []byte("data"), 0o644)
	require.NoError(t, err)

	seg := NewStagedSegmentImpl(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg)

	// flag=0: delete .log files, skip directories
	deleteCount, err := seg.DeleteFileData(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, deleteCount)

	// data.log deleted
	_, err = os.Stat(dataFile)
	assert.True(t, os.IsNotExist(err))

	// subdirectory should still exist (skipped by IsDir check)
	_, err = os.Stat(subDir)
	assert.NoError(t, err, "subdirectory should NOT be deleted")
}

func TestDeleteFileData_RemoveError(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("permission-based remove tests only reliable on Linux")
	}
	if os.Getuid() == 0 {
		t.Skip("cannot test permission errors as root")
	}

	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(1)
	segId := int64(0)

	segmentDir := getSegmentDir(dir, logId, segId)
	err = os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	// Create a data file
	dataFile := filepath.Join(segmentDir, "data.log")
	err = os.WriteFile(dataFile, []byte("data"), 0o644)
	require.NoError(t, err)

	// Make directory read-only so os.Remove fails
	err = os.Chmod(segmentDir, 0o555)
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Chmod(segmentDir, 0o755)
	})

	seg := NewStagedSegmentImpl(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg)

	// flag=0: try to delete .log file but os.Remove should fail
	_, err = seg.DeleteFileData(context.Background(), 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to delete")
}

func TestDeleteFileData_ReadDirError(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("permission-based readdir tests only reliable on Linux")
	}
	if os.Getuid() == 0 {
		t.Skip("cannot test permission errors as root")
	}

	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(1)
	segId := int64(0)

	segmentDir := getSegmentDir(dir, logId, segId)
	err = os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	// Make directory completely unreadable
	err = os.Chmod(segmentDir, 0o000)
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Chmod(segmentDir, 0o755)
	})

	seg := NewStagedSegmentImpl(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg)

	// flag=0: os.ReadDir should fail with permission error (not IsNotExist)
	_, err = seg.DeleteFileData(context.Background(), 0)
	assert.Error(t, err)
}
