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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/server/storage/serde"
)

func getTempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "staged_segment_test_*")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}

// TestNewStagedSegmentImpl tests the NewStagedSegmentImpl function.
func TestNewStagedSegmentImpl(t *testing.T) {
	tmpDir := getTempDir(t)
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)

	segmentImpl := NewStagedSegmentImpl(
		context.TODO(),
		"test-bucket",
		"test-root",
		tmpDir,
		1,
		0,
		nil, // no object storage client for this test
		cfg,
	).(*StagedSegmentImpl)

	assert.Equal(t, int64(1), segmentImpl.logId)
	assert.Equal(t, int64(0), segmentImpl.segmentId)
	assert.Equal(t, getSegmentDir(tmpDir, 1, 0), segmentImpl.segmentDir)
	assert.Equal(t, "test-bucket", segmentImpl.bucket)
	assert.Equal(t, "test-root", segmentImpl.rootPath)
}

// TestDeleteLocalFiles tests the deleteLocalFiles function focusing on local file operations.
// Note: deleteMinioObjects is not tested here as it requires a mock object storage client.
func TestDeleteLocalFiles(t *testing.T) {
	t.Run("EmptyDirectory", func(t *testing.T) {
		testDir := getTempDir(t)
		logId := int64(1)
		segmentId := int64(0)
		cfg, err := config.NewConfiguration()
		require.NoError(t, err)

		segmentDir := serde.GetSegmentDir(testDir, logId, segmentId)
		err = os.MkdirAll(segmentDir, 0755)
		require.NoError(t, err)

		segmentImpl := NewStagedSegmentImpl(
			context.TODO(), "bucket", "root", testDir, logId, segmentId, nil, cfg,
		).(*StagedSegmentImpl)

		deleteCount, err := segmentImpl.deleteLocalFiles(context.Background(), 0)
		assert.NoError(t, err, "deleteLocalFiles should not error with empty directory")
		assert.Equal(t, 0, deleteCount)
	})

	t.Run("NonExistentDirectory", func(t *testing.T) {
		nonExistDir := getTempDir(t)
		os.RemoveAll(nonExistDir)
		cfg, _ := config.NewConfiguration()

		segmentImpl := NewStagedSegmentImpl(
			context.TODO(), "bucket", "root", nonExistDir, 1, 0, nil, cfg,
		).(*StagedSegmentImpl)

		deleteCount, err := segmentImpl.deleteLocalFiles(context.Background(), 0)
		assert.NoError(t, err, "deleteLocalFiles should not error when directory doesn't exist")
		assert.Equal(t, 0, deleteCount)
	})

	t.Run("WithBlockFiles", func(t *testing.T) {
		dir := getTempDir(t)
		logId := int64(2)
		segmentId := int64(0)
		cfg, err := config.NewConfiguration()
		require.NoError(t, err)

		segmentDir := serde.GetSegmentDir(dir, logId, segmentId)
		err = os.MkdirAll(segmentDir, 0755)
		require.NoError(t, err)

		// Create block files
		blockFile0 := filepath.Join(segmentDir, "0.blk")
		blockFile1 := filepath.Join(segmentDir, "1.blk")
		blockFile2 := filepath.Join(segmentDir, "2.blk")
		footerFile := filepath.Join(segmentDir, "footer.blk")
		otherFile := filepath.Join(segmentDir, "readme.txt")

		for _, f := range []string{blockFile0, blockFile1, blockFile2, footerFile, otherFile} {
			err = os.WriteFile(f, []byte("data"), 0644)
			require.NoError(t, err)
		}

		segmentImpl := NewStagedSegmentImpl(
			context.TODO(), "bucket", "root", dir, logId, segmentId, nil, cfg,
		).(*StagedSegmentImpl)

		deleteCount, err := segmentImpl.deleteLocalFiles(context.Background(), 0)
		assert.NoError(t, err)
		assert.Equal(t, 4, deleteCount, "Should delete 4 .blk files")

		// Verify .blk files are deleted
		for _, f := range []string{blockFile0, blockFile1, blockFile2, footerFile} {
			_, err = os.Stat(f)
			assert.True(t, os.IsNotExist(err), "Block file should be deleted: %s", f)
		}

		// Verify other file is NOT deleted
		_, err = os.Stat(otherFile)
		assert.NoError(t, err, "Other file should NOT be deleted")
	})

	t.Run("WithInflightAndCompletedFiles", func(t *testing.T) {
		dir := getTempDir(t)
		logId := int64(3)
		segmentId := int64(0)
		cfg, err := config.NewConfiguration()
		require.NoError(t, err)

		segmentDir := serde.GetSegmentDir(dir, logId, segmentId)
		err = os.MkdirAll(segmentDir, 0755)
		require.NoError(t, err)

		// Create various file types
		files := map[string]bool{
			"0.blk":               true,  // should delete
			"1.blk.inflight":      true,  // should delete
			"2.blk.completed":     true,  // should delete
			"footer.blk":          true,  // should delete
			"footer.blk.inflight": true,  // should delete
			"write.lock":          true,  // should delete
			"other.txt":           false, // should NOT delete
		}

		expectedDeleteCount := 0
		for fileName, shouldDelete := range files {
			filePath := filepath.Join(segmentDir, fileName)
			err = os.WriteFile(filePath, []byte("data"), 0644)
			require.NoError(t, err)
			if shouldDelete {
				expectedDeleteCount++
			}
		}

		segmentImpl := NewStagedSegmentImpl(
			context.TODO(), "bucket", "root", dir, logId, segmentId, nil, cfg,
		).(*StagedSegmentImpl)

		deleteCount, err := segmentImpl.deleteLocalFiles(context.Background(), 0)
		assert.NoError(t, err)
		assert.Equal(t, expectedDeleteCount, deleteCount)

		// Verify deleted files
		for fileName, shouldDelete := range files {
			filePath := filepath.Join(segmentDir, fileName)
			_, err = os.Stat(filePath)
			if shouldDelete {
				assert.True(t, os.IsNotExist(err), "File should be deleted: %s", fileName)
			} else {
				assert.NoError(t, err, "File should NOT be deleted: %s", fileName)
			}
		}
	})

	t.Run("WithMergedBlockFiles", func(t *testing.T) {
		dir := getTempDir(t)
		logId := int64(4)
		segmentId := int64(0)
		cfg, err := config.NewConfiguration()
		require.NoError(t, err)

		segmentDir := serde.GetSegmentDir(dir, logId, segmentId)
		err = os.MkdirAll(segmentDir, 0755)
		require.NoError(t, err)

		// Create merged block files
		files := []string{
			"m_0.blk",
			"m_1.blk",
			"m_0.blk.inflight",
			"m_1.blk.completed",
		}

		for _, f := range files {
			err = os.WriteFile(filepath.Join(segmentDir, f), []byte("data"), 0644)
			require.NoError(t, err)
		}

		segmentImpl := NewStagedSegmentImpl(
			context.TODO(), "bucket", "root", dir, logId, segmentId, nil, cfg,
		).(*StagedSegmentImpl)

		deleteCount, err := segmentImpl.deleteLocalFiles(context.Background(), 0)
		assert.NoError(t, err)
		assert.Equal(t, 4, deleteCount, "Should delete all merged block files")

		// Verify all files are deleted
		for _, f := range files {
			_, err = os.Stat(filepath.Join(segmentDir, f))
			assert.True(t, os.IsNotExist(err), "Merged file should be deleted: %s", f)
		}
	})

	t.Run("WithFenceDirectories", func(t *testing.T) {
		dir := getTempDir(t)
		logId := int64(5)
		segmentId := int64(0)
		cfg, err := config.NewConfiguration()
		require.NoError(t, err)

		segmentDir := serde.GetSegmentDir(dir, logId, segmentId)
		err = os.MkdirAll(segmentDir, 0755)
		require.NoError(t, err)

		// Create block files
		blockFile0 := filepath.Join(segmentDir, "0.blk")
		blockFile1 := filepath.Join(segmentDir, "1.blk")
		err = os.WriteFile(blockFile0, []byte("data"), 0644)
		require.NoError(t, err)
		err = os.WriteFile(blockFile1, []byte("data"), 0644)
		require.NoError(t, err)

		// Create fence directories (directories named like block files)
		fenceDir2 := filepath.Join(segmentDir, "2.blk")
		fenceDir3 := filepath.Join(segmentDir, "3.blk")
		err = os.MkdirAll(fenceDir2, 0755)
		require.NoError(t, err)
		err = os.MkdirAll(fenceDir3, 0755)
		require.NoError(t, err)

		// Create a non-fence directory that should NOT be deleted
		otherDir := filepath.Join(segmentDir, "subdir")
		err = os.MkdirAll(otherDir, 0755)
		require.NoError(t, err)

		segmentImpl := NewStagedSegmentImpl(
			context.TODO(), "bucket", "root", dir, logId, segmentId, nil, cfg,
		).(*StagedSegmentImpl)

		deleteCount, err := segmentImpl.deleteLocalFiles(context.Background(), 0)
		assert.NoError(t, err)
		assert.Equal(t, 4, deleteCount, "Should delete 2 block files and 2 fence directories")

		// Verify block files are deleted
		_, err = os.Stat(blockFile0)
		assert.True(t, os.IsNotExist(err))
		_, err = os.Stat(blockFile1)
		assert.True(t, os.IsNotExist(err))

		// Verify fence directories are deleted
		_, err = os.Stat(fenceDir2)
		assert.True(t, os.IsNotExist(err))
		_, err = os.Stat(fenceDir3)
		assert.True(t, os.IsNotExist(err))

		// Verify other directory is NOT deleted
		_, err = os.Stat(otherDir)
		assert.NoError(t, err, "Non-fence directory should NOT be deleted")
	})

	t.Run("DeleteOnlyOriginalBlocks_Flag1", func(t *testing.T) {
		dir := getTempDir(t)
		logId := int64(6)
		segmentId := int64(0)
		cfg, err := config.NewConfiguration()
		require.NoError(t, err)

		segmentDir := serde.GetSegmentDir(dir, logId, segmentId)
		err = os.MkdirAll(segmentDir, 0755)
		require.NoError(t, err)

		// Create original block files
		originalFiles := []string{
			"0.blk",
			"1.blk",
			"2.blk.inflight",
		}
		// Create merged block files
		mergedFiles := []string{
			"m_0.blk",
			"m_1.blk",
		}
		// Create footer (should NOT be deleted with flag=1)
		footerFile := "footer.blk"

		for _, f := range originalFiles {
			err = os.WriteFile(filepath.Join(segmentDir, f), []byte("data"), 0644)
			require.NoError(t, err)
		}
		for _, f := range mergedFiles {
			err = os.WriteFile(filepath.Join(segmentDir, f), []byte("data"), 0644)
			require.NoError(t, err)
		}
		err = os.WriteFile(filepath.Join(segmentDir, footerFile), []byte("data"), 0644)
		require.NoError(t, err)

		// Create a fence directory
		fenceDir := filepath.Join(segmentDir, "3.blk")
		err = os.MkdirAll(fenceDir, 0755)
		require.NoError(t, err)

		segmentImpl := NewStagedSegmentImpl(
			context.TODO(), "bucket", "root", dir, logId, segmentId, nil, cfg,
		).(*StagedSegmentImpl)

		deleteCount, err := segmentImpl.deleteLocalFiles(context.Background(), 1)
		assert.NoError(t, err)
		// Should delete: 0.blk, 1.blk, 2.blk.inflight, fence dir (3.blk)
		assert.Equal(t, 4, deleteCount, "Should delete only original blocks and fence dirs")

		// Verify original files are deleted
		for _, f := range originalFiles {
			_, err = os.Stat(filepath.Join(segmentDir, f))
			assert.True(t, os.IsNotExist(err), "Original file should be deleted: %s", f)
		}

		// Verify fence directory is deleted
		_, err = os.Stat(fenceDir)
		assert.True(t, os.IsNotExist(err), "Fence directory should be deleted")

		// Verify merged files are NOT deleted
		for _, f := range mergedFiles {
			_, err = os.Stat(filepath.Join(segmentDir, f))
			assert.NoError(t, err, "Merged file should NOT be deleted: %s", f)
		}

		// Verify footer is NOT deleted
		_, err = os.Stat(filepath.Join(segmentDir, footerFile))
		assert.NoError(t, err, "Footer should NOT be deleted with flag=1")
	})

	t.Run("DeleteOnlyMergedBlocks_Flag2", func(t *testing.T) {
		dir := getTempDir(t)
		logId := int64(7)
		segmentId := int64(0)
		cfg, err := config.NewConfiguration()
		require.NoError(t, err)

		segmentDir := serde.GetSegmentDir(dir, logId, segmentId)
		err = os.MkdirAll(segmentDir, 0755)
		require.NoError(t, err)

		// Create original block files
		originalFiles := []string{
			"0.blk",
			"1.blk",
			"footer.blk",
		}
		// Create merged block files
		mergedFiles := []string{
			"m_0.blk",
			"m_1.blk",
			"m_2.blk.inflight",
			"m_3.blk.completed",
		}

		for _, f := range originalFiles {
			err = os.WriteFile(filepath.Join(segmentDir, f), []byte("data"), 0644)
			require.NoError(t, err)
		}
		for _, f := range mergedFiles {
			err = os.WriteFile(filepath.Join(segmentDir, f), []byte("data"), 0644)
			require.NoError(t, err)
		}

		segmentImpl := NewStagedSegmentImpl(
			context.TODO(), "bucket", "root", dir, logId, segmentId, nil, cfg,
		).(*StagedSegmentImpl)

		deleteCount, err := segmentImpl.deleteLocalFiles(context.Background(), 2)
		assert.NoError(t, err)
		assert.Equal(t, 4, deleteCount, "Should delete only merged blocks")

		// Verify merged files are deleted
		for _, f := range mergedFiles {
			_, err = os.Stat(filepath.Join(segmentDir, f))
			assert.True(t, os.IsNotExist(err), "Merged file should be deleted: %s", f)
		}

		// Verify original files are NOT deleted
		for _, f := range originalFiles {
			_, err = os.Stat(filepath.Join(segmentDir, f))
			assert.NoError(t, err, "Original file should NOT be deleted: %s", f)
		}
	})

	t.Run("MixedBlockTypes", func(t *testing.T) {
		dir := getTempDir(t)
		logId := int64(8)
		segmentId := int64(0)
		cfg, err := config.NewConfiguration()
		require.NoError(t, err)

		segmentDir := serde.GetSegmentDir(dir, logId, segmentId)
		err = os.MkdirAll(segmentDir, 0755)
		require.NoError(t, err)

		// Create mixed block files (original + merged + lock)
		files := []string{
			"0.blk",      // original block
			"1.blk",      // original block
			"footer.blk", // footer
			"m_0.blk",    // merged
			"write.lock", // lock
		}

		for _, f := range files {
			err = os.WriteFile(filepath.Join(segmentDir, f), []byte("data"), 0644)
			require.NoError(t, err)
		}

		segmentImpl := NewStagedSegmentImpl(
			context.TODO(), "bucket", "root", dir, logId, segmentId, nil, cfg,
		).(*StagedSegmentImpl)

		deleteCount, err := segmentImpl.deleteLocalFiles(context.Background(), 0)
		assert.NoError(t, err)
		assert.Equal(t, 5, deleteCount, "Should delete all segment files")

		// Verify all files are deleted
		for _, f := range files {
			_, err = os.Stat(filepath.Join(segmentDir, f))
			assert.True(t, os.IsNotExist(err), "File should be deleted: %s", f)
		}
	})
}
