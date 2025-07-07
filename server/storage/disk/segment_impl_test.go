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

package disk

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zilliztech/woodpecker/common/config"
)

func getTempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "disk_log_test_*")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}

// TestNewDiskSegmentImpl tests the NewDiskSegmentImpl function.
func TestNewDiskSegmentImpl(t *testing.T) {
	tmpDir := getTempDir(t)
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	baseDir := filepath.Join(tmpDir, "1/0")
	segmentImpl := NewDiskSegmentImpl(context.TODO(), 1, 0, baseDir, cfg).(*DiskSegmentImpl)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), segmentImpl.logId)
	assert.Equal(t, int64(0), segmentImpl.segmentId)
	assert.Equal(t, baseDir, segmentImpl.segmentFileParentDir)
	// The actual implementation uses fmt.Sprintf("%d.log", segId), not the getSegmentFilePath function
	expectedPath := filepath.Join(baseDir, "0.log")
	assert.Equal(t, expectedPath, segmentImpl.segmentFilePath)
}

// TestDeleteFileData tests the DeleteFileData function focusing on its ability
// to handle directory and logging operations, rather than actual file operations.
func TestDeleteFileData(t *testing.T) {
	t.Run("EmptyDirectory", func(t *testing.T) {
		// Set up test directory
		testDir := getTempDir(t)
		logId := int64(1)
		cfg, err := config.NewConfiguration()
		assert.NoError(t, err)

		// Create a DiskSegmentImpl object with a mock directory
		logDir := testDir
		err = os.MkdirAll(logDir, 0755)
		assert.NoError(t, err)

		// Create read-only log file
		roSegmentImpl := NewDiskSegmentImpl(context.TODO(), logId, 0, logDir, cfg).(*DiskSegmentImpl)
		assert.NotNil(t, roSegmentImpl)

		// Execute deletion operation
		deleteCount, err := roSegmentImpl.DeleteFileData(context.Background(), 0)
		assert.NoError(t, err, "DeleteFileData should not error with empty directory")
		assert.Equal(t, 0, deleteCount)
	})

	t.Run("NonExistentDirectory", func(t *testing.T) {
		// Test case where directory does not exist
		nonExistDir := getTempDir(t)
		os.RemoveAll(nonExistDir) // Ensure directory does not exist
		cfg, _ := config.NewConfiguration()
		segmentImpl2 := NewDiskSegmentImpl(context.TODO(), 1, 0, nonExistDir, cfg).(*DiskSegmentImpl)

		deleteCount, err := segmentImpl2.DeleteFileData(context.Background(), 0)
		assert.NoError(t, err, "DeleteFileData should not error when directory doesn't exist")
		assert.Equal(t, 0, deleteCount)
	})

	t.Run("WithFiles", func(t *testing.T) {
		dir := getTempDir(t)
		logId := int64(1)
		segmentId := int64(0)
		cfg, err := config.NewConfiguration()
		require.NoError(t, err)

		// Create the segment directory
		err = os.MkdirAll(dir, 0755)
		require.NoError(t, err)

		// Create test files that should be deleted
		segmentFile1 := filepath.Join(dir, "0.log")  // This matches the segment file pattern
		segmentFile2 := filepath.Join(dir, "1.log")  // Another segment file
		lockFile := filepath.Join(dir, "1_0.lock")   // Lock file
		otherFile := filepath.Join(dir, "other.txt") // This should NOT be deleted

		// Create segment files (.log files)
		err = os.WriteFile(segmentFile1, []byte("segment data 1"), 0644)
		require.NoError(t, err)
		err = os.WriteFile(segmentFile2, []byte("segment data 2"), 0644)
		require.NoError(t, err)

		// Create lock file
		err = os.WriteFile(lockFile, []byte("lock info"), 0644)
		require.NoError(t, err)

		// Create other file that should not be deleted
		err = os.WriteFile(otherFile, []byte("other data"), 0644)
		require.NoError(t, err)

		// Verify files exist before deletion
		_, err = os.Stat(segmentFile1)
		assert.NoError(t, err, "segment file 1 should exist before deletion")
		_, err = os.Stat(segmentFile2)
		assert.NoError(t, err, "segment file 2 should exist before deletion")
		_, err = os.Stat(lockFile)
		assert.NoError(t, err, "lock file should exist before deletion")
		_, err = os.Stat(otherFile)
		assert.NoError(t, err, "other file should exist before deletion")

		// Create DiskSegmentImpl and test deletion
		segmentImpl := NewDiskSegmentImpl(context.TODO(), logId, segmentId, dir, cfg).(*DiskSegmentImpl)
		require.NotNil(t, segmentImpl)

		// Execute deletion operation
		deleteCount, err := segmentImpl.DeleteFileData(context.Background(), 0)
		assert.NoError(t, err, "DeleteFileData should not error")
		assert.Equal(t, 2, deleteCount, "Should delete 2 .log files")

		// Verify .log files are deleted
		_, err = os.Stat(segmentFile1)
		assert.True(t, os.IsNotExist(err), "segment file 1 should be deleted")
		_, err = os.Stat(segmentFile2)
		assert.True(t, os.IsNotExist(err), "segment file 2 should be deleted")

		// Verify lock file and other files are NOT deleted (DeleteFileData only deletes .log files)
		_, err = os.Stat(lockFile)
		assert.NoError(t, err, "lock file should NOT be deleted by DeleteFileData")
		_, err = os.Stat(otherFile)
		assert.NoError(t, err, "other file should NOT be deleted by DeleteFileData")
	})
}
