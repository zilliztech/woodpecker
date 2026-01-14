// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package external

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
)

func TestLoonLocalFileOperations(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "loon_fs_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Initialize local filesystem
	err = InitLocalLoonFileSystem(tempDir)
	require.NoError(t, err, "Failed to initialize local filesystem")

	// Test data
	testFileName := "test_file.txt"
	testData := []byte("Hello, Loon! This is a test file for local filesystem.")
	testMetadata := map[string]string{
		"author":  "test_user",
		"version": "1.0",
		"created": time.Now().Format(time.RFC3339),
	}

	t.Run("WriteFileWithoutMetadata", func(t *testing.T) {
		err := WriteFile(testFileName, testData, nil)
		assert.NoError(t, err, "Failed to write file without metadata")
	})

	t.Run("FileExists", func(t *testing.T) {
		exists, err := FileExists(testFileName)
		assert.NoError(t, err, "Failed to check file existence")
		assert.True(t, exists, "File should exist after writing")

		exists, err = FileExists("non_existent_file.txt")
		assert.NoError(t, err, "Failed to check non-existent file")
		assert.False(t, exists, "Non-existent file should return false")
	})

	t.Run("GetFileInfo", func(t *testing.T) {
		info, err := GetFileInfo(testFileName)
		assert.NoError(t, err, "Failed to get file info")
		assert.True(t, info.Exists, "File should exist")
		assert.False(t, info.IsDir, "File should not be a directory")
		assert.Greater(t, info.MTimeNs, int64(0), "Modification time should be set")
		t.Logf("File info: exists=%v, isDir=%v, mtime=%d", info.Exists, info.IsDir, info.MTimeNs)
	})

	t.Run("GetFileStats", func(t *testing.T) {
		stats, err := GetFileStats(testFileName)
		assert.NoError(t, err, "Failed to get file stats")
		assert.Equal(t, int64(len(testData)), stats.Size, "File size mismatch")
		t.Logf("File stats: size=%d, metadata=%v", stats.Size, stats.Metadata)
	})

	t.Run("GetFileSize", func(t *testing.T) {
		size, err := GetFileSize(testFileName)
		assert.NoError(t, err, "Failed to get file size")
		assert.Equal(t, int64(len(testData)), size, "File size mismatch")
	})

	t.Run("ReadFile", func(t *testing.T) {
		data, err := ReadFile(testFileName)
		assert.NoError(t, err, "Failed to read file")
		assert.Equal(t, testData, data, "Read data does not match written data")
	})

	t.Run("WriteFileWithMetadata", func(t *testing.T) {
		metadataFileName := "file_with_metadata.txt"
		err := WriteFile(metadataFileName, testData, testMetadata)
		// Note: Local filesystem might not support metadata
		if err != nil {
			t.Logf("Warning: WriteFile with metadata returned error (expected for local FS): %v", err)
			// Try without metadata
			err = WriteFile(metadataFileName, testData, nil)
			assert.NoError(t, err, "Should be able to write without metadata")
		}

		// Clean up
		if exists, _ := FileExists(metadataFileName); exists {
			_ = DeleteFile(metadataFileName)
		}
	})

	t.Run("WriteAndReadEmptyFile", func(t *testing.T) {
		emptyFileName := "empty_file.txt"
		err := WriteFile(emptyFileName, []byte{}, nil)
		assert.NoError(t, err, "Failed to write empty file")

		data, err := ReadFile(emptyFileName)
		assert.NoError(t, err, "Failed to read empty file")
		assert.Equal(t, 0, len(data), "Empty file should have zero length")

		// Clean up
		_ = DeleteFile(emptyFileName)
	})

	t.Run("OverwriteFile", func(t *testing.T) {
		newData := []byte("Updated content")
		err := WriteFile(testFileName, newData, nil)
		assert.NoError(t, err, "Failed to overwrite file")

		data, err := ReadFile(testFileName)
		assert.NoError(t, err, "Failed to read overwritten file")
		assert.Equal(t, newData, data, "Overwritten data does not match")

		// Restore original content for other tests
		err = WriteFile(testFileName, testData, nil)
		assert.NoError(t, err, "Failed to restore original content")
	})

	t.Run("DeleteFile", func(t *testing.T) {
		// Create a temporary file for deletion test
		deleteTestFile := "file_to_delete.txt"
		err := WriteFile(deleteTestFile, []byte("delete me"), nil)
		assert.NoError(t, err, "Failed to create file for deletion test")

		err = DeleteFile(deleteTestFile)
		assert.NoError(t, err, "Failed to delete file")

		exists, err := FileExists(deleteTestFile)
		assert.NoError(t, err, "Failed to check file existence after deletion")
		assert.False(t, exists, "File should not exist after deletion")
	})

	t.Run("DeleteNonExistentFile", func(t *testing.T) {
		err := DeleteFile("non_existent_file.txt")
		assert.Error(t, err, "Deleting non-existent file should return error")
	})

	t.Run("MultipleFiles", func(t *testing.T) {
		// Write multiple files
		fileNames := make([]string, 5)
		for i := 0; i < 5; i++ {
			fileName := fmt.Sprintf("multi_file_%d.txt", i)
			fileNames[i] = fileName
			data := []byte(fmt.Sprintf("Content of file %d", i))
			err := WriteFile(fileName, data, nil)
			assert.NoError(t, err, "Failed to write file %d", i)
		}

		// Read and verify all files
		for i := 0; i < 5; i++ {
			data, err := ReadFile(fileNames[i])
			assert.NoError(t, err, "Failed to read file %d", i)
			expectedData := []byte(fmt.Sprintf("Content of file %d", i))
			assert.Equal(t, expectedData, data, "Data mismatch for file %d", i)
		}

		// Delete all files
		for i := 0; i < 5; i++ {
			err := DeleteFile(fileNames[i])
			assert.NoError(t, err, "Failed to delete file %d", i)
		}
	})

	t.Run("LargeFile", func(t *testing.T) {
		largeFileName := "large_file.bin"
		// Create a 1MB file
		largeData := make([]byte, 1024*1024)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		err := WriteFile(largeFileName, largeData, nil)
		assert.NoError(t, err, "Failed to write large file")

		size, err := GetFileSize(largeFileName)
		assert.NoError(t, err, "Failed to get large file size")
		assert.Equal(t, int64(len(largeData)), size, "Large file size mismatch")

		data, err := ReadFile(largeFileName)
		assert.NoError(t, err, "Failed to read large file")
		assert.Equal(t, largeData, data, "Large file data mismatch")

		// Clean up
		err = DeleteFile(largeFileName)
		assert.NoError(t, err, "Failed to delete large file")
	})

	// Clean up test file
	_ = DeleteFile(testFileName)
}

func TestLoonDirectoryOperations(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "loon_dir_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Initialize local filesystem
	err = InitLocalLoonFileSystem(tempDir)
	require.NoError(t, err, "Failed to initialize local filesystem")

	t.Run("CreateDirectory", func(t *testing.T) {
		dirName := "test_dir"
		err := CreateDir(dirName, false)
		assert.NoError(t, err, "Failed to create directory")

		exists, err := DirExists(dirName)
		assert.NoError(t, err, "Failed to check directory existence")
		assert.True(t, exists, "Directory should exist after creation")
	})

	t.Run("CreateNestedDirectory", func(t *testing.T) {
		nestedDir := "parent/child/grandchild"
		err := CreateDir(nestedDir, true)
		assert.NoError(t, err, "Failed to create nested directory")

		exists, err := DirExists(nestedDir)
		assert.NoError(t, err, "Failed to check nested directory existence")
		assert.True(t, exists, "Nested directory should exist")
	})

	t.Run("ListEmptyDirectory", func(t *testing.T) {
		emptyDir := "empty_dir"
		err := CreateDir(emptyDir, false)
		require.NoError(t, err, "Failed to create empty directory")

		entries, err := ListDir(emptyDir, false)
		assert.NoError(t, err, "Failed to list empty directory")
		assert.Equal(t, 0, len(entries), "Empty directory should have no entries")
	})

	t.Run("ListDirectoryWithFiles", func(t *testing.T) {
		dirName := "list_test_dir"
		err := CreateDir(dirName, false)
		require.NoError(t, err, "Failed to create directory for listing test")

		// Create some files in the directory
		fileNames := []string{"file1.txt", "file2.txt", "file3.txt"}
		for _, fileName := range fileNames {
			filePath := filepath.Join(dirName, fileName)
			err := WriteFile(filePath, []byte("test content"), nil)
			assert.NoError(t, err, "Failed to create file %s", fileName)
		}

		// List directory
		entries, err := ListDir(dirName, false)
		assert.NoError(t, err, "Failed to list directory")
		assert.GreaterOrEqual(t, len(entries), len(fileNames), "Should have at least the created files")

		// Verify entries
		for _, entry := range entries {
			t.Logf("Entry: path=%s, isDir=%v, size=%d, mtime=%d", entry.Path, entry.IsDir, entry.Size, entry.MTimeNs)
		}

		// Clean up
		for _, fileName := range fileNames {
			filePath := filepath.Join(dirName, fileName)
			_ = DeleteFile(filePath)
		}
	})

	t.Run("ListDirectoryRecursive", func(t *testing.T) {
		baseDir := "recursive_test"
		err := CreateDir(baseDir, false)
		require.NoError(t, err, "Failed to create base directory")

		// Create nested structure
		subDir := filepath.Join(baseDir, "subdir")
		err = CreateDir(subDir, false)
		require.NoError(t, err, "Failed to create subdirectory")

		// Create files at different levels
		file1 := filepath.Join(baseDir, "file1.txt")
		file2 := filepath.Join(subDir, "file2.txt")
		err = WriteFile(file1, []byte("content1"), nil)
		assert.NoError(t, err, "Failed to create file1")
		err = WriteFile(file2, []byte("content2"), nil)
		assert.NoError(t, err, "Failed to create file2")

		// List recursively
		entries, err := ListDir(baseDir, true)
		assert.NoError(t, err, "Failed to list directory recursively")
		assert.GreaterOrEqual(t, len(entries), 2, "Should have at least 2 entries (subdir and files)")

		// Log all entries
		for _, entry := range entries {
			t.Logf("Recursive entry: path=%s, isDir=%v, size=%d", entry.Path, entry.IsDir, entry.Size)
		}

		// Clean up
		_ = DeleteFile(file1)
		_ = DeleteFile(file2)
	})
}

func TestLoonRemoteFileOperations(t *testing.T) {
	// Load configuration
	configPath := "../../config/woodpecker.yaml"
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Skip("Config file not found, skipping remote filesystem test")
		return
	}

	cfg, err := config.NewConfiguration(configPath)
	require.NoError(t, err)

	// Configure for MinIO
	cfg.Woodpecker.Storage.Type = "minio"
	cfg.Minio.BucketName = "a-bucket"

	// Initialize remote filesystem
	err = InitStorageV2FileSystem(cfg)
	assert.NoError(t, err, "Failed to initialize remote filesystem")

	// Test data
	testFileName := fmt.Sprintf("%s/loon_remote_test_%d.txt", cfg.Minio.RootPath, time.Now().Unix())
	testData := []byte("Hello, Loon! This is a test file for remote MinIO storage.")
	testMetadata := map[string]string{
		"storage": "minio",
		"test":    "loon_remote_fs",
		"time":    time.Now().Format(time.RFC3339),
	}

	t.Run("WriteRemoteFile", func(t *testing.T) {
		err := WriteFile(testFileName, testData, testMetadata)
		assert.NoError(t, err, "Failed to write file to MinIO")
	})

	t.Run("RemoteFileExists", func(t *testing.T) {
		exists, err := FileExists(testFileName)
		assert.NoError(t, err, "Failed to check remote file existence")
		assert.True(t, exists, "File should exist in MinIO after writing")
	})

	t.Run("GetRemoteFileInfo", func(t *testing.T) {
		info, err := GetFileInfo(testFileName)
		assert.NoError(t, err, "Failed to get remote file info")
		assert.True(t, info.Exists, "File should exist")
		assert.False(t, info.IsDir, "File should not be a directory")
		t.Logf("Remote file info: exists=%v, isDir=%v, mtime=%d", info.Exists, info.IsDir, info.MTimeNs)
	})

	t.Run("GetRemoteFileStats", func(t *testing.T) {
		stats, err := GetFileStats(testFileName)
		assert.NoError(t, err, "Failed to get file stats from MinIO")
		assert.Equal(t, int64(len(testData)), stats.Size, "File size mismatch in MinIO")

		// Check that returned metadata contains the custom metadata we wrote
		if len(stats.Metadata) > 0 {
			t.Logf("Remote file stats: size=%d, metadata=%v", stats.Size, stats.Metadata)
			for key, expectedValue := range testMetadata {
				if actualValue, exists := stats.Metadata[key]; exists {
					assert.Equal(t, expectedValue, actualValue, "Metadata value for key '%s' should match", key)
				} else {
					t.Logf("Warning: Metadata key '%s' not found in returned metadata", key)
				}
			}
		} else {
			t.Logf("Remote file stats: size=%d, no metadata returned", stats.Size)
		}
	})

	t.Run("ReadRemoteFile", func(t *testing.T) {
		data, err := ReadFile(testFileName)
		assert.NoError(t, err, "Failed to read file from MinIO")
		assert.Equal(t, testData, data, "Read data from MinIO does not match")
	})

	t.Run("UpdateRemoteFile", func(t *testing.T) {
		newData := []byte("Updated MinIO content via Loon")
		err := WriteFile(testFileName, newData, nil)
		assert.NoError(t, err, "Failed to update file in MinIO")

		data, err := ReadFile(testFileName)
		assert.NoError(t, err, "Failed to read updated file from MinIO")
		assert.Equal(t, newData, data, "Updated data in MinIO does not match")
	})

	t.Run("DeleteRemoteFile", func(t *testing.T) {
		err := DeleteFile(testFileName)
		assert.NoError(t, err, "Failed to delete file from MinIO")

		exists, err := FileExists(testFileName)
		assert.NoError(t, err, "Failed to check file existence after deletion")
		assert.False(t, exists, "File should not exist in MinIO after deletion")
	})

	t.Run("LargeRemoteFile", func(t *testing.T) {
		largeFileName := fmt.Sprintf("%s/loon_large_file_%d.bin", cfg.Minio.RootPath, time.Now().Unix())

		// Create a 1MB file
		largeData := make([]byte, 1024*1024)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		err := WriteFile(largeFileName, largeData, nil)
		assert.NoError(t, err, "Failed to write large file to MinIO")

		size, err := GetFileSize(largeFileName)
		assert.NoError(t, err, "Failed to get large file size from MinIO")
		assert.Equal(t, int64(len(largeData)), size, "Large file size mismatch in MinIO")

		data, err := ReadFile(largeFileName)
		assert.NoError(t, err, "Failed to read large file from MinIO")
		assert.Equal(t, largeData, data, "Large file data mismatch in MinIO")

		// Clean up
		err = DeleteFile(largeFileName)
		assert.NoError(t, err, "Failed to delete large file from MinIO")
	})
}
