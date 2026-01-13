package external

//
//import (
//	"fmt"
//	"os"
//	"path/filepath"
//	"testing"
//	"time"
//
//	"github.com/stretchr/testify/assert"
//	"github.com/stretchr/testify/require"
//	"github.com/zilliztech/woodpecker/common/config"
//)
//
//func TestStorageV2LocalFS(t *testing.T) {
//	// Create a temporary directory for testing
//	tempDir, err := os.MkdirTemp("", "woodpecker_test_*")
//	require.NoError(t, err)
//	defer os.RemoveAll(tempDir)
//
//	// Load configuration
//	tempFile, err := os.OpenFile("../../config/woodpecker.yaml", os.O_RDWR|os.O_CREATE, 0o666)
//	require.NoError(t, err)
//	cfg, err := config.NewConfiguration(tempFile.Name())
//	require.NoError(t, err)
//
//	// Init local storage
//	cfg.Woodpecker.Storage.Type = "local"
//	cfg.Woodpecker.Storage.RootPath = tempDir
//	err = InitStorageV2FileSystem(cfg)
//	require.NoError(t, err)
//	defer CleanFileSystem()
//
//	// Test data
//	testFileName := "test_file.txt"
//	testData := []byte("Hello, Woodpecker! This is a test file for local filesystem.")
//	testMetadata := map[string]string{
//		"author":  "test_user",
//		"version": "1.0",
//		"created": time.Now().Format(time.RFC3339),
//	}
//
//	t.Run("WriteFile", func(t *testing.T) {
//		writeErr := WriteFile(testFileName, testData, testMetadata)
//		assert.Error(t, writeErr, "Should failed to write file")
//		assert.ErrorContainsf(t, writeErr, "not support", "must contains 'not support' keyword")
//		writeErr = WriteFile(testFileName, testData, nil)
//		assert.NoError(t, writeErr, "Should not failed to write file")
//	})
//
//	t.Run("FileExists", func(t *testing.T) {
//		exists, err := FileExists(testFileName)
//		assert.NoError(t, err)
//		assert.True(t, exists, "File should exist after writing")
//
//		exists, err = FileExists("non_existent_file.txt")
//		assert.NoError(t, err)
//		assert.False(t, exists, "Non-existent file should return false")
//	})
//
//	t.Run("GetFileStats", func(t *testing.T) {
//		info, err := GetFileStats(testFileName)
//		assert.NoError(t, err, "Failed to get file stats")
//		assert.Equal(t, int64(len(testData)), info.Size, "File size mismatch")
//		// Note: Metadata might not be supported yet, so we just log it
//		t.Logf("File info: size=%d, metadata=%v", info.Size, info.Metadata)
//	})
//
//	t.Run("GetFileSize", func(t *testing.T) {
//		size, err := GetFileSize(testFileName)
//		assert.NoError(t, err, "Failed to get file size")
//		assert.Equal(t, int64(len(testData)), size, "File size mismatch")
//	})
//
//	t.Run("ReadFile", func(t *testing.T) {
//		data, err := ReadFile(testFileName)
//		assert.NoError(t, err, "Failed to read file")
//		assert.Equal(t, testData, data, "Read data does not match written data")
//	})
//
//	t.Run("WriteAndReadEmptyFile", func(t *testing.T) {
//		emptyFileName := "empty_file.txt"
//		err := WriteFile(emptyFileName, []byte{}, nil)
//		assert.NoError(t, err, "Failed to write empty file")
//
//		data, err := ReadFile(emptyFileName)
//		assert.NoError(t, err, "Failed to read empty file")
//		assert.Equal(t, 0, len(data), "Empty file should have zero length")
//
//		// Clean up
//		_ = DeleteFile(emptyFileName)
//	})
//
//	t.Run("OverwriteFile", func(t *testing.T) {
//		newData := []byte("Updated content")
//		err := WriteFile(testFileName, newData, nil)
//		assert.NoError(t, err, "Failed to overwrite file")
//
//		data, err := ReadFile(testFileName)
//		assert.NoError(t, err, "Failed to read overwritten file")
//		assert.Equal(t, newData, data, "Overwritten data does not match")
//	})
//
//	t.Run("DeleteFile", func(t *testing.T) {
//		err := DeleteFile(testFileName)
//		assert.NoError(t, err, "Failed to delete file")
//
//		exists, err := FileExists(testFileName)
//		assert.NoError(t, err)
//		assert.False(t, exists, "File should not exist after deletion")
//
//		// Verify physical file is deleted
//		_, err = os.Stat(filepath.Join(tempDir, testFileName))
//		assert.True(t, os.IsNotExist(err), "Physical file should be deleted")
//	})
//
//	t.Run("DeleteNonExistentFile", func(t *testing.T) {
//		err := DeleteFile("non_existent_file.txt")
//		assert.Error(t, err, "Deleting non-existent file should return error")
//	})
//
//	t.Run("MultipleFiles", func(t *testing.T) {
//		// Write multiple files
//		for i := 0; i < 5; i++ {
//			fileName := fmt.Sprintf("file_%d.txt", i)
//			data := []byte(fmt.Sprintf("Content of file %d", i))
//			err := WriteFile(fileName, data, nil)
//			assert.NoError(t, err, "Failed to write file %d", i)
//		}
//
//		// Read and verify all files
//		for i := 0; i < 5; i++ {
//			fileName := fmt.Sprintf("file_%d.txt", i)
//			data, err := ReadFile(fileName)
//			assert.NoError(t, err, "Failed to read file %d", i)
//			expectedData := []byte(fmt.Sprintf("Content of file %d", i))
//			assert.Equal(t, expectedData, data, "Data mismatch for file %d", i)
//		}
//
//		// Delete all files
//		for i := 0; i < 5; i++ {
//			fileName := fmt.Sprintf("file_%d.txt", i)
//			err := DeleteFile(fileName)
//			assert.NoError(t, err, "Failed to delete file %d", i)
//		}
//	})
//}
//
//// Note: Must Create bucket first
//func TestStorageV2RemoteFS(t *testing.T) {
//	// Load configuration
//	tempFile, err := os.OpenFile("../../config/woodpecker.yaml", os.O_RDWR|os.O_CREATE, 0o666)
//	require.NoError(t, err)
//	cfg, err := config.NewConfiguration(tempFile.Name())
//	require.NoError(t, err)
//
//	// Configure for MinIO
//	cfg.Woodpecker.Storage.Type = "minio"
//	cfg.Minio.BucketName = "a-bucket"
//
//	// Init remote storage
//	err = InitStorageV2FileSystem(cfg)
//	if err != nil {
//		t.Skipf("Failed to initialize remote filesystem (MinIO might not be running): %v", err)
//		return
//	}
//	defer CleanFileSystem()
//
//	// Test base directory (bucket) management first
//	// Note: In S3 filesystem abstraction, bucketName is treated as the base directory
//	t.Run("BaseDirectoryManagement", func(t *testing.T) {
//		bucketName := cfg.Minio.BucketName // This is the base directory (bucket in S3)
//		rootPath := cfg.Minio.RootPath
//		baseDir := fmt.Sprintf("%s/%s", bucketName, rootPath)
//
//		// Check if base directory exists using DirExists
//		exists, err := DirExists(baseDir)
//		if err != nil {
//			t.Logf("Warning: DirExists failed: %v", err)
//			t.Skip("Skipping base directory management tests")
//			return
//		}
//
//		t.Logf("Base directory (baseDir='%s') exists: %v", baseDir, exists)
//
//		// Create base directory if it doesn't exist
//		// For S3, this creates the bucket; for local FS, this creates the directory
//		if !exists {
//			t.Logf("Creating base directory (baseDir='%s')...", baseDir)
//			err := CreateDir(baseDir, true)
//			if err != nil {
//				t.Logf("Warning: CreateDir failed: %v", err)
//			} else {
//				t.Logf("Base directory (baseDir='%s') created successfully", baseDir)
//
//				// Verify base directory was created
//				exists, err = DirExists(baseDir)
//				assert.NoError(t, err, "Failed to check base directory existence after creation")
//				assert.True(t, exists, "Base directory should exist after creation")
//			}
//		} else {
//			t.Logf("Base directory (baseDir='%s') already exists, skipping creation", baseDir)
//		}
//	})
//
//	// Test data
//	testFileName := fmt.Sprintf("%s/%s/remote_test_%d.txt", cfg.Minio.BucketName, cfg.Minio.RootPath, time.Now().Unix())
//	testData := []byte("Hello, Woodpecker! This is a test file for remote MinIO storage.")
//	testMetadata := map[string]string{
//		"storage": "minio",
//		"test":    "remote_fs",
//	}
//
//	t.Run("WriteRemoteFile", func(t *testing.T) {
//		err := WriteFile(testFileName, testData, testMetadata)
//		assert.NoErrorf(t, err, "Failed to write file:%s to MinIO", testFileName)
//	})
//
//	t.Run("RemoteFileExists", func(t *testing.T) {
//		exists, err := FileExists(testFileName)
//		assert.NoError(t, err)
//		assert.True(t, exists, "File should exist in MinIO after writing")
//	})
//
//	t.Run("GetRemoteFileStats", func(t *testing.T) {
//		info, err := GetFileStats(testFileName)
//		assert.NoError(t, err, "Failed to get file stats from MinIO")
//		assert.Equal(t, int64(len(testData)), info.Size, "File size mismatch in MinIO")
//
//		// Check that returned metadata contains all the custom metadata we wrote
//		for key, expectedValue := range testMetadata {
//			actualValue, exists := info.Metadata[key]
//			assert.True(t, exists, "Metadata key '%s' should exist in returned metadata", key)
//			assert.Equal(t, expectedValue, actualValue, "Metadata value for key '%s' should match", key)
//		}
//
//		t.Logf("Remote file info: size=%d, metadata=%v", info.Size, info.Metadata)
//	})
//
//	t.Run("ReadRemoteFile", func(t *testing.T) {
//		data, err := ReadFile(testFileName)
//		assert.NoError(t, err, "Failed to read file from MinIO")
//		assert.Equal(t, testData, data, "Read data from MinIO does not match")
//	})
//
//	t.Run("UpdateRemoteFile", func(t *testing.T) {
//		newData := []byte("Updated MinIO content")
//		err := WriteFile(testFileName, newData, nil)
//		assert.NoError(t, err, "Failed to update file in MinIO")
//
//		data, err := ReadFile(testFileName)
//		assert.NoError(t, err, "Failed to read updated file from MinIO")
//		assert.Equal(t, newData, data, "Updated data in MinIO does not match")
//	})
//
//	t.Run("DeleteRemoteFile", func(t *testing.T) {
//		err := DeleteFile(testFileName)
//		assert.NoError(t, err, "Failed to delete file from MinIO")
//
//		exists, err := FileExists(testFileName)
//		assert.NoError(t, err)
//		assert.False(t, exists, "File should not exist in MinIO after deletion")
//	})
//
//	t.Run("LargeRemoteFile", func(t *testing.T) {
//		largeFileName := fmt.Sprintf("%s/%s/large_file_%d.bin", cfg.Minio.BucketName, cfg.Minio.RootPath, time.Now().Unix())
//		// Create a 1MB file
//		largeData := make([]byte, 1024*1024)
//		for i := range largeData {
//			largeData[i] = byte(i % 256)
//		}
//
//		err := WriteFile(largeFileName, largeData, nil)
//		assert.NoError(t, err, "Failed to write large file to MinIO")
//
//		size, err := GetFileSize(largeFileName)
//		assert.NoError(t, err, "Failed to get large file size")
//		assert.Equal(t, int64(len(largeData)), size, "Large file size mismatch")
//
//		data, err := ReadFile(largeFileName)
//		assert.NoError(t, err, "Failed to read large file from MinIO")
//		assert.Equal(t, largeData, data, "Large file data mismatch")
//
//		// Clean up
//		err = DeleteFile(largeFileName)
//		assert.NoError(t, err, "Failed to delete large file")
//	})
//}
