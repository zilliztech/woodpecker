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

/*
#cgo pkg-config: milvus-storage

#include <stdlib.h>
#include <stdint.h>
#include "milvus-storage/ffi_filesystem_c.h"
*/
import "C"

import (
	"strings"
	"unsafe"
)

// FileInfo represents file metadata
type FileInfo struct {
	Size     int64
	Metadata map[string]string
}

// GetFileStats retrieves file size and optional metadata (for files only)
func GetFileStats(path string) (*FileInfo, error) {
	handle, err := GetFileSystemSingletonHandle()
	if err != nil {
		return nil, err
	}

	cPath := C.CString(path)
	pathLen := C.uint32_t(len(path))
	defer C.free(unsafe.Pointer(cPath))

	var size C.uint64_t
	var metaArray *C.LoonFileSystemMeta
	var metaCount C.uint32_t

	result := C.loon_filesystem_get_file_stats(handle, cPath, pathLen, &size, &metaArray, &metaCount)
	if err := HandleLoonFFIResult(&result, "GetFileStats failed"); err != nil {
		return nil, err
	}

	info := &FileInfo{
		Size:     int64(size),
		Metadata: make(map[string]string),
	}

	// Parse metadata if available
	if metaCount > 0 && metaArray != nil {
		defer C.loon_filesystem_free_meta_array(metaArray, metaCount)

		metaSlice := unsafe.Slice(metaArray, int(metaCount))
		for i := 0; i < int(metaCount); i++ {
			key := C.GoString(metaSlice[i].key)
			value := C.GoString(metaSlice[i].value)
			info.Metadata[key] = value
		}
	}

	return info, nil
}

// FileInfoDetail represents detailed file information
type FileInfoDetail struct {
	Exists  bool
	IsDir   bool
	MTimeNs int64 // Modification time in nanoseconds since epoch
}

// GetFileInfo retrieves file information (exists, type, timestamps)
func GetFileInfo(path string) (*FileInfoDetail, error) {
	handle, err := GetFileSystemSingletonHandle()
	if err != nil {
		return nil, err
	}

	cPath := C.CString(path)
	pathLen := C.uint32_t(len(path))
	defer C.free(unsafe.Pointer(cPath))

	var exists C.bool
	var isDir C.bool
	var mtimeNs C.int64_t

	result := C.loon_filesystem_get_path_info(handle, cPath, pathLen, &exists, &isDir, &mtimeNs)
	if err := HandleLoonFFIResult(&result, "GetFileInfo failed"); err != nil {
		return nil, err
	}

	return &FileInfoDetail{
		Exists:  bool(exists),
		IsDir:   bool(isDir),
		MTimeNs: int64(mtimeNs),
	}, nil
}

// ReadFile reads the entire file content
func ReadFile(path string) ([]byte, error) {
	handle, err := GetFileSystemSingletonHandle()
	if err != nil {
		return nil, err
	}

	cPath := C.CString(path)
	pathLen := C.uint32_t(len(path))
	defer C.free(unsafe.Pointer(cPath))

	var data *C.uint8_t
	var size C.uint64_t

	result := C.loon_filesystem_read_file_all(handle, cPath, pathLen, &data, &size)
	if err := HandleLoonFFIResult(&result, "ReadFile failed"); err != nil {
		return nil, err
	}

	if data == nil || size == 0 {
		return []byte{}, nil
	}

	// Copy data to Go slice
	goData := C.GoBytes(unsafe.Pointer(data), C.int(size))

	// Free the allocated memory
	C.free(unsafe.Pointer(data))

	return goData, nil
}

// WriteFile writes data to a file with optional metadata
func WriteFile(path string, data []byte, metadata map[string]string) error {
	handle, err := GetFileSystemSingletonHandle()
	if err != nil {
		return err
	}

	cPath := C.CString(path)
	pathLen := C.uint32_t(len(path))
	defer C.free(unsafe.Pointer(cPath))

	var cData *C.uint8_t
	var dataSize C.uint64_t

	if len(data) > 0 {
		cData = (*C.uint8_t)(unsafe.Pointer(&data[0]))
		dataSize = C.uint64_t(len(data))
	}

	// Prepare metadata if provided
	var metaArray *C.LoonFileSystemMeta
	var metaCount C.uint32_t

	if len(metadata) > 0 {
		metaCount = C.uint32_t(len(metadata))

		// Allocate array of LoonFileSystemMeta
		metaArray = (*C.LoonFileSystemMeta)(C.malloc(C.size_t(metaCount) * C.size_t(unsafe.Sizeof(C.LoonFileSystemMeta{}))))
		metaSlice := unsafe.Slice(metaArray, int(metaCount))

		i := 0
		for k, v := range metadata {
			metaSlice[i].key = C.CString(k)
			metaSlice[i].value = C.CString(v)
			i++
		}

		defer func() {
			for i := 0; i < len(metadata); i++ {
				C.free(unsafe.Pointer(metaSlice[i].key))
				C.free(unsafe.Pointer(metaSlice[i].value))
			}
			C.free(unsafe.Pointer(metaArray))
		}()
	}

	result := C.loon_filesystem_write_file(handle, cPath, pathLen, cData, dataSize, metaArray, metaCount)
	return HandleLoonFFIResult(&result, "WriteFile failed")
}

// DeleteFile deletes a file
func DeleteFile(path string) error {
	handle, err := GetFileSystemSingletonHandle()
	if err != nil {
		return err
	}

	cPath := C.CString(path)
	pathLen := C.uint32_t(len(path))
	defer C.free(unsafe.Pointer(cPath))

	result := C.loon_filesystem_delete_file(handle, cPath, pathLen)
	return HandleLoonFFIResult(&result, "DeleteFile failed")
}

// FileExists checks if a file exists and is a file (not a directory)
func FileExists(path string) (bool, error) {
	info, err := GetFileInfo(path)
	if err != nil {
		// TODO should use FFI Err Code instead
		if strings.Contains(err.Error(), "File not found") {
			return false, nil
		}
		return false, err
	}
	// File exists and is not a directory
	return info.Exists && !info.IsDir, nil
}

// GetFileSize is a convenience function to get only the file size
func GetFileSize(path string) (int64, error) {
	info, err := GetFileStats(path)
	if err != nil {
		return 0, err
	}
	return info.Size, nil
}

// CreateDir creates a directory or bucket (if path is bucket-only for S3)
func CreateDir(path string, recursive bool) error {
	handle, err := GetFileSystemSingletonHandle()
	if err != nil {
		return err
	}

	cPath := C.CString(path)
	pathLen := C.uint32_t(len(path))
	defer C.free(unsafe.Pointer(cPath))

	result := C.loon_filesystem_create_dir(handle, cPath, pathLen, C.bool(recursive))
	return HandleLoonFFIResult(&result, "CreateDir failed")
}

// DirExists checks if a directory or bucket exists
// For S3, directories are represented as empty objects or directory markers
func DirExists(path string) (bool, error) {
	info, err := GetFileInfo(path)
	if err != nil {
		return false, err
	}
	// Directory exists and is a directory
	return info.Exists && info.IsDir, nil
}

// DirEntry represents a single entry in a directory listing
type DirEntry struct {
	Path    string
	IsDir   bool
	Size    int64
	MTimeNs int64 // Modification time in nanoseconds since epoch
}

// ListDir lists directory contents
// Returns a slice of DirEntry containing information about each file/directory
func ListDir(path string, recursive bool) ([]DirEntry, error) {
	handle, err := GetFileSystemSingletonHandle()
	if err != nil {
		return nil, err
	}

	cPath := C.CString(path)
	pathLen := C.uint32_t(len(path))
	defer C.free(unsafe.Pointer(cPath))

	// loon_filesystem_list_dir is a caller-allocated output pattern: pass &outList
	// (LoonFileInfoList*), the FFI fills entries/count, and we free via
	// loon_filesystem_free_file_info_list which also takes the same pointer.
	var outList C.LoonFileInfoList
	result := C.loon_filesystem_list_dir(handle, cPath, pathLen, C.bool(recursive), &outList)
	if err := HandleLoonFFIResult(&result, "ListDir failed"); err != nil {
		return nil, err
	}
	defer C.loon_filesystem_free_file_info_list(&outList)

	count := int(outList.count)
	if count == 0 {
		return []DirEntry{}, nil
	}

	// cgo does not allow indexing a C pointer (outList.entries[i]); convert it to a
	// Go slice that views the same memory, valid until the deferred free runs.
	entriesSlice := unsafe.Slice(outList.entries, count)
	entries := make([]DirEntry, count)
	for i := 0; i < count; i++ {
		e := entriesSlice[i]
		// Use path_len so paths containing embedded null bytes survive intact.
		pathBytes := C.GoBytes(unsafe.Pointer(e.path), C.int(e.path_len))
		entries[i] = DirEntry{
			Path:    string(pathBytes),
			IsDir:   bool(e.is_dir),
			Size:    int64(e.size),
			MTimeNs: int64(e.mtime_ns),
		}
	}

	return entries, nil
}
