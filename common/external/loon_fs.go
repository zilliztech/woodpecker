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
// TODO to be removed, abi.h should be refactor in milvus-storage
#cgo CFLAGS: -I${SRCDIR}/../../cmake_build/thirdparty/milvus-storage/milvus-storage-src/rust

#include <stdlib.h>
#include <stdint.h>
#include "milvus-storage/ffi_filesystem_c.h"
*/
import "C"

import (
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
	var keys **C.char
	var values **C.char
	var count C.uint32_t

	result := C.loon_filesystem_get_file_stats(handle, cPath, pathLen, &size, &keys, &values, &count)
	if err := HandleLoonFFIResult(&result, "GetFileStats failed"); err != nil {
		return nil, err
	}

	info := &FileInfo{
		Size:     int64(size),
		Metadata: make(map[string]string),
	}

	// Parse metadata if available
	if count > 0 && keys != nil && values != nil {
		defer func() {
			// Free the metadata arrays
			keysSlice := unsafe.Slice(keys, int(count))
			valuesSlice := unsafe.Slice(values, int(count))
			for i := 0; i < int(count); i++ {
				C.free(unsafe.Pointer(keysSlice[i]))
				C.free(unsafe.Pointer(valuesSlice[i]))
			}
			C.free(unsafe.Pointer(keys))
			C.free(unsafe.Pointer(values))
		}()

		keysSlice := unsafe.Slice(keys, int(count))
		valuesSlice := unsafe.Slice(values, int(count))
		for i := 0; i < int(count); i++ {
			key := C.GoString(keysSlice[i])
			value := C.GoString(valuesSlice[i])
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
	var cKeys **C.char
	var cValues **C.char
	var metadataCount C.uint32_t

	if len(metadata) > 0 {
		metadataCount = C.uint32_t(len(metadata))

		// Allocate arrays for keys and values
		keysArray := make([]*C.char, len(metadata))
		valuesArray := make([]*C.char, len(metadata))

		i := 0
		for k, v := range metadata {
			keysArray[i] = C.CString(k)
			valuesArray[i] = C.CString(v)
			i++
		}

		defer func() {
			for i := 0; i < len(metadata); i++ {
				C.free(unsafe.Pointer(keysArray[i]))
				C.free(unsafe.Pointer(valuesArray[i]))
			}
		}()

		if len(keysArray) > 0 {
			cKeys = &keysArray[0]
			cValues = &valuesArray[0]
		}
	}

	result := C.loon_filesystem_write_file(handle, cPath, pathLen, cData, dataSize, cKeys, cValues, metadataCount)
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

	var paths **C.char
	var pathLens *C.uint32_t
	var isDirs *C.bool
	var sizes *C.uint64_t
	var mtimeNs *C.int64_t
	var count C.uint32_t

	result := C.loon_filesystem_list_dir(handle, cPath, pathLen, C.bool(recursive), &paths, &pathLens, &isDirs, &sizes, &mtimeNs, &count)
	if err := HandleLoonFFIResult(&result, "ListDir failed"); err != nil {
		return nil, err
	}

	if count == 0 {
		return []DirEntry{}, nil
	}

	// Convert to Go slice
	pathsSlice := unsafe.Slice(paths, int(count))
	pathLensSlice := unsafe.Slice(pathLens, int(count))
	isDirsSlice := unsafe.Slice(isDirs, int(count))
	sizesSlice := unsafe.Slice(sizes, int(count))
	mtimeNsSlice := unsafe.Slice(mtimeNs, int(count))

	defer func() {
		// Free the arrays
		for i := 0; i < int(count); i++ {
			C.free(unsafe.Pointer(pathsSlice[i]))
		}
		C.free(unsafe.Pointer(paths))
		C.free(unsafe.Pointer(pathLens))
		C.free(unsafe.Pointer(isDirs))
		C.free(unsafe.Pointer(sizes))
		C.free(unsafe.Pointer(mtimeNs))
	}()

	entries := make([]DirEntry, int(count))
	for i := 0; i < int(count); i++ {
		// Use path length to properly handle paths with null bytes
		pathBytes := C.GoBytes(unsafe.Pointer(pathsSlice[i]), C.int(pathLensSlice[i]))
		entries[i] = DirEntry{
			Path:    string(pathBytes),
			IsDir:   bool(isDirsSlice[i]),
			Size:    int64(sizesSlice[i]),
			MTimeNs: int64(mtimeNsSlice[i]),
		}
	}

	return entries, nil
}
