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

#pragma once

#include <stdint.h>

#include "common/type_c.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============ File System Initialization ============

CStatus
InitLocalArrowFileSystemSingleton(const char* c_path);

void
CleanArrowFileSystemSingleton();

CStatus
InitRemoteArrowFileSystemSingleton(CStorageConfig c_storage_config);

// ============ Reader Functions ============

/**
 * @brief Get file statistics including size and metadata (for files only)
 * @param c_path File path
 * @param out_size Output file size in bytes
 * @param out_keys Output array of metadata keys (caller must call FreeMemory for each key and the array, can be NULL)
 * @param out_values Output array of metadata values (caller must call FreeMemory for each value and the array, can be NULL)
 * @param out_count Output count of key-value pairs (can be NULL if metadata not needed)
 * @return CStatus indicating success or failure
 */
CStatus
GetFileStats(const char* c_path, 
             int64_t* out_size,
             char*** out_keys,
             char*** out_values,
             int* out_count);

/**
 * @brief Read file data content
 * @param c_path File path
 * @param out_data Output data pointer (caller must call FreeMemory to free)
 * @param out_size Output data size in bytes
 * @return CStatus indicating success or failure
 */
CStatus
ReadFileData(const char* c_path, uint8_t** out_data, int64_t* out_size);

// ============ Writer Functions ============

/**
 * @brief Write data to file with optional custom metadata (map<string, string>)
 * @param c_path File path
 * @param data Data bytes to write
 * @param data_size Size of data in bytes
 * @param metadata_keys Array of metadata keys (can be NULL if metadata_count is 0)
 * @param metadata_values Array of metadata values (can be NULL if metadata_count is 0)
 * @param metadata_count Number of metadata key-value pairs
 * @return CStatus indicating success or failure
 */
CStatus
WriteFileData(const char* c_path, 
              const uint8_t* data, 
              int64_t data_size,
              const char** metadata_keys,
              const char** metadata_values,
              int metadata_count);

/**
 * @brief Delete a file
 * @param c_path File path to delete
 * @return CStatus indicating success or failure
 */
CStatus
DeleteFile(const char* c_path);

/**
 * @brief Get file information (exists, type, timestamps)
 * @param c_path Path to check
 * @param out_exists Output boolean indicating if path exists
 * @param out_is_dir Output boolean indicating if path is a directory (can be NULL)
 * @param out_mtime_ns Output modification time in nanoseconds since epoch (can be NULL)
 * @param out_ctime_ns Output creation time in nanoseconds since epoch (can be NULL, may be same as mtime if not available)
 * @return CStatus indicating success or failure
 */
CStatus
GetFileInfo(const char* c_path, 
            bool* out_exists,
            bool* out_is_dir,
            int64_t* out_mtime_ns,
            int64_t* out_ctime_ns);

/**
 * @brief Create a directory (or bucket if path is bucket-only)
 * @param c_path Directory path to create
 * @param recursive Whether to create parent directories if they don't exist
 * @return CStatus indicating success or failure
 */
CStatus
CreateDir(const char* c_path, bool recursive);

/**
 * @brief List directory contents
 * @param c_path Directory path to list
 * @param recursive Whether to list recursively (include subdirectories)
 * @param out_paths Output array of file paths (caller must call FreeMemory for each path and the array)
 * @param out_is_dirs Output array of booleans indicating if each path is a directory (caller must call FreeMemory for the array)
 * @param out_sizes Output array of file sizes in bytes (caller must call FreeMemory for the array, directories have size 0)
 * @param out_mtime_ns Output array of modification times in nanoseconds (caller must call FreeMemory for the array)
 * @param out_count Output count of files/directories found
 * @return CStatus indicating success or failure
 */
CStatus
ListDir(const char* c_path,
        bool recursive,
        char*** out_paths,
        bool** out_is_dirs,
        int64_t** out_sizes,
        int64_t** out_mtime_ns,
        int* out_count);

// ============ Helper Functions for Memory Management ============

/**
 * @brief Free memory allocated by C functions
 * @param ptr Pointer to free
 */
 void
 FreeMemory(void* ptr);

#ifdef __cplusplus
}
#endif