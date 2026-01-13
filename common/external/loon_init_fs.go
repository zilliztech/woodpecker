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
#cgo CFLAGS: -I${SRCDIR}/../../external/output/include -I${SRCDIR}/../../cmake_build/thirdparty/milvus-storage/milvus-storage-src/rust
#cgo LDFLAGS: -L${SRCDIR}/../../external/output/lib -lmilvus-storage

#include <stdlib.h>
#include <stdint.h>
#include "milvus-storage/ffi_c.h"
#include "milvus-storage/ffi_filesystem_c.h"
*/
import "C"

import (
	"fmt"
	"strconv"
	"unsafe"

	"github.com/cockroachdb/errors"

	"github.com/zilliztech/woodpecker/common/config"
)

// InitStorageV2FileSystem initializes the filesystem singleton based on configuration
func InitStorageV2FileSystem(cfg *config.Configuration) error {
	if cfg.Woodpecker.Storage.IsStorageLocal() {
		return InitLocalLoonFileSystem(cfg.Woodpecker.Storage.RootPath)
	}
	return InitRemoteLoonFileSystem(cfg)
}

// InitLocalLoonFileSystem initializes a local filesystem singleton
func InitLocalLoonFileSystem(path string) error {
	properties := buildLocalProperties(path)
	defer freeLoonProperties(properties)

	result := C.loon_initialize_filesystem_singleton(properties)
	return HandleLoonFFIResult(&result, "InitLocalLoonFileSystem failed")
}

// InitRemoteLoonFileSystem initializes a remote filesystem singleton (S3, MinIO, etc.)
func InitRemoteLoonFileSystem(cfg *config.Configuration) error {
	properties := buildRemoteProperties(cfg)
	defer freeLoonProperties(properties)

	result := C.loon_initialize_filesystem_singleton(properties)
	return HandleLoonFFIResult(&result, "InitRemoteLoonFileSystem failed")
}

// CleanLoonFileSystem cleans up the filesystem singleton
func CleanLoonFileSystem() {
	// Note: The new Loon FFI doesn't have an explicit cleanup function for singleton
	// The filesystem will be cleaned up when the process exits
}

// buildLocalProperties creates LoonProperties for local filesystem
func buildLocalProperties(rootPath string) *C.LoonProperties {
	keys := []string{"storage.type", "storage.root_path"}
	values := []string{"local", rootPath}
	return buildLoonProperties(keys, values)
}

// buildRemoteProperties creates LoonProperties for remote filesystem (S3/MinIO)
func buildRemoteProperties(cfg *config.Configuration) *C.LoonProperties {
	keys := []string{
		"storage.type",
		"storage.address",
		"storage.bucket_name",
		"storage.access_key_id",
		"storage.access_key_value",
		"storage.root_path",
		"storage.cloud_provider",
		"storage.iam_endpoint",
		"storage.use_ssl",
		"storage.ssl_ca_cert",
		"storage.use_iam",
		"storage.log_level",
		"storage.region",
		"storage.use_virtual_host",
		"storage.request_timeout_ms",
		"storage.gcp_credential_json",
	}

	values := []string{
		"remote",
		fmt.Sprintf("%s:%d", cfg.Minio.Address, cfg.Minio.Port),
		cfg.Minio.BucketName,
		cfg.Minio.AccessKeyID,
		cfg.Minio.SecretAccessKey,
		cfg.Minio.RootPath,
		cfg.Minio.CloudProvider,
		cfg.Minio.IamEndpoint,
		strconv.FormatBool(cfg.Minio.UseSSL),
		cfg.Minio.Ssl.TlsCACert,
		strconv.FormatBool(cfg.Minio.UseIAM),
		cfg.Minio.LogLevel,
		cfg.Minio.Region,
		strconv.FormatBool(cfg.Minio.UseVirtualHost),
		strconv.Itoa(cfg.Minio.RequestTimeoutMs),
		cfg.Minio.GcpCredentialJSON,
	}

	return buildLoonProperties(keys, values)
}

// buildLoonProperties creates a C.LoonProperties from key-value pairs
func buildLoonProperties(keys, values []string) *C.LoonProperties {
	if len(keys) != len(values) {
		return nil
	}

	count := len(keys)
	if count == 0 {
		return &C.LoonProperties{
			properties: nil,
			count:      0,
		}
	}

	// Allocate array of LoonProperty
	properties := C.malloc(C.size_t(count) * C.size_t(unsafe.Sizeof(C.LoonProperty{})))
	propertiesArray := (*[1 << 30]C.LoonProperty)(properties)[:count:count]

	// Fill in each property
	for i := 0; i < count; i++ {
		propertiesArray[i].key = C.CString(keys[i])
		propertiesArray[i].value = C.CString(values[i])
	}

	return &C.LoonProperties{
		properties: (*C.LoonProperty)(properties),
		count:      C.size_t(count),
	}
}

// freeLoonProperties frees the memory allocated for LoonProperties
func freeLoonProperties(properties *C.LoonProperties) {
	if properties == nil || properties.properties == nil {
		return
	}

	count := int(properties.count)
	propertiesArray := (*[1 << 30]C.LoonProperty)(unsafe.Pointer(properties.properties))[:count:count]

	// Free each key and value
	for i := 0; i < count; i++ {
		C.free(unsafe.Pointer(propertiesArray[i].key))
		C.free(unsafe.Pointer(propertiesArray[i].value))
	}

	// Free the array itself
	C.free(unsafe.Pointer(properties.properties))
}

// HandleLoonFFIResult handles the LoonFFIResult returned from C functions
func HandleLoonFFIResult(result *C.LoonFFIResult, extraInfo string) error {
	if C.loon_ffi_is_success(result) != 0 {
		C.loon_ffi_free_result(result)
		return nil
	}

	// Get error message
	errMsg := C.loon_ffi_get_errmsg(result)
	var finalMsg string
	if errMsg != nil {
		finalMsg = C.GoString(errMsg)
	} else {
		finalMsg = fmt.Sprintf("Unknown error (code: %d)", result.err_code)
	}

	// Free the result
	C.loon_ffi_free_result(result)

	// Return error with extra info
	return errors.New(fmt.Sprintf("%s: %s", extraInfo, finalMsg))
}

// GetFileSystemSingletonHandle gets the filesystem singleton handle
func GetFileSystemSingletonHandle() (C.FileSystemHandle, error) {
	var handle C.FileSystemHandle
	result := C.loon_get_filesystem_singleton_handle(&handle)
	if err := HandleLoonFFIResult(&result, "GetFileSystemSingletonHandle failed"); err != nil {
		return 0, err
	}
	return handle, nil
}
