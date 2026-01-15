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
#cgo CFLAGS: -I${SRCDIR}/../../cmake_build/thirdparty/milvus-storage/milvus-storage-src/rust

#include <stdlib.h>
#include <stdint.h>
#include "milvus-storage/ffi_filesystem_c.h"
*/
import "C"

import (
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"github.com/cockroachdb/errors"

	"github.com/zilliztech/woodpecker/common/config"
)

// Property key accessors using loon_property_fs_*() functions from ffi_filesystem_c.h
// These variables are initialized at package load time by calling the C functions,
// ensuring they always match the C-side definitions.
var (
	PropertyFsStorageType       = C.GoString(C.loon_property_fs_storage_type())
	PropertyFsRootPath          = C.GoString(C.loon_property_fs_root_path())
	PropertyFsAddress           = C.GoString(C.loon_property_fs_address())
	PropertyFsBucketName        = C.GoString(C.loon_property_fs_bucket_name())
	PropertyFsRegion            = C.GoString(C.loon_property_fs_region())
	PropertyFsAccessKeyID       = C.GoString(C.loon_property_fs_access_key_id())
	PropertyFsAccessKeyValue    = C.GoString(C.loon_property_fs_access_key_value())
	PropertyFsUseIAM            = C.GoString(C.loon_property_fs_use_iam())
	PropertyFsIamEndpoint       = C.GoString(C.loon_property_fs_iam_endpoint())
	PropertyFsGcpCredentialJSON = C.GoString(C.loon_property_fs_gcp_credential_json())
	PropertyFsUseSSL            = C.GoString(C.loon_property_fs_use_ssl())
	PropertyFsSSLCACert         = C.GoString(C.loon_property_fs_ssl_ca_cert())
	PropertyFsUseVirtualHost    = C.GoString(C.loon_property_fs_use_virtual_host())
	PropertyFsRequestTimeoutMs  = C.GoString(C.loon_property_fs_request_timeout_ms())
	PropertyFsLogLevel          = C.GoString(C.loon_property_fs_log_level())
	PropertyFsCloudProvider     = C.GoString(C.loon_property_fs_cloud_provider())
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
// normalizeLogLevel converts log level string to the format required by milvus-storage
// Valid values: "Fatal", "Error", "Warn", "Info", "Debug", "Trace", "Off"
// TODO should be remove after storage v2 support case-insensitive
func normalizeLogLevel(level string) string {
	if level == "" {
		return "Info" // default
	}

	// Convert to lowercase first, then capitalize first letter
	level = strings.ToLower(level)

	// Map common variants
	switch level {
	case "fatal":
		return "Fatal"
	case "error":
		return "Error"
	case "warn", "warning":
		return "Warn"
	case "info":
		return "Info"
	case "debug":
		return "Debug"
	case "trace":
		return "Trace"
	case "off":
		return "Off"
	default:
		// If unknown, default to Info
		return "Info"
	}
}

func buildLocalProperties(rootPath string) *C.LoonProperties {
	keys := []string{
		PropertyFsStorageType,
		PropertyFsRootPath,
	}
	values := []string{"local", rootPath}
	return buildLoonProperties(keys, values)
}

// buildRemoteProperties creates LoonProperties for remote filesystem (S3/MinIO)
func buildRemoteProperties(cfg *config.Configuration) *C.LoonProperties {
	keys := []string{
		PropertyFsStorageType,
		PropertyFsAddress,
		PropertyFsBucketName,
		PropertyFsAccessKeyID,
		PropertyFsAccessKeyValue,
		PropertyFsRootPath,
		PropertyFsCloudProvider,
		PropertyFsIamEndpoint,
		PropertyFsUseSSL,
		PropertyFsSSLCACert,
		PropertyFsUseIAM,
		PropertyFsLogLevel,
		PropertyFsRegion,
		PropertyFsUseVirtualHost,
		PropertyFsRequestTimeoutMs,
		PropertyFsGcpCredentialJSON,
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
		normalizeLogLevel(cfg.Minio.LogLevel),
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

// parseLoonPropertiesToMap converts C LoonProperties to a Go map
// This is useful for testing and debugging
func parseLoonPropertiesToMap(props *C.LoonProperties) map[string]string {
	if props == nil || props.properties == nil || props.count == 0 {
		return make(map[string]string)
	}

	result := make(map[string]string)
	count := int(props.count)
	propertiesArray := (*[1 << 30]C.LoonProperty)(unsafe.Pointer(props.properties))[:count:count]

	for i := 0; i < count; i++ {
		key := C.GoString(propertiesArray[i].key)
		value := C.GoString(propertiesArray[i].value)
		result[key] = value
	}

	return result
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
