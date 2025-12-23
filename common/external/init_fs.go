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
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include <stdint.h>
//#include "common/init_c.h"
//#include "segcore/segcore_init_c.h"
//#include "storage/storage_c.h"
#include "segcore/arrow_fs_c.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/labstack/gommon/log"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"

	"github.com/zilliztech/woodpecker/common/config"
)

func InitStorageV2FileSystem(cfg *config.Configuration) error {
	if cfg.Woodpecker.Storage.Type == "local" {
		return InitLocalArrowFileSystem(cfg.Woodpecker.Storage.RootPath)
	}
	return InitRemoteArrowFileSystem(cfg)
}

func InitLocalArrowFileSystem(path string) error {
	CLocalRootPath := C.CString(path)
	defer C.free(unsafe.Pointer(CLocalRootPath))
	status := C.InitLocalArrowFileSystemSingleton(CLocalRootPath)
	return HandleCStatus(&status, "InitLocalArrowFileSystemSingleton failed")
}

func InitRemoteArrowFileSystem(cfg *config.Configuration) error {
	cAddress := C.CString(fmt.Sprintf("%s:%d", cfg.Minio.Address, cfg.Minio.Port))
	cBucketName := C.CString(cfg.Minio.BucketName)
	cAccessKey := C.CString(cfg.Minio.AccessKeyID)
	cAccessValue := C.CString(cfg.Minio.SecretAccessKey)
	cRootPath := C.CString(cfg.Minio.RootPath)
	cStorageType := C.CString("remote") // always remote currently
	cIamEndPoint := C.CString(cfg.Minio.IamEndpoint)
	cCloudProvider := C.CString(cfg.Minio.CloudProvider)
	cLogLevel := C.CString(cfg.Minio.LogLevel)
	cRegion := C.CString(cfg.Minio.Region)
	cSslCACert := C.CString(cfg.Minio.Ssl.TlsCACert)
	cGcpCredentialJSON := C.CString(cfg.Minio.GcpCredentialJSON)
	defer C.free(unsafe.Pointer(cAddress))
	defer C.free(unsafe.Pointer(cBucketName))
	defer C.free(unsafe.Pointer(cAccessKey))
	defer C.free(unsafe.Pointer(cAccessValue))
	defer C.free(unsafe.Pointer(cRootPath))
	defer C.free(unsafe.Pointer(cStorageType))
	defer C.free(unsafe.Pointer(cIamEndPoint))
	defer C.free(unsafe.Pointer(cLogLevel))
	defer C.free(unsafe.Pointer(cRegion))
	defer C.free(unsafe.Pointer(cCloudProvider))
	defer C.free(unsafe.Pointer(cSslCACert))
	defer C.free(unsafe.Pointer(cGcpCredentialJSON))
	storageConfig := C.CStorageConfig{
		address:                cAddress,
		bucket_name:            cBucketName,
		access_key_id:          cAccessKey,
		access_key_value:       cAccessValue,
		root_path:              cRootPath,
		storage_type:           cStorageType,
		iam_endpoint:           cIamEndPoint,
		cloud_provider:         cCloudProvider,
		useSSL:                 C.bool(cfg.Minio.UseSSL),
		sslCACert:              cSslCACert,
		useIAM:                 C.bool(cfg.Minio.UseIAM),
		log_level:              cLogLevel,
		region:                 cRegion,
		useVirtualHost:         C.bool(cfg.Minio.UseVirtualHost),
		requestTimeoutMs:       C.int64_t(int64(cfg.Minio.RequestTimeoutMs)),
		gcp_credential_json:    cGcpCredentialJSON,
		use_custom_part_upload: true,
		max_connections:        C.uint32_t(uint32(100)), // TODO update minio to make this param configurable
	}

	status := C.InitRemoteArrowFileSystemSingleton(storageConfig)
	return HandleCStatus(&status, "InitRemoteArrowFileSystemSingleton failed")
}

// HandleCStatus deals with the error returned from CGO
func HandleCStatus(status *C.CStatus, extraInfo string) error {
	if status.error_code == 0 {
		return nil
	}
	errorCode := status.error_code
	errorName, ok := commonpb.ErrorCode_name[int32(errorCode)]
	if !ok {
		errorName = "UnknownError"
	}
	errorMsg := C.GoString(status.error_msg)
	defer C.free(unsafe.Pointer(status.error_msg))

	finalMsg := fmt.Sprintf("[%s] %s", errorName, errorMsg)
	logMsg := fmt.Sprintf("%s, C Runtime Exception: %s\n", extraInfo, finalMsg)
	log.Warn(logMsg)
	return errors.New(finalMsg)
}

//func serializeHeaders(headerstr string) string {
//	if len(headerstr) == 0 {
//		return ""
//	}
//	decodeheaders, err := base64.StdEncoding.DecodeString(headerstr)
//	if err != nil {
//		return headerstr
//	}
//	return string(decodeheaders)
//}

//func InitPluginLoader() error {
//	if hookutil.IsClusterEncryptionEnabled() {
//		cSoPath := C.CString(paramtable.GetCipherParams().SoPathCpp.GetValue())
//		log.Info("Init PluginLoader", zap.String("soPath", paramtable.GetCipherParams().SoPathCpp.GetValue()))
//		defer C.free(unsafe.Pointer(cSoPath))
//		status := C.InitPluginLoader(cSoPath)
//		return HandleCStatus(&status, "InitPluginLoader failed")
//	}
//	return nil
//}
//
//func CleanPluginLoader() {
//	C.CleanPluginLoader()
//}
