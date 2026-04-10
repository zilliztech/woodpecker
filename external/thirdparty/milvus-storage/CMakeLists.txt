#-------------------------------------------------------------------------------
# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.
#-------------------------------------------------------------------------------

# Update milvus-storage_VERSION for the first occurrence
milvus_add_pkg_config("milvus-storage")
set_property(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES "")
set( milvus-storage_VERSION b86636f)
set( GIT_REPOSITORY  "https://github.com/milvus-io/milvus-storage.git")
message(STATUS "milvus-storage repo: ${GIT_REPOSITORY}")
message(STATUS "milvus-storage version: ${milvus-storage_VERSION}")

message(STATUS "Building milvus-storage-${milvus-storage_SOURCE_VER} from source")
message(STATUS ${CMAKE_BUILD_TYPE})

# Inject a FindAzure.cmake shim. Upstream milvus-storage cpp/CMakeLists.txt calls
# find_package(Azure REQUIRED) unconditionally, but conan installs the Azure SDK
# transitively (via arrow:with_azure=True) under the package name azure-sdk-for-cpp,
# producing Findazure-sdk-for-cpp.cmake — the names don't match. Generate a stub
# FindAzure.cmake that delegates to azure-sdk-for-cpp; the conan-generated target
# azure-sdk-for-cpp::azure-sdk-for-cpp is what milvus-storage links against, so no
# target aliasing is needed.
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/cmake-shim/FindAzure.cmake"
     "find_package(azure-sdk-for-cpp REQUIRED)\n"
     "set(Azure_FOUND \${azure-sdk-for-cpp_FOUND})\n")
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_BINARY_DIR}/cmake-shim")

set( CMAKE_PREFIX_PATH ${CONAN_BOOST_ROOT} )
FetchContent_Declare(
        milvus-storage
        GIT_REPOSITORY  ${GIT_REPOSITORY}
        GIT_TAG         ${milvus-storage_VERSION}
        SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/milvus-storage-src
        BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/milvus-storage-build
        SOURCE_SUBDIR   cpp
        DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH} )

FetchContent_GetProperties( milvus-storage )
if ( NOT milvus-storage_POPULATED )
    FetchContent_Populate( milvus-storage )

    # Adding the following target:
    # milvus-storage
    add_subdirectory( ${milvus-storage_SOURCE_DIR}/cpp
                      ${milvus-storage_BINARY_DIR} )
endif()

set( MILVUS_STORAGE_INCLUDE_DIR ${milvus-storage_SOURCE_DIR}/cpp/include CACHE INTERNAL "Path to milvus-storage include directory" )
