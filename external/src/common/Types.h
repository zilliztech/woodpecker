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

#include <cstdint>
#include <sstream>

// Forward declarations for proto types
namespace proto {
namespace schema {
struct FieldSchema;
}  // namespace schema
}  // namespace proto

namespace milvus {

// Common type aliases
using Timestamp = uint64_t;

// DataType enum matching proto schema DataType
enum class DataType {
    NONE = 0,
    BOOL = 1,
    INT8 = 2,
    INT16 = 3,
    INT32 = 4,
    INT64 = 5,
    FLOAT = 10,
    DOUBLE = 11,
    STRING = 20,
    VARCHAR = 21,
    ARRAY = 22,
    JSON = 23,
    BINARYVECTOR = 100,
    FLOATVECTOR = 101,
    FLOAT16VECTOR = 102,
    BFLOAT16VECTOR = 103,
    SPARSEFLOATVECTOR = 104,
    INT8VECTOR = 105,
};

}  // namespace milvus

// Placeholder for proto::schema::FieldSchema if not defined elsewhere
namespace proto {
namespace schema {
struct FieldSchema {
    int64_t fieldID;
    std::string name;
    bool is_primary_key;
    std::string description;
    milvus::DataType data_type;
    // Add other fields as needed
};
}  // namespace schema
}  // namespace proto
