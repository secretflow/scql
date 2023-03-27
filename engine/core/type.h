// Copyright 2023 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "arrow/type.h"

#include "api/core.pb.h"
#include "libspu/spu.pb.h"
namespace scql::engine {

enum class Visibility {
  Unknown = 0,
  Private,
  Public,
  Secret,
};

/// @returns pb::PrimitiveDataType::PrimitiveDataType_UNDEFINED
/// if @param[in] dtype is not supported
pb::PrimitiveDataType FromArrowDataType(
    const std::shared_ptr<arrow::DataType>& dtype);

/// @brief convert scql primitive data type to arrow data type
/// @returns nullptr if @param[in] dtype is not supported
std::shared_ptr<arrow::DataType> ToArrowDataType(pb::PrimitiveDataType dtype);

/// @brief convert arrow data type to spu plaintext type enum
/// @returns spu::PT_INVALID if @param[in] dtype is not supported
spu::PtType ArrowDataTypeToSpuPtType(
    const std::shared_ptr<arrow::DataType>& dtype);

/// @brief convert spu plaintext type enum to arrow data type
/// @returns nullptr if @param[in] pt_type is not supported
std::shared_ptr<arrow::DataType> SpuPtTypeToArrowDataType(spu::PtType pt_type);

/// @brief convert scql primitive data type to spu plaintext type enum
/// @returns spu::PT_INVALID if @param[in] dtype is not supported
spu::PtType DataTypeToSpuPtType(pb::PrimitiveDataType dtype);

}  // namespace scql::engine