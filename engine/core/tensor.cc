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

#include "engine/core/tensor.h"

#include "arrow/compute/cast.h"
#include "yacl/base/exception.h"

#include "engine/core/type.h"

namespace scql::engine {

Tensor::Tensor(std::shared_ptr<arrow::ChunkedArray> chunked_arr)
    : chunked_arr_(std::move(chunked_arr)) {
  // try to cast decimal128(x, 0) to int64
  if (chunked_arr_->type()->id() == arrow::Type::DECIMAL128) {
    auto decimal_type =
        std::dynamic_pointer_cast<arrow::Decimal128Type>(chunked_arr_->type());
    if (decimal_type->scale() == 0) {
      auto to_datatype = arrow::int64();

      arrow::compute::CastOptions options;
      options.allow_decimal_truncate = true;
      auto result = arrow::compute::Cast(chunked_arr_, to_datatype, options);
      YACL_ENFORCE(result.ok(), "caught error while cast {} to {}: {}",
                   chunked_arr_->type()->ToString(), to_datatype->ToString(),
                   result.status().ToString());
      chunked_arr_ = result.ValueOrDie().chunked_array();
    }
  }
  dtype_ = FromArrowDataType(chunked_arr_->type());
  YACL_ENFORCE(dtype_ != pb::PrimitiveDataType::PrimitiveDataType_UNDEFINED,
               "unsupported arrow data type: {}",
               chunked_arr_->type()->ToString());
}

}  // namespace scql::engine