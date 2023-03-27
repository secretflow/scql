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

#include "yacl/base/exception.h"

#include "engine/core/type.h"

namespace scql::engine {

Tensor::Tensor(std::shared_ptr<arrow::ChunkedArray> chunked_arr)
    : chunked_arr_(std::move(chunked_arr)) {
  dtype_ = FromArrowDataType(chunked_arr_->type());
  YACL_ENFORCE(dtype_ != pb::PrimitiveDataType::PrimitiveDataType_UNDEFINED,
               "unsupported arrow data type: {}",
               chunked_arr_->type()->ToString());
}

}  // namespace scql::engine