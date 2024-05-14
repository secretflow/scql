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

#include <memory>

#include "arrow/chunked_array.h"

#include "api/core.pb.h"

namespace scql::engine {

/// @brief A Tensor reprensents a column of a relation
class Tensor {
 public:
  explicit Tensor(std::shared_ptr<arrow::ChunkedArray> chunked_arr);

  Tensor(const Tensor&) = delete;
  Tensor& operator=(const Tensor&) = delete;

  int64_t Length() const { return chunked_arr_->length(); }

  // return the total number of nulls
  int64_t GetNullCount() const { return chunked_arr_->null_count(); }

  /// @returns the data type of tensor element
  pb::PrimitiveDataType Type() const { return dtype_; }

  /// @returns as arrow chunked array
  std::shared_ptr<arrow::ChunkedArray> ToArrowChunkedArray() const {
    return chunked_arr_;
  }

 protected:
  friend class TensorChunkReader;

  std::shared_ptr<arrow::ChunkedArray> chunked_arr_;
  pb::PrimitiveDataType dtype_;
};

using TensorPtr = std::shared_ptr<Tensor>;
using RepeatedPbTensor = google::protobuf::RepeatedPtrField<pb::Tensor>;

}  // namespace scql::engine