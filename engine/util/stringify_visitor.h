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

#include "arrow/visit_array_inline.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor.h"
#include "engine/core/tensor_chunk.h"

namespace scql::engine::util {

/// @brief StringifyVisitor converts tensor to string representation
class StringifyVisitor {
 public:
  explicit StringifyVisitor(const Tensor& tensor);

  /// @brief Stringify next batch_size elements
  /// @returns empty vector if reach end.
  std::vector<std::string> StringifyBatch(size_t batch_size);

  template <typename T>
  arrow::Status Visit(const T& array) {
    return arrow::Status::NotImplemented(fmt::format(
        "Stringify for type {} is not implemented", array.type()->name()));
  }

  template <typename TYPE>
  arrow::Status Visit(const arrow::NumericArray<TYPE>& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      strs_.push_back(std::to_string(array.GetView(i)));
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanArray& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      strs_.push_back(array.GetView(i) ? "true" : "false");
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringArray& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      strs_.push_back(array.GetString(i));
    }
    return arrow::Status::OK();
  }

 private:
  void Stringify(const arrow::Array& array) {
    THROW_IF_ARROW_NOT_OK(arrow::VisitArrayInline(array, this));
  }

  void FetchNextChunk();

  TensorChunkReader reader_;

  // current chunk_
  std::shared_ptr<TensorChunk> chunk_;

  // offset in current chunk
  int64_t offset_ = 0;

  // intermediate string representation for elements
  std::vector<std::string> strs_;
};

}  // namespace scql::engine::util