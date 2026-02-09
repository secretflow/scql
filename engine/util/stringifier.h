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
#include "engine/core/tensor_batch_reader.h"

namespace scql::engine::util {

/// @brief Stringifier converts tensor to string representation
class Stringifier {
 public:
  explicit Stringifier(const TensorPtr& tensor, size_t batch_size);

  /// @brief Stringify next batch_size elements
  /// @returns empty vector if reach end.
  std::vector<std::string> StringifyBatch();

 private:
  std::shared_ptr<TensorBatchReader> reader_;
};

std::vector<std::string> Stringify(const std::shared_ptr<arrow::Array>& array);

std::vector<std::string> Stringify(
    const std::shared_ptr<arrow::ChunkedArray>& arrays);

}  // namespace scql::engine::util
