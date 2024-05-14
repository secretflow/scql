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

#include <cstddef>
#include <memory>

#include "engine/core/primitive_builder.h"
#include "engine/core/tensor.h"

namespace scql::engine {

/// @brief construct tensor from json
/// It would be helpful in unittests.
/// TODO(shunde.csd): use wrapped data type instead of arrow::DataType directly
TensorPtr TensorFromJSON(const std::shared_ptr<arrow::DataType>& dtype,
                         const std::string& json);

template <typename T>
TensorPtr FullNumericTensor(size_t count, typename T::c_type value) {
  NumericTensorBuilder<T> builder;
  builder.Reserve(count);
  for (size_t i = 0; i < count; ++i) {
    builder.UnsafeAppend(value);
  }

  TensorPtr result_tensor;
  builder.Finish(&result_tensor);
  return result_tensor;
}

}  // namespace scql::engine