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

#include "engine/util/stringifier.h"

#include <arrow/scalar.h>

#include "arrow/compute/cast.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_constructor.h"

namespace scql::engine::util {

Stringifier::Stringifier(const TensorPtr& tensor, size_t batch_size) {
  reader_ = tensor->CreateBatchReader(batch_size);
}

std::vector<std::string> Stringifier::StringifyBatch() {
  auto chunked_array = reader_->Next();
  if (chunked_array == nullptr) {
    return {};
  }
  return Stringify(chunked_array);
}

std::vector<std::string> Stringify(
    const std::shared_ptr<arrow::ChunkedArray>& arrays) {
  std::shared_ptr<arrow::ChunkedArray> chunked_array = arrays;
  if (arrays->type()->id() != arrow::Type::LARGE_STRING) {
    auto cast_result = arrow::compute::Cast(arrays, arrow::large_utf8());
    YACL_ENFORCE(cast_result.ok(), "cast to string failed: {}",
                 cast_result.status().ToString());
    chunked_array = cast_result.ValueOrDie().chunked_array();
  }
  std::vector<std::string> result;
  result.reserve(chunked_array->length());
  for (int i = 0; i < chunked_array->num_chunks(); i++) {
    auto array = chunked_array->chunk(i);
    auto string_array =
        arrow::internal::checked_pointer_cast<arrow::LargeStringArray>(array);
    for (int j = 0; j < string_array->length(); j++) {
      result.emplace_back(string_array->IsNull(j) ? "null"
                                                  : string_array->GetView(j));
    }
  }

  return result;
}

std::vector<std::string> Stringify(const std::shared_ptr<arrow::Array>& array) {
  auto arrays = arrow::ChunkedArray::Make({array}).ValueOrDie();
  return Stringify(arrays);
}

}  // namespace scql::engine::util
