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

#include "engine/util/stringify_visitor.h"

#include "fmt/format.h"

#include "engine/core/tensor_constructor.h"

namespace scql::engine::util {

StringifyVisitor::StringifyVisitor(TensorPtr tensor, size_t batch_size) {
  reader_ = tensor->CreateBatchReader(batch_size);
}

std::vector<std::string> StringifyVisitor::StringifyBatch() {
  auto array = reader_->Next();
  if (array == nullptr) {
    return {};
  }
  strs_.clear();
  strs_.reserve(array->length());
  for (int i = 0; i < array->num_chunks(); i++) {
    Stringify(*array->chunk(i));
  }

  return std::move(strs_);
}

std::vector<std::string> Stringify(
    std::shared_ptr<arrow::ChunkedArray> arrays) {
  StringifyVisitor visitor(TensorFrom(arrays), arrays->length());
  return visitor.StringifyBatch();
}

}  // namespace scql::engine::util