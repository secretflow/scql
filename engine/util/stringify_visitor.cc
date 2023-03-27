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

namespace scql::engine::util {

StringifyVisitor::StringifyVisitor(const Tensor& tensor) : reader_(tensor) {
  FetchNextChunk();
}

void StringifyVisitor::FetchNextChunk() {
  chunk_ = reader_.ReadNext();
  offset_ = 0;
}

std::vector<std::string> StringifyVisitor::StringifyBatch(size_t batch_size) {
  if (chunk_ == nullptr) {
    return std::vector<std::string>{};
  }

  strs_.clear();
  strs_.reserve(batch_size);

  size_t len = batch_size;

  do {
    auto array = chunk_->ToArrowArray();
    auto slice = array->Slice(offset_, len);

    Stringify(*slice);

    offset_ += slice->length();
    len -= slice->length();

    if (offset_ >= array->length()) {
      FetchNextChunk();
    }
  } while (chunk_ != nullptr && len > 0);

  return std::move(strs_);
}

}  // namespace scql::engine::util