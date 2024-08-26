// Copyright 2024 Ant Group Co., Ltd.
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

#include "engine/core/tensor_batch_reader.h"

namespace scql::engine {

std::shared_ptr<arrow::ChunkedArray> MemTensorBatchReader::Next() {
  if (offset_ >= arrays_->length()) {
    return nullptr;
  }
  auto array = arrays_->Slice(offset_, batch_size_);
  offset_ += array->length();
  return array;
}

std::shared_ptr<arrow::ChunkedArray> DiskTensorBatchReader::Next() {
  if (offset_ >= tensor_->Length() || tensor_->GetFileNum() == 0) {
    return nullptr;
  }
  if (cur_reader_ == nullptr) {
    if (!FreshReader()) {
      return nullptr;
    }
  }
  size_t num_to_read = batch_size_;
  std::vector<std::shared_ptr<arrow::Array>> result_arrays;
  while (num_to_read > 0) {
    auto chunked_array = cur_reader_->ReadNext(num_to_read);
    if (chunked_array == nullptr) {
      if (!FreshReader()) {
        break;
      }
      chunked_array = cur_reader_->ReadNext(num_to_read);
    }
    num_to_read -= chunked_array->length();
    offset_ += chunked_array->length();
    result_arrays.insert(result_arrays.end(), chunked_array->chunks().begin(),
                         chunked_array->chunks().end());
  }
  if (result_arrays.empty()) {
    return nullptr;
  }
  return std::make_shared<arrow::ChunkedArray>(result_arrays);
}

}  // namespace scql::engine