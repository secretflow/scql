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

#include "engine/core/tensor_chunk.h"

namespace scql::engine {

TensorChunk::TensorChunk(std::shared_ptr<arrow::Array> array)
    : arr_(std::move(array)) {}

TensorChunk::TensorChunk(TensorChunk&& other) noexcept
    : arr_(std::move(other.arr_)) {}

TensorChunk& TensorChunk::operator=(TensorChunk&& other) noexcept {
  arr_ = std::move(other.arr_);
  return *this;
}

TensorChunkReader::TensorChunkReader(const Tensor& tensor) : tensor_(tensor) {}

std::shared_ptr<TensorChunk> TensorChunkReader::ReadNext() {
  if (chunk_idx_ >= static_cast<size_t>(tensor_.chunked_arr_->num_chunks())) {
    return nullptr;
  }
  return std::make_shared<TensorChunk>(
      tensor_.chunked_arr_->chunk(chunk_idx_++));
}

}  // namespace scql::engine