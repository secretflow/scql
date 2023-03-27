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

#include "arrow/array.h"

#include "engine/core/tensor.h"

namespace scql::engine {

/// @brief A Tensor Chunk represents a slice of a Tensor
/// The tensor chunk class is the intermediate representation used by the SCQL
/// execution engine.
class TensorChunk {
 public:
  TensorChunk();
  explicit TensorChunk(std::shared_ptr<arrow::Array> array);

  TensorChunk(TensorChunk&&);
  TensorChunk& operator=(TensorChunk&& other);

  TensorChunk(const TensorChunk&) = delete;
  TensorChunk& operator=(const TensorChunk&) = delete;

  int64_t Length() const { return arr_->length(); }

  int64_t GetNullCount() const { return arr_->null_count(); }

  /// @returns underlying arrow array
  std::shared_ptr<arrow::Array> ToArrowArray() const { return arr_; }

 private:
  std::shared_ptr<arrow::Array> arr_;
};

/// @brief TensorChunkReader reads stream of tensor chunks from Tensor
class TensorChunkReader {
 public:
  explicit TensorChunkReader(const Tensor& tensor);

  /// @returns nullptr if reach end
  std::shared_ptr<TensorChunk> ReadNext();

 private:
  const Tensor& tensor_;

  size_t chunk_idx_ = 0;
};

}  // namespace scql::engine