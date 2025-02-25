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

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "engine/core/tensor.h"
#include "engine/core/tensor_batch_reader.h"
#include "engine/core/tensor_constructor.h"

namespace scql::engine {

class TensorSlice {
 public:
  explicit TensorSlice(size_t slice_size) {}
  /// @returns tensor ptr, return nullptr if no more slices
  virtual TensorPtr Next() = 0;
  virtual size_t GetSliceNum() = 0;
  virtual TensorPtr GetSlice(size_t i) = 0;
};

class MemTensorSlice : public TensorSlice {
 public:
  explicit MemTensorSlice(
      std::shared_ptr<MemTensor> tensor,
      size_t slice_size = std::numeric_limits<int64_t>::max())
      : TensorSlice(slice_size),
        tensor_(std::move(tensor)),
        slice_size_(slice_size) {
    reader_ = std::make_shared<MemTensorBatchReader>(tensor_, slice_size);
  }
  MemTensorSlice(MemTensorSlice&) = delete;

  TensorPtr Next() override {
    auto arrays = reader_->Next();
    if (arrays == nullptr) {
      arrays =
          arrow::ChunkedArray::MakeEmpty(tensor_->ArrowType()).ValueOrDie();
    }
    return TensorFrom(arrays);
  }

  size_t GetSliceNum() override { return 1; }
  TensorPtr GetSlice(size_t i) override {
    if (i == 0 && static_cast<size_t>(tensor_->Length()) < slice_size_) {
      return tensor_;
    }
    YACL_THROW("unsupport get specific slice");
  }

 private:
  std::shared_ptr<MemTensorBatchReader> reader_;
  std::shared_ptr<MemTensor> tensor_;
  size_t slice_size_;
};

class DiskTensorSlice : public TensorSlice {
 public:
  explicit DiskTensorSlice(
      std::shared_ptr<DiskTensor> tensor,
      size_t slice_size = std::numeric_limits<int64_t>::max())
      : TensorSlice(slice_size),
        tensor_(std::move(tensor)),
        slice_size_(slice_size) {
    if (!tensor_->IsBucketTensor()) {
      reader_ = std::make_shared<DiskTensorBatchReader>(tensor_, slice_size);
    }
  }
  DiskTensorSlice(DiskTensorSlice&) = delete;

  TensorPtr Next() override;

  size_t GetSliceNum() override {
    if (tensor_->IsBucketTensor()) {
      return tensor_->GetFileNum();
    }
    return 1;
  }

  TensorPtr GetSlice(size_t i) override {
    // specially for tensor created by bucket op
    if (tensor_->IsBucketTensor()) {
      if (i >= tensor_->GetFileNum()) {
        return TensorFrom(
            arrow::ChunkedArray::MakeEmpty(tensor_->ArrowType()).ValueOrDie());
      }
      auto cur_path = {tensor_->GetFileArray(i)};
      auto result_tensor = std::make_shared<DiskTensor>(
          cur_path, tensor_->Type(), tensor_->ArrowType());
      return result_tensor;
    }
    if (i == 0 && static_cast<size_t>(tensor_->Length()) < slice_size_) {
      return tensor_;
    }
    YACL_THROW("unsupport get specific slice");
  }

 private:
  std::shared_ptr<DiskTensor> tensor_;
  std::shared_ptr<DiskTensorBatchReader> reader_;
  // file index
  size_t cur_slice_idx_ = 0;
  int64_t offset_ = 0;
  size_t slice_size_;
};

std::shared_ptr<TensorSlice> CreateTensorSlice(
    std::shared_ptr<Tensor>& tensor,
    size_t slice_size = std::numeric_limits<int64_t>::max());

}  // namespace scql::engine