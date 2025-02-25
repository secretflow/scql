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

#include "engine/core/tensor_slice.h"

namespace scql::engine {

TensorPtr DiskTensorSlice::Next() {
  // specially for tensor created by bucket op
  if (tensor_->IsBucketTensor()) {
    if (cur_slice_idx_ >= tensor_->GetFileNum()) {
      return TensorFrom(
          arrow::ChunkedArray::MakeEmpty(tensor_->ArrowType()).ValueOrDie());
    }
    auto cur_path = {tensor_->GetFileArray(cur_slice_idx_)};
    auto result_tensor = std::make_shared<DiskTensor>(cur_path, tensor_->Type(),
                                                      tensor_->ArrowType());
    cur_slice_idx_++;
    offset_ += result_tensor->Length();
    return result_tensor;
  }

  if (offset_ >= tensor_->Length() || tensor_->GetFileNum() == 0) {
    return TensorFrom(
        arrow::ChunkedArray::MakeEmpty(tensor_->ArrowType()).ValueOrDie());
  }

  auto arrays = reader_->Next();
  if (arrays == nullptr) {
    return TensorFrom(
        arrow::ChunkedArray::MakeEmpty(tensor_->ArrowType()).ValueOrDie());
  }
  offset_ += arrays->length();
  return TensorFrom(arrays);
}

std::shared_ptr<TensorSlice> CreateTensorSlice(std::shared_ptr<Tensor>& tensor,
                                               size_t slice_size) {
  if (typeid(*tensor) == typeid(MemTensor)) {
    std::shared_ptr<MemTensor> mem_tensor =
        std::dynamic_pointer_cast<MemTensor>(tensor);
    return std::make_shared<MemTensorSlice>(mem_tensor);
  } else if (typeid(*tensor) == typeid(DiskTensor)) {
    std::shared_ptr<DiskTensor> disk_tensor =
        std::dynamic_pointer_cast<DiskTensor>(tensor);
    return std::make_shared<DiskTensorSlice>(disk_tensor);
  }
  YACL_THROW("unsupported tensor type");
}
}  // namespace scql::engine