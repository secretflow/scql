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

#include "engine/services/pipeline.h"

#include <filesystem>
#include <memory>

#include "arrow/array.h"
#include "arrow/type.h"
#include "yacl/base/exception.h"

#include "engine/core/tensor_constructor.h"
#include "engine/core/type.h"
#include "engine/util/filepath_helper.h"

namespace scql::engine {

class TensorSlice {
 public:
  TensorSlice(size_t slice_size) {}
  /// @returns tensor ptr, return nullptr if no more slices
  virtual TensorPtr Next() = 0;
  virtual size_t GetSliceNum() = 0;
};

class MemTensorSlice : public TensorSlice {
 public:
  MemTensorSlice(std::shared_ptr<MemTensor> tensor,
                 size_t slice_size = std::numeric_limits<size_t>::max())
      : TensorSlice(slice_size) {
    reader_ = std::make_shared<MemTensorBatchReader>(tensor, slice_size);
  }
  MemTensorSlice(MemTensorSlice&) = delete;

  TensorPtr Next() override {
    auto arrays = reader_->Next();
    if (arrays == nullptr) {
      return nullptr;
    }
    return TensorFrom(arrays);
  }

  size_t GetSliceNum() override { return 1; }

 private:
  std::shared_ptr<MemTensorBatchReader> reader_;
};

class DiskTensorSlice : public TensorSlice {
 public:
  DiskTensorSlice(std::shared_ptr<DiskTensor> tensor,
                  size_t slice_size = std::numeric_limits<size_t>::max())
      : TensorSlice(slice_size), tensor_(std::move(tensor)) {
    if (!tensor_->IsBucketTensor()) {
      reader_ = std::make_shared<DiskTensorBatchReader>(tensor_, slice_size);
    }
  }
  DiskTensorSlice(DiskTensorSlice&) = delete;

  TensorPtr Next() override {
    // specially for tensor created by bucket op
    if (tensor_->IsBucketTensor()) {
      if (cur_slice_idx_ >= tensor_->GetFileNum()) {
        return nullptr;
      }
      std::vector<FileArray> cur_path = {tensor_->GetFileArray(cur_slice_idx_)};
      auto result_tensor = std::make_shared<DiskTensor>(
          cur_path, tensor_->Type(), tensor_->ArrowType());
      cur_slice_idx_++;
      offset_ += result_tensor->Length();
      return result_tensor;
    }

    if (offset_ >= tensor_->Length() || tensor_->GetFileNum() == 0) {
      return nullptr;
    }

    auto arrays = reader_->Next();
    if (arrays == nullptr) {
      return nullptr;
    }
    offset_ += arrays->length();
    return TensorFrom(arrays);
  }

  size_t GetSliceNum() override {
    if (tensor_->IsBucketTensor()) {
      return tensor_->GetFileNum();
    }
    return 1;
  }

 private:
  std::shared_ptr<DiskTensor> tensor_;
  std::shared_ptr<DiskTensorBatchReader> reader_;
  // file index
  size_t cur_slice_idx_ = 0;
  int64_t offset_ = 0;
};

std::shared_ptr<TensorSlice> CreateTensorSlice(
    std::shared_ptr<Tensor> tensor,
    size_t slice_size = std::numeric_limits<size_t>::max()) {
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

size_t GetMaxSliceNum(const std::shared_ptr<yacl::link::Context>& link,
                      size_t self_slice_num) {
  auto tag = "get_peer_slice_num";
  auto num_bufs = yacl::link::AllGather(
      link, yacl::ByteContainerView(&self_slice_num, sizeof(size_t)), tag);
  size_t max_slice_num = 0;
  for (const auto& o : num_bufs) {
    if (o.data<size_t>()[0] > max_slice_num) {
      max_slice_num = o.data<size_t>()[0];
    }
  }
  return max_slice_num;
}

PipelineExecutor::PipelineExecutor(const pb::Pipeline& pipeline,
                                   Session* session)
    : session_(session),
      first_batch_(true),
      batched_(pipeline.batched()),
      batch_num_(1) {
  if (!batched_) {
    return;
  }
  for (int i = 0; i < pipeline.inputs().size(); i++) {
    auto& pb_tensor = pipeline.inputs()[i];
    auto tensor = session->GetTensorTable()->GetTensor(pb_tensor.name());
    YACL_ENFORCE(tensor != nullptr, "failed to get input tensor: {}",
                 pb_tensor.name());
    input_tensors_.push_back(tensor);
    auto slicer = CreateTensorSlice(tensor);
    if (i != 0 && slicer->GetSliceNum() != batch_num_) {
      YACL_THROW("input tensors has different batch num {}:{}", batch_num_,
                 slicer->GetSliceNum());
    }
    batch_num_ = slicer->GetSliceNum();
    if (slicer->GetSliceNum() == 1) {
      continue;
    }
    tensor_readers_.push_back(CreateTensorSlice(tensor));
    input_tensor_names_.push_back(pb_tensor.name());
  }
  batch_num_ = GetMaxSliceNum(session_->GetLink(), batch_num_);
  if (batch_num_ == 1) {
    batched_ = false;
    return;
  }
  for (auto& pb_tensor : pipeline.outputs()) {
    output_tensor_names_.push_back(pb_tensor.name());
  }
}

void PipelineExecutor::UpdateTensorTable() {
  if (!batched_) {
    return;
  }
  // insert batched tensor into tensor table
  for (size_t i = 0; i < input_tensors_.size(); i++) {
    auto batched_tensor = tensor_readers_[i]->Next();
    YACL_ENFORCE(batched_tensor != nullptr);
    session_->GetTensorTable()->AddOrUpdateTensor(input_tensor_names_[i],
                                                  batched_tensor);
  }
}

void PipelineExecutor::FetchOutputTensors() {
  if (!batched_) {
    return;
  }
  for (size_t i = 0; i < output_tensor_names_.size(); i++) {
    auto tensor =
        session_->GetTensorTable()->GetTensor(output_tensor_names_[i]);
    YACL_ENFORCE(tensor != nullptr,
                 "failed to get output tensor: {}" + output_tensor_names_[i]);
    // remove tensor to avoid error that tensor already exits
    session_->GetTensorTable()->RemoveTensor(output_tensor_names_[i]);
    if (first_batch_) {
      output_writers_.push_back(std::make_shared<TensorWriter>(
          output_tensor_names_[i], ToArrowDataType(tensor->Type()),
          util::CreateDirWithRandSuffix(
              session_->GetStreamingOptions().dump_file_dir,
              output_tensor_names_[i])));
    }
    output_writers_[i]->WriteBatch(*tensor->ToArrowChunkedArray().get());
  }
  if (first_batch_) {
    first_batch_ = false;
  }
}

void PipelineExecutor::Finish() {
  if (!batched_) {
    return;
  }
  for (size_t i = 0; i < output_writers_.size(); i++) {
    TensorPtr tensor;
    output_writers_[i]->Finish(&tensor);
    session_->GetTensorTable()->AddTensor(output_tensor_names_[i], tensor);
  }
  // put input tensors back to tensor table if need
  for (size_t i = 0; i < input_tensor_names_.size(); i++) {
    // if tensor removed by tensor table, don't put it back
    if (session_->GetTensorTable()->GetTensor(input_tensor_names_[i]) !=
        nullptr) {
      session_->GetTensorTable()->AddOrUpdateTensor(input_tensor_names_[i],
                                                    input_tensors_[i]);
    }
  }
}

}  // namespace scql::engine