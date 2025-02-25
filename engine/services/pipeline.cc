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

#include <algorithm>
#include <memory>

#include "yacl/base/exception.h"

#include "engine/core/tensor_batch_reader.h"
#include "engine/core/tensor_constructor.h"
#include "engine/core/tensor_slice.h"
#include "engine/core/type.h"
#include "engine/util/filepath_helper.h"

namespace scql::engine {

size_t GetMaxSliceNum(const std::shared_ptr<yacl::link::Context>& link,
                      size_t self_slice_num) {
  const auto* tag = "get_peer_slice_num";
  auto num_bufs = yacl::link::AllGather(
      link, yacl::ByteContainerView(&self_slice_num, sizeof(size_t)), tag);
  size_t max_slice_num = 0;
  for (const auto& o : num_bufs) {
    max_slice_num = std::max(o.data<size_t>()[0], max_slice_num);
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
    const auto& pb_tensor = pipeline.inputs()[i];
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
  for (const auto& pb_tensor : pipeline.outputs()) {
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
    output_writers_[i]->WriteBatch(*tensor->ToArrowChunkedArray());
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