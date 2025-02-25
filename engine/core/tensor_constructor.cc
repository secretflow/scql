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

#include "engine/core/tensor_constructor.h"

#include <memory>

#include "arrow/array/array_base.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/record_batch.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor.h"
#include "engine/core/type.h"

namespace scql::engine {

TensorPtr TensorFrom(const std::shared_ptr<arrow::DataType>& dtype,
                     const std::string& json) {
  using arrow::ipc::internal::json::ChunkedArrayFromJSON;

  std::shared_ptr<arrow::ChunkedArray> chunked_arr;
  THROW_IF_ARROW_NOT_OK(ChunkedArrayFromJSON(
      dtype, std::vector<std::string>{json}, &chunked_arr));

  return std::make_shared<MemTensor>(std::move(chunked_arr));
}

TensorPtr TensorFrom(std::shared_ptr<arrow::ChunkedArray> arrays) {
  return std::make_shared<MemTensor>(arrays);
}

TensorPtr ConcatTensors(const std::vector<TensorPtr>& tensors) {
  for (const auto& tensor : tensors) {
    YACL_ENFORCE(typeid(*tensor) == typeid(MemTensor));
  }
  arrow::ArrayVector arrays;
  for (const auto& tensor : tensors) {
    auto tmp_v = tensor->ToArrowChunkedArray();
    arrays.insert(arrays.end(), tmp_v->chunks().begin(), tmp_v->chunks().end());
  }
  return std::make_shared<MemTensor>(
      arrow::ChunkedArray::Make(arrays).ValueOrDie());
}

// create a new writer to write when current writer is null or current writer is
// full
void TensorWriter::FreshCurWriter() {
  if (current_writer_ != nullptr) {
    auto file_array = std::make_shared<FileArray>(
        current_writer_->GetFilePath(), current_writer_->GetRowNum(),
        current_writer_->GetNullCount());
    file_arrays_.push_back(file_array);
  }
  std::filesystem::path path = parent_path_ / std::to_string(file_index_);
  current_writer_ = std::make_shared<util::disk::ArrowWriter>(schema_, path);
  file_index_++;
}

size_t TensorWriter::WriteBatch(const arrow::RecordBatch& batch) {
  if (batch.num_rows() == 0) {
    return 0;
  }
  if (current_writer_ == nullptr) {
    FreshCurWriter();
  }
  auto num_to_write = batch.num_rows();
  if (num_to_write <= max_single_file_row_num_ -
                          static_cast<int64_t>(current_writer_->GetRowNum())) {
    current_writer_->WriteBatch(batch);
    return batch.num_rows();
  }
  while (num_to_write > 0) {
    if (static_cast<int64_t>(current_writer_->GetRowNum()) >=
        max_single_file_row_num_) {
      FreshCurWriter();
    }
    auto write_row_num = max_single_file_row_num_ -
                         static_cast<int64_t>(current_writer_->GetRowNum());
    auto sliced_batch =
        batch.Slice(batch.num_rows() - num_to_write, write_row_num);
    num_to_write -= write_row_num;
    current_writer_->WriteBatch(*sliced_batch);
  }
  return batch.num_rows();
}

size_t TensorWriter::WriteBatch(const arrow::ChunkedArray& batch) {
  size_t offset = 0;
  for (int i = 0; i < batch.num_chunks(); ++i) {
    std::vector<std::shared_ptr<arrow::Array>> arrays = {batch.chunk(i)};
    offset += WriteBatch(
        *arrow::RecordBatch::Make(schema_, batch.chunk(i)->length(), arrays));
  }
  return offset;
}

void TensorWriter::Finish(std::shared_ptr<Tensor>* out) {
  // add last file to tensor
  if (current_writer_ != nullptr && current_writer_->GetRowNum() > 0) {
    auto file_array = std::make_shared<FileArray>(
        current_writer_->GetFilePath(), current_writer_->GetRowNum(),
        current_writer_->GetNullCount());
    file_arrays_.push_back(file_array);
  }

  current_writer_.reset();
  *out = std::make_shared<DiskTensor>(
      file_arrays_, FromArrowDataType(schema_->field(0)->type()),
      schema_->field(0)->type());
}

void DiskBucketTensorConstructor::InsertBucket(const TensorPtr& tensor,
                                               size_t bucket_index) {
  InsertBucket(tensor->ToArrowChunkedArray(), bucket_index);
}

void DiskBucketTensorConstructor::InsertBucket(
    const std::shared_ptr<arrow::ChunkedArray>& arrays, size_t bucket_index) {
  writers_[bucket_index]->WriteBatch(*arrays);
}

void DiskBucketTensorConstructor::Finish(TensorPtr* tensor) {
  std::vector<std::shared_ptr<FileArray>> file_arrays(writers_.size());
  for (size_t j = 0; j < file_arrays.size(); j++) {
    auto file_array = std::make_shared<FileArray>(writers_[j]->GetFilePath(),
                                                  writers_[j]->GetRowNum(),
                                                  writers_[j]->GetNullCount());
    file_arrays[j] = file_array;
  }
  auto type = writers_[0]->GetSchema()->field(0)->type();
  // remove writers to call ipc writer close()
  writers_.clear();
  auto disk_tensor =
      std::make_shared<DiskTensor>(file_arrays, dtype_, data_type_);
  disk_tensor->SetAsBucketTensor();
  *tensor = std::move(disk_tensor);
}

void MemoryBucketTensorConstructor::InsertBucket(const TensorPtr& tensor,
                                                 size_t bucket_index) {
  InsertBucket(tensor->ToArrowChunkedArray(), bucket_index);
}
void MemoryBucketTensorConstructor::InsertBucket(
    const std::shared_ptr<arrow::ChunkedArray>& arrays, size_t bucket_index) {
  array_vecs_[bucket_index].push_back(arrays);
}
void MemoryBucketTensorConstructor::Finish(TensorPtr* tensor) {
  arrow::ArrayVector arrays;
  for (const auto& arrs : array_vecs_) {
    for (const auto& tmp_v : arrs) {
      arrays.insert(arrays.end(), tmp_v->chunks().begin(),
                    tmp_v->chunks().end());
    }
  }
  // TODO: no bucket mem tensor for now, add it later if needed
  *tensor = std::make_shared<MemTensor>(
      arrow::ChunkedArray::Make(arrays).ValueOrDie());
}
}  // namespace scql::engine
