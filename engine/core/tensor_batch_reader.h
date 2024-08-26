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

#include "arrow/array.h"
#include "arrow/table.h"
#include "yacl/base/exception.h"

#include "engine/core/tensor.h"
#include "engine/util/disk/arrow_reader.h"

namespace scql::engine {

class TensorBatchReader {
 public:
  TensorBatchReader(size_t batch_size) : batch_size_(batch_size), offset_(0) {}
  /// @returns tensor ptr, return nullptr if no more batches
  virtual std::shared_ptr<arrow::ChunkedArray> Next() = 0;

 protected:
  size_t batch_size_;
  int64_t offset_;
};

class MemTensorBatchReader : public TensorBatchReader {
 public:
  MemTensorBatchReader(std::shared_ptr<MemTensor> tensor,
                       size_t batch_size = std::numeric_limits<size_t>::max())
      : TensorBatchReader(batch_size),
        arrays_(std::move(tensor->ToArrowChunkedArray())) {}

  MemTensorBatchReader(const MemTensorBatchReader&) = delete;

  MemTensorBatchReader& operator=(const MemTensorBatchReader&) = delete;

  std::shared_ptr<arrow::ChunkedArray> Next() override;

 private:
  std::shared_ptr<arrow::ChunkedArray> arrays_;
};

class DiskTensorBatchReader : public TensorBatchReader {
 public:
  DiskTensorBatchReader(std::shared_ptr<DiskTensor> tensor,
                        size_t batch_size = std::numeric_limits<size_t>::max())
      : TensorBatchReader(batch_size), tensor_(std::move(tensor)) {}

  DiskTensorBatchReader(const DiskTensorBatchReader&) = delete;

  DiskTensorBatchReader& operator=(const DiskTensorBatchReader&) = delete;

  std::shared_ptr<arrow::ChunkedArray> Next() override;

 private:
  bool FreshReader() {
    // skip empty files;
    while (cur_file_idx_ < tensor_->GetFileNum() &&
           tensor_->GetFileArray(cur_file_idx_).len == 0) {
      cur_file_idx_++;
    }
    if (cur_file_idx_ >= tensor_->GetFileNum()) {
      return false;
    }
    cur_reader_ = std::make_shared<util::disk::FileBatchReader>(
        tensor_->GetFileArray(cur_file_idx_).file_path);
    cur_file_idx_++;
    return true;
  }

 private:
  std::shared_ptr<DiskTensor> tensor_;
  // file index
  size_t cur_file_idx_ = 0;
  std::shared_ptr<util::disk::FileBatchReader> cur_reader_;
};

}  // namespace scql::engine