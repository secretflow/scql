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

#include <filesystem>
#include <memory>

#include "arrow/chunked_array.h"
#include "spdlog/spdlog.h"
#include "yacl/base/exception.h"

#include "api/core.pb.h"

namespace scql::engine {

class TensorBatchReader;
class DiskTensorBatchReader;
class MemTensorBatchReader;
class DiskTensorSlice;
class MemTensorSlice;

/// @brief A Tensor reprensents a column of a relation
class Tensor {
 public:
  Tensor(const Tensor&) = delete;
  Tensor& operator=(const Tensor&) = delete;
  explicit Tensor(pb::PrimitiveDataType dtype) : dtype_(dtype) {}

  virtual int64_t Length() const = 0;

  // return the total number of nulls
  virtual int64_t GetNullCount() const = 0;

  /// @returns the data type of tensor element
  pb::PrimitiveDataType Type() const { return dtype_; }

  virtual std::shared_ptr<arrow::DataType> ArrowType() const = 0;

  /// @returns as arrow chunked array
  virtual std::shared_ptr<arrow::ChunkedArray> ToArrowChunkedArray() const = 0;

  /// @return as tensor batch reader
  virtual std::shared_ptr<TensorBatchReader> CreateBatchReader(
      size_t batch_size) = 0;

 protected:
  pb::PrimitiveDataType dtype_;
};

class MemTensor : public Tensor,
                  public std::enable_shared_from_this<MemTensor> {
 public:
  explicit MemTensor(std::shared_ptr<arrow::ChunkedArray> chunked_arr);

  int64_t Length() const override { return chunked_arr_->length(); }

  // return the total number of nulls
  int64_t GetNullCount() const override { return chunked_arr_->null_count(); }

  /// @returns as arrow chunked array
  std::shared_ptr<arrow::ChunkedArray> ToArrowChunkedArray() const override {
    return chunked_arr_;
  }

  std::shared_ptr<arrow::DataType> ArrowType() const override {
    return chunked_arr_->type();
  }

  std::shared_ptr<TensorBatchReader> CreateBatchReader(
      size_t batch_size) override;

 private:
  std::shared_ptr<arrow::ChunkedArray> chunked_arr_;
};

struct FileArray {
  std::filesystem::path file_path;
  size_t len;
  size_t null_count;
};

class DiskTensor : public Tensor,
                   public std::enable_shared_from_this<DiskTensor> {
 public:
  explicit DiskTensor(std::vector<FileArray> file_arrays,
                      scql::pb::PrimitiveDataType dtype,
                      std::shared_ptr<arrow::DataType> arrow_type);

  ~DiskTensor() {
    for (auto& file_array : file_arrays_) {
      std::error_code ec;
      std::filesystem::remove(file_array.file_path, ec);
      if (ec.value() != 0) {
        SPDLOG_WARN("can not remove tmp dir: {}, msg: {}",
                    file_array.file_path.string(), ec.message());
      }
    }
  }

  int64_t Length() const override { return len_; }

  // return the total number of nulls
  int64_t GetNullCount() const override { return null_count_; }

  /// @returns as arrow chunked array
  std::shared_ptr<arrow::ChunkedArray> ToArrowChunkedArray() const override;

  std::shared_ptr<arrow::DataType> ArrowType() const override {
    return arrow_type_;
  }

  std::shared_ptr<TensorBatchReader> CreateBatchReader(
      size_t batch_size) override;

  void SetAsBucketTensor() { is_bucket_tensor_ = true; }

  bool IsBucketTensor() const { return is_bucket_tensor_; }

 private:
  FileArray GetFileArray(size_t i) const { return file_arrays_[i]; }
  size_t GetFileNum() const { return file_arrays_.size(); }

 private:
  friend class DiskTensorBatchReader;
  friend class DiskTensorSlice;
  size_t len_ = 0;
  size_t null_count_ = 0;
  bool is_bucket_tensor_ = false;
  std::vector<FileArray> file_arrays_;
  std::shared_ptr<arrow::DataType> arrow_type_;
};

// TODO(xiaoyuan): move to tensor builder
struct TensorBuildOptions {
  bool dump_to_disk;
  std::filesystem::path dump_dir;
  size_t max_row_num_one_file = std::numeric_limits<int64_t>::max();
};

using TensorPtr = std::shared_ptr<Tensor>;
using RepeatedPbTensor = google::protobuf::RepeatedPtrField<pb::Tensor>;

}  // namespace scql::engine