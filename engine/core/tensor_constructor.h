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

#include <cstddef>
#include <memory>
#include <vector>

#include "yacl/base/exception.h"

#include "engine/core/tensor.h"
#include "engine/util/disk/arrow_writer.h"

namespace scql::engine {

/// @brief construct tensor from json
/// It would be helpful in unittests.
/// TODO(shunde.csd): use wrapped data type instead of arrow::DataType directly
TensorPtr TensorFrom(const std::shared_ptr<arrow::DataType>& dtype,
                     const std::string& json);

TensorPtr TensorFrom(std::shared_ptr<arrow::ChunkedArray> arrays);

TensorPtr ConcatTensors(const std::vector<TensorPtr>& tensors);

class TensorWriter {
 public:
  TensorWriter(
      std::shared_ptr<arrow::Schema> schema, std::filesystem::path parent_path,
      int64_t max_single_file_row_num = std::numeric_limits<int64_t>::max())
      : schema_(std::move(schema)),
        parent_path_(std::move(parent_path)),
        max_single_file_row_num_(max_single_file_row_num) {
    YACL_ENFORCE(std::filesystem::is_empty(parent_path_),
                 "directory for tensor writer must be empty: {}",
                 parent_path_.string());
  }

  TensorWriter(
      const std::string& field_name,
      const std::shared_ptr<arrow::DataType>& data_type,
      std::filesystem::path parent_path,
      int64_t max_single_file_row_num = std::numeric_limits<int64_t>::max())
      : parent_path_(std::move(parent_path)),
        max_single_file_row_num_(max_single_file_row_num) {
    YACL_ENFORCE(std::filesystem::is_empty(parent_path_),
                 "directory for tensor writer must be empty: {}",
                 parent_path_.string());
    auto field = std::make_shared<arrow::Field>(field_name, data_type);
    arrow::FieldVector fields = {field};
    schema_ = std::make_shared<arrow::Schema>(fields);
  }

  virtual ~TensorWriter() = default;

  size_t WriteBatch(const arrow::RecordBatch& batch);
  size_t WriteBatch(const arrow::ChunkedArray& batch);
  // Return result of builder as a Tensor object.
  void Finish(std::shared_ptr<Tensor>* out);

  std::shared_ptr<arrow::Schema> GetSchema() const { return schema_; }

  size_t GetFilesNum() const { return file_arrays_.size(); }

 private:
  void FreshCurWriter();

  std::shared_ptr<arrow::Schema> schema_;
  const std::filesystem::path parent_path_;
  std::vector<std::shared_ptr<FileArray>> file_arrays_;
  std::shared_ptr<util::disk::ArrowWriter> current_writer_;
  size_t file_index_ = 0;

  // max row num in a single file, if more than this, will create a new file
  // to write.
  const int64_t max_single_file_row_num_;
};

class BucketTensorConstructor {
 public:
  virtual void InsertBucket(const TensorPtr& tensor, size_t bucket_index) = 0;
  virtual void InsertBucket(const std::shared_ptr<arrow::ChunkedArray>& arrays,
                            size_t bucket_index) = 0;
  virtual void Finish(TensorPtr* tensor) = 0;
};

class DiskBucketTensorConstructor : public BucketTensorConstructor {
 public:
  DiskBucketTensorConstructor(const std::string& field_name,
                              const std::shared_ptr<arrow::DataType>& data_type,
                              scql::pb::PrimitiveDataType dtype,
                              const std::string& file_path, size_t bucket_num)
      : writers_(bucket_num), data_type_(data_type), dtype_(dtype) {
    for (size_t j = 0; j < bucket_num; j++) {
      writers_[j] = std::make_shared<util::disk::ArrowWriter>(
          field_name, data_type,
          std::filesystem::path(file_path) / std::to_string(j));
    }
  }
  void InsertBucket(const TensorPtr& tensor, size_t bucket_index) override;
  void InsertBucket(const std::shared_ptr<arrow::ChunkedArray>& arrays,
                    size_t bucket_index) override;
  void Finish(TensorPtr* tensor) override;

 private:
  std::vector<std::shared_ptr<util::disk::ArrowWriter>> writers_;
  std::shared_ptr<arrow::DataType> data_type_;
  scql::pb::PrimitiveDataType dtype_;
};

class MemoryBucketTensorConstructor : public BucketTensorConstructor {
 public:
  explicit MemoryBucketTensorConstructor(size_t bucket_num)
      : array_vecs_(bucket_num) {}
  void InsertBucket(const TensorPtr& tensor, size_t bucket_index) override;
  void InsertBucket(const std::shared_ptr<arrow::ChunkedArray>& arrays,
                    size_t bucket_index) override;
  void Finish(TensorPtr* tensor) override;

 private:
  std::vector<arrow::ChunkedArrayVector> array_vecs_;
};

}  // namespace scql::engine