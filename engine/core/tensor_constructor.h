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

#include "engine/core/tensor.h"
#include "engine/util/disk/arrow_writer.h"

namespace scql::engine {

/// @brief construct tensor from json
/// It would be helpful in unittests.
/// TODO(shunde.csd): use wrapped data type instead of arrow::DataType directly
TensorPtr TensorFrom(const std::shared_ptr<arrow::DataType>& dtype,
                     const std::string& json);

TensorPtr TensorFrom(std::shared_ptr<arrow::ChunkedArray> arrays);

class TensorWriter {
 public:
  TensorWriter(
      std::shared_ptr<arrow::Schema> schema,
      const std::filesystem::path parent_path,
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
      const std::filesystem::path parent_path,
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
  std::vector<FileArray> file_arrays_;
  std::shared_ptr<util::disk::ArrowWriter> current_writer_;
  size_t file_index_ = 0;

  // max row num in a single file, if more than this, will create a new file
  // to write.
  const int64_t max_single_file_row_num_;
};

}  // namespace scql::engine