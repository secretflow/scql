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

#include "arrow/ipc/writer.h"
#include "arrow/type.h"

namespace scql::engine::util::disk {

class ArrowWriter final {
 public:
  explicit ArrowWriter(std::shared_ptr<arrow::Schema> schema,
                       const std::string& file_path)
      : schema_(std::move(schema)), file_path_(file_path) {
    Init();
  }

  explicit ArrowWriter(const std::string& field_name,
                       const std::shared_ptr<arrow::DataType>& data_type,
                       const std::string& file_path)
      : file_path_(file_path) {
    auto field = std::make_shared<arrow::Field>(field_name, data_type);
    arrow::FieldVector fields = {field};
    schema_ = std::make_shared<arrow::Schema>(fields);
    Init();
  }

  ~ArrowWriter() {
    if (ipc_writer_) {
      static_cast<void>(ipc_writer_->Close());
    }
  };

  size_t WriteBatch(const arrow::RecordBatch& batch);
  size_t WriteBatch(const arrow::ChunkedArray& batch);

  size_t GetRowNum() { return num_rows_; }
  size_t GetNullCount() { return null_count_; }
  std::shared_ptr<arrow::Schema> GetSchema() { return schema_; }
  std::string GetFilePath() { return file_path_; }

 private:
  // init before writing
  void Init();
  std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer_;
  std::shared_ptr<arrow::Schema> schema_;
  std::string file_path_;
  size_t num_rows_ = 0;
  size_t null_count_ = 0;
};

}  // namespace scql::engine::util::disk