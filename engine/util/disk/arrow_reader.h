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

#include "arrow/io/file.h"
#include "arrow/ipc/reader.h"

#include "engine/core/arrow_helper.h"

namespace scql::engine::util::disk {

class SimpleArrowReader final {
 public:
  explicit SimpleArrowReader(std::string file_name)
      : file_name_(std::move(file_name)) {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ASSIGN_OR_THROW_ARROW_STATUS(
        infile, arrow::io::ReadableFile::Open(file_name_,
                                              arrow::default_memory_pool()));
    ASSIGN_OR_THROW_ARROW_STATUS(
        ipc_reader_, arrow::ipc::RecordBatchFileReader::Open(infile));
    record_batch_num_ = ipc_reader_->num_record_batches();
    schema_ = ipc_reader_->schema();
    YACL_ENFORCE(schema_->num_fields() == 1,
                 "Simple file array must only keep one column");
  }
  ~SimpleArrowReader() = default;
  std::shared_ptr<arrow::RecordBatch> ReadNext();
  size_t GetBatchNum() const { return record_batch_num_; }
  std::shared_ptr<arrow::Schema> GetSchema() const { return schema_; }

 private:
  std::shared_ptr<arrow::Schema> schema_;
  std::string file_name_;
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> ipc_reader_;
  size_t batch_index_ = 0;
  size_t record_batch_num_;
};

class FileBatchReader {
 public:
  explicit FileBatchReader(std::string file_name)
      : reader_(std::move(SimpleArrowReader(std::move(file_name)))) {
    // read first batch
    cur_batch_ = reader_.ReadNext();
  }

  virtual ~FileBatchReader() = default;
  std::shared_ptr<arrow::ChunkedArray> ReadNext(int64_t batch_size);
  std::shared_ptr<arrow::Schema> GetSchema() const {
    return reader_.GetSchema();
  }

 private:
  SimpleArrowReader reader_;
  std::shared_ptr<arrow::RecordBatch> cur_batch_;
};

std::shared_ptr<arrow::ChunkedArray> ReadFileArray(
    const std::string& file_name);

}  // namespace scql::engine::util::disk