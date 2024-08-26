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

#include "engine/util/disk/arrow_reader.h"

#include "arrow/compute/cast.h"
#include "arrow/table.h"
#include "spdlog/spdlog.h"

namespace scql::engine::util::disk {

std::shared_ptr<arrow::RecordBatch> SimpleArrowReader::ReadNext() {
  if (batch_index_ >= record_batch_num_) {
    return nullptr;
  }
  std::shared_ptr<arrow::RecordBatch> rbatch;
  ASSIGN_OR_THROW_ARROW_STATUS(rbatch,
                               ipc_reader_->ReadRecordBatch(batch_index_));
  batch_index_++;
  return rbatch;
}

std::shared_ptr<arrow::ChunkedArray> ReadFileArray(
    const std::string& file_name) {
  SPDLOG_INFO("ReadFileArray: {}", file_name);
  SimpleArrowReader reader(file_name);
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batches;
  result_batches.reserve(reader.GetBatchNum());
  auto rbatch = reader.ReadNext();
  while (rbatch != nullptr) {
    result_batches.push_back(rbatch);
    rbatch = reader.ReadNext();
  }
  std::shared_ptr<arrow::Table> table;
  ASSIGN_OR_THROW_ARROW_STATUS(table, arrow::Table::FromRecordBatches(
                                          reader.GetSchema(), result_batches));
  THROW_IF_ARROW_NOT_OK(table->Validate());
  return table->column(0);
}

std::shared_ptr<arrow::ChunkedArray> FileBatchReader::ReadNext(
    int64_t batch_size) {
  if (cur_batch_ == nullptr) {
    return nullptr;
  }
  int64_t num_to_read = batch_size;
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batches;
  while (cur_batch_ != nullptr && num_to_read > 0) {
    if (cur_batch_->num_rows() <= num_to_read) {
      result_batches.push_back(cur_batch_);
      num_to_read -= cur_batch_->num_rows();
      cur_batch_ = reader_.ReadNext();
      continue;
    }
    auto batch = cur_batch_->Slice(0, num_to_read);
    cur_batch_ = cur_batch_->Slice(num_to_read);
    num_to_read -= batch->num_rows();
    result_batches.push_back(batch);
  }
  std::shared_ptr<arrow::Table> table;
  ASSIGN_OR_THROW_ARROW_STATUS(table, arrow::Table::FromRecordBatches(
                                          reader_.GetSchema(), result_batches));
  THROW_IF_ARROW_NOT_OK(table->Validate());
  return table->column(0);
}

}  // namespace scql::engine::util::disk