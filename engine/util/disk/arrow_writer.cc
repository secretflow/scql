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

#include "engine/util/disk/arrow_writer.h"

#include "arrow/array/array_base.h"
#include "arrow/chunked_array.h"
#include "arrow/io/file.h"
#include "arrow/record_batch.h"
#include "yacl/base/exception.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/type.h"

namespace scql::engine::util::disk {

void ArrowWriter::Init() {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  ASSIGN_OR_THROW_ARROW_STATUS(outfile,
                               arrow::io::FileOutputStream::Open(file_path_));
  ASSIGN_OR_THROW_ARROW_STATUS(ipc_writer_,
                               arrow::ipc::MakeFileWriter(outfile, schema_));
}

size_t ArrowWriter::WriteBatch(const arrow::RecordBatch& batch) {
  YACL_ENFORCE(batch.num_columns() == 1, "columns num in batch must be 1");
  if (batch.num_rows() == 0) {
    return 0;
  }
  THROW_IF_ARROW_NOT_OK(ipc_writer_->WriteRecordBatch(batch));
  num_rows_ += batch.num_rows();
  null_count_ += batch.column(0)->null_count();
  return batch.num_rows();
}

size_t ArrowWriter::WriteBatch(const arrow::ChunkedArray& batch) {
  size_t old_rows = num_rows_;
  for (int i = 0; i < batch.num_chunks(); ++i) {
    std::vector<std::shared_ptr<arrow::Array>> arrays = {batch.chunk(i)};
    WriteBatch(
        *arrow::RecordBatch::Make(schema_, batch.chunk(i)->length(), arrays));
  }
  return num_rows_ - old_rows;
}

}  // namespace scql::engine::util::disk