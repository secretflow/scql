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

#include "engine/datasource/csvdb_adaptor.h"

#include <filesystem>
#include <future>
#include <memory>
#include <optional>

#include "absl/strings/ascii.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/main/client_properties.hpp"
#include "duckdb/main/query_result.hpp"
#include "google/protobuf/util/json_util.h"
#include "yacl/base/exception.h"

#include "engine/core/arrow_helper.h"
#include "engine/datasource/duckdb_wrapper.h"
#include "engine/util/spu_io.h"

#include "api/v1/column.pb.h"
#include "engine/datasource/csvdb_conf.pb.h"

namespace scql::engine {

CsvdbAdaptor::CsvdbAdaptor(const std::string& json_str) {
  auto status =
      google::protobuf::util::JsonStringToMessage(json_str, &csvdb_conf_);
  YACL_ENFORCE(status.ok(),
               "failed to parse json to csvdb conf: json={}, error={}",
               json_str, status.ToString());
  // check column data type
  for (const auto& tbl : csvdb_conf_.tables()) {
    for (const auto& col : tbl.columns()) {
      scql::api::v1::DataType dtype;
      if (!scql::api::v1::DataType_Parse(
              absl::AsciiStrToUpper(col.column_type()), &dtype)) {
        YACL_THROW("unknown column type={} of column={} in table={}",
                   col.column_type(), col.column_name(), tbl.table_name());
      }
    }
  }
}

std::shared_ptr<ChunkedResult> CsvdbAdaptor::SendQuery(
    const std::string& query) {
  // idea from:
  // https://github.com/duckdb/duckdb/blob/0d2d7930d2789405a0d07a15e37485fe70faee3e/test/arrow/parquet_test.cpp#L86-L104
  duckdb::DuckDB db = DuckDBWrapper::CreateDB(&csvdb_conf_);

  duckdb::Connection conn(db);
  conn.BeginTransaction();
  DuckDBWrapper::CreateCSVScanFunction(conn);
  conn.Commit();

  auto duck_result = conn.SendQuery(query);
  YACL_ENFORCE(!duck_result->HasError(), "send query to DuckDB failed, msg={}",
               duck_result->GetError());
  ArrowSchema abi_arrow_schema;
  // avoid converting from string to large_string
  duck_result->client_properties.arrow_offset_size =
      duckdb::ArrowOffsetSize::LARGE;
  duckdb::ArrowConverter::ToArrowSchema(&abi_arrow_schema, duck_result->types,
                                        duck_result->names,
                                        duck_result->client_properties);
  std::shared_ptr<arrow::Schema> schema;
  ASSIGN_OR_THROW_ARROW_STATUS(schema, arrow::ImportSchema(&abi_arrow_schema));
  return std::make_shared<CsvdbChunkedResult>(schema, std::move(duck_result));
};

std::optional<arrow::ChunkedArrayVector> CsvdbChunkedResult::Fetch() {
  auto data_chunk = duck_result_->Fetch();
  YACL_ENFORCE(!duck_result_->HasError(), "fetch result failed, msg={}",
               duck_result_->GetError());
  if (!data_chunk || data_chunk->size() == 0) {
    return std::nullopt;
  }
  data_chunk->Verify();
  ArrowArray arrow_array;
  duckdb::ArrowConverter::ToArrowArray(*data_chunk, &arrow_array,
                                       duck_result_->client_properties);
  std::shared_ptr<arrow::RecordBatch> batch;
  ASSIGN_OR_THROW_ARROW_STATUS(batch,
                               arrow::ImportRecordBatch(&arrow_array, schema_));
  arrow::ChunkedArrayVector chunked_arrs;
  for (int i = 0; i < batch->num_columns(); ++i) {
    chunked_arrs.push_back(
        arrow::ChunkedArray::Make({batch->column(i)}).ValueOrDie());
  }
  return chunked_arrs;
}

}  // namespace scql::engine
