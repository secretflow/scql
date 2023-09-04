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

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/compute/cast.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/main/query_result.hpp"
#include "google/protobuf/util/json_util.h"
#include "spdlog/spdlog.h"
#include "yacl/base/exception.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/type.h"
#include "engine/datasource/duckdb_wrapper.h"
#include "engine/util/spu_io.h"

#include "engine/datasource/csvdb_conf.pb.h"

namespace scql::engine {

namespace {

// idea from:
// https://github.com/duckdb/duckdb/blob/0d2d7930d2789405a0d07a15e37485fe70faee3e/test/arrow/parquet_test.cpp#L86-L104
void ConvertDuckResultToTensors(std::unique_ptr<duckdb::QueryResult> result,
                                const std::vector<ColumnDesc>& expected_outputs,
                                std::vector<TensorPtr>* tensors) {
  ArrowSchema abi_arrow_schema;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_result;
  auto timezone_config = duckdb::QueryResult::GetConfigTimezone(*result);
  duckdb::ArrowConverter::ToArrowSchema(&abi_arrow_schema, result->types,
                                        result->names, timezone_config);
  std::shared_ptr<arrow::Schema> schema;
  ASSIGN_OR_THROW_ARROW_STATUS(schema, arrow::ImportSchema(&abi_arrow_schema));

  while (true) {
    auto data_chunk = result->Fetch();
    if (!data_chunk || data_chunk->size() == 0) {
      break;
    }
    data_chunk->Verify();
    ArrowArray arrow_array;
    duckdb::ArrowConverter::ToArrowArray(*data_chunk, &arrow_array);
    std::shared_ptr<arrow::RecordBatch> batch;
    ASSIGN_OR_THROW_ARROW_STATUS(
        batch, arrow::ImportRecordBatch(&arrow_array, schema));

    batches_result.push_back(std::move(batch));
  }

  std::shared_ptr<arrow::Table> table;
  ASSIGN_OR_THROW_ARROW_STATUS(
      table, arrow::Table::FromRecordBatches(schema, batches_result));
  THROW_IF_ARROW_NOT_OK(table->Validate());
  YACL_ENFORCE_EQ(table->num_columns(), expected_outputs.size(),
                  "query result column size={} not equal to expected size={}",
                  table->num_columns(), expected_outputs.size());

  tensors->clear();
  for (int i = 0; i < table->num_columns(); ++i) {
    auto chunked_arr = table->column(i);
    YACL_ENFORCE(chunked_arr, "get column(idx={}) from table failed", i);
    if (FromArrowDataType(chunked_arr->type()) != expected_outputs[i].dtype ||
        // strings from duckdb use arrow::utf8() by default, which limits size
        // < 2G, so should be converted to large_utf8() to support large scale
        chunked_arr->type() == arrow::utf8()) {
      auto to_type = ToArrowDataType(expected_outputs[i].dtype);
      SPDLOG_WARN("arrow type mismatch, convert from {} to {}",
                  chunked_arr->type()->ToString(), to_type->ToString());

      auto result = arrow::compute::Cast(chunked_arr, to_type);
      YACL_ENFORCE(result.ok(), "caught error while invoking arrow cast: {}",
                   result.status().ToString());

      chunked_arr = result.ValueOrDie().chunked_array();
    }

    auto tensor = std::make_shared<Tensor>(std::move(chunked_arr));
    tensors->push_back(std::move(tensor));
  }
}

}  // namespace

CsvdbAdaptor::CsvdbAdaptor(const std::string& json_str) {
  google::protobuf::util::JsonParseOptions options;
  options.case_insensitive_enum_parsing = true;
  auto status = google::protobuf::util::JsonStringToMessage(
      json_str, &csvdb_conf_, options);
  YACL_ENFORCE(status.ok(),
               "failed to parse json to csvdb conf: json={}, error={}",
               json_str, status.ToString());
}

std::vector<TensorPtr> CsvdbAdaptor::ExecQuery(
    const std::string& query, const std::vector<ColumnDesc>& expected_outputs) {
  duckdb::DuckDB db = DuckDBWrapper::CreateDB(&csvdb_conf_);

  duckdb::Connection conn(db);
  conn.BeginTransaction();
  DuckDBWrapper::CreateCSVScanFunction(conn);
  conn.Commit();

  auto duck_result = conn.SendQuery(query);
  YACL_ENFORCE(!duck_result->HasError(), "send query to DuckDB failed, msg={}",
               duck_result->GetError());

  std::vector<TensorPtr> result;
  ConvertDuckResultToTensors(std::move(duck_result), expected_outputs, &result);

  return result;
}

}  // namespace scql::engine
