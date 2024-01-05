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
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/main/query_result.hpp"
#include "google/protobuf/util/json_util.h"
#include "spdlog/spdlog.h"
#include "yacl/base/exception.h"

#include "engine/core/arrow_helper.h"
#include "engine/datasource/duckdb_wrapper.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

#include "engine/datasource/csvdb_conf.pb.h"

namespace scql::engine {

namespace {

// idea from:
// https://github.com/duckdb/duckdb/blob/0d2d7930d2789405a0d07a15e37485fe70faee3e/test/arrow/parquet_test.cpp#L86-L104
std::vector<TensorPtr> ConvertDuckResultToTensors(
    std::unique_ptr<duckdb::QueryResult> result) {
  ArrowSchema abi_arrow_schema;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_result;
  duckdb::ArrowConverter::ToArrowSchema(&abi_arrow_schema, result->types,
                                        result->names,
                                        result->client_properties);
  std::shared_ptr<arrow::Schema> schema;
  ASSIGN_OR_THROW_ARROW_STATUS(schema, arrow::ImportSchema(&abi_arrow_schema));

  while (true) {
    auto data_chunk = result->Fetch();
    if (!data_chunk || data_chunk->size() == 0) {
      break;
    }
    data_chunk->Verify();
    ArrowArray arrow_array;
    duckdb::ArrowConverter::ToArrowArray(*data_chunk, &arrow_array,
                                         result->client_properties);
    std::shared_ptr<arrow::RecordBatch> batch;
    ASSIGN_OR_THROW_ARROW_STATUS(
        batch, arrow::ImportRecordBatch(&arrow_array, schema));

    batches_result.push_back(std::move(batch));
  }

  std::shared_ptr<arrow::Table> table;
  ASSIGN_OR_THROW_ARROW_STATUS(
      table, arrow::Table::FromRecordBatches(schema, batches_result));
  THROW_IF_ARROW_NOT_OK(table->Validate());

  std::vector<TensorPtr> tensors;
  for (int i = 0; i < table->num_columns(); ++i) {
    auto chunked_arr = table->column(i);
    auto chunked_arr_type = chunked_arr->type()->id();
    std::shared_ptr<Tensor> tensor;
    if (arrow::is_temporal(chunked_arr_type)) {
      tensor = util::ConvertDateTimeToInt64(chunked_arr);
    } else {
      tensor = std::make_shared<Tensor>(chunked_arr);
    }
    tensors.push_back(tensor);
  }
  return tensors;
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

std::vector<TensorPtr> CsvdbAdaptor::GetQueryResult(const std::string& query) {
  duckdb::DuckDB db = DuckDBWrapper::CreateDB(&csvdb_conf_);

  duckdb::Connection conn(db);
  conn.BeginTransaction();
  DuckDBWrapper::CreateCSVScanFunction(conn);
  conn.Commit();

  auto duck_result = conn.SendQuery(query);
  YACL_ENFORCE(!duck_result->HasError(), "send query to DuckDB failed, msg={}",
               duck_result->GetError());

  auto tensors = ConvertDuckResultToTensors(std::move(duck_result));

  return tensors;
}

}  // namespace scql::engine
