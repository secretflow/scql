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

#include "engine/datasource/arrow_sql_adaptor.h"

#include "arrow/compute/cast.h"
#include "arrow/flight/client.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "spdlog/spdlog.h"
#include "yacl/base/exception.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/type.h"
#include "engine/util/spu_io.h"

namespace scql::engine {

arrow::Status ArrowSqlAdaptor::Connect(const std::string& uri) {
  ARROW_ASSIGN_OR_RAISE(auto location, arrow::flight::Location::Parse(uri));
  std::unique_ptr<arrow::flight::FlightClient> flight_client;
  ARROW_ASSIGN_OR_RAISE(flight_client,
                        arrow::flight::FlightClient::Connect(location));
  sql_client_ = std::make_unique<arrow::flight::sql::FlightSqlClient>(
      std::move(flight_client));
  return arrow::Status::OK();
}

ArrowSqlAdaptor::ArrowSqlAdaptor(const std::string& uri) {
  auto status = Connect(uri);
  YACL_ENFORCE(status.ok(), "fail to connect arrow sql server: {}",
               status.ToString());
}

std::vector<TensorPtr> ArrowSqlAdaptor::ExecQuery(
    const std::string& query, const std::vector<ColumnDesc>& expected_outputs) {
  std::unique_ptr<arrow::flight::FlightInfo> flight_info;
  // use default call options
  // TODO(@xiaoyuan) set call options by long time benchmark
  ASSIGN_OR_THROW_ARROW_STATUS(flight_info,
                               sql_client_->Execute(call_options_, query));
  std::vector<TensorPtr> tensors;
  for (const arrow::flight::FlightEndpoint& endpoint :
       flight_info->endpoints()) {
    std::unique_ptr<arrow::flight::FlightStreamReader> stream;
    ASSIGN_OR_THROW_ARROW_STATUS(
        stream, sql_client_->DoGet(call_options_, endpoint.ticket));
    std::shared_ptr<arrow::Table> table;
    ASSIGN_OR_THROW_ARROW_STATUS(table, stream->ToTable());
    THROW_IF_ARROW_NOT_OK(table->Validate());
    YACL_ENFORCE_EQ(table->num_columns(), expected_outputs.size(),
                    "query result column size={} not equal to expected size={}",
                    table->num_columns(), expected_outputs.size());
    for (int i = 0; i < table->num_columns(); ++i) {
      auto chunked_arr = table->column(i);
      YACL_ENFORCE(chunked_arr, "get column(idx={}) from table failed", i);
      if (FromArrowDataType(chunked_arr->type()) != expected_outputs[i].dtype) {
        auto to_type = ToArrowDataType(expected_outputs[i].dtype);
        SPDLOG_WARN("arrow type mismatch, convert from {} to {}",
                    chunked_arr->type()->ToString(), to_type->ToString());
        arrow::Datum cast_result;
        ASSIGN_OR_THROW_ARROW_STATUS(
            cast_result, arrow::compute::Cast(chunked_arr, to_type));
        chunked_arr = cast_result.chunked_array();
      }
      auto tensor = std::make_shared<Tensor>(std::move(chunked_arr));
      tensors.push_back(std::move(tensor));
    }
  }
  return tensors;
}

ArrowSqlAdaptor::~ArrowSqlAdaptor() { static_cast<void>(sql_client_->Close()); }
}  // namespace scql::engine