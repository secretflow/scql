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

#include "engine/datasource/dp_adaptor.h"

#include <memory>
#include <optional>

#include "arrow/table.h"
#include "google/protobuf/util/json_util.h"

#include "engine/core/arrow_helper.h"

#include "engine/datasource/dataproxy_conf.pb.h"
#include "google/protobuf/any.pb.h"
namespace scql::engine {

DpAdaptor::DpAdaptor(std::string json_str) {
  datasource::DataProxyConf dp_conf;
  auto status = google::protobuf::util::JsonStringToMessage(json_str, &dp_conf);
  YACL_ENFORCE(status.ok(),
               "failed to parse json to dataproxy conf: json={}, error={}",
               json_str, status.ToString());

  // construct command_ and client_
  command_.mutable_datasource()->CopyFrom(dp_conf.datasource());

  arrow::flight::Location location;
  ASSIGN_OR_THROW_ARROW_STATUS(
      location, arrow::flight::Location::Parse(dp_conf.dp_uri()));
  arrow::flight::FlightClientOptions
      options;  // TODO: support tls and more options
  ASSIGN_OR_THROW_ARROW_STATUS(
      client_, arrow::flight::FlightClient::Connect(location, options));
}

std::shared_ptr<ChunkedResult> DpAdaptor::SendQuery(const std::string& query) {
  command_.mutable_query()->set_sql(query);
  google::protobuf::Any any_msg;
  any_msg.PackFrom(command_);
  std::string serialized_data;
  YACL_ENFORCE(any_msg.SerializeToString(&serialized_data),
               "failed to serialize the flight command");

  std::unique_ptr<arrow::flight::FlightInfo> flight_info;
  ASSIGN_OR_THROW_ARROW_STATUS(
      flight_info,
      client_->GetFlightInfo(
          arrow::flight::FlightDescriptor::Command(serialized_data)));

  arrow::ipc::DictionaryMemo memo;
  std::shared_ptr<arrow::Schema> schema;
  ASSIGN_OR_THROW_ARROW_STATUS(schema, flight_info->GetSchema(&memo));
  return std::make_shared<DpChunkedResult>(schema, std::move(flight_info),
                                           client_.get());
}

std::optional<arrow::ChunkedArrayVector> DpChunkedResult::Fetch() {
  if (endpoint_index_ >= flight_info_->endpoints().size()) {
    return std::nullopt;
  }

  auto endpoint = flight_info_->endpoints()[endpoint_index_];
  endpoint_index_++;

  std::unique_ptr<arrow::flight::FlightStreamReader> stream;
  YACL_ENFORCE(client_);
  ASSIGN_OR_THROW_ARROW_STATUS(stream, client_->DoGet(endpoint.ticket));

  std::shared_ptr<arrow::Table> table;
  // TODO: maybe we should use stream.Next() to get data by batch
  ASSIGN_OR_THROW_ARROW_STATUS(table, stream->ToTable());
  THROW_IF_ARROW_NOT_OK(table->Validate());

  arrow::ChunkedArrayVector arrs;
  for (int i = 0; i < table->num_columns(); ++i) {
    arrs.push_back(table->column(i));
  }

  return arrs;
}

}  // namespace scql::engine
