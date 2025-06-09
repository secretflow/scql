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

#include <future>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>

#include "absl/strings/str_split.h"
#include "arrow/flight/client.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/util/base64.h"
#include "butil/file_util.h"
#include "gflags/gflags.h"
#include "spdlog/spdlog.h"
#include "yacl/base/exception.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_constructor.h"
#include "engine/util/spu_io.h"

DEFINE_bool(arrow_client_disable_server_verification, false,
            "arrow sql client disable server verification");
DEFINE_string(arrow_cert_pem_path, "",
              "work in tls/mtls, used in server verification when "
              "arrow_client_disable_server_verification is false");
DEFINE_string(arrow_client_key_pem_path, "", "work in mtls");
DEFINE_string(arrow_client_cert_pem_path, "", "work in mtls");

namespace scql::engine {
arrow::flight::FlightClientOptions GetFlightClientOptions() {
  arrow::flight::FlightClientOptions options;
  options.disable_server_verification =
      FLAGS_arrow_client_disable_server_verification;
  if ((!options.disable_server_verification) &&
      (!FLAGS_arrow_cert_pem_path.empty())) {
    std::string cert_pem_content;
    YACL_ENFORCE(
        butil::ReadFileToString(butil::FilePath(FLAGS_arrow_cert_pem_path),
                                &cert_pem_content),
        "fail to read from {}", FLAGS_arrow_cert_pem_path);
    options.tls_root_certs = cert_pem_content;
  }
  if (!FLAGS_arrow_client_key_pem_path.empty()) {
    std::string client_private_key;
    YACL_ENFORCE(butil::ReadFileToString(
                     butil::FilePath(FLAGS_arrow_client_key_pem_path),
                     &client_private_key),
                 "fail to read from {}", FLAGS_arrow_client_key_pem_path);
    options.private_key = client_private_key;
  }
  if (!FLAGS_arrow_client_cert_pem_path.empty()) {
    std::string client_cert_perm;
    YACL_ENFORCE(butil::ReadFileToString(
                     butil::FilePath(FLAGS_arrow_client_cert_pem_path),
                     &client_cert_perm),
                 "fail to read from {}", FLAGS_arrow_client_cert_pem_path);
    options.cert_chain = client_cert_perm;
  }
  return options;
}

void ArrowClientManager::CreateDefaultClient(const std::string& uri) {
  auto location = arrow::flight::Location::Parse(uri);
  YACL_ENFORCE(location.ok(), "fail to parse arrow uri: {}", uri);
  auto flight_client = arrow::flight::FlightClient::Connect(
      location.ValueOrDie(), GetFlightClientOptions());
  YACL_ENFORCE(flight_client.ok(), "fail to connect arrow sql server: {}",
               flight_client.status().ToString());
  sql_client_ = std::make_shared<arrow::flight::sql::FlightSqlClient>(
      std::move(flight_client.ValueOrDie()));
  client_map_.emplace(location->ToString(), sql_client_);
}

ArrowSqlAdaptor::ArrowSqlAdaptor(const std::string& conn_str) {
  // use default call options
  // TODO(@xiaoyuan) set call options by long time benchmark
  arrow::flight::FlightCallOptions call_options;
  constexpr char kAuthHeader[] = "authorization";
  constexpr char kBasicPrefix[] = "Basic";
  std::vector<absl::string_view> fields = absl::StrSplit(conn_str, '@');
  YACL_ENFORCE(fields.size() <= 2,
               "invalid conn_str for arrow sql with more than one '@'");
  if (fields.size() == 2) {
    call_options.headers.emplace_back(
        kAuthHeader, fmt::format("{} {}", kBasicPrefix,
                                 arrow::util::base64_encode(fields[1])));
  }
  client_creator_ = std::make_shared<ArrowClientManager>(call_options);

  client_creator_->CreateDefaultClient(std::string(fields[0]));
}

SqlClientPtr ArrowClientManager::GetClientFromEndpoint(
    const arrow::flight::FlightEndpoint& endpoint) {
  SqlClientPtr client;
  if (endpoint.locations.empty()) {
    // use current service
    client = sql_client_;
  } else {
    bool has_connected_location = false;
    for (const auto& location : endpoint.locations) {
      const auto& iter = client_map_.find(location.ToString());
      if (iter != client_map_.end()) {
        client = iter->second;
        has_connected_location = true;
        break;
      }
    }
    if (!has_connected_location) {
      for (const auto& location : endpoint.locations) {
        auto flight_client = arrow::flight::FlightClient::Connect(location);
        if (flight_client.ok()) {
          client = std::make_shared<arrow::flight::sql::FlightSqlClient>(
              std::move(flight_client.ValueOrDie()));
          client_map_.emplace(endpoint.locations[0].ToString(), client);
          return client;
        }
      }
      std::stringstream err_str;
      for (const auto& location : endpoint.locations) {
        err_str << location.ToString();
      }
      YACL_THROW("fail to connect any location in endpoint: {}", err_str.str());
    }
  }
  return client;
}

std::shared_ptr<ChunkedResult> ArrowSqlAdaptor::SendQuery(
    const std::string& query) {
  std::unique_ptr<arrow::flight::FlightInfo> flight_info;
  ASSIGN_OR_THROW_ARROW_STATUS(flight_info,
                               client_creator_->GetDefaultClient()->Execute(
                                   client_creator_->GetCallOptions(), query));
  arrow::ipc::DictionaryMemo memo;
  auto schema = flight_info->GetSchema(&memo).ValueOrDie();
  return std::make_shared<ArrowSqlChunkedResult>(schema, std::move(flight_info),
                                                 client_creator_);
}

std::optional<arrow::ChunkedArrayVector> ArrowSqlChunkedResult::Fetch() {
  if (endpoint_index_ >= flight_info_->endpoints().size()) {
    return std::nullopt;
  }
  arrow::ChunkedArrayVector arrs;
  auto endpoint = flight_info_->endpoints()[endpoint_index_];
  endpoint_index_++;
  std::unique_ptr<arrow::flight::FlightStreamReader> stream;
  SqlClientPtr client = client_creator_->GetClientFromEndpoint(endpoint);
  ASSIGN_OR_THROW_ARROW_STATUS(
      stream,
      client->DoGet(client_creator_->GetCallOptions(), endpoint.ticket));
  std::shared_ptr<arrow::Table> table;
  // stream read data until all data has been read or error occurs
  // TODO(@xiaoyuan): maybe we should use stream.Next() to get data by batch
  ASSIGN_OR_THROW_ARROW_STATUS(table, stream->ToTable());
  THROW_IF_ARROW_NOT_OK(table->Validate());
  for (int i = 0; i < table->num_columns(); ++i) {
    arrs.push_back(table->column(i));
  }

  return arrs;
}

}  // namespace scql::engine
