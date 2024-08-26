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

#include <memory>
#include <sstream>

#include "arrow/flight/client.h"
#include "arrow/status.h"
#include "arrow/table.h"
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

ArrowSqlAdaptor::ArrowSqlAdaptor(const std::string& uri) {
  auto location = arrow::flight::Location::Parse(uri);
  YACL_ENFORCE(location.ok(), "fail to parse arrow uri: {}", uri);
  auto flight_client = arrow::flight::FlightClient::Connect(
      location.ValueOrDie(), GetFlightClientOptions());
  YACL_ENFORCE(flight_client.ok(), "fail to connect arrow sql server: {}",
               flight_client.status().ToString());
  // arrow c++ doesn't support authenticate for now
  // https://github.com/apache/arrow/blob/main/cpp/src/arrow/flight/transport.cc#L55
  sql_client_ = std::make_shared<arrow::flight::sql::FlightSqlClient>(
      std::move(flight_client.ValueOrDie()));
  client_map_.emplace(location->ToString(), sql_client_);
}

SqlClientPtr ArrowSqlAdaptor::GetClientFromEndpoint(
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
          client_map_.emplace(endpoint.locations[0].ToString(), sql_client_);
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

std::vector<TensorPtr> ArrowSqlAdaptor::GetQueryResult(
    const std::string& query, const TensorBuildOptions& options) {
  std::unique_ptr<arrow::flight::FlightInfo> flight_info;
  // use default call options
  // TODO(@xiaoyuan) set call options by long time benchmark
  ASSIGN_OR_THROW_ARROW_STATUS(flight_info,
                               sql_client_->Execute(call_options_, query));
  std::vector<TensorPtr> tensors;
  for (const arrow::flight::FlightEndpoint& endpoint :
       flight_info->endpoints()) {
    std::unique_ptr<arrow::flight::FlightStreamReader> stream;
    SqlClientPtr client = GetClientFromEndpoint(endpoint);

    ASSIGN_OR_THROW_ARROW_STATUS(stream,
                                 client->DoGet(call_options_, endpoint.ticket));
    std::shared_ptr<arrow::Table> table;
    ASSIGN_OR_THROW_ARROW_STATUS(table, stream->ToTable());
    THROW_IF_ARROW_NOT_OK(table->Validate());
    for (int i = 0; i < table->num_columns(); ++i) {
      auto tensor = TensorFrom(table->column(i));
      tensors.push_back(tensor);
    }
  }
  return tensors;
}

ArrowSqlAdaptor::~ArrowSqlAdaptor() { static_cast<void>(sql_client_->Close()); }
}  // namespace scql::engine