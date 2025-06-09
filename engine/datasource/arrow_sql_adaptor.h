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

#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "arrow/flight/sql/client.h"

#include "engine/core/tensor.h"
#include "engine/datasource/datasource_adaptor.h"

namespace scql::engine {

using SqlClientPtr = std::shared_ptr<arrow::flight::sql::FlightSqlClient>;

class ArrowClientManager {
 public:
  explicit ArrowClientManager(arrow::flight::FlightCallOptions& call_options)
      : call_options_(call_options) {}

  void CreateDefaultClient(const std::string& uri);
  SqlClientPtr GetClientFromEndpoint(
      const arrow::flight::FlightEndpoint& endpoint);
  arrow::flight::FlightCallOptions GetCallOptions() { return call_options_; }
  SqlClientPtr GetDefaultClient() { return sql_client_; }
  ~ArrowClientManager() {
    for (auto& client : client_map_) {
      static_cast<void>(client.second->Close());
    }
  }

 private:
  SqlClientPtr sql_client_;
  std::unordered_map<std::string, SqlClientPtr> client_map_;
  arrow::flight::FlightCallOptions call_options_;
};

class ArrowSqlChunkedResult : public ChunkedResult {
 public:
  ArrowSqlChunkedResult(std::shared_ptr<arrow::Schema> schema,
                        std::unique_ptr<arrow::flight::FlightInfo> flight_info,
                        std::shared_ptr<ArrowClientManager> client_creator)
      : schema_(std::move(schema)),
        flight_info_(std::move(flight_info)),
        client_creator_(std::move(client_creator)) {}

  std::optional<arrow::ChunkedArrayVector> Fetch() override;
  std::shared_ptr<arrow::Schema> GetSchema() override { return schema_; };

 private:
  std::shared_ptr<arrow::Schema> schema_;
  std::unique_ptr<arrow::flight::FlightInfo> flight_info_;
  size_t endpoint_index_ = 0;
  std::shared_ptr<ArrowClientManager> client_creator_;
};

class ArrowSqlAdaptor : public DatasourceAdaptor {
 public:
  // example: grpc+tcp://127.0.0.1:12929
  // with authorization: grpc+tcp://127.0.0.1:12929@username:password
  explicit ArrowSqlAdaptor(const std::string& conn_str);

  ~ArrowSqlAdaptor() override = default;

 private:
  // Send query and return schema
  std::shared_ptr<ChunkedResult> SendQuery(const std::string& query) override;

  std::shared_ptr<ArrowClientManager> client_creator_;
};

}  // namespace scql::engine
