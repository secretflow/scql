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
class ArrowSqlAdaptor : public DatasourceAdaptor {
 public:
  // example: grpc+tcp://127.0.0.1:12929
  explicit ArrowSqlAdaptor(const std::string& uri);

  ~ArrowSqlAdaptor() override;

 private:
  std::vector<TensorPtr> GetQueryResult(
      const std::string& query, const TensorBuildOptions& options) override;
  SqlClientPtr GetClientFromEndpoint(
      const arrow::flight::FlightEndpoint& endpoint);

  SqlClientPtr sql_client_;
  std::unordered_map<std::string, SqlClientPtr> client_map_;
  arrow::flight::FlightCallOptions call_options_;
};

}  // namespace scql::engine
