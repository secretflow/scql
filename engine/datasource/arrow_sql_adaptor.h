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

#include <string>

#include "arrow/flight/sql/client.h"

#include "engine/datasource/datasource_adaptor.h"

namespace scql::engine {

class ArrowSqlAdaptor : public DatasourceAdaptor {
 public:
  explicit ArrowSqlAdaptor(const std::string& uri);

  ~ArrowSqlAdaptor() override;

  std::vector<TensorPtr> ExecQuery(
      const std::string& query,
      const std::vector<ColumnDesc>& expected_outputs) override;

 private:
  // example: grpc+tcp://127.0.0.1:12929
  arrow::Status Connect(const std::string& uri);

  std::unique_ptr<arrow::flight::sql::FlightSqlClient> sql_client_;
  arrow::flight::FlightCallOptions call_options_;
};

}  // namespace scql::engine
