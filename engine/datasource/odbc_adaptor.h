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

#include "Poco/Data/Session.h"
#include "Poco/Data/SessionPool.h"

#include "engine/datasource/datasource_adaptor.h"
#include "engine/datasource/odbc_connector.h"

#include "engine/datasource/datasource.pb.h"

namespace scql::engine {

/// @brief OdbcAdaptor provides a way to access general SQL databases.
/// It is powered by [POCO Data](https://pocoproject.org/)
/// The following database systems are supported:
///   - MySQL
///   - SQLite
class OdbcAdaptor : public DatasourceAdaptor {
 public:
  explicit OdbcAdaptor(const std::string& db_kind,
                       const std::string& connection_str)
      : OdbcAdaptor(db_kind, connection_str, ConnectionType::Short, 0) {}
  explicit OdbcAdaptor(const std::string& db_kind,
                       const std::string& connection_str, ConnectionType type,
                       size_t pool_size);

  ~OdbcAdaptor() override = default;

 private:
  void Init();

  std::vector<TensorPtr> GetQueryResult(
      const std::string& query, const TensorBuildOptions& options) override;
  std::vector<TensorPtr> GetQueryResultImpl(
      const std::string& query, const TensorBuildOptions& options = {});

 private:
  std::unique_ptr<OdbcConnector> connector_;
};

}  // namespace scql::engine
