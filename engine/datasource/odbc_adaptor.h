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

#include "api/datasource.pb.h"

namespace scql::engine {

struct OdbcAdaptorOptions {
  DataSourceKind kind;
  std::string connection_str;
  ConnectionType connection_type;
  // pool_size valid when connection_type = ConnectionType::Pooled
  size_t pool_size;
};

/// @brief OdbcAdaptor provides a way to access general SQL databases.
/// It is powered by [POCO Data](https://pocoproject.org/)
/// The following database systems are supported:
///   - MySQL
///   - SQLite
class OdbcAdaptor : public DatasourceAdaptor {
 public:
  explicit OdbcAdaptor(OdbcAdaptorOptions options);
  ~OdbcAdaptor() = default;

  std::vector<TensorPtr> ExecQuery(
      const std::string& query,
      const std::vector<ColumnDesc>& expected_outputs) override;

 private:
  void Init();

  std::vector<TensorPtr> ExecQueryImpl(
      const std::string& query,
      const std::vector<ColumnDesc>& expected_outputs);

  // Returns a session from session pool if using pooled session,
  // Otherwise, creates a new session.
  Poco::Data::Session CreateSession();

  const OdbcAdaptorOptions options_;
  // Poco data connector name
  std::string connector_;
  std::unique_ptr<Poco::Data::SessionPool> pool_;
};

}  // namespace scql::engine
