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
#include <utility>

#include "Poco/Data/RecordSet.h"
#include "Poco/Data/Session.h"
#include "Poco/Data/SessionPool.h"
#include "arrow/type_fwd.h"

#include "engine/core/primitive_builder.h"
#include "engine/core/string_tensor_builder.h"
#include "engine/datasource/datasource_adaptor.h"
#include "engine/datasource/odbc_connector.h"

#include "engine/datasource/datasource.pb.h"

namespace scql::engine {

class OdbcChunkedResult : public ChunkedResult {
 public:
  explicit OdbcChunkedResult(Poco::Data::Session session,
                             const std::string& query)
      : session_(std::move(session)) {
    Init(query);
  }

  std::optional<arrow::ChunkedArrayVector> Fetch() override;
  std::shared_ptr<arrow::Schema> GetSchema() override { return schema_; };
  // 1M
  static const size_t kBatchSize = 1024 * 1024;

 private:
  void Init(const std::string& query);
  Poco::Data::Session session_;
  std::shared_ptr<Poco::Data::Statement> select_;
  std::unique_ptr<Poco::Data::RecordSet> rs_;
  std::shared_ptr<arrow::Schema> schema_;
  bool more_ = false;
};

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

  // Send query and return schema
  std::shared_ptr<ChunkedResult> SendQuery(const std::string& query) override;

 private:
  std::unique_ptr<OdbcConnector> connector_;
};

}  // namespace scql::engine
