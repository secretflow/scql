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

#pragma once

#include <memory>

#include "Poco/Data/Session.h"
#include "Poco/Data/SessionPool.h"

namespace scql::engine {

enum ConnectionType {
  Unknown = 0,
  Short = 1,
  Pooled = 2,
};

/// @brief OdbcAdaptor provides a way to access general SQL databases.
/// It is powered by [POCO Data](https://pocoproject.org/)
/// The following database systems are supported:
///   - MySQL
///   - SQLite
///   - PostgreSQL
class OdbcConnector {
 public:
  explicit OdbcConnector(const std::string& db_kind,
                         const std::string& connection_str)
      : OdbcConnector(db_kind, connection_str, ConnectionType::Short, 0) {}

  explicit OdbcConnector(const std::string& db_kind,
                         const std::string& connection_str, ConnectionType type,
                         size_t pool_size);

  // Returns a session from session pool if using pooled session,
  // Otherwise, creates a new session.
  Poco::Data::Session CreateSession();

 private:
  std::string db_kind_;
  std::string connection_str_;
  std::unique_ptr<Poco::Data::SessionPool> pool_;
};

}  // namespace scql::engine
