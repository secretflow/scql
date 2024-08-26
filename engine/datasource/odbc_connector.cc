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

#include "engine/datasource/odbc_connector.h"

#include <algorithm>
#include <mutex>

#include "Poco/Data/MySQL/Connector.h"
#include "Poco/Data/PostgreSQL/Connector.h"
#include "Poco/Data/SQLite/Connector.h"
#include "yacl/base/exception.h"

namespace scql::engine {

namespace {
std::once_flag mysql_register_flag;
}

OdbcConnector::OdbcConnector(const std::string& db_kind,
                             const std::string& connection_str,
                             ConnectionType type, size_t pool_size)
    : db_kind_(db_kind), connection_str_(connection_str) {
  std::transform(db_kind_.begin(), db_kind_.end(), db_kind_.begin(), ::tolower);
  try {
    if (db_kind_ == "mysql") {
      // `mysql_library_init` is not thread-safe in multithread environment,
      // see: https://dev.mysql.com/doc/c-api/5.7/en/mysql-library-init.html
      // MySQL::Connector::registerConnector() invokes `mysql_library_init`
      // refer to
      // https://github.com/pocoproject/poco/blob/poco-1.12.2-release/Data/MySQL/src/Connector.cpp#L55
      std::call_once(mysql_register_flag,
                     Poco::Data::MySQL::Connector::registerConnector);
    } else if (db_kind_ == "sqlite") {
      Poco::Data::SQLite::Connector::registerConnector();
    } else if (db_kind_ == "postgresql") {
      Poco::Data::PostgreSQL::Connector::registerConnector();
    } else {
      YACL_THROW("unsupported DatabaseKind: {}", db_kind_);
    }

    if (type == ConnectionType::Pooled) {
      pool_ = std::make_unique<Poco::Data::SessionPool>(
          db_kind_, connection_str_, 1, pool_size);
    }
  } catch (const Poco::Data::DataException& e) {
    // NOTE: Poco Exception's what() method only return the exception category
    // without detail information, so we rethrow it with displayText() method.
    // https://docs.pocoproject.org/current/Poco.Exception.html#13533
    YACL_THROW("catch unexpected Poco::Data::DataException: {}",
               e.displayText());
  }
}

Poco::Data::Session OdbcConnector::CreateSession() {
  try {
    if (pool_) {
      return pool_->get();
    }

    return Poco::Data::Session(db_kind_, connection_str_);
  } catch (const Poco::Data::DataException& e) {
    YACL_THROW("create session catch unexpected Poco::Data::DataException: {}",
               e.displayText());
  }
}

}  // namespace scql::engine