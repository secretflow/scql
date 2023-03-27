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

#include "engine/datasource/odbc_adaptor.h"

#include <mutex>

#include "Poco/Data/MySQL/Connector.h"
#ifdef ENABLE_POSTGRESQL
#include "Poco/Data/PostgreSQL/Connector.h"
#endif  // defined(ENABLE_POSTGRESQL)
#include "Poco/Data/MetaColumn.h"
#include "Poco/Data/RecordSet.h"
#include "Poco/Data/SQLite/Connector.h"
#include "yacl/base/exception.h"

#include "engine/core/primitive_builder.h"
#include "engine/core/string_tensor_builder.h"

namespace scql::engine {

namespace {
std::once_flag mysql_register_flag;
}

OdbcAdaptor::OdbcAdaptor(OdbcAdaptorOptions options)
    : options_(std::move(options)), pool_(nullptr) {
  try {
    Init();
  } catch (const Poco::Data::DataException& e) {
    // NOTE: Poco Exception's what() method only return the exception category
    // without detail information, so we rethrow it with displayText() method.
    // https://docs.pocoproject.org/current/Poco.Exception.html#13533
    YACL_THROW("catch unexpected Poco::Data::DataException: {}",
               e.displayText());
  }
}

std::vector<TensorPtr> OdbcAdaptor::ExecQuery(
    const std::string& query, const std::vector<ColumnDesc>& expected_outputs) {
  std::vector<TensorPtr> result;
  try {
    result = ExecQueryImpl(query, expected_outputs);
  } catch (const Poco::Data::DataException& e) {
    YACL_THROW("catch unexpected Poco::Data::DataException: {}",
               e.displayText());
  }
  return result;
}

std::vector<TensorPtr> OdbcAdaptor::ExecQueryImpl(
    const std::string& query, const std::vector<ColumnDesc>& expected_outputs) {
  auto session = CreateSession();

  Poco::Data::Statement select(session);
  select << query;
  select.execute();

  Poco::Data::RecordSet rs(select);

  // check amount of columns
  std::size_t column_cnt = rs.columnCount();
  if (column_cnt != expected_outputs.size()) {
    YACL_THROW("expect output contains {} #columns, but got {} #columns",
               expected_outputs.size(), column_cnt);
  }

  // TODO(shunde.csd): check output data type
  using Poco::Data::MetaColumn;

  std::vector<std::unique_ptr<TensorBuilder>> builders;
  for (std::size_t i = 0; i < column_cnt; ++i) {
    std::unique_ptr<TensorBuilder> builder;
    switch (rs.columnType(i)) {
      case MetaColumn::ColumnDataType::FDT_BOOL:
        builder = std::make_unique<BooleanTensorBuilder>();
        break;
      case MetaColumn::ColumnDataType::FDT_INT8:
      case MetaColumn::ColumnDataType::FDT_UINT8:
      case MetaColumn::ColumnDataType::FDT_INT16:
      case MetaColumn::ColumnDataType::FDT_UINT16:
      case MetaColumn::ColumnDataType::FDT_INT32:
      case MetaColumn::ColumnDataType::FDT_UINT32:
      case MetaColumn::ColumnDataType::FDT_INT64:
      // FIXME: convert uint64 to int64 may overflow
      case MetaColumn::ColumnDataType::FDT_UINT64:
        builder = std::make_unique<Int64TensorBuilder>();
        break;
      case MetaColumn::ColumnDataType::FDT_FLOAT:
      case MetaColumn::ColumnDataType::FDT_DOUBLE:
        builder = std::make_unique<DoubleTensorBuilder>();
        break;
      case MetaColumn::ColumnDataType::FDT_STRING:
      case MetaColumn::ColumnDataType::FDT_WSTRING:
        builder = std::make_unique<StringTensorBuilder>();
        break;
      default:
        YACL_THROW("unsupported Poco::Data::MetaColumn::ColumnDataType {}",
                   rs.columnType(i));
    }
    builders.push_back(std::move(builder));
  }

  for (auto it = rs.begin(); it != rs.end(); ++it) {
    auto& row = *it;
    for (std::size_t col_index = 0; col_index < column_cnt; col_index++) {
      auto& var = row[col_index];
      if (var.isEmpty()) {
        builders[col_index]->AppendNull();
        continue;
      }

      switch (rs.columnType(col_index)) {
        case MetaColumn::ColumnDataType::FDT_BOOL: {
          BooleanTensorBuilder* builder =
              static_cast<BooleanTensorBuilder*>(builders[col_index].get());
          builder->Append(var.convert<bool>());
          break;
        }
        case MetaColumn::ColumnDataType::FDT_INT8:
        case MetaColumn::ColumnDataType::FDT_UINT8:
        case MetaColumn::ColumnDataType::FDT_INT16:
        case MetaColumn::ColumnDataType::FDT_UINT16:
        case MetaColumn::ColumnDataType::FDT_INT32:
        case MetaColumn::ColumnDataType::FDT_UINT32:
        case MetaColumn::ColumnDataType::FDT_INT64:
        case MetaColumn::ColumnDataType::FDT_UINT64: {
          Int64TensorBuilder* builder =
              static_cast<Int64TensorBuilder*>(builders[col_index].get());
          builder->Append(var.convert<int64_t>());
          break;
        }
        case MetaColumn::ColumnDataType::FDT_STRING:
        case MetaColumn::ColumnDataType::FDT_WSTRING: {
          StringTensorBuilder* builder =
              static_cast<StringTensorBuilder*>(builders[col_index].get());
          builder->Append(var.convert<std::string>());
          break;
        }
        case MetaColumn::ColumnDataType::FDT_FLOAT:
        case MetaColumn::ColumnDataType::FDT_DOUBLE: {
          DoubleTensorBuilder* builder =
              static_cast<DoubleTensorBuilder*>(builders[col_index].get());
          builder->Append(var.convert<double>());
          break;
        }
        default:
          YACL_THROW("unsupported Poco::Data::MetaColumn::ColumnDataType {}",
                     rs.columnType(col_index));
      }
    }
  }

  std::vector<TensorPtr> results(column_cnt);
  for (std::size_t i = 0; i < column_cnt; ++i) {
    builders[i]->Finish(&results[i]);
  }

  return results;
}

Poco::Data::Session OdbcAdaptor::CreateSession() {
  if (pool_) {
    return pool_->get();
  }
  return Poco::Data::Session(connector_, options_.connection_str);
}

void OdbcAdaptor::Init() {
  switch (options_.kind) {
    case DataSourceKind::MYSQL:
      connector_ = "mysql";
      // `mysql_library_init` is not thread-safe in multithread environment,
      // see: https://dev.mysql.com/doc/c-api/5.7/en/mysql-library-init.html
      // MySQL::Connector::registerConnector() invokes `mysql_library_init`
      // refer to
      // https://github.com/pocoproject/poco/blob/poco-1.12.2-release/Data/MySQL/src/Connector.cpp#L55
      std::call_once(mysql_register_flag,
                     Poco::Data::MySQL::Connector::registerConnector);
      break;
    case DataSourceKind::SQLITE:
      connector_ = "sqlite";
      Poco::Data::SQLite::Connector::registerConnector();
      break;
#ifdef ENABLE_POSTGRESQL
    case DataSourceKind::POSTGRESQL:
      connector_ = "postgresql";
      Poco::Data::PostgreSQL::Connector::registerConnector();
      break;
#endif  // defined(ENABLE_POSTGRESQL)
    default:
      YACL_THROW("unsupported DataSourceKind: {}",
                 DataSourceKind_Name(options_.kind));
  }

  if (options_.connection_type == ConnectionType::Pooled) {
    pool_ = std::make_unique<Poco::Data::SessionPool>(
        connector_, options_.connection_str, 1, options_.pool_size);
  }
}

}  // namespace scql::engine