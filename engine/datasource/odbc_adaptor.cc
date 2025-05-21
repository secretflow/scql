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

#include <future>
#include <memory>
#include <mutex>
#include <optional>

#include "Poco/Data/MetaColumn.h"
#include "Poco/DateTime.h"
#include "Poco/Timestamp.h"
#include "yacl/base/exception.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor.h"
#include "engine/util/spu_io.h"

namespace scql::engine {

OdbcAdaptor::OdbcAdaptor(const std::string& db_kind,
                         const std::string& connection_str, ConnectionType type,
                         size_t pool_size) {
  connector_ =
      std::make_unique<OdbcConnector>(db_kind, connection_str, type, pool_size);
}

void OdbcChunkedResult::Init() {
  using Poco::Data::MetaColumn;
  try {
    more_ = rs_->moveFirst();
    arrow::FieldVector fields;
    std::size_t column_cnt = rs_->columnCount();
    builders_.reserve(column_cnt);
    column_value_handlers_.reserve(column_cnt);
    for (std::size_t i = 0; i < column_cnt; ++i) {
      std::shared_ptr<arrow::DataType> field_arrow_type;
      switch (rs_->columnType(i)) {
        case MetaColumn::ColumnDataType::FDT_BOOL: {
          auto concrete_builder = std::make_unique<BooleanTensorBuilder>();
          field_arrow_type = concrete_builder->Type();
          BooleanTensorBuilder* typed_ptr = concrete_builder.get();

          column_value_handlers_.emplace_back(
              [typed_ptr](Poco::Dynamic::Var& var) {
                if (var.isEmpty()) {
                  typed_ptr->AppendNull();
                } else {
                  typed_ptr->Append(var.convert<bool>());
                }
              });
          builders_.push_back(std::move(concrete_builder));
          break;
        }
        case MetaColumn::ColumnDataType::FDT_INT8:
        case MetaColumn::ColumnDataType::FDT_UINT8:
        case MetaColumn::ColumnDataType::FDT_INT16:
        case MetaColumn::ColumnDataType::FDT_UINT16:
        case MetaColumn::ColumnDataType::FDT_INT32:
        case MetaColumn::ColumnDataType::FDT_UINT32:
        case MetaColumn::ColumnDataType::FDT_INT64:
        // FIXME: convert uint64 to int64 may overflow
        case MetaColumn::ColumnDataType::FDT_UINT64: {
          auto concrete_builder = std::make_unique<Int64TensorBuilder>();
          field_arrow_type = concrete_builder->Type();
          Int64TensorBuilder* typed_ptr = concrete_builder.get();

          column_value_handlers_.emplace_back(
              [typed_ptr](Poco::Dynamic::Var& var) {
                if (var.isEmpty()) {
                  typed_ptr->AppendNull();
                } else {
                  typed_ptr->Append(var.convert<int64_t>());
                }
              });
          builders_.push_back(std::move(concrete_builder));
          break;
        }
        case MetaColumn::ColumnDataType::FDT_DATE:
        case MetaColumn::ColumnDataType::FDT_TIME:
        case MetaColumn::ColumnDataType::FDT_TIMESTAMP: {
          auto concrete_builder = std::make_unique<Int64TensorBuilder>();
          field_arrow_type = concrete_builder->Type();
          Int64TensorBuilder* typed_ptr = concrete_builder.get();

          column_value_handlers_.emplace_back(
              [typed_ptr](Poco::Dynamic::Var& var) {
                if (var.isEmpty()) {
                  typed_ptr->AppendNull();
                } else {
                  auto epoch =
                      var.convert<Poco::DateTime>().timestamp().epochTime();
                  typed_ptr->Append(static_cast<int64_t>(epoch));
                }
              });
          builders_.push_back(std::move(concrete_builder));
          break;
        }
        case MetaColumn::ColumnDataType::FDT_FLOAT: {
          auto concrete_builder = std::make_unique<FloatTensorBuilder>();
          field_arrow_type = concrete_builder->Type();
          FloatTensorBuilder* typed_ptr = concrete_builder.get();

          column_value_handlers_.emplace_back(
              [typed_ptr](Poco::Dynamic::Var& var) {
                if (var.isEmpty()) {
                  typed_ptr->AppendNull();
                } else {
                  typed_ptr->Append(var.convert<float>());
                }
              });
          builders_.push_back(std::move(concrete_builder));
          break;
        }
        case MetaColumn::ColumnDataType::FDT_DOUBLE: {
          auto concrete_builder = std::make_unique<DoubleTensorBuilder>();
          field_arrow_type = concrete_builder->Type();
          DoubleTensorBuilder* typed_ptr = concrete_builder.get();

          column_value_handlers_.emplace_back(
              [typed_ptr](Poco::Dynamic::Var& var) {
                if (var.isEmpty()) {
                  typed_ptr->AppendNull();
                } else {
                  typed_ptr->Append(var.convert<double>());
                }
              });
          builders_.push_back(std::move(concrete_builder));
          break;
        }
        case MetaColumn::ColumnDataType::FDT_STRING:
        case MetaColumn::ColumnDataType::FDT_WSTRING:
        case MetaColumn::ColumnDataType::FDT_BLOB:
        case MetaColumn::ColumnDataType::FDT_CLOB: {
          auto concrete_builder = std::make_unique<StringTensorBuilder>();
          field_arrow_type = concrete_builder->Type();
          StringTensorBuilder* typed_ptr = concrete_builder.get();

          column_value_handlers_.emplace_back(
              [typed_ptr](Poco::Dynamic::Var& var) {
                if (var.isEmpty()) {
                  typed_ptr->AppendNull();
                } else {
                  typed_ptr->Append(var.convert<std::string>());
                }
              });
          builders_.push_back(std::move(concrete_builder));
          break;
        }
        default:
          YACL_THROW("unsupported Poco::Data::MetaColumn::ColumnDataType {}",
                     fmt::underlying(rs_->columnType(i)));
      }
      auto field =
          std::make_shared<arrow::Field>(rs_->columnName(i), field_arrow_type);
      fields.push_back(field);
    }
    schema_ = std::make_shared<arrow::Schema>(fields);
  } catch (const Poco::Data::DataException& e) {
    YACL_THROW("catch unexpected Poco::Data::DataException: {}",
               e.displayText());
  }
}

std::optional<arrow::ChunkedArrayVector> OdbcChunkedResult::Fetch() {
  if (!more_) {
    return std::nullopt;
  }
  using Poco::Data::MetaColumn;
  constexpr size_t batch_size = 3;
  std::size_t column_cnt = rs_->columnCount();
  for (size_t i = 0; i < batch_size && more_; ++i) {
    for (std::size_t col_index = 0; col_index < column_cnt; col_index++) {
      auto var = (*rs_)[col_index];
      column_value_handlers_[col_index](var);
    }
    more_ = rs_->moveNext();
  }
  arrow::ChunkedArrayVector chunked_arrs;
  for (const auto& builder : builders_) {
    TensorPtr t;
    builder->Finish(&t);
    chunked_arrs.push_back(t->ToArrowChunkedArray());
  }
  return chunked_arrs;
}

std::shared_ptr<ChunkedResult> OdbcAdaptor::SendQuery(
    const std::string& query) {
  try {
    auto session = connector_->CreateSession();

    Poco::Data::Statement select(session);
    select << query;
    select.execute();

    auto rs = std::make_unique<Poco::Data::RecordSet>(select);
    return std::make_shared<OdbcChunkedResult>(std::move(rs));
  } catch (const Poco::Data::DataException& e) {
    YACL_THROW("catch unexpected Poco::Data::DataException: {}",
               e.displayText());
  }
}

}  // namespace scql::engine
