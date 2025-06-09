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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "Poco/Data/MetaColumn.h"
#include "Poco/Data/Range.h"
#include "Poco/DateTime.h"
#include "Poco/Dynamic/Var.h"
#include "Poco/Timestamp.h"
#include "yacl/base/exception.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/primitive_builder.h"
#include "engine/core/string_tensor_builder.h"
#include "engine/core/tensor.h"
#include "engine/core/tensor_constructor.h"
#include "engine/util/spu_io.h"

namespace scql::engine {

OdbcAdaptor::OdbcAdaptor(const std::string& db_kind,
                         const std::string& connection_str, ConnectionType type,
                         size_t pool_size) {
  connector_ =
      std::make_unique<OdbcConnector>(db_kind, connection_str, type, pool_size);
}

namespace {
using Poco::Data::MetaColumn;
#define META_COLUMN_TYPE_CASE(type) case MetaColumn::ColumnDataType::type:

#define SWITCH_DATE_BRANCH        \
  META_COLUMN_TYPE_CASE(FDT_DATE) \
  META_COLUMN_TYPE_CASE(FDT_TIME) \
  META_COLUMN_TYPE_CASE(FDT_TIMESTAMP)

#define SWITCH_BOOL_BRANCH META_COLUMN_TYPE_CASE(FDT_BOOL)

// FIXME: convert uint64 to int64 may overflow
#define SWITCH_INT64_BRANCH         \
  META_COLUMN_TYPE_CASE(FDT_INT8)   \
  META_COLUMN_TYPE_CASE(FDT_UINT8)  \
  META_COLUMN_TYPE_CASE(FDT_INT16)  \
  META_COLUMN_TYPE_CASE(FDT_UINT16) \
  META_COLUMN_TYPE_CASE(FDT_INT32)  \
  META_COLUMN_TYPE_CASE(FDT_UINT32) \
  META_COLUMN_TYPE_CASE(FDT_INT64)  \
  META_COLUMN_TYPE_CASE(FDT_UINT64)

#define SWITCH_FLOAT_BRANCH META_COLUMN_TYPE_CASE(FDT_FLOAT)
#define SWITCH_DOUBLE_BRANCH META_COLUMN_TYPE_CASE(FDT_DOUBLE)
#define SWITCH_STRING_BRANCH         \
  META_COLUMN_TYPE_CASE(FDT_STRING)  \
  META_COLUMN_TYPE_CASE(FDT_WSTRING) \
  META_COLUMN_TYPE_CASE(FDT_BLOB)    \
  META_COLUMN_TYPE_CASE(FDT_CLOB)

template <typename builder_type, typename data_type>
void ConvertDataToArray(
    const std::unique_ptr<Poco::Data::RecordSet>& recordset,
    const int64_t col_index, const int64_t begin, const int64_t end,
    const int64_t chunk_size,
    const std::function<data_type(const Poco::Dynamic::Var&)>& convert_func,
    TensorPtr* t) {
  auto builder = builder_type();
  for (int64_t row_index = begin * chunk_size;
       row_index < end * chunk_size &&
       static_cast<size_t>(row_index) < recordset->rowCount();
       ++row_index) {
    if (recordset->isNull(col_index, row_index)) {
      builder.AppendNull();
    } else {
      builder.Append(convert_func(recordset->value(col_index, row_index)));
    }
  }
  builder.Finish(t);
};

}  // namespace

void OdbcChunkedResult::Init(const std::string& query) {
  select_ = std::make_shared<Poco::Data::Statement>(session_);
  *select_ << query, Poco::Data::Keywords::limit(OdbcChunkedResult::kBatchSize);
  rs_ = std::make_unique<Poco::Data::RecordSet>(*select_);
  // execute to get first batch
  select_->execute();
  rs_->moveFirst();
  more_ = rs_->totalRowCount() > 0;
  using Poco::Data::MetaColumn;
  try {
    arrow::FieldVector fields;
    std::size_t column_cnt = rs_->columnCount();
    for (std::size_t i = 0; i < column_cnt; ++i) {
      std::shared_ptr<arrow::DataType> field_arrow_type;
      switch (rs_->columnType(i)) {
        SWITCH_BOOL_BRANCH {
          field_arrow_type = arrow::boolean();
          break;
        }
        SWITCH_DATE_BRANCH
        SWITCH_INT64_BRANCH {
          field_arrow_type = arrow::int64();
          break;
        }
        SWITCH_FLOAT_BRANCH {
          field_arrow_type = arrow::float32();
          break;
        }
        SWITCH_DOUBLE_BRANCH {
          field_arrow_type = arrow::float64();
          break;
        }
        SWITCH_STRING_BRANCH {
          field_arrow_type = arrow::large_utf8();
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
  select_->wait();
  more_ = !select_->done();
  auto batch_num = yacl::get_num_threads();
  // if row number in one batch is too small, we will use single thread to
  // convert
  if (rs_->rowCount() < kBatchSize / batch_num) {
    batch_num = 1;
  }
  auto chunk_size = yacl::divup(rs_->rowCount(), batch_num);
  std::vector<std::vector<TensorPtr>> tensor_vecs(
      rs_->columnCount(), std::vector<TensorPtr>(batch_num));
  try {
    yacl::parallel_for(0, batch_num, 1, [&](int64_t begin, int64_t end) {
      for (size_t col_index = 0; col_index < rs_->columnCount(); ++col_index) {
        TensorPtr t;
        switch (rs_->columnType(col_index)) {
          SWITCH_BOOL_BRANCH {
            ConvertDataToArray<BooleanTensorBuilder, bool>(
                rs_, col_index, begin, end, chunk_size,
                [](const Poco::Dynamic::Var& value) {
                  return value.convert<bool>();
                },
                &t);
            break;
          }
          SWITCH_INT64_BRANCH {
            ConvertDataToArray<Int64TensorBuilder, int64_t>(
                rs_, col_index, begin, end, chunk_size,
                [](const Poco::Dynamic::Var& value) {
                  return value.convert<int64_t>();
                },
                &t);
            break;
          }
          SWITCH_DATE_BRANCH {
            ConvertDataToArray<Int64TensorBuilder, int64_t>(
                rs_, col_index, begin, end, chunk_size,
                [](const Poco::Dynamic::Var& value) {
                  return value.convert<Poco::DateTime>()
                      .timestamp()
                      .epochTime();
                },
                &t);
            break;
          }
          SWITCH_FLOAT_BRANCH {
            ConvertDataToArray<FloatTensorBuilder, float>(
                rs_, col_index, begin, end, chunk_size,
                [](const Poco::Dynamic::Var& value) {
                  return value.convert<float>();
                },
                &t);
            break;
          }
          SWITCH_DOUBLE_BRANCH {
            ConvertDataToArray<DoubleTensorBuilder, double>(
                rs_, col_index, begin, end, chunk_size,
                [](const Poco::Dynamic::Var& value) {
                  return value.convert<double>();
                },
                &t);
            break;
          }
          SWITCH_STRING_BRANCH {
            ConvertDataToArray<StringTensorBuilder, std::string>(
                rs_, col_index, begin, end, chunk_size,
                [](const Poco::Dynamic::Var& value) {
                  return value.convert<std::string>();
                },
                &t);
            break;
          }
          default:
            YACL_THROW("unsupported Poco::Data::MetaColumn::ColumnDataType {}",
                       fmt::underlying(rs_->columnType(col_index)));
        }
        tensor_vecs[col_index][begin] = t;
      }
    });
  } catch (const Poco::Data::DataException& e) {
    YACL_THROW("catch unexpected Poco::Data::DataException: {}",
               e.displayText());
  }
  if (more_) {
    select_->executeAsync();
  }
  arrow::ChunkedArrayVector chunked_arrs;
  for (auto& tensor_v : tensor_vecs) {
    chunked_arrs.push_back(ConcatTensors(tensor_v)->ToArrowChunkedArray());
  }
  return chunked_arrs;
}

std::shared_ptr<ChunkedResult> OdbcAdaptor::SendQuery(
    const std::string& query) {
  try {
    auto session = connector_->CreateSession();
    return std::make_shared<OdbcChunkedResult>(session, query);
  } catch (const Poco::Data::DataException& e) {
    YACL_THROW("catch unexpected Poco::Data::DataException: {}",
               e.displayText());
  }
}

}  // namespace scql::engine
