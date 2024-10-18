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

#include "engine/operator/insert_table.h"

#include "absl/strings/match.h"
#include "arrow/array.h"
#include "arrow/visit_array_inline.h"
#include "gflags/gflags.h"

#include "engine/datasource/odbc_connector.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"
#include "engine/util/time_util.h"

namespace scql::engine::op {

DEFINE_string(output_db_kind, "",
              "the kind of output db, support mysql/sqlite/postgresql");
DEFINE_string(output_db_connection_str, "",
              "the connection str to connect to output db");

namespace {

static constexpr int64_t kBatchSize = 1000;

class ValueVistor {
 public:
  ValueVistor() = delete;

  explicit ValueVistor(std::vector<std::string>* result_vector, bool is_time)
      : result_vector_(result_vector), is_time_(is_time) {
    YACL_ENFORCE(result_vector_, "result_vector_ can not be null");
    result_vector_->clear();
  }

  template <typename T>
  arrow::Status Visit(const T& array) {
    return arrow::Status::NotImplemented(fmt::format(
        "type {} is not implemented in ValueVistor", array.type()->name()));
  }

  template <typename TYPE>
  arrow::Status Visit(const arrow::NumericArray<TYPE>& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      result_vector_->push_back(
          array.IsNull(i) ? "NULL" : std::to_string(array.GetView(i)));
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanArray& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      result_vector_->push_back(array.IsNull(i)    ? "NULL"
                                : array.GetView(i) ? "TRUE"
                                                   : "FALSE");
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::NumericArray<arrow::Int64Type>& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      if (array.IsNull(i)) {
        result_vector_->push_back("NULL");
      } else {
        if (is_time_) {
          auto time_str = QuotingString(
              util::ConvertEpochToStr(static_cast<time_t>(array.GetView(i))));
          result_vector_->push_back(std::move(time_str));
        } else {
          result_vector_->push_back(std::to_string(array.GetView(i)));
        }
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::LargeStringArray& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      if (array.IsNull(i)) {
        result_vector_->push_back("NULL");
      } else {
        auto quoted_str = QuotingString(array.Value(i));
        result_vector_->push_back(std::move(quoted_str));
      }
    }
    return arrow::Status::OK();
  }

 private:
  std::string QuotingString(std::string_view str) {
    // FIXME(jingshi): the string may cantain "'", which should be escaped
    return absl::StrCat("'", str, "'");
  }

 private:
  std::vector<std::string>* result_vector_;
  bool is_time_;
};

// concatenate columns to insert values, e.g:
// columns = [["a", "b", "c"], ["1", "2", "3"]]
// return "(a,1),(b,2),(c,3)"
std::string ColumnsToValues(
    const std::vector<std::vector<std::string>>& columns) {
  YACL_ENFORCE(columns.size() > 0 && columns[0].size() > 0,
               "columns can not be empty");
  std::vector<std::string> rows;
  for (size_t i = 0; i < columns[0].size(); ++i) {
    std::string row = absl::StrCat("(", columns[0][i]);
    for (size_t j = 1; j < columns.size(); ++j) {
      absl::StrAppend(&row, ",", columns[j][i]);
    }
    absl::StrAppend(&row, ")");
    rows.push_back(std::move(row));
  }
  return absl::StrJoin(rows, ",");
}

}  // namespace

const std::string InsertTable::kOpType("InsertTable");

const std::string& InsertTable::Type() const { return kOpType; }

void InsertTable::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);

  YACL_ENFORCE(inputs.size() > 0, "inputs can not be empty");

  YACL_ENFORCE(util::AreTensorsStatusMatched(
                   inputs, pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "input tensors status should be private");
}

void InsertTable::Execute(ExecContext* ctx) {
  std::vector<TensorPtr> tensors;
  std::vector<pb::PrimitiveDataType> input_types;
  int64_t num_rows = 0;
  const auto& input_pbs = ctx->GetInput(kIn);
  for (int i = 0; i < input_pbs.size(); ++i) {
    const auto& input_pb = input_pbs[i];
    auto from_tensor = ctx->GetTensorTable()->GetTensor(input_pb.name());
    YACL_ENFORCE(from_tensor, "get private tensor={} failed", input_pb.name());
    if (i == 0) {
      num_rows = from_tensor->Length();
    } else {
      YACL_ENFORCE_EQ(num_rows, from_tensor->Length(),
                      "num rows in tensor#{}: {} not matched previous {}", i,
                      from_tensor->Length(), num_rows);
    }

    if (input_pb.elem_type() == pb::PrimitiveDataType::TIMESTAMP &&
        !ctx->GetSession()->TimeZone().empty()) {
      from_tensor =
          util::CompensateTimeZone(from_tensor, ctx->GetSession()->TimeZone());
    }

    tensors.push_back(from_tensor);
    input_types.push_back(input_pbs[i].elem_type());
  }

  InsertInTransaction(ctx, tensors, input_types);
  ctx->GetSession()->SetAffectedRows(num_rows);
}

void InsertTable::InsertInTransaction(
    ExecContext* ctx, const std::vector<TensorPtr>& tensors,
    const std::vector<pb::PrimitiveDataType>& input_types) {
  // 1. connect to db
  OdbcConnector connector(FLAGS_output_db_kind, FLAGS_output_db_connection_str);
  auto session = connector.CreateSession();
  session.begin();

  // 2. insert in batch
  auto tableName = ctx->GetStringValueFromAttribute(kAttrTableName);
  auto columnNames =
      absl::StrJoin(ctx->GetStringValuesFromAttribute(kAttrColumnNames), ",");
  try {
    int64_t offset = 0;
    while (offset < tensors[0]->Length()) {
      std::vector<std::vector<std::string>> columns;
      for (size_t i = 0; i < tensors.size(); ++i) {
        auto cur_chunk =
            tensors[i]->ToArrowChunkedArray()->Slice(offset, kBatchSize);
        auto array = util::ConcatenateChunkedArray(cur_chunk);
        std::vector<std::string> column;
        ValueVistor vistor(
            &column, input_types[i] == pb::PrimitiveDataType::DATETIME ||
                         input_types[i] == pb::PrimitiveDataType::TIMESTAMP);
        THROW_IF_ARROW_NOT_OK(arrow::VisitArrayInline(*array, &vistor));
        columns.push_back(std::move(column));
      }

      auto values = ColumnsToValues(columns);
      auto insert_stmt = absl::StrCat("INSERT INTO ", tableName, " (",
                                      columnNames, ") VALUES ", values);
      Poco::Data::Statement stmt(session);
      stmt << insert_stmt, Poco::Data::Keywords::now;
      // TODO(jingshi): try bind to prevent SQL injection
      offset += columns[0].size();
    }

    session.commit();
  } catch (const Poco::Data::DataException& e) {
    session.rollback();
    std::string err_txt = e.displayText();
    auto idx = err_txt.find("INSERT INTO");
    if (idx != std::string::npos) {
      err_txt = err_txt.substr(0, idx);  // hide statement in error message
    }
    YACL_THROW("insert catch unexpected Poco::Data::DataException: {}",
               err_txt);
  }

  SPDLOG_INFO("inserted {} rows into table {}", tensors[0]->Length(),
              tableName);
}

}  // namespace scql::engine::op