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

#include "Poco/Data/SQLite/Connector.h"
#include "Poco/Data/Session.h"
#include "absl/strings/match.h"
#include "absl/strings/str_format.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

DECLARE_string(output_db_kind);
DECLARE_string(output_db_connection_str);

struct InsertTableTestCase {
  std::vector<test::NamedTensor> inputs;
  std::vector<pb::PrimitiveDataType> input_types;
  std::string tableName;
  std::vector<std::string> columnNames;
};

class InsertTableTest : public ::testing::TestWithParam<InsertTableTestCase> {
 protected:
  void SetUp() override {
    Poco::Data::SQLite::Connector::registerConnector();
    session_ =
        std::make_unique<Poco::Data::Session>("SQLite", db_connection_str_);
  }
  static pb::ExecNode MakeInsertTableExecNode(const InsertTableTestCase& tc);

  static void FeedInputs(ExecContext* ctxs, const InsertTableTestCase& tc);
  void CreateTable(const InsertTableTestCase& tc);

  std::string db_connection_str_ =
      "file:obdc_adaptor_test?mode=memory&cache=shared";
  std::unique_ptr<Poco::Data::Session> session_;
};

INSTANTIATE_TEST_SUITE_P(
    InsertTablePrivateTest, InsertTableTest,
    testing::Values(InsertTableTestCase{
        .inputs =
            {test::NamedTensor("c0",
                               TensorFrom(arrow::int64(), "[0,1,null,3]")),
             test::NamedTensor("c1",
                               TensorFrom(arrow::int64(), "[0,10,null,1000]")),
             test::NamedTensor("c2",
                               TensorFrom(arrow::int64(), "[0,10,null,1000]")),
             test::NamedTensor("c3",
                               TensorFrom(arrow::large_utf8(),
                                          R"json(["B","A","CCC","B"])json")),
             test::NamedTensor(
                 "c4", TensorFrom(arrow::float32(),
                                  "[1.1025, 100.245, -10.2, 3.1415926]")),
             test::NamedTensor("c5", TensorFrom(arrow::boolean(),
                                                "[true, false, null, false]"))},
        .input_types =
            {pb::PrimitiveDataType::INT64, pb::PrimitiveDataType::TIMESTAMP,
             pb::PrimitiveDataType::DATETIME, pb::PrimitiveDataType::STRING,
             pb::PrimitiveDataType::FLOAT32, pb::PrimitiveDataType::BOOL},
        .tableName = "test_table",
        .columnNames = {"c0", "c1", "c2", "c3", "c4", "c5"}}));

TEST_P(InsertTableTest, Works) {
  // Give
  auto tc = GetParam();
  CreateTable(tc);
  auto node = MakeInsertTableExecNode(tc);
  auto session = test::Make1PCSession();
  ExecContext ctx(node, session.get());
  FeedInputs(&ctx, tc);
  FLAGS_output_db_kind = "sqlite";
  FLAGS_output_db_connection_str = db_connection_str_;

  // When
  InsertTable op;
  ASSERT_NO_THROW(op.Run(&ctx););

  // Then check output
  Poco::Data::Statement select(*session_);
  select << "select * from " << tc.tableName << ";", Poco::Data::Keywords::now;
  EXPECT_EQ(select.columnsExtracted(), tc.inputs.size());
  EXPECT_EQ(select.rowsExtracted(), tc.inputs[0].tensor->Length());
}

/// ===========================
/// InsertTableTest impl
/// ===========================

pb::ExecNode InsertTableTest::MakeInsertTableExecNode(
    const InsertTableTestCase& tc) {
  test::ExecNodeBuilder builder(InsertTable::kOpType);

  builder.SetNodeName("InsertTable-test");
  builder.AddStringAttr(InsertTable::kAttrTableName, tc.tableName);
  builder.AddStringsAttr(InsertTable::kAttrColumnNames, tc.columnNames);
  // Add inputs
  std::vector<pb::Tensor> input_datas;
  for (size_t i = 0; i < tc.inputs.size(); ++i) {
    auto data =
        test::MakePrivateTensorReference(tc.inputs[i].name, tc.input_types[i]);
    input_datas.push_back(std::move(data));
  }
  builder.AddInput(InsertTable::kIn, input_datas);

  return builder.Build();
}

void InsertTableTest::FeedInputs(ExecContext* ctx,
                                 const InsertTableTestCase& tc) {
  test::FeedInputsAsPrivate(ctx, tc.inputs);
}

void InsertTableTest::CreateTable(const InsertTableTestCase& tc) {
  if (tc.inputs.size() == 0) {
    return;
  }

  auto sqlite_type = [](pb::PrimitiveDataType type) {
    switch (type) {
      case pb::PrimitiveDataType::BOOL:
        return "BOOLEAN";
      case pb::PrimitiveDataType::INT32:
      case pb::PrimitiveDataType::INT64:
      case pb::PrimitiveDataType::TIMESTAMP:
        return "INTEGER";
      case pb::PrimitiveDataType::FLOAT32:
      case pb::PrimitiveDataType::FLOAT64:
        return "REAL";
      case pb::PrimitiveDataType::STRING:
      case pb::PrimitiveDataType::DATETIME:
        return "TEXT";
      default:
        YACL_THROW("unsupported type {}", pb::PrimitiveDataType_Name(type));
    }
  };

  std::string stmt =
      absl::StrFormat("CREATE TABLE %s (%s %s", tc.tableName, tc.inputs[0].name,
                      sqlite_type(tc.inputs[0].tensor->Type()));
  for (size_t i = 1; i < tc.inputs.size(); ++i) {
    absl::StrAppend(&stmt,
                    absl::StrFormat(", %s %s", tc.inputs[i].name,
                                    sqlite_type(tc.inputs[i].tensor->Type())));
  }
  absl::StrAppend(&stmt, ")");
  SPDLOG_INFO("create table sql: {}", stmt);

  *session_ << stmt, Poco::Data::Keywords::now;
}

}  // namespace scql::engine::op