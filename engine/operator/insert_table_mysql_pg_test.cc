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

#include "Poco/Data/SQLite/Connector.h"
#include "Poco/Data/Session.h"
#include "absl/strings/match.h"
#include "absl/strings/str_format.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/datasource/odbc_connector.h"
#include "engine/operator/insert_table.h"
#include "engine/operator/test_util.h"

// clang-format off

// NOTE: 1. you need to prepare table before test insert mysql/pg, like:
//   CREATE DATABASE IF NOT EXISTS test_db;
//   DROP TABLE IF EXISTS test_db.test_insert_table;
//   CREATE TABLE test_db.test_insert_table (c0 integer, c1 timestamp, c2 datetime, c3 varchar(64), c4 double, c5 boolean);
// 2. specify db_kind and db_connection_str in commond like below: (please replace the '?' to correct value)
//   bazel-bin/engine/operator/insert_table_mysql_pg_test --output_db_kind="mysql"  --output_db_connection_str="db=test_db;user=?;password=?;host=?;port=?;"

// clang-format on
namespace scql::engine::op {

DECLARE_string(output_db_kind);
DECLARE_string(output_db_connection_str);

struct InsertMysqlOrPgTestCase {
  std::vector<test::NamedTensor> inputs;
  std::vector<pb::PrimitiveDataType> input_types;
  std::string tableName;
  std::vector<std::string> columnNames;
};

class InsertMysqlOrPgTest
    : public ::testing::TestWithParam<InsertMysqlOrPgTestCase> {
 protected:
  static pb::ExecNode MakeInsertTableExecNode(
      const InsertMysqlOrPgTestCase& tc);

  static void FeedInputs(ExecContext* ctxs, const InsertMysqlOrPgTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    InsertMysqlOrPgPrivateTest, InsertMysqlOrPgTest,
    testing::Values(InsertMysqlOrPgTestCase{
        .inputs =
            {test::NamedTensor("c0",
                               TensorFrom(arrow::int64(), "[0,1,null,3]")),
             test::NamedTensor("c1",
                               TensorFrom(arrow::int64(),
                                          "[10,946656000,null,1722244717]")),
             test::NamedTensor("c2", TensorFrom(arrow::int64(),
                                                "[0,10,null,1722244717]")),
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
        .tableName = "test_insert_table",
        .columnNames = {"c0", "c1", "c2", "c3", "c4", "c5"}}));

TEST_P(InsertMysqlOrPgTest, Works) {
  // Give
  auto tc = GetParam();
  auto node = MakeInsertTableExecNode(tc);
  auto session = test::Make1PCSession();
  ExecContext ctx(node, session.get());
  FeedInputs(&ctx, tc);

  // When
  InsertTable op;
  ASSERT_NO_THROW(op.Run(&ctx););

  // Then check output
  OdbcConnector connector(FLAGS_output_db_kind, FLAGS_output_db_connection_str);
  auto sess = connector.CreateSession();
  Poco::Data::Statement select(sess);
  select << "select * from " << tc.tableName << ";", Poco::Data::Keywords::now;
  EXPECT_EQ(select.columnsExtracted(), tc.inputs.size());
  EXPECT_EQ(select.rowsExtracted(), tc.inputs[0].tensor->Length());
}

/// ===========================
/// InsertMysqlOrPgTest impl
/// ===========================

pb::ExecNode InsertMysqlOrPgTest::MakeInsertTableExecNode(
    const InsertMysqlOrPgTestCase& tc) {
  test::ExecNodeBuilder builder(InsertTable::kOpType);

  builder.SetNodeName("insert-mysql-pg-test");
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

void InsertMysqlOrPgTest::FeedInputs(ExecContext* ctx,
                                     const InsertMysqlOrPgTestCase& tc) {
  test::FeedInputsAsPrivate(ctx, tc.inputs);
}

}  // namespace scql::engine::op

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}