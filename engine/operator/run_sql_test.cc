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

#include "engine/operator/run_sql.h"

#include "Poco/Data/SQLite/Connector.h"
#include "Poco/Data/Session.h"
#include "absl/debugging/failure_signal_handler.h"
#include "absl/debugging/symbolize.h"
#include "absl/flags/parse.h"
#include "gtest/gtest.h"

#include "engine/datasource/datasource_adaptor_mgr.h"
#include "engine/datasource/embed_router.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

class RunSQLTest : public ::testing::Test {
 protected:
  void SetUp() override {
    Poco::Data::SQLite::Connector::registerConnector();

    session_ =
        std::make_unique<Poco::Data::Session>("SQLite", db_connection_str_);

    using Poco::Data::Keywords::now;
    // create table
    *session_ << "CREATE TABLE person(name VARCHAR(30), age INTEGER(3))", now;
    // insert some rows
    *session_ << "INSERT INTO person VALUES(\"alice\", 18)", now;
    *session_ << "INSERT INTO person VALUES(\"bob\", 20)", now;
    *session_ << "INSERT INTO person VALUES(\"carol\", NULL)", now;
    *session_ << "INSERT INTO person VALUES(NULL, NULL)", now;
  }

 protected:
  std::unique_ptr<Poco::Data::Session> session_;
  // https://www.sqlite.org/inmemorydb.html
  std::string db_connection_str_ = "file:runsql_test?mode=memory&cache=shared";
  std::string embed_router_conf_ = R"json({
      "datasources": [
        {
          "id": "ds001",
          "name": "sqlite3",
          "kind": "SQLITE",
          "connection_str": "file:runsql_test?mode=memory&cache=shared"
        }
      ],
      "rules":[
        {
          "db": "*",
          "table": "*",
          "datasource_id": "ds001"
        }
      ]
    })json";
};

TEST_F(RunSQLTest, normal) {
  // Given
  const std::string query = "SELECT name, age FROM person";
  const std::string out_name = "person.name";
  const std::string out_age = "person.age";
  const std::vector<std::string> table_refs = {"db.person"};

  test::ExecNodeBuilder node_builder(RunSQL::kOpType);
  node_builder.AddStringAttr(RunSQL::kSQLAttr, query);
  node_builder.AddStringsAttr(RunSQL::kTableRefsAttr, table_refs);
  auto out0 = test::MakeTensorReference(out_name, pb::PrimitiveDataType::STRING,
                                        pb::TensorStatus::TENSORSTATUS_PRIVATE);
  // test type cast: INT64 -> INT32
  auto out1 = test::MakeTensorReference(out_age, pb::PrimitiveDataType::INT32,
                                        pb::TensorStatus::TENSORSTATUS_PRIVATE);
  node_builder.AddOutput(RunSQL::kOut, {out0, out1});
  auto node = node_builder.Build();

  std::unique_ptr<Router> router = EmbedRouter::FromJsonStr(embed_router_conf_);
  DatasourceAdaptorMgr ds_mgr;
  auto session = test::Make1PCSession(router.get(), &ds_mgr);
  ExecContext ctx(node, session.get());
  // When
  RunSQL op;
  op.Run(&ctx);
  // Then
  auto out_tensor = ctx.GetTensorTable()->GetTensor(out_name);
  ASSERT_NE(nullptr, out_tensor);
  EXPECT_EQ(out_tensor->Type(), pb::PrimitiveDataType::STRING);
  EXPECT_EQ(out_tensor->Length(), 4);
  EXPECT_EQ(out_tensor->GetNullCount(), 1);

  out_tensor = ctx.GetTensorTable()->GetTensor(out_age);
  ASSERT_NE(nullptr, out_tensor);
  EXPECT_EQ(out_tensor->Type(), pb::PrimitiveDataType::INT32);
  EXPECT_EQ(out_tensor->Length(), 4);
  EXPECT_EQ(out_tensor->GetNullCount(), 2);
}

}  // namespace scql::engine::op

int main(int argc, char* argv[]) {
  {
    absl::InitializeSymbolizer(argv[0]);

    absl::FailureSignalHandlerOptions options;
    absl::InstallFailureSignalHandler(options);
  }

  ::testing::InitGoogleTest(&argc, argv);

  absl::ParseCommandLine(argc, argv);
  return RUN_ALL_TESTS();
}