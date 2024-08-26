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

#include "Poco/Data/SQLite/Connector.h"
#include "gtest/gtest.h"

#include "engine/datasource/odbc_adaptor.h"

namespace scql::engine {

class OdbcAdaptorSQLiteTest : public ::testing::Test {
 protected:
  void SetUp() override {
    Poco::Data::SQLite::Connector::registerConnector();

    session_ =
        std::make_unique<Poco::Data::Session>("SQLite", db_connection_str_);

    using Poco::Data::Keywords::now;
    // create table
    *session_
        << "CREATE TABLE person(name VARCHAR(30), age INTEGER(3), credit REAL)",
        now;
    // insert some rows
    *session_ << "INSERT INTO person VALUES(\"alice\", 18, 675.0)", now;
    *session_ << "INSERT INTO person VALUES(\"bob\", 20, 798.0)", now;
    *session_ << "INSERT INTO person VALUES(\"carol\", NULL, 880)", now;
    *session_ << "INSERT INTO person VALUES(NULL, NULL, NULL)", now;
  }

  // https://www.sqlite.org/inmemorydb.html
  std::string db_connection_str_ =
      "file:obdc_adaptor_test?mode=memory&cache=shared";
  std::unique_ptr<Poco::Data::Session> session_;
};

TEST_F(OdbcAdaptorSQLiteTest, works) {
  // Given
  OdbcAdaptor adaptor("sqlite", db_connection_str_);

  // When
  const std::string query = "SELECT name, age, credit FROM person";
  std::vector<ColumnDesc> outputs{{"name", pb::PrimitiveDataType::STRING},
                                  {"age", pb::PrimitiveDataType::INT32},
                                  {"credit", pb::PrimitiveDataType::FLOAT64}};

  auto results = adaptor.ExecQuery(query, outputs);

  // Then
  EXPECT_EQ(results.size(), 3);

  // column name
  EXPECT_EQ(results[0]->Length(), 4);
  EXPECT_EQ(results[0]->GetNullCount(), 1);

  // column age
  EXPECT_EQ(results[1]->Length(), 4);
  EXPECT_EQ(results[1]->GetNullCount(), 2);

  // column credit
  EXPECT_EQ(results[2]->Length(), 4);
  EXPECT_EQ(results[2]->GetNullCount(), 1);
}

}  // namespace scql::engine