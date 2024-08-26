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

#include "Poco/Data/MySQL/Connector.h"
#include "Poco/Data/MySQL/MySQLException.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "gtest/gtest.h"

#include "engine/datasource/odbc_adaptor.h"

ABSL_FLAG(
    std::string, connection_str, "",
    "connection string for mysql, checkout it's format on: "
    "https://docs.pocoproject.org/current/Poco.Data.MySQL.SessionImpl.html");

namespace scql::engine {

class OdbcAdaptorMySQLTest : public ::testing::Test {
 protected:
  void SetUp() override {
    Poco::Data::MySQL::Connector::registerConnector();

    std::string connection_str = getConnectionStr();
    try {
      session_ = std::make_unique<Poco::Data::Session>("mysql", connection_str);

      using Poco::Data::Keywords::now;
      // create table
      *session_ << "DROP TABLE IF EXISTS person", now;
      *session_ << "CREATE TABLE person(name VARCHAR(30), age INTEGER(3))", now;
      // insert some rows
      *session_ << "INSERT INTO person VALUES(\"alice\", 18)", now;
      *session_ << "INSERT INTO person VALUES(\"bob\", 20)", now;
      *session_ << "INSERT INTO person VALUES(\"carol\", NULL)", now;
      *session_ << "INSERT INTO person VALUES(NULL, NULL)", now;
    } catch (const Poco::Data::MySQL::MySQLException& e) {
      FAIL() << "catch MySQL exception: " << e.displayText();
    } catch (const Poco::Data::DataException& e) {
      FAIL() << "catch poco data exception: " << e.displayText();
    } catch (const std::exception& e) {
      FAIL() << "catch std::exception: " << e.what();
    }
  }

  std::string getConnectionStr() { return absl::GetFlag(FLAGS_connection_str); }

  std::unique_ptr<Poco::Data::Session> session_;
};

TEST_F(OdbcAdaptorMySQLTest, works) {
  // Given
  OdbcAdaptor adaptor("mysql", getConnectionStr());

  // When
  const std::string query = "SELECT name, age FROM person";
  std::vector<ColumnDesc> outputs{{"name", pb::PrimitiveDataType::STRING},
                                  {"age", pb::PrimitiveDataType::INT32}};

  auto results = adaptor.ExecQuery(query, outputs);

  // Then
  EXPECT_EQ(results.size(), 2);

  // column name
  EXPECT_EQ(results[0]->Length(), 4);
  EXPECT_EQ(results[0]->GetNullCount(), 1);

  // column age
  EXPECT_EQ(results[1]->Length(), 4);
  EXPECT_EQ(results[1]->GetNullCount(), 2);
}

}  // namespace scql::engine

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  absl::ParseCommandLine(argc, argv);
  return RUN_ALL_TESTS();
}
