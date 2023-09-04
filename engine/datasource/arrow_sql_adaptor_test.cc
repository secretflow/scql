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

#include "engine/datasource/arrow_sql_adaptor.h"

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "gtest/gtest.h"

ABSL_FLAG(std::string, url, "", "url of arrow sql server");

namespace scql::engine {
// you must mock these data in your arrow server
// create table
// "DROP TABLE IF EXISTS person"
// "CREATE TABLE person(name VARCHAR(30), age INTEGER(3))"
// insert some rows
//  "INSERT INTO person VALUES(\"alice\", 18)"
//  "INSERT INTO person VALUES(\"bob\", 20)"
//  "INSERT INTO person VALUES(\"carol\", NULL)"
//  "INSERT INTO person VALUES(NULL, NULL)"
class ArrowSqlAdaptorTest : public ::testing::Test {
 protected:
  std::string getUrl() { return absl::GetFlag(FLAGS_url); }
};

TEST_F(ArrowSqlAdaptorTest, works) {
  // Given
  ArrowSqlAdaptor adaptor(getUrl());

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
