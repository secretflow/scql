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

#include "engine/datasource/embed_router.h"

#include "gtest/gtest.h"
#include "yacl/base/exception.h"

namespace scql::engine {

class EmbedRouterTest : public ::testing::Test {};

TEST_F(EmbedRouterTest, FromJsonStr) {
  // Given
  const std::string conf = R"json({
    "datasources": [
        {
            "id": "ds001",
            "name": "mysql db",
            "kind": "MYSQL",
            "connection_str": "host=127.0.0.1 db=test"
        },
        {
            "id": "ds002",
            "name": "sqlite db",
            "kind": "SQLITE",
            "connection_str": "file:obdc_adaptor_test?mode=memory&cache=shared"
        },
        {
            "id": "ds003",
            "name": "postgresql db",
            "kind": "POSTGRESQL",
            "connection_str": "test str for postgresql"
        }
    ],
    "rules": [
        {
            "db": "d1",
            "table": "t1",
            "datasource_id": "ds001"
        },
        {
            "db": "d2",
            "table": "*",
            "datasource_id": "ds001"
        },
        {
            "db": "d3",
            "table": "*",
            "datasource_id": "ds002"
        },
        {
            "db": "*",
            "table": "*",
            "datasource_id": "ds001"
        },
        {
            "db": "postgresql",
            "table": "t1",
            "datasource_id": "ds003"
        }
    ]
}
)json";

  // When
  std::unique_ptr<EmbedRouter> router;

  EXPECT_NO_THROW({ router = EmbedRouter::FromJsonStr(conf); });

  EXPECT_TRUE(router != nullptr);

  std::vector<DataSource> datasources;
  EXPECT_NO_THROW({
    datasources = router->Route(std::vector<std::string>{
        "d1.t1", "d1.t2", "d2.t1", "d3.t3", "postgresql.t1"});
  });

  // Then
  EXPECT_EQ(datasources.size(), 5);
  EXPECT_EQ(datasources[0].id(), "ds001");
  EXPECT_EQ(datasources[0].kind(), DataSourceKind::MYSQL);

  EXPECT_EQ(datasources[1].id(), "ds001");

  EXPECT_EQ(datasources[2].id(), "ds001");

  EXPECT_EQ(datasources[3].id(), "ds002");
  EXPECT_EQ(datasources[3].kind(), DataSourceKind::SQLITE);

  EXPECT_EQ(datasources[4].id(), "ds003");
  EXPECT_EQ(datasources[4].kind(), DataSourceKind::POSTGRESQL);
}

TEST_F(EmbedRouterTest, FromConnectionStr) {
  // Given
  const std::string connection_str = "host=127.0.0.1 db=test";

  // When
  std::unique_ptr<EmbedRouter> router;

  EXPECT_NO_THROW({ router = EmbedRouter::FromConnectionStr(connection_str); });

  EXPECT_TRUE(router != nullptr);

  std::vector<DataSource> datasources;
  EXPECT_NO_THROW({
    datasources =
        router->Route(std::vector<std::string>{"d1.t1", "d1.*", "*.*"});
  });

  // Then
  EXPECT_EQ(datasources.size(), 3);
  for (size_t i = 0; i < datasources.size(); ++i) {
    EXPECT_EQ(datasources[i].id(), "ds001");
    EXPECT_EQ(datasources[i].kind(), DataSourceKind::MYSQL);
  }
}

TEST_F(EmbedRouterTest, FromFilePaths_SingleTableNames_ShouldThrow) {
  // Given: Input with single table names
  google::protobuf::Map<std::string, std::string> input_file_paths;
  input_file_paths["table1"] = "/path/to/table1.csv";

  // When & Then: Should throw exception for invalid format
  std::unique_ptr<EmbedRouter> router;
  EXPECT_THROW(
      { router = EmbedRouter::FromFilePaths(input_file_paths); },
      yacl::Exception);
}

TEST_F(EmbedRouterTest, FromFilePaths_SingleDbMultipleTables) {
  // Given: Input with single database and multiple tables
  google::protobuf::Map<std::string, std::string> input_file_paths;
  input_file_paths["testdb.table1"] = "/path/to/table1.csv";
  input_file_paths["testdb.table2"] = "/path/to/table2.csv";
  input_file_paths["testdb.tbl_0"] = "/data/tbl_0.csv";

  // When
  std::unique_ptr<EmbedRouter> router;
  EXPECT_NO_THROW({ router = EmbedRouter::FromFilePaths(input_file_paths); });

  EXPECT_TRUE(router != nullptr);

  // Then: Test routing for db.table format
  std::vector<DataSource> datasources;
  EXPECT_NO_THROW({
    datasources = router->Route(std::vector<std::string>{
        "testdb.table1", "testdb.table2", "testdb.tbl_0"});
  });

  EXPECT_EQ(datasources.size(), 3);
  for (const auto& datasource : datasources) {
    EXPECT_EQ(datasource.id(), "csvdb_testdb");
    EXPECT_EQ(datasource.kind(), DataSourceKind::CSVDB);
  }
}

TEST_F(EmbedRouterTest, FromFilePaths_MultipleDatabases_ShouldThrow) {
  // Given: Input with multiple databases
  google::protobuf::Map<std::string, std::string> input_file_paths;
  input_file_paths["db1.table1"] = "/path/to/db1_table1.csv";
  input_file_paths["db2.table2"] = "/path/to/db2_table2.csv";

  // When & Then: Should throw exception for multiple databases
  std::unique_ptr<EmbedRouter> router;
  EXPECT_THROW(
      { router = EmbedRouter::FromFilePaths(input_file_paths); },
      yacl::Exception);
}

TEST_F(EmbedRouterTest, FromFilePaths_InvalidFormat) {
  // Given: Invalid table reference format (too many dots)
  google::protobuf::Map<std::string, std::string> input_file_paths;
  input_file_paths["db.table.invalid"] = "/path/to/invalid.csv";

  // When & Then: Should throw exception
  std::unique_ptr<EmbedRouter> router;
  EXPECT_THROW(
      { router = EmbedRouter::FromFilePaths(input_file_paths); },
      yacl::Exception);
}

TEST_F(EmbedRouterTest, FromFilePaths_EmptyInput_ShouldThrow) {
  // Given: Empty input map
  google::protobuf::Map<std::string, std::string> input_file_paths;

  // When & Then: Should throw exception for empty input
  std::unique_ptr<EmbedRouter> router;
  EXPECT_THROW(
      { router = EmbedRouter::FromFilePaths(input_file_paths); },
      yacl::Exception);
}

}  // namespace scql::engine
