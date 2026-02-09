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

#include "engine/datasource/csvdb_adaptor.h"

#include <exception>

#include "arrow/type.h"
#include "butil/files/temp_file.h"
#include "gflags/gflags.h"
#include "google/protobuf/util/json_util.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

#include "engine/datasource/csvdb_conf.pb.h"

namespace scql::engine {

DECLARE_string(restricted_read_path);

class CsvdbAdaptorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    FLAGS_restricted_read_path = "./";
    temp_file_ = std::make_unique<butil::TempFile>("csv");
    temp_file_->save(csv_content_.c_str());

    {
      csvdb_conf_.set_db_name("csvdb");
      auto table = csvdb_conf_.add_tables();
      table->set_table_name("staff");
      table->set_data_path(temp_file_->fname());

      auto column = table->add_columns();
      column->set_column_name("id");
      column->set_column_type("int64");
      column = table->add_columns();
      column->set_column_name("age");
      column->set_column_type("int64");
      column = table->add_columns();
      column->set_column_name("name");
      column->set_column_type("string");
      column = table->add_columns();
      column->set_column_name("salary");
      column->set_column_type("double");
      column = table->add_columns();
      column->set_column_name("dt");
      column->set_column_type("datetime");
      column = table->add_columns();
      column->set_column_name("tt");
      column->set_column_type("timestamp");
    }
    auto status = google::protobuf::util::MessageToJsonString(csvdb_conf_,
                                                              &csvdb_conf_str_);
    EXPECT_TRUE(status.ok());
  }

 public:
  csv::CsvdbConf csvdb_conf_;
  std::string csvdb_conf_str_;
  std::unique_ptr<butil::TempFile> temp_file_;
  std::string csv_content_ = R"csv(id, age, name, salary, dt, tt
1,21,alice,2100.2,2025-06-06 13:00:00,2025-06-06T13:00:00-07:00
2,42,bob,4500.8,2025-06-16 16:00:00,2025-06-16T16:00:00-07:00
3,19,carol,1900.5,2025-06-26 16:00:00,2025-06-26T16:00:00-07:00
4,NULL,NULL,NULL,NULL,NULL)csv";
};

TEST_F(CsvdbAdaptorTest, NormalQuery) {
  // Given
  CsvdbAdaptor csvdb_adaptor(csvdb_conf_str_);

  const std::string query = "select * from csvdb.staff";
  std::vector<ColumnDesc> outputs{{"id", pb::PrimitiveDataType::INT64},
                                  {"age", pb::PrimitiveDataType::INT64},
                                  {"name", pb::PrimitiveDataType::STRING},
                                  {"salary", pb::PrimitiveDataType::FLOAT64},
                                  {"dt", pb::PrimitiveDataType::DATETIME},
                                  {"tt", pb::PrimitiveDataType::TIMESTAMP}};

  // When
  auto results = csvdb_adaptor.ExecQuery(query, outputs);
  // Then
  EXPECT_EQ(results.size(), 6);
  scql::engine::op::test::CheckTensorEqual(
      results[0], TensorFrom(arrow::int64(), "[1,2,3,4]"));
  scql::engine::op::test::CheckTensorEqual(
      results[1], TensorFrom(arrow::int64(), "[21,42,19,null]"));
  scql::engine::op::test::CheckTensorEqual(
      results[2], TensorFrom(arrow::large_utf8(),
                             R"json(["alice","bob","carol",null])json"));
  scql::engine::op::test::CheckTensorEqual(
      results[3], TensorFrom(arrow::float64(), "[2100.2,4500.8,1900.5,null]"));
  scql::engine::op::test::CheckTensorEqual(
      results[4],
      TensorFrom(arrow::int64(), "[1749214800,1750089600,1750953600,null]"));
  scql::engine::op::test::CheckTensorEqual(
      results[5],
      TensorFrom(arrow::int64(), "[1749240000,1750114800,1750978800,null]"));
}

TEST_F(CsvdbAdaptorTest, WriteToFile) {
  // Given
  CsvdbAdaptor csvdb_adaptor(csvdb_conf_str_);

  const std::string query = "select * from csvdb.staff";
  std::vector<ColumnDesc> outputs{{"id", pb::PrimitiveDataType::INT64},
                                  {"age", pb::PrimitiveDataType::INT64},
                                  {"name", pb::PrimitiveDataType::STRING},
                                  {"salary", pb::PrimitiveDataType::FLOAT64},
                                  {"dt", pb::PrimitiveDataType::DATETIME},
                                  {"tt", pb::PrimitiveDataType::TIMESTAMP}};

  // When
  auto results = csvdb_adaptor.ExecQuery(
      query, outputs,
      TensorBuildOptions{.dump_to_disk = true,
                         .dump_dir = std::filesystem::temp_directory_path(),
                         .max_row_num_one_file = 2});

  // Then
  EXPECT_EQ(results.size(), 6);
  scql::engine::op::test::CheckTensorEqual(
      results[0], TensorFrom(arrow::int64(), "[1,2,3,4]"));
  scql::engine::op::test::CheckTensorEqual(
      results[1], TensorFrom(arrow::int64(), "[21,42,19,null]"));
  scql::engine::op::test::CheckTensorEqual(
      results[2], TensorFrom(arrow::large_utf8(),
                             R"json(["alice","bob","carol",null])json"));
  scql::engine::op::test::CheckTensorEqual(
      results[3], TensorFrom(arrow::float64(), "[2100.2,4500.8,1900.5,null]"));
  scql::engine::op::test::CheckTensorEqual(
      results[4],
      TensorFrom(arrow::int64(), "[1749214800,1750089600,1750953600,null]"));
  scql::engine::op::test::CheckTensorEqual(
      results[5],
      TensorFrom(arrow::int64(), "[1749240000,1750114800,1750978800,null]"));
}

TEST_F(CsvdbAdaptorTest, QueryWithAggregation) {
  // Given
  CsvdbAdaptor csvdb_adaptor(csvdb_conf_str_);

  const std::string query = "select SUM(age), SUM(salary) from csvdb.staff";
  std::vector<ColumnDesc> outputs{
      {"age_sum", pb::PrimitiveDataType::INT64},
      {"salary_sum", pb::PrimitiveDataType::FLOAT64}};

  // When
  auto results = csvdb_adaptor.ExecQuery(query, outputs);

  // Then
  EXPECT_EQ(results.size(), 2);
  scql::engine::op::test::CheckTensorEqual(results[0],
                                           TensorFrom(arrow::int64(), "[82]"));
  scql::engine::op::test::CheckTensorEqual(
      results[1], TensorFrom(arrow::float64(), "[8501.5]"));
}

TEST_F(CsvdbAdaptorTest, QueryWithPredicate) {
  // Given
  CsvdbAdaptor csvdb_adaptor(csvdb_conf_str_);

  const std::string query =
      "select age, name, salary from csvdb.staff where age > 30";
  std::vector<ColumnDesc> outputs{{"age", pb::PrimitiveDataType::INT64},
                                  {"name", pb::PrimitiveDataType::STRING},
                                  {"salary", pb::PrimitiveDataType::FLOAT64}};

  // When
  auto results = csvdb_adaptor.ExecQuery(query, outputs);

  // Then
  EXPECT_EQ(results.size(), 3);

  scql::engine::op::test::CheckTensorEqual(results[0],
                                           TensorFrom(arrow::int64(), "[42]"));
  scql::engine::op::test::CheckTensorEqual(
      results[1], TensorFrom(arrow::large_utf8(), R"json(["bob"])json"));
  scql::engine::op::test::CheckTensorEqual(
      results[2], TensorFrom(arrow::float64(), "[4500.8]"));
}

TEST_F(CsvdbAdaptorTest, QueryWithDomainDataID) {
  // Given
  csvdb_conf_.clear_db_name();
  ASSERT_EQ(csvdb_conf_.tables_size(), 1);
  csvdb_conf_.mutable_tables()->at(0).set_table_name(
      "usercredit-0afb3b4c-d160-4050-b71a-c6674a11d2f9");
  std::string conf_str;
  auto status =
      google::protobuf::util::MessageToJsonString(csvdb_conf_, &conf_str);
  EXPECT_TRUE(status.ok());
  CsvdbAdaptor csvdb_adaptor(conf_str);

  const std::string query =
      R"str(select "usercredit-0afb3b4c-d160-4050-b71a-c6674a11d2f9"."ID",
                   "usercredit-0afb3b4c-d160-4050-b71a-c6674a11d2f9"."Age",
                   "usercredit-0afb3b4c-d160-4050-b71a-c6674a11d2f9"."name",
                   "usercredit-0afb3b4c-d160-4050-b71a-c6674a11d2f9"."salarY"
            from "usercredit-0afb3b4c-d160-4050-b71a-c6674a11d2f9")str";
  std::vector<ColumnDesc> outputs{{"id", pb::PrimitiveDataType::INT64},
                                  {"age", pb::PrimitiveDataType::INT64},
                                  {"name", pb::PrimitiveDataType::STRING},
                                  {"salary", pb::PrimitiveDataType::FLOAT64}};

  // When
  auto results = csvdb_adaptor.ExecQuery(query, outputs);

  // Then
  EXPECT_EQ(results.size(), 4);
  scql::engine::op::test::CheckTensorEqual(
      results[0], TensorFrom(arrow::int64(), "[1,2,3,4]"));
  scql::engine::op::test::CheckTensorEqual(
      results[1], TensorFrom(arrow::int64(), "[21,42,19,null]"));
  scql::engine::op::test::CheckTensorEqual(
      results[2], TensorFrom(arrow::large_utf8(),
                             R"json(["alice","bob","carol",null])json"));
  scql::engine::op::test::CheckTensorEqual(
      results[3], TensorFrom(arrow::float64(), "[2100.2,4500.8,1900.5,null]"));
}

TEST_F(CsvdbAdaptorTest, MergeCsvdbDatasources) {
  // Given
  DataSource ds1;
  ds1.set_id("test-datasource-1");
  ds1.set_name("test-datasource-1");
  ds1.set_kind(DataSourceKind::CSVDB);

  csv::CsvdbConf conf1;
  {
    auto table = conf1.add_tables();
    table->set_table_name("table1");
    table->set_data_path("/path/to/table1.csv");

    auto column = table->add_columns();
    column->set_column_name("id");
    column->set_column_type("int64");
    column = table->add_columns();
    column->set_column_name("name");
    column->set_column_type("string");
  }
  std::string conf1_str;
  auto status = google::protobuf::util::MessageToJsonString(conf1, &conf1_str);
  EXPECT_TRUE(status.ok());
  ds1.set_connection_str(conf1_str);

  DataSource ds2;
  ds2.set_id("test-datasource-1");
  ds2.set_name("test-datasource-2");
  ds2.set_kind(DataSourceKind::CSVDB);

  csv::CsvdbConf conf2;
  {
    auto table = conf2.add_tables();
    table->set_table_name("table2");
    table->set_data_path("/path/to/table2.csv");

    auto column = table->add_columns();
    column->set_column_name("age");
    column->set_column_type("int32");
    column = table->add_columns();
    column->set_column_name("salary");
    column->set_column_type("double");
  }
  std::string conf2_str;
  status = google::protobuf::util::MessageToJsonString(conf2, &conf2_str);
  EXPECT_TRUE(status.ok());
  ds2.set_connection_str(conf2_str);

  std::vector<DataSource> datasources = {ds1, ds2};

  // When
  auto merged = MergeCsvdbDatasources(datasources);

  // Then
  EXPECT_EQ(merged.id(), "test-datasource-1");
  EXPECT_EQ(merged.kind(), DataSourceKind::CSVDB);

  csv::CsvdbConf merged_conf;
  status = google::protobuf::util::JsonStringToMessage(merged.connection_str(),
                                                       &merged_conf);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(merged_conf.tables_size(), 2);

  // Check first table
  const auto& merged_table1 = merged_conf.tables(0);
  EXPECT_EQ(merged_table1.table_name(), "table1");
  EXPECT_EQ(merged_table1.data_path(), "/path/to/table1.csv");
  EXPECT_EQ(merged_table1.columns_size(), 2);
  EXPECT_EQ(merged_table1.columns(0).column_name(), "id");
  EXPECT_EQ(merged_table1.columns(0).column_type(), "int64");
  EXPECT_EQ(merged_table1.columns(1).column_name(), "name");
  EXPECT_EQ(merged_table1.columns(1).column_type(), "string");

  // Check second table
  const auto& merged_table2 = merged_conf.tables(1);
  EXPECT_EQ(merged_table2.table_name(), "table2");
  EXPECT_EQ(merged_table2.data_path(), "/path/to/table2.csv");
  EXPECT_EQ(merged_table2.columns_size(), 2);
  EXPECT_EQ(merged_table2.columns(0).column_name(), "age");
  EXPECT_EQ(merged_table2.columns(0).column_type(), "int32");
  EXPECT_EQ(merged_table2.columns(1).column_name(), "salary");
  EXPECT_EQ(merged_table2.columns(1).column_type(), "double");
}

}  // namespace scql::engine
