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

#include "arrow/type.h"
#include "butil/files/temp_file.h"
#include "gflags/gflags.h"
#include "google/protobuf/util/json_util.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_from_json.h"

#include "engine/datasource/csvdb_conf.pb.h"

namespace scql::engine {

DECLARE_string(restricted_read_path);

class CsvdbAdaptorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    FLAGS_restricted_read_path = "./";
    temp_file_ = std::make_unique<butil::TempFile>("csv");
    temp_file_->save(csv_content_.c_str());

    csv::CsvdbConf csvdb_conf;
    {
      csvdb_conf.set_db_name("csvdb");
      auto table = csvdb_conf.add_tables();
      table->set_table_name("staff");
      table->set_data_path(temp_file_->fname());

      auto column = table->add_columns();
      column->set_column_name("id");
      column->set_column_type(csv::ColumnType::LONG);
      column = table->add_columns();
      column->set_column_name("age");
      column->set_column_type(csv::ColumnType::LONG);
      column = table->add_columns();
      column->set_column_name("name");
      column->set_column_type(csv::ColumnType::STRING);
      column = table->add_columns();
      column->set_column_name("salary");
      column->set_column_type(csv::ColumnType::DOUBLE);
    }
    auto status = google::protobuf::util::MessageToJsonString(csvdb_conf,
                                                              &csvdb_conf_str_);
    EXPECT_TRUE(status.ok());
  }

  void CheckTensorEqual(TensorPtr left, TensorPtr right) {
    auto left_arr = left->ToArrowChunkedArray();
    auto right_arr = right->ToArrowChunkedArray();

    EXPECT_EQ(left_arr->type(), right_arr->type())
        << "left type = " << left_arr->type()
        << ",right type = " << right_arr->type();

    EXPECT_TRUE(left_arr->ApproxEquals(*right_arr))
        << "left = " << left_arr->ToString()
        << "\nright = " << right_arr->ToString();
  }

 public:
  std::string csvdb_conf_str_;
  std::unique_ptr<butil::TempFile> temp_file_;
  std::string csv_content_ = R"csv(id, age, name, salary
1,21,alice,2100.2
2,42,bob,4500.8
3,19,carol,1900.5
4,32,dave,8900.0)csv";
};

TEST_F(CsvdbAdaptorTest, NormalQuery) {
  // Given
  CsvdbAdaptor csvdb_adaptor(csvdb_conf_str_);

  const std::string query = "select * from csvdb.staff";
  std::vector<ColumnDesc> outputs{{"id", pb::PrimitiveDataType::INT64},
                                  {"age", pb::PrimitiveDataType::INT64},
                                  {"name", pb::PrimitiveDataType::STRING},
                                  {"salary", pb::PrimitiveDataType::FLOAT64}};

  // When
  auto results = csvdb_adaptor.ExecQuery(query, outputs);

  // Then
  EXPECT_EQ(results.size(), 4);
  CheckTensorEqual(results[0], TensorFromJSON(arrow::int64(), "[1,2,3,4]"));
  CheckTensorEqual(results[1], TensorFromJSON(arrow::int64(), "[21,42,19,32]"));
  CheckTensorEqual(results[2],
                   TensorFromJSON(arrow::utf8(),
                                  R"json(["alice","bob","carol","dave"])json"));
  CheckTensorEqual(results[3], TensorFromJSON(arrow::float64(),
                                              "[2100.2,4500.8,1900.5,8900]"));
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

  CheckTensorEqual(results[0], TensorFromJSON(arrow::int64(), "[42,32]"));
  CheckTensorEqual(results[1],
                   TensorFromJSON(arrow::utf8(), R"json(["bob", "dave"])json"));
  CheckTensorEqual(results[2],
                   TensorFromJSON(arrow::float64(), "[4500.8,8900]"));
}

}  // namespace scql::engine