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

#include "engine/core/tensor_constructor.h"

#include <filesystem>

#include "arrow/array.h"
#include "arrow/compute/cast.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "gtest/gtest.h"

#include "engine/util/filepath_helper.h"

namespace scql::engine {

class ReaderWriterTest : public ::testing::Test {
 public:
  ReaderWriterTest()
      : tmp_dir_(util::ScopedDir(util::CreateDirWithRandSuffix(
            std::filesystem::temp_directory_path(), "test"))) {}

 protected:
  util::ScopedDir tmp_dir_;
};

TEST_F(ReaderWriterTest, WriteOneFile) {
  constexpr arrow::random::SeedType randomSeed = 0x0ff1ce;
  arrow::ArrayVector arrays;
  constexpr size_t array_num = 3;
  constexpr size_t array_num_rows = 1000;
  auto field = std::make_shared<arrow::Field>("a", arrow::int64());
  for (size_t i = 0; i < array_num; ++i) {
    arrays.push_back(
        arrow::random::GenerateArray(*field, array_num_rows, randomSeed));
  }
  arrow::FieldVector fields = {field};
  auto schema = std::make_shared<arrow::Schema>(fields);
  auto expected_chunked_array = std::make_shared<arrow::ChunkedArray>(arrays);
  TensorPtr ptr;
  {
    TensorWriter writer(schema, tmp_dir_.path().string());
    auto write_num = writer.WriteBatch(*expected_chunked_array);
    ASSERT_EQ(array_num * array_num_rows, write_num);
    writer.Finish(&ptr);
    ASSERT_EQ(write_num, ptr->Length());
  }
  {
    auto chunked_array = ptr->ToArrowChunkedArray();
    ASSERT_EQ(array_num * array_num_rows, chunked_array->length());
    EXPECT_TRUE(chunked_array->Equals(expected_chunked_array))
        << "\nexpect=" << expected_chunked_array->ToString()
        << "\n,but got=" << chunked_array->ToString();
  }
}

TEST_F(ReaderWriterTest, WriteMultiFile) {
  constexpr arrow::random::SeedType randomSeed = 0x0ff1ce;
  arrow::ArrayVector arrays;
  constexpr size_t array_num = 3;
  constexpr size_t array_num_rows = 1000;
  auto field = std::make_shared<arrow::Field>("a", arrow::large_utf8());
  for (size_t i = 0; i < array_num; ++i) {
    arrays.push_back(
        arrow::random::GenerateArray(*field, array_num_rows, randomSeed));
  }
  arrow::FieldVector fields = {field};
  auto schema = std::make_shared<arrow::Schema>(fields);
  auto expected_chunked_array = std::make_shared<arrow::ChunkedArray>(arrays);
  TensorPtr ptr;
  {
    size_t file_row_num = 500;
    TensorWriter writer(schema, tmp_dir_.path().string(), file_row_num);
    auto write_num = writer.WriteBatch(*expected_chunked_array);
    ASSERT_EQ(array_num * array_num_rows, write_num);
    writer.Finish(&ptr);
    // 1000 / 500 * 3 = 6
    ASSERT_EQ(6, writer.GetFilesNum());
  }
  {
    auto chunked_array = ptr->ToArrowChunkedArray();
    ASSERT_EQ(array_num * array_num_rows, chunked_array->length());
    EXPECT_TRUE(chunked_array->Equals(expected_chunked_array))
        << "\nexpect=" << expected_chunked_array->ToString()
        << "\n,but got=" << chunked_array->ToString();
  }
}

TEST_F(ReaderWriterTest, WriteNullArray) {
  arrow::ArrayVector arrays;
  auto expected_chunked_array =
      std::make_shared<arrow::ChunkedArray>(arrays, arrow::large_utf8());
  TensorPtr ptr;
  {
    size_t file_row_num = 500;
    TensorWriter writer("a", arrow::large_utf8(), tmp_dir_.path().string(),
                        file_row_num);
    auto write_num = writer.WriteBatch(*expected_chunked_array);
    ASSERT_EQ(0, write_num);
    writer.Finish(&ptr);
    ASSERT_EQ(0, writer.GetFilesNum());
  }
  {
    auto chunked_array = ptr->ToArrowChunkedArray();
    ASSERT_EQ(0, chunked_array->length());
    EXPECT_TRUE(chunked_array->Equals(expected_chunked_array))
        << "\nexpect=" << expected_chunked_array->ToString()
        << "\n,but got=" << chunked_array->ToString();
  }
}

TEST_F(ReaderWriterTest, TypeConvert) {
  constexpr arrow::random::SeedType randomSeed = 0x0ff1ce;
  arrow::ArrayVector arrays;
  constexpr size_t array_num = 3;
  constexpr size_t array_num_rows = 1000;
  auto field = std::make_shared<arrow::Field>("a", arrow::decimal128(5, 0));
  for (size_t i = 0; i < array_num; ++i) {
    arrays.push_back(
        arrow::random::GenerateArray(*field, array_num_rows, randomSeed));
  }
  arrow::FieldVector fields = {field};
  auto schema = std::make_shared<arrow::Schema>(fields);
  auto expected_chunked_array = std::make_shared<arrow::ChunkedArray>(arrays);
  TensorPtr ptr;
  {
    TensorWriter writer(schema, tmp_dir_.path().string());
    auto write_num = writer.WriteBatch(*expected_chunked_array);
    ASSERT_EQ(array_num * array_num_rows, write_num);
    writer.Finish(&ptr);
    ASSERT_EQ(write_num, ptr->Length());
    ASSERT_EQ(arrow::int64(), ptr->ArrowType());
  }
  {
    auto chunked_array = ptr->ToArrowChunkedArray();
    arrow::compute::CastOptions options;
    options.allow_decimal_truncate = true;
    auto result =
        arrow::compute::Cast(expected_chunked_array, arrow::int64(), options)
            .ValueOrDie()
            .chunked_array();
    ASSERT_EQ(array_num * array_num_rows, chunked_array->length());
    EXPECT_TRUE(chunked_array->Equals(result))
        << "\nexpect=" << result->ToString()
        << "\n,but got=" << chunked_array->ToString();
  }
}

}  // namespace scql::engine