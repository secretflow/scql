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

#include "engine/core/tensor_batch_reader.h"

#include <filesystem>

#include "arrow/array.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/util/filepath_helper.h"

namespace scql::engine {

class TensorBatchReaderTest : public ::testing::Test {
 public:
  TensorBatchReaderTest()
      : tmp_dir_(util::ScopedDir(util::CreateDirWithRandSuffix(
            std::filesystem::temp_directory_path(), "test"))) {}

 protected:
  util::ScopedDir tmp_dir_;
};

TEST_F(TensorBatchReaderTest, ReadDisk) {
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
  auto chunked_array = std::make_shared<arrow::ChunkedArray>(arrays);
  TensorPtr ptr;
  {
    TensorWriter writer(schema, tmp_dir_.path().string());
    auto write_num = writer.WriteBatch(*chunked_array);
    ASSERT_EQ(array_num * array_num_rows, write_num);
    writer.Finish(&ptr);
  }
  size_t batch_size = 400;
  auto reader = ptr->CreateBatchReader(batch_size);
  size_t offset = 0;
  while (true) {
    auto array = reader->Next();
    if (!array) break;
    offset += array->length();
    ASSERT_TRUE(batch_size == static_cast<size_t>(array->length()) ||
                offset == array_num * array_num_rows);
  }
  ASSERT_TRUE(offset == array_num * array_num_rows);
}

TEST_F(TensorBatchReaderTest, ReadDiskOnlyOneBatch) {
  constexpr arrow::random::SeedType randomSeed = 0x0ff1ce;
  arrow::ArrayVector arrays;
  constexpr size_t array_num = 1;
  constexpr size_t array_num_rows = 1000;
  auto field = std::make_shared<arrow::Field>("a", arrow::int64());
  for (size_t i = 0; i < array_num; ++i) {
    arrays.push_back(
        arrow::random::GenerateArray(*field, array_num_rows, randomSeed));
  }
  arrow::FieldVector fields = {field};
  auto schema = std::make_shared<arrow::Schema>(fields);
  auto chunked_array = std::make_shared<arrow::ChunkedArray>(arrays);
  TensorPtr ptr;
  {
    TensorWriter writer(schema, tmp_dir_.path().string());
    auto write_num = writer.WriteBatch(*chunked_array);
    ASSERT_EQ(array_num * array_num_rows, write_num);
    writer.Finish(&ptr);
  }
  size_t batch_size = 1000;
  auto reader = ptr->CreateBatchReader(batch_size);
  size_t offset = 0;
  while (true) {
    auto array = reader->Next();
    if (!array) break;
    offset += array->length();
    ASSERT_TRUE(batch_size == static_cast<size_t>(array->length()) ||
                offset == array_num * array_num_rows);
  }
  ASSERT_TRUE(offset == array_num * array_num_rows);
}

TEST_F(TensorBatchReaderTest, ReadMemory) {
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
  auto chunked_array = std::make_shared<arrow::ChunkedArray>(arrays);
  TensorPtr ptr = std::make_shared<MemTensor>(chunked_array);
  size_t batch_size = 400;
  auto reader = ptr->CreateBatchReader(batch_size);
  size_t offset = 0;
  while (true) {
    auto array = reader->Next();
    if (!array) break;
    offset += array->length();
    ASSERT_TRUE(batch_size == static_cast<size_t>(array->length()) ||
                offset == array_num * array_num_rows);
  }
  ASSERT_TRUE(offset == array_num * array_num_rows);
}

}  // namespace scql::engine