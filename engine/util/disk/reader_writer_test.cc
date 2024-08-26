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

#include <filesystem>

#include "arrow/array.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "gtest/gtest.h"

#include "engine/util/disk/arrow_reader.h"
#include "engine/util/disk/arrow_writer.h"
#include "engine/util/filepath_helper.h"

namespace scql::engine::util::disk {

class ReaderWriterTest : public ::testing::Test {
 public:
  ReaderWriterTest()
      : tmp_dir_(util::ScopedDir(util::CreateDirWithRandSuffix(
            std::filesystem::temp_directory_path(), "test"))) {}

 protected:
  util::ScopedDir tmp_dir_;
};

TEST_F(ReaderWriterTest, ReadAndWrite) {
  constexpr arrow::random::SeedType randomSeed = 0x0ff1ce;
  arrow::ArrayVector arrays;
  constexpr size_t array_num = 3;
  constexpr size_t array_num_rows = 1000;
  auto field = std::make_shared<arrow::Field>("a", arrow::decimal128(30, 0));
  for (size_t i = 0; i < array_num; ++i) {
    arrays.push_back(
        arrow::random::GenerateArray(*field, array_num_rows, randomSeed));
  }
  arrow::FieldVector fields = {field};
  auto schema = std::make_shared<arrow::Schema>(fields);
  auto path = tmp_dir_.path() / "test.arrow";
  auto expected_chunked_array = std::make_shared<arrow::ChunkedArray>(arrays);
  {
    ArrowWriter writer(schema, path.string());
    auto write_num = writer.WriteBatch(*expected_chunked_array);
    ASSERT_EQ(array_num * array_num_rows, write_num);
  }
  {
    auto chunked_array = ReadFileArray(path.string());
    ASSERT_EQ(array_num * array_num_rows, chunked_array->length());
    EXPECT_TRUE(chunked_array->Equals(expected_chunked_array))
        << "\nexpect=" << expected_chunked_array->ToString()
        << "\n,but got=" << chunked_array->ToString();
  }
}

}  // namespace scql::engine::util::disk