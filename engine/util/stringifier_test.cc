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

#include "engine/util/stringifier.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <gtest/gtest.h>

namespace scql::engine::util {

class StringifierTest : public ::testing::Test {
 protected:
  void SetUp() override {}
};

TEST_F(StringifierTest, StringifyBooleanArray) {
  // Create boolean array with true, false, null values
  arrow::BooleanBuilder builder;
  ASSERT_TRUE(builder.Append(true).ok());
  ASSERT_TRUE(builder.Append(false).ok());
  ASSERT_TRUE(builder.AppendNull().ok());
  ASSERT_TRUE(builder.Append(true).ok());

  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(builder.Finish(&array).ok());

  auto result = Stringify(array);

  ASSERT_EQ(4, result.size());
  EXPECT_EQ("true", result[0]);
  EXPECT_EQ("false", result[1]);
  EXPECT_EQ("null", result[2]);
  EXPECT_EQ("true", result[3]);
}

TEST_F(StringifierTest, StringifyInt32Array) {
  // Create int32 array with positive, negative, and null values
  arrow::Int32Builder builder;
  ASSERT_TRUE(builder.Append(42).ok());
  ASSERT_TRUE(builder.Append(-123).ok());
  ASSERT_TRUE(builder.AppendNull().ok());
  ASSERT_TRUE(builder.Append(0).ok());
  ASSERT_TRUE(builder.Append(2147483647).ok());   // Max int32
  ASSERT_TRUE(builder.Append(-2147483648).ok());  // Min int32

  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(builder.Finish(&array).ok());

  auto result = Stringify(array);

  ASSERT_EQ(6, result.size());
  EXPECT_EQ("42", result[0]);
  EXPECT_EQ("-123", result[1]);
  EXPECT_EQ("null", result[2]);
  EXPECT_EQ("0", result[3]);
  EXPECT_EQ("2147483647", result[4]);
  EXPECT_EQ("-2147483648", result[5]);
}

TEST_F(StringifierTest, StringifyInt64Array) {
  // Create int64 array with large values
  arrow::Int64Builder builder;
  ASSERT_TRUE(builder.Append(9223372036854775807LL).ok());       // Max int64
  ASSERT_TRUE(builder.Append(-9223372036854775807LL - 1).ok());  // Min int64
  ASSERT_TRUE(builder.Append(0).ok());
  ASSERT_TRUE(builder.AppendNull().ok());

  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(builder.Finish(&array).ok());

  auto result = Stringify(array);

  ASSERT_EQ(4, result.size());
  EXPECT_EQ("9223372036854775807", result[0]);
  EXPECT_EQ("-9223372036854775808", result[1]);
  EXPECT_EQ("0", result[2]);
  EXPECT_EQ("null", result[3]);
}

TEST_F(StringifierTest, StringifyFloatArray) {
  // Create float array with various floating point values
  arrow::FloatBuilder builder;
  ASSERT_TRUE(builder.Append(3.14159f).ok());
  ASSERT_TRUE(builder.Append(-2.71828f).ok());
  ASSERT_TRUE(builder.Append(0.0f).ok());
  ASSERT_TRUE(builder.AppendNull().ok());
  ASSERT_TRUE(builder.Append(std::numeric_limits<float>::infinity()).ok());
  ASSERT_TRUE(builder.Append(-std::numeric_limits<float>::infinity()).ok());

  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(builder.Finish(&array).ok());

  auto result = Stringify(array);

  ASSERT_EQ(6, result.size());
  EXPECT_EQ("3.14159", result[0]);
  EXPECT_EQ("-2.71828", result[1]);
  EXPECT_EQ("0", result[2]);
  EXPECT_EQ("null", result[3]);
  EXPECT_EQ("inf", result[4]);
  EXPECT_EQ("-inf", result[5]);
}

TEST_F(StringifierTest, StringifyDoubleArray) {
  // Create double array with high precision values
  arrow::DoubleBuilder builder;
  ASSERT_TRUE(builder.Append(3.141592653589793).ok());
  ASSERT_TRUE(builder.Append(-2.718281828459045).ok());
  ASSERT_TRUE(builder.Append(0.0).ok());
  ASSERT_TRUE(builder.Append(1).ok());
  ASSERT_TRUE(builder.AppendNull().ok());

  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(builder.Finish(&array).ok());

  auto result = Stringify(array);

  ASSERT_EQ(5, result.size());
  EXPECT_EQ("3.141592653589793", result[0]);
  EXPECT_EQ("-2.718281828459045", result[1]);
  EXPECT_EQ("0", result[2]);
  EXPECT_EQ("1", result[3]);
  EXPECT_EQ("null", result[4]);
}

TEST_F(StringifierTest, StringifyStringArray) {
  // Create string array with various string values
  arrow::StringBuilder builder;
  ASSERT_TRUE(builder.Append("hello").ok());
  ASSERT_TRUE(builder.Append("world").ok());
  ASSERT_TRUE(builder.Append("").ok());  // empty string
  ASSERT_TRUE(builder.AppendNull().ok());
  ASSERT_TRUE(builder.Append("test string with spaces").ok());
  ASSERT_TRUE(builder.Append("special_chars: !@#$%^&*()").ok());

  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(builder.Finish(&array).ok());

  auto result = Stringify(array);

  ASSERT_EQ(6, result.size());
  EXPECT_EQ("hello", result[0]);
  EXPECT_EQ("world", result[1]);
  EXPECT_EQ("", result[2]);
  EXPECT_EQ("null", result[3]);
  EXPECT_EQ("test string with spaces", result[4]);
  EXPECT_EQ("special_chars: !@#$%^&*()", result[5]);
}

TEST_F(StringifierTest, StringifyLargeStringArray) {
  // Create large string array
  arrow::LargeStringBuilder builder;
  ASSERT_TRUE(builder.Append("large string 1").ok());
  ASSERT_TRUE(builder.Append("large string 2").ok());
  ASSERT_TRUE(builder.AppendNull().ok());

  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(builder.Finish(&array).ok());

  auto result = Stringify(array);

  ASSERT_EQ(3, result.size());
  EXPECT_EQ("large string 1", result[0]);
  EXPECT_EQ("large string 2", result[1]);
  EXPECT_EQ("null", result[2]);
}

TEST_F(StringifierTest, StringifyEmptyArray) {
  // Test empty array
  arrow::Int32Builder builder;
  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(builder.Finish(&array).ok());

  auto result = Stringify(array);

  EXPECT_TRUE(result.empty());
}

TEST_F(StringifierTest, StringifyAllNullArray) {
  // Test array with all null values
  arrow::Int32Builder builder;
  ASSERT_TRUE(builder.AppendNull().ok());
  ASSERT_TRUE(builder.AppendNull().ok());
  ASSERT_TRUE(builder.AppendNull().ok());

  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(builder.Finish(&array).ok());

  auto result = Stringify(array);

  ASSERT_EQ(3, result.size());
  EXPECT_EQ("null", result[0]);
  EXPECT_EQ("null", result[1]);
  EXPECT_EQ("null", result[2]);
}

}  // namespace scql::engine::util
