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

#include "engine/datasource/dp_adaptor.h"

#include <exception>

#include "arrow/array/builder_primitive.h"
#include "arrow/scalar.h"
#include "gtest/gtest.h"

namespace scql::engine {

class DpAdaptorTest : public ::testing::Test {};

TEST_F(DpAdaptorTest, TestDataTypeConvert) {
  {
    // Given
    arrow::Date64Builder builder;
    EXPECT_TRUE(builder.Append(1000000).ok());
    auto array = Dataproxy::ConvertDatetimeToInt64(
        arrow::ChunkedArray::Make({builder.Finish().ValueOrDie()})
            .ValueOrDie());
    // Then
    EXPECT_EQ(array->type()->id(), arrow::Type::INT64);
    EXPECT_EQ("1000", array->GetScalar(0).ValueOrDie()->ToString());
  }

  {
    // Given
    arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::MILLI),
                                    arrow::default_memory_pool());
    EXPECT_TRUE(builder.Append(1000000).ok());
    auto array = Dataproxy::ConvertDatetimeToInt64(
        arrow::ChunkedArray::Make({builder.Finish().ValueOrDie()})
            .ValueOrDie());
    // Then
    EXPECT_EQ(array->type()->id(), arrow::Type::INT64);
    EXPECT_EQ("1000", array->GetScalar(0).ValueOrDie()->ToString());
  }
}
}  // namespace scql::engine