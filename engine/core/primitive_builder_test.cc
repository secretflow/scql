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

#include "engine/core/primitive_builder.h"

#include "gtest/gtest.h"

namespace scql::engine {

TEST(BooleanTensorBuilderTest, appendWorks) {
  // Given
  BooleanTensorBuilder builder;

  builder.Append(true);
  builder.Append(false);
  builder.Append(false);
  builder.Append(true);

  // When
  std::shared_ptr<Tensor> tensor;
  builder.Finish(&tensor);

  // Then
  EXPECT_EQ(tensor->Length(), 4);
  EXPECT_EQ(tensor->GetNullCount(), 0);
}

TEST(BooleanTensorBuilderTest, AppendNullWorks) {
  // Given
  BooleanTensorBuilder builder;

  builder.Append(true);
  builder.AppendNull();
  builder.Append(false);
  builder.AppendNull();
  builder.AppendNull();

  // When
  std::shared_ptr<Tensor> tensor;
  builder.Finish(&tensor);

  // Then
  EXPECT_EQ(tensor->Length(), 5);
  EXPECT_EQ(tensor->GetNullCount(), 3);
}

TEST(Int64TensorBuilderTest, works) {
  // Given
  Int64TensorBuilder builder;

  builder.Append(1);
  builder.Append(2);
  builder.Append(3);
  builder.Append(4);
  builder.Append(5);
  builder.AppendNull();
  builder.Append(7);
  builder.Append(8);

  // When
  std::shared_ptr<Tensor> tensor;
  builder.Finish(&tensor);

  // Then
  EXPECT_EQ(tensor->Length(), 8);
  EXPECT_EQ(tensor->GetNullCount(), 1);
}

TEST(FloatTensorBuilderTest, works) {
  // Given
  FloatTensorBuilder builder;

  builder.Append(0.1);
  builder.Append(0.2);
  builder.Append(0.3);
  builder.Append(3.1415826);
  builder.AppendNull();

  // When
  std::shared_ptr<Tensor> tensor;
  builder.Finish(&tensor);

  // Then
  EXPECT_EQ(tensor->Length(), 5);
  EXPECT_EQ(tensor->GetNullCount(), 1);
}

};  // namespace scql::engine