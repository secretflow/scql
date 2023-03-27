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

#include "engine/core/string_tensor_builder.h"

#include "gtest/gtest.h"

namespace scql::engine {

TEST(StringTensorBuilderTest, works) {
  // Given
  StringTensorBuilder builder;

  builder.Append("hello");
  builder.Append("scql");
  builder.AppendNull();
  builder.Append("only only will been appened", 4);

  // When
  std::shared_ptr<Tensor> tensor;
  builder.Finish(&tensor);

  // Then
  EXPECT_EQ(tensor->Length(), 4);
  EXPECT_EQ(tensor->GetNullCount(), 1);
}

}  // namespace scql::engine