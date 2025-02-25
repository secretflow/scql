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

#include "engine/util/psi/batch_provider.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "engine/core/primitive_builder.h"
#include "engine/core/string_tensor_builder.h"

namespace scql::engine::util {

class BatchProviderTest : public ::testing::Test {
 protected:
  /// @returns int64 tensor [0, 1, ..., len-1]
  static std::shared_ptr<Tensor> MakeInt64SequenceTensor(int64_t len) {
    Int64TensorBuilder builder;
    for (int64_t i = 0; i < len; ++i) {
      builder.Append(i);
    }
    std::shared_ptr<Tensor> result;
    builder.Finish(&result);
    return result;
  }

  /// @returns string tensor ["a", "b", ..., "y", "z", "a", "b", ...]
  static std::shared_ptr<Tensor> MakeAlphabetStringTensor(int64_t len) {
    StringTensorBuilder builder;
    for (int64_t i = 0; i < len; ++i) {
      std::string str(1, 'a' + (i % 26));
      builder.Append(str);
    }
    std::shared_ptr<Tensor> result;
    builder.Finish(&result);
    return result;
  }
};

TEST_F(BatchProviderTest, singleKey) {
  const int64_t seq_len = 10;
  auto tensor = MakeInt64SequenceTensor(seq_len);

  BatchProvider provider(std::vector<TensorPtr>{tensor});

  auto batch1 = provider.ReadNextBatch();
  EXPECT_EQ(batch1.size(), 10);
  EXPECT_THAT(batch1, ::testing::ElementsAre("0", "1", "2", "3", "4", "5", "6",
                                             "7", "8", "9"));

  auto empty_batch = provider.ReadNextBatch();
  EXPECT_EQ(empty_batch.size(), 0);
}

TEST_F(BatchProviderTest, twoKeys) {
  const int64_t seq_len = 10;
  auto tensor1 = MakeInt64SequenceTensor(seq_len);
  auto tensor2 = MakeAlphabetStringTensor(seq_len);

  BatchProvider provider(std::vector<TensorPtr>{tensor1, tensor2});

  auto batch1 = provider.ReadNextBatch();
  EXPECT_EQ(batch1.size(), seq_len);
  EXPECT_THAT(batch1,
              ::testing::ElementsAre("0,a", "1,b", "2,c", "3,d", "4,e", "5,f",
                                     "6,g", "7,h", "8,i", "9,j"));

  auto empty_batch = provider.ReadNextBatch();
  EXPECT_EQ(empty_batch.size(), 0);
}

}  // namespace scql::engine::util