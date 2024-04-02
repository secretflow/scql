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

#include "engine/util/psi_helper.h"

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

/// ======================================
/// Test for JoinIndices
/// ======================================

struct JoinTestCase {
  std::vector<std::string> left;
  std::vector<std::string> right;
  // join indices in following format:
  // ["left_idx1,right_idx1", "left_idx2,right_idx2",...]
  std::vector<std::string> join_indices;
};

class JoinIndicesTest : public ::testing::TestWithParam<JoinTestCase> {
 protected:
  std::shared_ptr<psi::HashBucketEcPointStore> MakeLeftStore(
      const JoinTestCase& tc) {
    auto store =
        std::make_shared<psi::HashBucketEcPointStore>("/tmp", kNumBins);

    for (const auto& str : tc.left) {
      store->Save(str);
    }

    return store;
  }

  std::shared_ptr<psi::HashBucketEcPointStore> MakeRightStore(
      const JoinTestCase& tc) {
    auto store =
        std::make_shared<psi::HashBucketEcPointStore>("/tmp", kNumBins);
    for (const auto& str : tc.right) {
      store->Save(str);
    }

    return store;
  }
};

INSTANTIATE_TEST_SUITE_P(
    JoinIndicesBatchTest, JoinIndicesTest,
    testing::Values(JoinTestCase{.left = {"A", "B", "C", "D", "E", "F"},
                                 .right = {"B", "C", "A", "E", "G", "H"},
                                 .join_indices = {"0,2", "1,0", "2,1", "4,3"}},
                    JoinTestCase{.left = {"A", "B", "C", "D", "E", "F"},
                                 .right = {"H", "I", "G", "K"},
                                 .join_indices = {}},
                    JoinTestCase{.left = {"A", "B", "C", "D", "E", "A"},
                                 .right = {"C", "A", "A", "K", "F", "B"},
                                 .join_indices = {"0,1", "0,2", "1,5", "2,0",
                                                  "5,1", "5,2"}}));

TEST_P(JoinIndicesTest, works) {
  // Given
  auto tc = GetParam();

  auto left_store = MakeLeftStore(tc);
  auto right_store = MakeRightStore(tc);

  // When
  auto left_indices =
      FinalizeAndComputeJoinIndices(true, left_store, right_store, 0);
  auto right_indices =
      FinalizeAndComputeJoinIndices(false, right_store, left_store, 0);

  // Then
  EXPECT_EQ(left_indices->Length(), right_indices->Length());
  EXPECT_EQ(left_indices->Length(), tc.join_indices.size());

  BatchProvider provider(std::vector<TensorPtr>{left_indices, right_indices});

  auto indices_got = provider.ReadNextBatch();
  EXPECT_THAT(indices_got,
              ::testing::UnorderedElementsAreArray(tc.join_indices));
}

}  // namespace scql::engine::util