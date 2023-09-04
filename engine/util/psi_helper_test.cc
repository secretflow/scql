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
  const int64_t seq_len = 21;
  const int64_t batch_size = 10;
  auto tensor = MakeInt64SequenceTensor(seq_len);

  BatchProvider provider(std::vector<TensorPtr>{tensor});

  auto batch1 = provider.ReadNextBatch(batch_size);
  EXPECT_EQ(batch1.size(), 10);
  EXPECT_THAT(batch1, ::testing::ElementsAre("0", "1", "2", "3", "4", "5", "6",
                                             "7", "8", "9"));

  auto batch2 = provider.ReadNextBatch(batch_size);
  EXPECT_EQ(batch2.size(), 10);
  EXPECT_THAT(batch2, ::testing::ElementsAre("10", "11", "12", "13", "14", "15",
                                             "16", "17", "18", "19"));

  auto batch3 = provider.ReadNextBatch(batch_size);
  EXPECT_EQ(batch3.size(), 1);
  EXPECT_THAT(batch3, ::testing::ElementsAre("20"));

  auto empty_batch = provider.ReadNextBatch(batch_size);
  EXPECT_EQ(empty_batch.size(), 0);
}

TEST_F(BatchProviderTest, twoKeys) {
  const int64_t seq_len = 28;
  const int64_t batch_size = 10;
  auto tensor1 = MakeInt64SequenceTensor(seq_len);
  auto tensor2 = MakeAlphabetStringTensor(seq_len);

  BatchProvider provider(std::vector<TensorPtr>{tensor1, tensor2});

  auto batch1 = provider.ReadNextBatch(batch_size);
  EXPECT_EQ(batch1.size(), 10);
  EXPECT_THAT(batch1,
              ::testing::ElementsAre("0,a", "1,b", "2,c", "3,d", "4,e", "5,f",
                                     "6,g", "7,h", "8,i", "9,j"));

  auto batch2 = provider.ReadNextBatch(batch_size);
  EXPECT_EQ(batch2.size(), 10);
  EXPECT_THAT(batch2,
              ::testing::ElementsAre("10,k", "11,l", "12,m", "13,n", "14,o",
                                     "15,p", "16,q", "17,r", "18,s", "19,t"));

  auto batch3 = provider.ReadNextBatch(batch_size);
  EXPECT_EQ(batch3.size(), 8);
  EXPECT_THAT(batch3, ::testing::ElementsAre("20,u", "21,v", "22,w", "23,x",
                                             "24,y", "25,z", "26,a", "27,b"));

  auto empty_batch = provider.ReadNextBatch(batch_size);
  EXPECT_EQ(empty_batch.size(), 0);
}

/// ======================================
/// Test for JoinCipherStore
/// ======================================

struct JoinTestCase {
  std::vector<std::string> left;
  std::vector<std::string> right;
  // join indices in following format:
  // ["left_idx1,right_idx1", "left_idx2,right_idx2",...]
  std::vector<std::string> join_indices;
};

class JoinCipherStoreTest : public ::testing::TestWithParam<JoinTestCase> {
 protected:
  std::unique_ptr<JoinCipherStore> MakeLeftStore(const JoinTestCase& tc) {
    auto store = std::make_unique<JoinCipherStore>("/tmp", 2);

    for (const auto& str : tc.left) {
      store->SaveSelf(str);
    }
    for (const auto& str : tc.right) {
      store->SavePeer(str);
    }

    return store;
  }

  std::unique_ptr<JoinCipherStore> MakeRightStore(const JoinTestCase& tc) {
    auto store = std::make_unique<JoinCipherStore>("/tmp", 2);

    for (const auto& str : tc.left) {
      store->SavePeer(str);
    }
    for (const auto& str : tc.right) {
      store->SaveSelf(str);
    }

    return store;
  }
};

INSTANTIATE_TEST_SUITE_P(
    JoinCipherStoreBatchTest, JoinCipherStoreTest,
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

TEST_P(JoinCipherStoreTest, works) {
  // Given
  auto tc = GetParam();

  auto left_store = MakeLeftStore(tc);
  auto right_store = MakeRightStore(tc);

  // When
  auto left_indices = left_store->FinalizeAndComputeJoinIndices(0, true);
  auto right_indices = right_store->FinalizeAndComputeJoinIndices(0, false);

  // Then
  EXPECT_EQ(left_indices->Length(), right_indices->Length());
  EXPECT_EQ(left_indices->Length(), tc.join_indices.size());

  BatchProvider provider(std::vector<TensorPtr>{left_indices, right_indices});

  auto indices_got = provider.ReadNextBatch(left_indices->Length());
  EXPECT_THAT(indices_got,
              ::testing::UnorderedElementsAreArray(tc.join_indices));
}

}  // namespace scql::engine::util