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

#include "engine/util/psi/cipher_intersection.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "psi/utils/ec_point_store.h"

#include "engine/util/psi/batch_provider.h"
#include "engine/util/psi/common.h"

namespace scql::engine::util {
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
  static std::shared_ptr<psi::HashBucketEcPointStore> MakeLeftStore(
      const JoinTestCase& tc) {
    auto store =
        std::make_shared<psi::HashBucketEcPointStore>("/tmp", kNumBins);

    for (const auto& str : tc.left) {
      store->Save(str, 0);
    }

    return store;
  }

  static std::shared_ptr<psi::HashBucketEcPointStore> MakeRightStore(
      const JoinTestCase& tc) {
    auto store =
        std::make_shared<psi::HashBucketEcPointStore>("/tmp", kNumBins);
    for (const auto& str : tc.right) {
      store->Save(str, 0);
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