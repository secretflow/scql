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

#include "engine/framework/party_info.h"

#include "gtest/gtest.h"

#include "engine/operator/test_util.h"

namespace scql::engine {

TEST(PartyInfoTest, normal) {
  // Given
  pb::JobStartParams params;
  {
    params.set_party_code(op::test::kPartyAlice);

    auto* alice = params.add_parties();
    alice->CopyFrom(op::test::BuildParty(op::test::kPartyAlice, 0));

    auto* bob = params.add_parties();
    bob->CopyFrom(op::test::BuildParty(op::test::kPartyBob, 1));
  }

  // When
  PartyInfo parties(params);

  // Then
  EXPECT_EQ(parties.SelfRank(), 0);
  EXPECT_EQ(parties.WorldSize(), 2);
  EXPECT_EQ(parties.SelfPartyCode(), op::test::kPartyAlice);
  EXPECT_EQ(parties.GetRank(op::test::kPartyBob), 1);
  EXPECT_EQ(parties.GetRank(op::test::kPartyCarol), -1);
}

}  // namespace scql::engine