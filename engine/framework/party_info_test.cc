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

namespace scql::engine {

TEST(PartyInfoTest, normal) {
  // Given
  pb::JobStartParams params;
  {
    params.set_party_code("alice");
    auto alice = params.add_parties();
    alice->set_code("alice");
    alice->set_name("party alice");
    alice->set_host("alice.com");
    alice->set_rank(0);

    auto bob = params.add_parties();
    bob->set_code("bob");
    bob->set_name("party bob");
    bob->set_host("bob.com");
    bob->set_rank(1);
  }

  // When
  PartyInfo parties(params);

  // Then
  EXPECT_EQ(parties.SelfRank(), 0);
  EXPECT_EQ(parties.WorldSize(), 2);
  EXPECT_EQ(parties.SelfPartyCode(), "alice");
  EXPECT_EQ(parties.GetRank("bob"), 1);
  EXPECT_EQ(parties.GetRank("carol"), -1);
}

}  // namespace scql::engine