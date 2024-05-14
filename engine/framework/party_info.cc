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

#include "yacl/base/exception.h"

namespace scql::engine {

PartyInfo::PartyInfo(const pb::JobStartParams& params) {
  my_party_code_ = params.party_code();

  parties_.resize(params.parties_size());

  std::vector<bool> found(params.parties_size(), false);

  for (int i = 0; i < params.parties_size(); ++i) {
    const auto& party = params.parties(i);
    if (party.rank() < 0 || party.rank() >= params.parties_size()) {
      YACL_THROW("party({}) rank is out of range [0, {})",
                 party.ShortDebugString(), params.parties_size());
    }
    if (found[party.rank()]) {
      YACL_THROW("party({}) rank already exists", party.ShortDebugString());
    }
    size_t rank = party.rank();

    found[rank] = true;

    parties_[rank].id = party.code();
    parties_[rank].host = party.host();

    code_rank_map_[party.code()] = rank;
  }

  auto iter = code_rank_map_.find(my_party_code_);
  if (iter == code_rank_map_.end()) {
    YACL_THROW(
        "invalid JobStartParams, self party code not found in the parties "
        "list: {}",
        params.ShortDebugString());
  }
}

size_t PartyInfo::SelfRank() const {
  ssize_t rank = GetRank(SelfPartyCode());
  YACL_ENFORCE(rank != -1, "invalid PartyInfo, self party info not found");
  return rank;
}

ssize_t PartyInfo::GetRank(const std::string& party_code) const {
  auto iter = code_rank_map_.find(party_code);
  if (iter != code_rank_map_.end()) {
    return iter->second;
  }
  return -1;
}

}  // namespace scql::engine