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

#pragma once

#include <map>
#include <string>
#include <vector>

#include "api/engine.pb.h"

namespace scql::engine {

/// @brief Computational parties information
class PartyInfo {
 public:
  struct Party {
    std::string id;
    std::string host;
  };

 public:
  explicit PartyInfo(const pb::JobStartParams& params);
  PartyInfo() = delete;

  size_t SelfRank() const;

  size_t WorldSize() const { return parties_.size(); }

  const std::string& SelfPartyCode() const { return my_party_code_; }

  /// @brief query the rank of given party code, returns -1 if not found.
  ssize_t GetRank(const std::string& party_code) const;

  const std::vector<PartyInfo::Party>& AllParties() const { return parties_; }

 private:
  std::string my_party_code_;

  std::vector<Party> parties_;
  // party code -> rank map
  std::map<std::string, size_t> code_rank_map_;
};

}  // namespace scql::engine