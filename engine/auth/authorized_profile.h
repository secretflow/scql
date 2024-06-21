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

#include <string>
#include <unordered_map>

namespace scql::engine::auth {
class AuthorizedProfile {
 public:
  explicit AuthorizedProfile(const std::string& json_str);

  void VerifyParty(const std::string& party_code,
                   const std::string& pub_key) const;

  void VerifyDriver(const std::string& pub_key) const;

 private:
  std::string driver_pub_key_;
  std::unordered_map<std::string, std::string> trusted_parties_;
};

}  // namespace scql::engine::auth