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

#include "engine/auth/authorized_profile.h"

#include "google/protobuf/util/json_util.h"
#include "yacl/base/exception.h"

#include "engine/auth/authorized_profile.pb.h"

namespace scql {
namespace engine {
namespace auth {

AuthorizedProfile::AuthorizedProfile(const std::string& json_str) {
  pb::AuthorizedProfile profile_pb;
  auto status =
      google::protobuf::util::JsonStringToMessage(json_str, &profile_pb);
  YACL_ENFORCE(
      status.ok(),
      "failed to parse json to message AuthorizedProfile: json={}, error={}",
      json_str, status.ToString());

  driver_pub_key_ = profile_pb.driver().public_key();

  for (const auto& pi : profile_pb.parties()) {
    YACL_ENFORCE(trusted_parties_.count(pi.party_code()) == 0,
                 "duplicate party code '{}' in AuthorizedProfile",
                 pi.party_code());
    trusted_parties_[pi.party_code()] = pi.public_key();
  }
}

void AuthorizedProfile::VerifyParty(const std::string& party_code,
                                    const std::string& pub_key) const {
  auto iter = trusted_parties_.find(party_code);
  YACL_ENFORCE(iter != trusted_parties_.end(),
               "party code `{}` not found in trusted parties", party_code);
  YACL_ENFORCE(iter->second == pub_key,
               "public key mismatched for party code `{}`", party_code);
}

void AuthorizedProfile::VerifyDriver(const std::string& pub_key) const {
  YACL_ENFORCE(driver_pub_key_ == pub_key, "driver public key mismatched");
}

}  // namespace auth
}  // namespace engine
}  // namespace scql