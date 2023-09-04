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

#include "engine/auth/authenticator.h"

#include "absl/strings/escaping.h"
#include "butil/file_util.h"
#include "yacl/base/exception.h"

#include "engine/auth/authorized_profile.h"
#include "engine/auth/pkey.h"

namespace scql::engine::auth {

Authenticator::Authenticator(const AuthOption& option) : option_(option) {
  if (option_.enable_self_auth) {
    // load private key
    auto priv_key = PrivateKey::LoadFromPemFile(option_.private_key_pem_path);
    self_public_key_ = absl::Base64Escape(priv_key->GetPublicKeyInDER());
  }

  if (option_.enable_peer_auth) {
    // load authorized profile
    std::string json_str;
    YACL_ENFORCE(
        butil::ReadFileToString(
            butil::FilePath(option_.authorized_profile_path), &json_str),
        "failed to read authorized profile file: {}",
        option_.authorized_profile_path);
    auth_profile_ = std::make_unique<AuthorizedProfile>(json_str);
  }
}

void Authenticator::Verify(std::string self_party_code,
                           const std::vector<PartyIdentity>& parties) {
  if (!option_.enable_self_auth && !option_.enable_peer_auth) {
    return;
  }

  bool self_auth_done = false;
  for (const auto& pi : parties) {
    if (pi.party_code == self_party_code) {
      if (option_.enable_self_auth) {
        YACL_ENFORCE(self_public_key_ == pi.pub_key,
                     "self public key mismatched");
        self_auth_done = true;
      }
    } else if (option_.enable_peer_auth) {
      auth_profile_->VerifyParty(pi.party_code, pi.pub_key);
    }
  }

  if (option_.enable_self_auth) {
    YACL_ENFORCE(self_auth_done,
                 "self public key not found in parameter parties");
  }
}

}  // namespace scql::engine::auth