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

#include <memory>
#include <vector>

#include "engine/auth/authorized_profile.h"

namespace scql::engine::auth {

struct AuthOption {
  bool enable_self_auth;
  bool enable_peer_auth;
  std::string private_key_pem_path;
  std::string authorized_profile_path;
};

struct PartyIdentity {
  std::string party_code;
  std::string pub_key;
};

class Authenticator {
 public:
  explicit Authenticator(AuthOption option);

  void Verify(const std::string& self_party_code,
              const std::vector<PartyIdentity>& parties) const;

 private:
  const AuthOption option_;
  std::string self_public_key_;
  std::unique_ptr<AuthorizedProfile> auth_profile_;
};

}  // namespace scql::engine::auth