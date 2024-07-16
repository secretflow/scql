// Copyright 2024 Ant Group Co., Ltd.
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

#include <sstream>

#include "butil/files/temp_file.h"
#include "gtest/gtest.h"
#include "yacl/base/exception.h"

namespace scql::engine::auth {

class AuthenticatorTest : public ::testing::Test {
 protected:
  butil::TempFile pem_file;
  butil::TempFile profile_file;
  std::string self_party_code;
  std::string self_public_key;

  void SetUp() override {
    // create private key pem
    pem_file.save(R"pem(-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEINTZWUEn/Jt6TV9OxGxjD+6CtqKB3MtcJdFAzFUg3fk/
-----END PRIVATE KEY-----
)pem");

    // create authorized profile json
    profile_file.save(
        R"({
          "driver": {
              "public_key": "driver_public_key"
          },
          "parties": [
              {
                  "party_code": "alice",
                  "public_key": "MCowBQYDK2VwAyEAPBDjfKgiUSIjVLrvsR+pxw5i9unTpr8S5BL04T13r6w="
              },
              {
                  "party_code": "bob",
                  "public_key": "party_public_key_bob"
              },
              {
                  "party_code": "carol",
                  "public_key": "party_public_key_carol"
              }
          ]
        })");

    self_party_code = "alice";
    self_public_key =
        "MCowBQYDK2VwAyEAPBDjfKgiUSIjVLrvsR+pxw5i9unTpr8S5BL04T13r6w=";
  }

  AuthOption CreateAuthOption() const {
    return AuthOption{true, true, pem_file.fname(), profile_file.fname()};
  }
};

TEST_F(AuthenticatorTest, ConstructorInitializesCorrectly) {
  AuthOption auth_option = CreateAuthOption();
  EXPECT_NO_THROW(Authenticator authenticator(auth_option));
}

TEST_F(AuthenticatorTest, VerifyWorksCorrectly) {
  AuthOption auth_option = CreateAuthOption();
  Authenticator authenticator(auth_option);

  std::vector<PartyIdentity> parties = {{self_party_code, self_public_key},
                                        {"bob", "party_public_key_bob"},
                                        {"carol", "party_public_key_carol"}};
  EXPECT_NO_THROW(authenticator.Verify(self_party_code, parties));

  std::vector<PartyIdentity> invalid_parties = {
      {self_party_code, self_public_key},
      {"bob", "invalid_party_public_key_bob"},
      {"carol", "invalid_party_public_key_carol"}};
  EXPECT_THROW(authenticator.Verify(self_party_code, invalid_parties),
               ::yacl::EnforceNotMet);
}

}  // namespace scql::engine::auth
