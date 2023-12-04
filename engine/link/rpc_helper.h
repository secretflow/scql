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
#include <string>

#include "brpc/authenticator.h"
#include "brpc/channel.h"

namespace scql::engine {

// NOTE: Using Singleton for SimpleAuthenticator to
// remain accessible at all times
class SimpleAuthenticator : public brpc::Authenticator {
 public:
  SimpleAuthenticator(std::string credential)
      : credential_(std::move(credential)) {}

  virtual ~SimpleAuthenticator() = default;

  int GenerateCredential(std::string* auth_str) const override {
    *auth_str = credential_;
    return 0;
  }

  int VerifyCredential(const std::string& auth_str,
                       const butil::EndPoint& client_addr,
                       brpc::AuthContext* out_ctx) const override {
    if (auth_str == credential_) {
      return 0;
    }
    return 1;
  }

 private:
  const std::string credential_;
};

void SetDefaultAuthenticator(std::unique_ptr<brpc::Authenticator> auth);

const brpc::Authenticator* DefaultAuthenticator();

}  // namespace scql::engine
