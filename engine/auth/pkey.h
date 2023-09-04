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
#include <string_view>

#include "openssl/evp.h"

namespace scql::engine::auth {

namespace internal {

using UniquePkey = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;

}

class PrivateKey {
 private:
  struct private_ctor_tag {
    private_ctor_tag() = default;
  };

 public:
  // take ownership of pkey
  PrivateKey(EVP_PKEY* pkey, private_ctor_tag);

  // Get public key in DER format
  // NOTE: DER is truely binary format
  std::string GetPublicKeyInDER() const;

 public:
  static std::unique_ptr<PrivateKey> LoadFromPemFile(
      const std::string& file_path);

  static std::unique_ptr<PrivateKey> ParseFromPem(std::string_view pem_content);

 private:
  internal::UniquePkey pkey_;
};

}  // namespace scql::engine::auth